package raft

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dshulyak/raft/chant"
	"github.com/dshulyak/raftlog"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type nodeCluster struct {
	t      TestingHelper
	logger *zap.SugaredLogger

	net   *chant.Network
	nodes map[NodeID]*Node
	apps  map[NodeID]*keyValueApp

	lastLeader uint64

	encoder keyValueOpEncoder

	ctx    context.Context
	cancel func()
}

func newNodeCluster(t TestingHelper, n int) *nodeCluster {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
	})
	logger := testLogger(t)
	nc := &nodeCluster{
		t:       t,
		logger:  logger.Sugar(),
		net:     chant.New(),
		nodes:   map[NodeID]*Node{},
		apps:    map[NodeID]*keyValueApp{},
		ctx:     ctx,
		encoder: newKeyValueApp(),
	}

	template := &Config{
		EntriesPerAppend:       1,
		ProposalsBuffer:        2,
		PendingProposalsBuffer: 10,
		Backoff:                100 * time.Millisecond,
		TickInterval:           20 * time.Millisecond,
		HeartbeatTimeout:       3,
		ElectionTimeoutMin:     10,
		ElectionTimeoutMax:     20,
		Configuration:          &Configuration{},
	}

	// pass final configuration before starting all sorts of asynchronous tasks
	for i := 1; i <= n; i++ {
		template.Configuration.Nodes = append(template.Configuration.Nodes,
			ConfNode{ID: NodeID(i)})
	}

	var err error
	for i := 1; i <= n; i++ {
		c := *template
		c.ID = NodeID(i)
		c.Transport = nc.net.Transport(c.ID)
		app := newKeyValueApp()
		nc.apps[c.ID] = app
		c.App = app
		c.Logger = logger.Named(fmt.Sprintf("node=%d", i))
		c.Storage, err = raftlog.New(c.Logger, nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() {
			c.Storage.Delete()
		})
		f, err := ioutil.TempFile("", "raft-node-test")
		require.NoError(t, err)
		t.Cleanup(func() {
			os.Remove(f.Name())
		})
		c.State, err = NewDurableState(f)
		t.Cleanup(func() {
			c.State.Close()
		})
		n := NewNode(&c)
		nc.nodes[c.ID] = n
		t.Cleanup(func() {
			n.Close()
			require.NoError(nc.t, n.Wait())
		})
	}
	return nc
}

func (c *nodeCluster) knownLeader() NodeID {
	return NodeID(atomic.LoadUint64(&c.lastLeader))
}

func (c *nodeCluster) nextLeader(leader NodeID) {
	atomic.CompareAndSwapUint64(&c.lastLeader, uint64(leader), (uint64(leader)%uint64(len(c.nodes)))+1)
}

func (c *nodeCluster) blockLeader() {
	leader := c.knownLeader()
	require.NotEqual(c.t, None, leader, "leader must be known")
	for id := range c.nodes {
		if id != leader {
			c.net.Block(leader, id)
		}
	}
}

func (c *nodeCluster) propose(ctx context.Context, op []byte) error {
	c.nextLeader(None)
	var (
		proposal ReadRequest
		err      error
	)
	for {
		leader := c.knownLeader()
		n := c.nodes[leader]
		c.logger.Debugw("proposed to a leader", "leader", leader)

		if op != nil {
			proposal, err = n.Propose(ctx, op)
		} else {
			proposal, err = n.Read(ctx)
		}

		if err != nil {
			c.nextLeader(leader)
			return err
		}
		err = proposal.Wait(ctx)
		if err == nil {
			return nil
		}
		c.logger.Debugw("proposal result", "proposal", proposal, "error", err)
		redirect := &ErrRedirect{}
		if errors.As(err, &redirect) {
			atomic.StoreUint64(&c.lastLeader, uint64(redirect.Leader.ID))
		} else if errors.Is(err, ErrLeaderStepdown) {
			// just switch to any other node
			c.logger.Debugw("observed leader stepdown", "leader", leader)
			c.nextLeader(leader)
		} else if errors.Is(err, ErrProposalsOverflow) {
			// simple backoff
			time.Sleep(100 * time.Millisecond)
		} else {
			c.nextLeader(leader)
			return err
		}
	}
}

// retries will retry f() at most n times.
func retries(n int, f func() error) (err error) {
	for n >= 0 {
		err = f()
		if err == nil {
			return
		}
		n--
	}
	return
}

func TestNodeProposalsSequential(t *testing.T) {
	c := newNodeCluster(t, 3)
	for i := 1; i <= 10; i++ {
		op, err := c.encoder.Insert(uint64(i), nil)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, c.propose(ctx, op))
	}
}

func TestNodeProposalsConcurrent(t *testing.T) {
	c := newNodeCluster(t, 3)
	var wg sync.WaitGroup

	for i := 1; i <= 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			op, err := c.encoder.Insert(uint64(i), nil)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			require.NoError(t, c.propose(ctx, op))
		}(i)
	}
	wg.Wait()

}

func TestNodeRecoverFromPartition(t *testing.T) {
	c := newNodeCluster(t, 3)
	c.net.Block(1, 2)
	c.net.Block(1, 3)
	c.net.Block(2, 3)
	op, err := c.encoder.Insert(uint64(1), nil)
	// time.Second depends on the configured election timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, err)
	require.Error(t, context.Canceled, c.propose(ctx, op))

	c.net.Restore()

	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, err)
	require.NoError(t, c.propose(ctx, op))
}

func TestNodeLeaderPartitioned(t *testing.T) {
	c := newNodeCluster(t, 3)
	failpoint := 4
	for i := 1; i <= 10; i++ {
		if i == failpoint {
			c.blockLeader()
		}
		op, err := c.encoder.Insert(uint64(i), nil)
		require.NoError(t, err)
		require.NoError(t, retries(3, func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			return c.propose(ctx, op)
		}))
	}
}

func TestNodeReadWithoutLog(t *testing.T) {
	c := newNodeCluster(t, 3)
	closer := make(chan struct{})
	n := 10
	key := uint64(1)
	var value uint64
	go func() {
		for i := 1; i <= n; i++ {
			op, err := c.encoder.Insert(key, i)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			require.NoError(t, c.propose(ctx, op))
			cancel()

			atomic.StoreUint64(&value, uint64(i))
		}
		close(closer)
	}()

	for {
		select {
		case <-closer:
			return
		default:
		}
		// FIXME this is not a very good test. it will pass even if read requests
		// completed prematurely.
		// this value is updated only after write request receives notification
		// that the entry is applied.
		// for a better test we need to get here the value that was commited
		// but not necessarily applied.
		applied := atomic.LoadUint64(&value)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		require.NoError(t, c.propose(ctx, nil), "read timed out")
		cancel()

		value, set := c.apps[c.knownLeader()].Get(key)
		if applied > 0 {
			require.True(t, set)
		}
		if set {
			require.GreaterOrEqual(t, value.(int), int(applied))
		}
	}
}

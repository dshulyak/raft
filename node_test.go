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

	"github.com/dshulyak/raft/raftlog"
	"github.com/dshulyak/raft/transport/channel"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type nodeCluster struct {
	t      TestingHelper
	logger *zap.SugaredLogger

	net   *channel.Network
	nodes map[NodeID]*Node
	apps  map[NodeID]*keyValueApp

	client *clusterClient

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
		net:     channel.New(),
		nodes:   map[NodeID]*Node{},
		apps:    map[NodeID]*keyValueApp{},
		ctx:     ctx,
		encoder: newKeyValueApp(),
	}
	// this is for backward compatibility and blocking known leader
	nc.client = nc.makeClient()

	template := &Config{
		EntriesPerAppend:       10,
		ProposalsBuffer:        100,
		PendingProposalsBuffer: 100,
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
		c.Storage, err = raftlog.New(raftlog.WithLogger(logger), raftlog.WithTempDir())
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
			require.NoError(nc.t, n.Wait(), "node %d exited with error", c.ID)
		})
	}
	return nc
}

func (c *nodeCluster) leaderApp() *keyValueApp {
	leader := c.client.knownLeader()
	require.NotEqual(c.t, None, leader, "leader must be known")
	return c.apps[leader]
}

func (c *nodeCluster) makeClient() *clusterClient {
	return &clusterClient{
		cluster: c,
	}
}

// FIXME client can learned who the current leader is only by submitting requests.
// it will either get redirected or request will be accepted.
// it might be confusing, and would be better to expose a leader id.
func (c *nodeCluster) blockLeader() {
	leader := c.client.knownLeader()
	require.NotEqual(c.t, None, leader, "leader must be known")
	c.logger.Debugw("blocking comms with a current leader", "id", leader)
	for id := range c.nodes {
		if id != leader {
			c.net.Block(leader, id)
		}
	}
}

func (c *nodeCluster) propose(ctx context.Context, op []byte) error {
	return c.client.propose(ctx, op)
}

// proposeWithRetries is required only if in the test we will partition a node, and never recover it
// in such case failure by timeout seems to be the best option.
func (c *nodeCluster) proposeWithRetries(ctx context.Context, n int, timeout time.Duration, op []byte) error {
	return c.client.proposeWithRetries(ctx, n, timeout, op)
}

type clusterClient struct {
	cluster *nodeCluster
	leader  uint64
}

func (c *clusterClient) knownLeader() NodeID {
	return NodeID(atomic.LoadUint64(&c.leader))
}

func (c *clusterClient) nextLeader(leader NodeID) {
	atomic.CompareAndSwapUint64(&c.leader, uint64(leader), (uint64(leader)%uint64(len(c.cluster.nodes)))+1)
}

func (c *clusterClient) propose(ctx context.Context, op []byte) error {
	c.nextLeader(None)
	var (
		proposal ReadRequest
		err      error
	)
	for {
		leader := c.knownLeader()
		n := c.cluster.nodes[leader]
		c.cluster.logger.Debugw("proposed to a leader", "leader", leader)

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
		c.cluster.logger.Debugw("proposal result", "proposal", proposal, "error", err)
		redirect := &ErrRedirect{}
		if errors.As(err, &redirect) {
			atomic.StoreUint64(&c.leader, uint64(redirect.Leader.ID))
		} else if errors.Is(err, ErrLeaderStepdown) {
			// just switch to any other node
			c.cluster.logger.Debugw("observed leader stepdown", "leader", leader)
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

// proposeWithRetries will retry propose() at most n times.
func (c *clusterClient) proposeWithRetries(ctx context.Context, n int, timeout time.Duration, op []byte) (err error) {
	for n >= 0 {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		err = c.propose(ctx, op)
		cancel()
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
	failpoint := 10
	ctx := context.TODO()
	timeout := 500 * time.Millisecond

	for i := 1; i <= 100; i++ {
		if i%failpoint == 0 {
			c.net.Restore()
			c.blockLeader()
		}
		op, err := c.encoder.Insert(uint64(i), nil)
		require.NoError(t, err)
		// with retries because if we reach a partitioned leader, which
		// will stepdown eventually, it won't be able to learn who is a new leader
		// and client will have to fail by timeout, or in certain cases with a proposals queue overflow
		require.NoError(t, c.proposeWithRetries(ctx, 3, timeout, op))
	}
}

func TestNodeReadWithoutLog(t *testing.T) {
	c := newNodeCluster(t, 3)
	closer := make(chan struct{})
	n := 100
	key := uint64(1)
	var value uint64
	go func() {
		client := c.makeClient()
		for i := 1; i <= n; i++ {
			op, err := c.encoder.Insert(key, i)
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			require.NoError(t, client.propose(ctx, op))
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
		// linearizable read must not read a value that is older then the commited
		// value. applied is change once write request is commited.
		// therefore if read request returns value lower then applied we know
		// that the read returned result that was already overwritten
		// by commited entry.
		//
		// this test does fail if read request confirmations are screwed.
		// TODO another part of the test must verify that we can't read stale value.
		applied := atomic.LoadUint64(&value)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		require.NoError(t, c.propose(ctx, nil), "read timed out")
		cancel()

		value, set := c.leaderApp().Get(key)
		if applied > 0 {
			require.True(t, set, "value should be set if applied is %v", applied)
		}
		if set {
			require.GreaterOrEqual(t, value.(int), int(applied))
		}
	}
}

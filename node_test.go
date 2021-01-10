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
)

type nodeCluster struct {
	t     TestingHelper
	net   *chant.Network
	nodes map[NodeID]*node
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

	nc := &nodeCluster{
		t:       t,
		net:     chant.New(),
		nodes:   map[NodeID]*node{},
		apps:    map[NodeID]*keyValueApp{},
		ctx:     ctx,
		encoder: newKeyValueApp(),
	}

	logger := testLogger(t)

	template := &Context{
		Context:                ctx,
		EntriesPerAppend:       1,
		ProposalsBuffer:        2,
		PendingProposalsBuffer: 10,
		Backoff:                200 * time.Millisecond,
		TickInterval:           20 * time.Millisecond,
		HeartbeatTimeout:       3,
		ElectionTimeoutMin:     10,
		ElectionTimeoutMax:     20,
		Configuration:          &Configuration{},
	}

	// pass final configuration before starting all sorts of asynchronous tasks
	for i := 1; i <= n; i++ {
		template.Configuration.Nodes = append(template.Configuration.Nodes,
			Node{ID: NodeID(i)})
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
		n := newNode(&c)
		nc.nodes[c.ID] = n
		t.Cleanup(func() {
			n.Close()
			require.NoError(nc.t, n.Wait())
		})
	}
	return nc
}

func (c *nodeCluster) propose(ctx context.Context, op []byte) error {
	if NodeID(atomic.LoadUint64(&c.lastLeader)) == None {
		// or choose randomly
		atomic.StoreUint64(&c.lastLeader, 1)
	}
	for {
		n := c.nodes[NodeID(atomic.LoadUint64(&c.lastLeader))]
		proposal, err := n.Propose(ctx, op)
		require.NoError(c.t, err)
		err = proposal.Wait(ctx)
		if err == nil {
			return nil
		}
		redirect := &ErrRedirect{}
		if errors.As(err, &redirect) {
			atomic.StoreUint64(&c.lastLeader, uint64(redirect.Leader.ID))
		} else if errors.Is(err, ErrLeaderStepdown) {
		} else if errors.Is(err, ErrProposalsOverflow) {
			time.Sleep(100 * time.Millisecond)
		} else {
			return fmt.Errorf("%w: entry %v", err, proposal.Entry)
		}

	}
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

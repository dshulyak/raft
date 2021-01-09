package raft

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
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

	lastLeader NodeID

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
		ProposalsBuffer:        10,
		PendingProposalsBuffer: 10,
		TickInterval:           100 * time.Millisecond,
		HeartbeatTimeout:       5,
		ElectionTimeoutMin:     20,
		ElectionTimeoutMax:     40,
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
		nc.nodes[c.ID] = newNode(&c)
	}
	return nc
}

func (c *nodeCluster) propose(ctx context.Context, op []byte) {
	if c.lastLeader == None {
		// or choose randomly
		c.lastLeader = NodeID(1)
	}
	for {
		n := c.nodes[c.lastLeader]
		proposal, err := n.Propose(ctx, op)
		require.NoError(c.t, err)
		err = proposal.Wait(ctx)
		if err == nil {
			return
		}
		redirect := &ErrRedirect{}
		if errors.As(err, &redirect) {
			c.lastLeader = redirect.Leader.ID
			continue
		} else if errors.Is(err, ErrLeaderStepdown) {
			continue
		} else {
			require.NoError(c.t, err)
		}

	}
}

func TestNodeProposal(t *testing.T) {
	c := newNodeCluster(t, 3)
	op, err := c.encoder.Insert(10, nil)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c.propose(ctx, op)
}

package raft

import (
	"context"
	"io/ioutil"
	"os"
	"time"

	"github.com/dshulyak/raft/chant"
	"github.com/dshulyak/raftlog"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type nodeCluster struct {
	t     TestingHelper
	net   *chant.Network
	nodes map[NodeID]*node
	apps  map[NodeID]*keyValueApp

	ctx    context.Context
	cancel func()
}

func newNodeCluster(t TestingHelper, n int) *nodeCluster {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
	})

	nc := &nodeCluster{
		t:     t,
		net:   chant.New(),
		nodes: map[NodeID]*node{},
		apps:  map[NodeID]*keyValueApp{},
		ctx:   ctx,
	}

	logger := testLogger(t)

	template := &Context{
		EntriesPerAppend:       1,
		ProposalsBuffer:        10,
		PendingProposalsBuffer: 10,
		TickInterval:           10 * time.Millisecond,
		HeartbeatTimeout:       5,
		ElectionTimeoutMin:     20,
		ElectionTimeoutMax:     40,
	}

	var err error
	for i := 1; i <= n; i++ {
		c := *template
		c.ID = NodeID(i)
		c.Transport = nc.net.Transport(c.ID)
		app := newKeyValueApp()
		nc.apps[c.ID] = app
		c.App = app
		c.Logger = logger.With(zap.Uint64("node", uint64(c.ID)))
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

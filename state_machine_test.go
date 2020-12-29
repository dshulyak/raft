package raft

import (
	"flag"
	"testing"

	"github.com/dshulyak/raftlog"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type TestingHelper interface {
	Helper()
	Cleanup(func())
	require.TestingT
}

var logLevel = flag.String("log-level", "debug", "test environment log level")

func testLogger(t TestingHelper) *zap.Logger {
	t.Helper()
	var level zapcore.Level
	require.NoError(t, level.Set(*logLevel))
	log, _ := zap.NewDevelopment(zap.IncreaseLevel(level))
	return log
}

func getTestCluster(t TestingHelper, n, minTicks, maxTicks int) *testCluster {
	t.Helper()
	logger := testLogger(t)

	configuration := &Configuration{}
	// 0 is reserved as None
	for i := 1; i <= n; i++ {
		configuration.Nodes = append(configuration.Nodes, Node{ID: NodeID(i)})
	}

	cluster := &testCluster{states: map[NodeID]*StateMachine{}}
	for _, node := range configuration.Nodes {
		log, err := raftlog.New(logger, nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, log.Delete())
		})
		cluster.states[node.ID] = NewStateMachine(logger, StateMachineConfig{
			ID:            node.ID,
			MinTicks:      minTicks,
			MaxTicks:      maxTicks,
			Configuration: configuration,
		}, log)
	}
	return cluster
}

type testCluster struct {
	states map[NodeID]*StateMachine
}

func (c *testCluster) runToCompletion(t TestingHelper, u *Update) *Update {
	t.Helper()
	stack := []*Update{u}
	for len(stack) > 0 {
		u = stack[0]
		for _, msg := range u.Msgs {
			sm := c.states[msg.To]
			update := sm.Next(msg.Message)
			if update != nil {
				stack = append(stack, update)
			}
		}
		stack[0] = nil
		stack = stack[1:]
	}
	return u
}

func TestRaftReplicationAfterInitialElection(t *testing.T) {
	timeout := 20
	cluster := getTestCluster(t, 3, 10, timeout) // 3 node cluster
	update := cluster.states[1].Tick(timeout)
	require.NotNil(t, update)
	update = cluster.runToCompletion(t, update)
	require.NotNil(t, update)
	require.Len(t, update.Proposals, 1)
	entry := update.Proposals[0].Entry
	require.Equal(t, raftlog.LogNoop, entry.OpType)
}

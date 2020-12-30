package raft

import (
	"flag"
	"testing"

	"github.com/dshulyak/raftlog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type TestingHelper interface {
	Helper()
	Cleanup(func())
	require.TestingT
}

var logLevel = flag.String("log-level", "panic", "test environment log level")

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

	cluster := &testCluster{
		states:        map[NodeID]*StateMachine{},
		blockedRoutes: map[NodeID]map[NodeID]struct{}{},
	}
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
	states        map[NodeID]*StateMachine
	messages      []interface{}
	blockedRoutes map[NodeID]map[NodeID]struct{}
}

func (c *testCluster) blockRoute(from, to NodeID) {
	if c.blockedRoutes[from] == nil {
		c.blockedRoutes[from] = map[NodeID]struct{}{}
	}
	if c.blockedRoutes[to] == nil {
		c.blockedRoutes[to] = map[NodeID]struct{}{}
	}
	c.blockedRoutes[from][to] = struct{}{}
	c.blockedRoutes[to][from] = struct{}{}
}

func (c *testCluster) restoreRoutes() {
	c.blockedRoutes = map[NodeID]map[NodeID]struct{}{}
}

func (c *testCluster) isBlocked(from, to NodeID) bool {
	fromRoutes := c.blockedRoutes[from]
	if fromRoutes == nil {
		return false
	}
	_, exist := fromRoutes[to]
	return exist
}

func (c *testCluster) run(t TestingHelper, u *Update, from NodeID) *Update {
	t.Helper()
	stack := []*Update{u}
	for len(stack) > 0 {
		u = stack[0]
		for _, msg := range u.Msgs {
			if c.isBlocked(from, msg.To) {
				continue
			}
			sm := c.states[msg.To]
			c.messages = append(c.messages, msg.Message)
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

func (c *testCluster) compareMsgHistories(t TestingHelper, expected []interface{}) {
	t.Helper()
	max := len(c.messages)
	for i := range expected {
		if i > max {
			break
		}
		assert.Equal(t, expected[i], c.messages[i])
	}
	assert.Len(t, expected, max)
}

func (c *testCluster) resetHistory() {
	c.messages = nil
}

func TestRaftReplicationAfterInitialElection(t *testing.T) {
	timeout := 20
	cluster := getTestCluster(t, 3, 0, timeout) // 3 node cluster
	update := cluster.states[1].Tick(timeout)
	require.NotNil(t, update)
	update = cluster.run(t, update, 1)
	require.NotNil(t, update)
	require.Len(t, update.Proposals, 1)
	entry := update.Proposals[0].Entry
	require.Equal(t, raftlog.LogNoop, entry.OpType)
	cluster.compareMsgHistories(t, []interface{}{
		&RequestVote{
			Term:      1,
			Candidate: 1,
		},
		&RequestVote{
			Term:      1,
			Candidate: 1,
		},
		&RequestVoteResponse{
			Term:        1,
			Voter:       2,
			VoteGranted: true,
		},
		&RequestVoteResponse{
			Term:        1,
			Voter:       3,
			VoteGranted: true,
		},
		&AppendEntries{
			Term:    1,
			Leader:  1,
			Entries: []*raftlog.LogEntry{{Index: 1, Term: 1, OpType: raftlog.LogNoop}},
		},
		&AppendEntries{
			Term:    1,
			Leader:  1,
			Entries: []*raftlog.LogEntry{{Index: 1, Term: 1, OpType: raftlog.LogNoop}},
		},
		&AppendEntriesResponse{
			Term:     1,
			Follower: 2,
			Success:  true,
			LastLog:  LogHeader{Index: 1, Term: 1},
		},
		&AppendEntriesResponse{
			Term:     1,
			Follower: 3,
			Success:  true,
			LastLog:  LogHeader{Index: 1, Term: 1},
		},
	})
}

func TestRaftFailedElection(t *testing.T) {
	timeout := 20
	n := 3
	cluster := getTestCluster(t, n, 0, timeout) // 3 node cluster
	for i := 1; i < n; i++ {
		_ = cluster.states[NodeID(i)].Tick(timeout)
	}
	update := cluster.states[3].Tick(timeout)
	require.NotNil(t, update)
	_ = cluster.run(t, update, 3)
	cluster.compareMsgHistories(t, []interface{}{
		&RequestVote{
			Term:      1,
			Candidate: 3,
		},
		&RequestVote{
			Term:      1,
			Candidate: 3,
		},
		&RequestVoteResponse{
			Term:  1,
			Voter: 1,
		},
		&RequestVoteResponse{
			Term:  1,
			Voter: 2,
		},
	})
}

func TestRaftLeaderDisrupted(t *testing.T) {
	timeout := 20
	n := 3
	cluster := getTestCluster(t, n, 0, timeout)
	cluster.run(t, cluster.states[1].Tick(timeout), 1)
	cluster.resetHistory()

	cluster.run(t, cluster.states[2].Tick(timeout), 2)
	cluster.compareMsgHistories(t, []interface{}{
		&RequestVote{
			Term:      2,
			Candidate: 2,
			LastLog:   LogHeader{Term: 1, Index: 1},
		},
		&RequestVote{
			Term:      2,
			Candidate: 2,
			LastLog:   LogHeader{Term: 1, Index: 1},
		},
		&RequestVoteResponse{
			Term:        2,
			Voter:       1,
			VoteGranted: true,
		},
		&RequestVoteResponse{
			Term:        2,
			Voter:       3,
			VoteGranted: true,
		},
		&AppendEntries{
			Term:    2,
			Leader:  2,
			Entries: []*raftlog.LogEntry{{Index: 2, Term: 2, OpType: raftlog.LogNoop}},
			PrevLog: LogHeader{Term: 1, Index: 1},
		},
		&AppendEntries{
			Term:    2,
			Leader:  2,
			Entries: []*raftlog.LogEntry{{Index: 2, Term: 2, OpType: raftlog.LogNoop}},
			PrevLog: LogHeader{Term: 1, Index: 1},
		},
		&AppendEntriesResponse{
			Term:     2,
			Follower: 1,
			Success:  true,
			LastLog:  LogHeader{Index: 2, Term: 2},
		},
		&AppendEntriesResponse{
			Term:     2,
			Follower: 3,
			Success:  true,
			LastLog:  LogHeader{Index: 2, Term: 2},
		},
	})
}

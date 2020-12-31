package raft

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
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
		logger:        logger,
		minTicks:      minTicks,
		maxTicks:      maxTicks,
		configuration: configuration,
		logs:          map[NodeID]*raftlog.Storage{},
		ds:            map[NodeID]*DurableState{},
		states:        map[NodeID]*StateMachine{},
		blockedRoutes: map[NodeID]map[NodeID]struct{}{},
	}
	for _, node := range configuration.Nodes {
		f, err := ioutil.TempFile("", "durable-state-")
		require.NoError(t, err)
		t.Cleanup(func() {
			f.Close()
			os.Remove(f.Name())
		})

		ds, err := NewDurableState(f)
		require.NoError(t, err)
		cluster.ds[node.ID] = ds

		log, err := raftlog.New(logger, nil, nil)
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, log.Delete())
		})
		cluster.logs[node.ID] = log
		cluster.restart(node.ID)
	}
	return cluster
}

type testCluster struct {
	logger *zap.Logger

	minTicks, maxTicks int
	configuration      *Configuration

	logs map[NodeID]*raftlog.Storage
	ds   map[NodeID]*DurableState

	states        map[NodeID]*StateMachine
	messages      []interface{}
	blockedRoutes map[NodeID]map[NodeID]struct{}
}

func (t *testCluster) restart(id NodeID) {
	t.states[id] = NewStateMachine(t.logger, StateMachineConfig{
		ID:            id,
		MinTicks:      t.minTicks,
		MaxTicks:      t.maxTicks,
		Configuration: t.configuration,
	}, t.logs[id], t.ds[id])
}

func (t *testCluster) size() int {
	return len(t.states)
}

func (t *testCluster) propose(id NodeID, entry *raftlog.LogEntry) *Update {
	return t.states[id].Next(&Proposal{Entry: entry})
}

func (t *testCluster) triggerTimeout(id NodeID) *Update {
	return t.states[id].Tick(t.maxTicks)
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

func testReplicationAfterElection(t *testing.T, term uint64) {
	cluster := getTestCluster(t, 3, 0, 20)
	var update *Update
	for i := uint64(0); i < term; i++ {
		update = cluster.triggerTimeout(1)
		require.NotNil(t, update)
	}
	update = cluster.run(t, update, 1)
	require.NotNil(t, update)
	require.Len(t, update.Proposals, 1)
	entry := update.Proposals[0].Entry
	require.Equal(t, raftlog.LogNoop, entry.OpType)
	cluster.compareMsgHistories(t, []interface{}{
		&RequestVote{
			Term:      term,
			Candidate: 1,
		},
		&RequestVote{
			Term:      term,
			Candidate: 1,
		},
		&RequestVoteResponse{
			Term:        term,
			Voter:       2,
			VoteGranted: true,
		},
		&RequestVoteResponse{
			Term:        term,
			Voter:       3,
			VoteGranted: true,
		},
		&AppendEntries{
			Term:    term,
			Leader:  1,
			Entries: []*raftlog.LogEntry{{Index: 1, Term: term, OpType: raftlog.LogNoop}},
		},
		&AppendEntries{
			Term:    term,
			Leader:  1,
			Entries: []*raftlog.LogEntry{{Index: 1, Term: term, OpType: raftlog.LogNoop}},
		},
		&AppendEntriesResponse{
			Term:     term,
			Follower: 2,
			Success:  true,
			LastLog:  LogHeader{Index: 1, Term: term},
		},
		&AppendEntriesResponse{
			Term:     term,
			Follower: 3,
			Success:  true,
			LastLog:  LogHeader{Index: 1, Term: term},
		},
	})
}

func TestRaftReplicationAfterInitialElection(t *testing.T) {
	for term := uint64(1); term <= 3; term++ {
		t.Run(fmt.Sprintf("Term_%d", term), func(t *testing.T) {
			testReplicationAfterElection(t, term)
		})
	}
}

func TestRaftFailedElection(t *testing.T) {
	cluster := getTestCluster(t, 3, 0, 20) // 3 node cluster
	for i := 1; i < cluster.size(); i++ {
		_ = cluster.triggerTimeout(NodeID(i))
	}
	update := cluster.triggerTimeout(3)
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
	cluster := getTestCluster(t, 3, 0, 20)
	cluster.run(t, cluster.triggerTimeout(1), 1)
	cluster.resetHistory()
	cluster.run(t, cluster.triggerTimeout(2), 2)
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

func TestRaftCandidateTransitionToFollower(t *testing.T) {
	cluster := getTestCluster(t, 3, 0, 1)
	_ = cluster.triggerTimeout(1)
	_ = cluster.run(t, cluster.triggerTimeout(2), 2)
	cluster.compareMsgHistories(t, []interface{}{
		&RequestVote{
			Term:      1,
			Candidate: 2,
		},
		&RequestVote{
			Term:      1,
			Candidate: 2,
		},
		&RequestVoteResponse{
			Term:  1,
			Voter: 1,
		},
		&RequestVoteResponse{
			Term:        1,
			Voter:       3,
			VoteGranted: true,
		},
		&AppendEntries{
			Term:    1,
			Leader:  2,
			Entries: []*raftlog.LogEntry{{Index: 1, Term: 1, OpType: raftlog.LogNoop}},
		},
		&AppendEntries{
			Term:    1,
			Leader:  2,
			Entries: []*raftlog.LogEntry{{Index: 1, Term: 1, OpType: raftlog.LogNoop}},
		},
		&AppendEntriesResponse{
			Term:     1,
			Follower: 1,
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

func TestRaftReplicatonWithMajority(t *testing.T) {
	cluster := getTestCluster(t, 5, 0, 1)
	cluster.blockRoute(1, 4)
	cluster.blockRoute(1, 5)

	update := cluster.run(t, cluster.triggerTimeout(1), 1)
	require.Len(t, update.Proposals, 1)
}

func TestRaftProposalReplication(t *testing.T) {
	cluster := getTestCluster(t, 3, 0, 1)
	_ = cluster.run(t, cluster.triggerTimeout(1), 1)
	cluster.resetHistory()

	update := cluster.run(t, cluster.propose(1, &raftlog.LogEntry{OpType: raftlog.LogApplication}), 1)
	require.NotNil(t, update)
	require.Len(t, update.Proposals, 1)
	require.Equal(t, raftlog.LogApplication, update.Proposals[0].Entry.OpType)
}

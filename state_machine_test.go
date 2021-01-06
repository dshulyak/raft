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
	"go.uber.org/zap/zaptest"
	"pgregory.net/rapid"
)

type TestingHelper interface {
	Helper()
	Cleanup(func())
	zaptest.TestingT
}

var logLevel = flag.String("log-level", "panic", "test environment log level")

func testLogger(t TestingHelper) *zap.Logger {
	t.Helper()
	var level zapcore.Level
	require.NoError(t, level.Set(*logLevel))
	return zaptest.NewLogger(t, zaptest.Level(level))
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
		t:             t,
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
		t.Cleanup(func() {
			ds.Close()
		})

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
	t      TestingHelper
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

func (t *testCluster) runReplication(peer *peerState, to NodeID, next *AppendEntries) {
	peer.init(next)
	sm := t.states[to]
	for next != nil {
		t.messages = append(t.messages, next)
		require.NoError(t.t, sm.Next(next))
		update := sm.Update()
		require.NotNil(t.t, update)
		require.Len(t.t, update.Msgs, 1)

		msg, ok := update.Msgs[0].Message.(*AppendEntriesResponse)
		require.True(t.t, ok, "not an AppendEntriesResponse")
		t.messages = append(t.messages, msg)
		peer.onResponse(msg)

		next = peer.next()
	}
}

func (t *testCluster) replicationPeer(id NodeID, maxEntries uint64, timeout int) *peerState {
	return newPeerState(t.logger.Sugar(), maxEntries, timeout, t.logs[id])
}

func (t *testCluster) size() int {
	return len(t.states)
}

func (t *testCluster) propose(id NodeID, entry *raftlog.LogEntry) *Update {
	require.NoError(t.t, t.states[id].Next(&Proposal{Entry: entry}))
	return t.states[id].Update()
}

func (t *testCluster) triggerTimeout(id NodeID) *Update {
	require.NoError(t.t, t.states[id].Tick(t.maxTicks))
	return t.states[id].Update()
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
	c.t.Helper()
	stack := []*Update{u}
	for len(stack) > 0 {
		u = stack[0]
		stack[0] = nil
		stack = stack[1:]
		if u == nil {
			continue
		}
		for _, msg := range u.Msgs {
			if c.isBlocked(from, msg.To) {
				continue
			}
			sm := c.states[msg.To]
			c.messages = append(c.messages, msg.Message)
			require.NoError(c.t, sm.Next(msg.Message))
			update := sm.Update()
			if update != nil {
				stack = append(stack, update)
			}
		}
	}
	return u
}

func (c *testCluster) iterateLogs(f func(*raftlog.Storage) bool) {
	for _, log := range c.logs {
		if !f(log) {
			return
		}
	}
}

func (c *testCluster) compareMsgHistories(t TestingHelper, expected []interface{}) {
	t.Helper()
	max := len(expected)
	for i := range c.messages {
		if i >= max {
			assert.Equal(t, nil, c.messages[i], "message %d", i)
			continue
		}
		assert.Equal(t, expected[i], c.messages[i], "message %d", i)
	}
}

func (c *testCluster) resetHistory() {
	c.messages = nil
}

func testReplicationAfterElection(t *testing.T, term uint64) {
	t.Helper()
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
	require.Equal(t, raftlog.LogNoop, update.Proposals[0].Entry.OpType)
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

func TestRaftLogClean(t *testing.T) {
	cluster := getTestCluster(t, 3, 0, 1)
	update := cluster.run(t, cluster.triggerTimeout(1), 1)
	require.Len(t, update.Proposals, 1)

	for i := 1; i <= cluster.size(); i++ {
		cluster.states[NodeID(i)].Next(&AppendEntries{Term: 2})
		require.True(t, cluster.logs[NodeID(i)].IsEmpty())
	}
}

func TestRaftLogOverwrite(t *testing.T) {
	cluster := getTestCluster(t, 3, 0, 1)
	update := cluster.run(t, cluster.triggerTimeout(1), 1)
	require.Len(t, update.Proposals, 1)

	for i := 1; i <= cluster.size(); i++ {
		cluster.states[NodeID(i)].Next(&AppendEntries{Term: 2, PrevLog: LogHeader{Index: 1}})
		require.True(t, cluster.logs[NodeID(i)].IsEmpty())
	}
}

type rapidCleanup struct {
	*rapid.T
	cleanups []func()
}

func (c *rapidCleanup) Cleanup(f func()) {
	c.cleanups = append(c.cleanups, f)
}

func (c *rapidCleanup) Skipped() bool {
	return false
}

func (c *rapidCleanup) TempDir() string {
	return ""
}

func (c *rapidCleanup) Run() {
	for i := len(c.cleanups) - 1; i >= 0; i-- {
		c.cleanups[i]()
	}
}

type rapidTestState struct {
	leader NodeID
	commit LogHeader
}

type clusterMachine struct {
	cluster *testCluster
	cleanup *rapidCleanup

	state *rapidTestState
}

func (c *clusterMachine) Init(t *rapid.T) {
	c.cleanup = &rapidCleanup{T: t}
	n := rapid.IntRange(3, 7).Draw(t, "n").(int)
	c.cluster = getTestCluster(c.cleanup, n, 0, 1)
	c.state = &rapidTestState{}
}

func (c *clusterMachine) Timeout(t *rapid.T) {
	node := rapid.IntRange(1, c.cluster.size()).Draw(t, "timeout_node").(int)
	update := c.cluster.run(c.cleanup, c.cluster.triggerTimeout(NodeID(node)), NodeID(node))
	if update != nil && update.State == RaftLeader {
		c.state.leader = NodeID(node)
	}
}

func (c *clusterMachine) Restart(t *rapid.T) {
	node := rapid.IntRange(1, c.cluster.size()).Draw(t, "restart_node").(int)
	c.cluster.restart(NodeID(node))
}

func (c *clusterMachine) Partition(t *rapid.T) {
	from := rapid.IntRange(1, c.cluster.size()).Draw(t, "block_from").(int)
	to := rapid.IntRange(1, c.cluster.size()).Draw(t, "block_to").(int)
	if from != to {
		c.cluster.blockRoute(NodeID(from), NodeID(to))
	}
}

func (c *clusterMachine) Restore(t *rapid.T) {
	c.cluster.restoreRoutes()
}

func (c *clusterMachine) Propose(t *rapid.T) {
	if c.state.leader == None {
		t.Skip("leader is not yet elected")
	}
	update := c.cluster.propose(c.state.leader, &raftlog.LogEntry{OpType: raftlog.LogApplication})
	update = c.cluster.run(c.cleanup, update, c.state.leader)
	if update != nil && update.CommitLog.Term != 0 {
		c.state.commit = update.CommitLog
	}
}

func (c *clusterMachine) Check(t *rapid.T) {
	if c.state.commit.Term == 0 && c.state.commit.Index == 0 {
		return
	}
	majority := c.cluster.size()/2 + 1
	n := 0
	c.cluster.iterateLogs(func(log *raftlog.Storage) bool {
		entry, err := log.Get(int(c.state.commit.Index))
		if err != nil {
			require.EqualError(t, err, raftlog.ErrEntryNotFound.Error())
		}
		require.Equal(t, c.state.commit.Term, entry.Term)
		n++
		return true
	})
	require.LessOrEqual(t, majority, n,
		"commited entry is not replicated on the majority of servers")
}

func (c *clusterMachine) Cleanup() {
	c.cleanup.Run()
}

func TestRaftConsistency(t *testing.T) {
	if testing.Short() {
		t.Skip("consistency testing is skipped")
	}
	rapid.Check(t, rapid.Run(new(clusterMachine)))
}

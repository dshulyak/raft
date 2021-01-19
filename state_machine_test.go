package raft

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/dshulyak/raft/raftlog"
	"github.com/dshulyak/raft/types"
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
	return zaptest.NewLogger(t, zaptest.Level(level), zaptest.WrapOptions(zap.AddCaller()))
}

type testClusterOp func(*testCluster)

func withPreVote() testClusterOp {
	return func(c *testCluster) {
		c.features |= FeaturePreVote
	}
}

func withSize(size int) testClusterOp {
	return func(c *testCluster) {
		c.size = size
	}
}

func getTestCluster(t TestingHelper, opts ...testClusterOp) *testCluster {
	t.Helper()
	logger := testLogger(t)

	configuration := &Configuration{}

	cluster := &testCluster{
		t:             t,
		logger:        logger,
		size:          3,
		minTicks:      1,
		maxTicks:      10,
		configuration: configuration,
		logs:          map[NodeID]*raftlog.Storage{},
		ds:            map[NodeID]*DurableState{},
		states:        map[NodeID]*stateMachine{},
		blockedRoutes: map[NodeID]map[NodeID]struct{}{},
	}
	for _, opt := range opts {
		opt(cluster)
	}
	for i := 1; i <= cluster.size; i++ {
		configuration.Nodes = append(configuration.Nodes, ConfNode{ID: NodeID(i)})
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

	size               int
	features           uint32
	minTicks, maxTicks int
	configuration      *Configuration

	logs map[NodeID]*raftlog.Storage
	ds   map[NodeID]*DurableState

	states        map[NodeID]*stateMachine
	messages      []interface{}
	blockedRoutes map[NodeID]map[NodeID]struct{}

	proposals []*request
}

func (t *testCluster) restart(id NodeID) {
	t.states[id] = newStateMachine(t.logger, id, t.features,
		t.minTicks, t.maxTicks,
		t.configuration, t.logs[id], t.ds[id])
}

func (t *testCluster) runReplication(peer *replicationState, to NodeID, next *AppendEntries) {
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

func (t *testCluster) replicationPeer(id NodeID, maxEntries uint64, timeout int) *replicationState {
	return newReplicationState(t.logger.Sugar(), maxEntries, timeout, t.logs[id])
}

func (t *testCluster) propose(id NodeID, proposal *request) *Update {
	require.NoError(t.t, t.states[id].Next(proposal))
	return t.states[id].Update()
}

func (t *testCluster) applied(id NodeID, applied uint64) *Update {
	t.states[id].Applied(applied)
	return t.states[id].Update()
}

func (t *testCluster) triggerTimeout(id NodeID) *Update {
	require.NoError(t.t, t.states[id].Tick(t.maxTicks))
	return t.states[id].Update()
}

func (t *testCluster) incrementTimeout() {
	for _, st := range t.states {
		st.Tick(1)
	}
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
		c.proposals = append(c.proposals, u.Proposals...)
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

func (c *testCluster) compareMsgHistories(expected []interface{}) {
	c.t.Helper()
	c.compareMsgs(expected, c.messages)
}

func (c *testCluster) compareMsgs(expected, generated []interface{}) {
	c.t.Helper()
	max := len(expected)
	for i := range generated {
		if i >= max {
			assert.Equal(c.t, nil, generated[i], "message %d", i)
			continue
		}
		assert.Equal(c.t, expected[i], generated[i], "message %d", i)
	}
}

func (c *testCluster) resetHistory() {
	c.messages = nil
}

func testReplicationAfterElection(t *testing.T, term uint64) {
	t.Helper()
	cluster := getTestCluster(t)
	var update *Update
	for i := uint64(0); i < term; i++ {
		update = cluster.triggerTimeout(1)
		require.NotNil(t, update)
	}
	update = cluster.run(t, update, 1)
	require.NotNil(t, update)
	cluster.compareMsgHistories([]interface{}{
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
			Entries: []*types.Entry{{Index: 1, Term: term, Type: types.Entry_NOOP}},
		},
		&AppendEntries{
			Term:    term,
			Leader:  1,
			Entries: []*types.Entry{{Index: 1, Term: term, Type: types.Entry_NOOP}},
		},
		&AppendEntriesResponse{
			Term:     term,
			Follower: 2,
			Success:  true,
			LastLog:  types.LogHeader{Index: 1, Term: term},
		},
		&AppendEntriesResponse{
			Term:     term,
			Follower: 3,
			Success:  true,
			LastLog:  types.LogHeader{Index: 1, Term: term},
		},
		&AppendEntries{
			Term:     term,
			Leader:   1,
			Commited: 1,
			PrevLog:  types.LogHeader{Index: 1, Term: term},
		},
		&AppendEntries{
			Term:     term,
			Leader:   1,
			Commited: 1,
			PrevLog:  types.LogHeader{Index: 1, Term: term},
		},
		&AppendEntriesResponse{
			Term:     term,
			Follower: 2,
			Success:  true,
			LastLog:  types.LogHeader{Index: 1, Term: term},
		},
		&AppendEntriesResponse{
			Term:     term,
			Follower: 3,
			Success:  true,
			LastLog:  types.LogHeader{Index: 1, Term: term},
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
	cluster := getTestCluster(t) // 3 node cluster
	for i := 1; i < cluster.size; i++ {
		_ = cluster.triggerTimeout(NodeID(i))
	}
	update := cluster.triggerTimeout(3)
	require.NotNil(t, update)
	_ = cluster.run(t, update, 3)
	cluster.compareMsgHistories([]interface{}{
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
	cluster := getTestCluster(t)
	cluster.run(t, cluster.triggerTimeout(1), 1)
	cluster.resetHistory()

	cluster.run(t, cluster.triggerTimeout(2), 2)
	cluster.compareMsgHistories([]interface{}{
		&RequestVote{
			Term:      2,
			Candidate: 2,
			LastLog:   types.LogHeader{Term: 1, Index: 1},
		},
		&RequestVote{
			Term:      2,
			Candidate: 2,
			LastLog:   types.LogHeader{Term: 1, Index: 1},
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
			Term:     2,
			Leader:   2,
			Commited: 1,
			Entries:  []*raftlog.Entry{{Index: 2, Term: 2, Type: types.Entry_NOOP}},
			PrevLog:  types.LogHeader{Term: 1, Index: 1},
		},
		&AppendEntries{
			Term:     2,
			Leader:   2,
			Commited: 1,
			Entries:  []*raftlog.Entry{{Index: 2, Term: 2, Type: types.Entry_NOOP}},
			PrevLog:  types.LogHeader{Term: 1, Index: 1},
		},
		&AppendEntriesResponse{
			Term:     2,
			Follower: 1,
			Success:  true,
			LastLog:  types.LogHeader{Index: 2, Term: 2},
		},
		&AppendEntriesResponse{
			Term:     2,
			Follower: 3,
			Success:  true,
			LastLog:  types.LogHeader{Index: 2, Term: 2},
		},
		&AppendEntries{
			Term:     2,
			Leader:   2,
			Commited: 2,
			PrevLog:  types.LogHeader{Index: 2, Term: 2},
		},
		&AppendEntries{
			Term:     2,
			Leader:   2,
			Commited: 2,
			PrevLog:  types.LogHeader{Index: 2, Term: 2},
		},
		&AppendEntriesResponse{
			Term:     2,
			Follower: 1,
			Success:  true,
			LastLog:  types.LogHeader{Index: 2, Term: 2},
		},
		&AppendEntriesResponse{
			Term:     2,
			Follower: 3,
			Success:  true,
			LastLog:  types.LogHeader{Index: 2, Term: 2},
		},
	})
}

func TestRaftCandidateTransitionToFollower(t *testing.T) {
	cluster := getTestCluster(t)
	_ = cluster.triggerTimeout(1)
	_ = cluster.run(t, cluster.triggerTimeout(2), 2)
	cluster.compareMsgHistories([]interface{}{
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
			Entries: []*raftlog.Entry{{Index: 1, Term: 1, Type: types.Entry_NOOP}},
		},
		&AppendEntries{
			Term:    1,
			Leader:  2,
			Entries: []*raftlog.Entry{{Index: 1, Term: 1, Type: types.Entry_NOOP}},
		},
		&AppendEntriesResponse{
			Term:     1,
			Follower: 1,
			Success:  true,
			LastLog:  types.LogHeader{Index: 1, Term: 1},
		},
		&AppendEntriesResponse{
			Term:     1,
			Follower: 3,
			Success:  true,
			LastLog:  types.LogHeader{Index: 1, Term: 1},
		},
		&AppendEntries{
			Term:     1,
			Leader:   2,
			Commited: 1,
			PrevLog:  types.LogHeader{Index: 1, Term: 1},
		},
		&AppendEntries{
			Term:     1,
			Leader:   2,
			Commited: 1,
			PrevLog:  types.LogHeader{Index: 1, Term: 1},
		},
		&AppendEntriesResponse{
			Term:     1,
			Follower: 1,
			Success:  true,
			LastLog:  types.LogHeader{Index: 1, Term: 1},
		},
		&AppendEntriesResponse{
			Term:     1,
			Follower: 3,
			Success:  true,
			LastLog:  types.LogHeader{Index: 1, Term: 1},
		},
	})
}

func TestRaftReplicatonWithMajority(t *testing.T) {
	cluster := getTestCluster(t, withSize(5))
	cluster.blockRoute(1, 4)
	cluster.blockRoute(1, 5)

	cluster.run(t, cluster.triggerTimeout(1), 1)
	require.Len(t, cluster.proposals, 1)
	require.Equal(t, types.Entry_NOOP, cluster.proposals[0].Entry.Type)
}

func TestRaftProposalReplication(t *testing.T) {
	cluster := getTestCluster(t)
	_ = cluster.run(t, cluster.triggerTimeout(1), 1)
	cluster.resetHistory()

	cluster.run(t, cluster.propose(1, &request{Entry: &raftlog.Entry{Type: types.Entry_APP}}), 1)
	require.Len(t, cluster.proposals, 2)
	require.Equal(t, types.Entry_NOOP, cluster.proposals[0].Entry.Type)
	require.Equal(t, types.Entry_APP, cluster.proposals[1].Entry.Type)
}

func TestRaftLogClean(t *testing.T) {
	cluster := getTestCluster(t)
	cluster.run(t, cluster.triggerTimeout(1), 1)

	for i := 1; i <= cluster.size; i++ {
		cluster.states[NodeID(i)].Next(&AppendEntries{Term: 2})
		require.True(t, cluster.logs[NodeID(i)].IsEmpty())
	}
}

func TestRaftLogOverwrite(t *testing.T) {
	cluster := getTestCluster(t)
	cluster.run(t, cluster.triggerTimeout(1), 1)

	for i := 1; i <= cluster.size; i++ {
		cluster.states[NodeID(i)].Next(&AppendEntries{Term: 2, PrevLog: types.LogHeader{Index: 1}})
		require.True(t, cluster.logs[NodeID(i)].IsEmpty())
	}
}

func TestRaftReads(t *testing.T) {
	cluster := getTestCluster(t)
	leader := NodeID(1)
	cluster.run(t, cluster.triggerTimeout(leader), leader)
	rr1 := newReadRequest(context.Background())
	u1 := cluster.propose(leader, rr1)
	rr2 := newReadRequest(context.Background())
	u2 := cluster.propose(leader, rr2)

	cluster.applied(leader, 1)
	// this update carries a message with readIndex=1
	cluster.run(t, u1, leader)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.NoError(t, rr1.Wait(ctx))
	err := rr2.Wait(ctx)
	require.True(t, errors.Is(err, context.DeadlineExceeded), err)

	cluster.run(t, u2, leader)
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.NoError(t, rr2.Wait(ctx))
}

func TestRaftPreVoteSuccess(t *testing.T) {
	cluster := getTestCluster(t, withPreVote())
	leader := NodeID(1)
	cluster.run(t, cluster.triggerTimeout(leader), leader)
	require.True(t, len(cluster.messages) > 8)
	cluster.compareMsgs([]interface{}{
		&RequestVote{
			Term:      0,
			Candidate: 1,
			PreVote:   true,
		},
		&RequestVote{
			Term:      0,
			Candidate: 1,
			PreVote:   true,
		},
		&RequestVoteResponse{
			Term:        0,
			Voter:       2,
			VoteGranted: true,
			PreVote:     true,
		},
		&RequestVoteResponse{
			Term:        0,
			Voter:       3,
			VoteGranted: true,
			PreVote:     true,
		},
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
	},
		cluster.messages[:8],
	)
}

func TestRaftPreVoteNoDisrupt(t *testing.T) {
	cluster := getTestCluster(t, withPreVote())
	leader := NodeID(1)
	blocked := NodeID(2)

	cluster.blockRoute(leader, blocked)
	cluster.run(t, cluster.triggerTimeout(leader), leader)

	cluster.restoreRoutes()
	cluster.resetHistory()
	cluster.run(t, cluster.triggerTimeout(blocked), blocked)

	cluster.compareMsgHistories([]interface{}{
		&RequestVote{
			Term:      0,
			Candidate: blocked,
			PreVote:   true,
		},
		&RequestVote{
			Term:      0,
			Candidate: blocked,
			PreVote:   true,
		},
		&RequestVoteResponse{
			Term:    1,
			Voter:   1,
			PreVote: true,
		},
		&RequestVoteResponse{
			Term:    1,
			Voter:   3,
			PreVote: true,
		},
	})
}

func TestRaftLinkTimeout(t *testing.T) {
	cluster := getTestCluster(t, withPreVote())
	leader := NodeID(1)
	timedout := NodeID(2)

	cluster.run(t, cluster.triggerTimeout(leader), leader)

	cluster.resetHistory()
	cluster.run(t, cluster.triggerTimeout(timedout), timedout)
	electionHistory := func(success bool) []interface{} {
		return []interface{}{
			&RequestVote{
				Term:      1,
				Candidate: timedout,
				PreVote:   true,
				LastLog: types.LogHeader{
					Index: 1,
					Term:  1,
				},
			},
			&RequestVote{
				Term:      1,
				Candidate: timedout,
				PreVote:   true,
				LastLog: types.LogHeader{
					Index: 1,
					Term:  1,
				},
			},
			// leader will always reply false. but it will convert
			// to follower when sees higher term
			&RequestVoteResponse{
				Term:        1,
				Voter:       leader,
				VoteGranted: false,
				PreVote:     true,
			},
			&RequestVoteResponse{
				Term:        1,
				Voter:       3,
				VoteGranted: success,
				PreVote:     true,
			},
		}
	}
	cluster.compareMsgHistories(electionHistory(false))

	cluster.resetHistory()

	// now link timeout will allow new election to progress
	cluster.incrementTimeout()
	cluster.run(t, cluster.triggerTimeout(timedout), timedout)
	require.True(t, len(cluster.messages) > 4, len(cluster.messages))
	cluster.compareMsgs(electionHistory(true), cluster.messages[:4])
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
	commit uint64
}

type clusterMachine struct {
	cluster *testCluster
	cleanup *rapidCleanup

	state *rapidTestState
}

func (c *clusterMachine) Init(t *rapid.T) {
	c.cleanup = &rapidCleanup{T: t}
	n := rapid.IntRange(3, 7).Draw(t, "n").(int)
	c.cluster = getTestCluster(c.cleanup, withSize(n))
	c.state = &rapidTestState{}
}

func (c *clusterMachine) Timeout(t *rapid.T) {
	node := rapid.IntRange(1, c.cluster.size).Draw(t, "timeout_node").(int)
	update := c.cluster.run(c.cleanup, c.cluster.triggerTimeout(NodeID(node)), NodeID(node))
	if update != nil && update.State == RaftLeader {
		c.state.leader = NodeID(node)
	}
}

func (c *clusterMachine) Restart(t *rapid.T) {
	node := rapid.IntRange(1, c.cluster.size).Draw(t, "restart_node").(int)
	c.cluster.restart(NodeID(node))
}

func (c *clusterMachine) Partition(t *rapid.T) {
	from := rapid.IntRange(1, c.cluster.size).Draw(t, "block_from").(int)
	to := rapid.IntRange(1, c.cluster.size).Draw(t, "block_to").(int)
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
	update := c.cluster.propose(c.state.leader, &request{Entry: &raftlog.Entry{Type: types.Entry_APP}})
	update = c.cluster.run(c.cleanup, update, c.state.leader)
	if update != nil && update.Commit != 0 {
		c.state.commit = update.Commit
	}
}

func (c *clusterMachine) Check(t *rapid.T) {
	if c.state.commit == 0 {
		return
	}
	majority := c.cluster.size/2 + 1
	n := 0
	c.cluster.iterateLogs(func(log *raftlog.Storage) bool {
		_, err := log.Get(int(c.state.commit))
		if err != nil {
			require.EqualError(t, err, raftlog.ErrEntryNotFound.Error())
		}
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

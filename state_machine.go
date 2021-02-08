package raft

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"runtime/debug"
	"time"

	"github.com/dshulyak/raft/raftlog"
	"github.com/dshulyak/raft/state"
	"github.com/dshulyak/raft/types"

	"go.uber.org/zap"
)

var (
	ErrLeaderStepdown = errors.New("leader stepdown")
	ErrUnexpected     = errors.New("unexpected error")
)

type ErrRedirect struct {
	Leader *types.Node
}

func (e *ErrRedirect) Error() string {
	return fmt.Sprintf("redirect to %s", e.Leader)
}

var None NodeID

type (
	NodeID                = types.NodeID
	LogHeader             = types.LogHeader
	RequestVote           = types.RequestVote
	RequestVoteResponse   = types.RequestVoteResponse
	AppendEntries         = types.AppendEntries
	AppendEntriesResponse = types.AppendEntriesResponse
)

type Message interface{}

type MessageTo struct {
	To      NodeID
	Message interface{}
}

type RaftState uint8

const (
	RaftFollower = iota + 1
	RaftCandidate
	RaftLeader
)

var raftStateString = [...]string{
	"Empty", "Follower", "Candidate", "Leader",
}

func (s RaftState) String() string {
	return raftStateString[s]
}

const (
	LeaderEnmpty = iota
	LeaderKnown
	LeaderUnknown
)

type Update struct {
	Updated     bool
	Msgs        []MessageTo
	Proposals   []*request
	LeaderState int
	State       RaftState
	Commit      uint64
}

type raftState struct {
	ctx    context.Context
	logger *zap.SugaredLogger

	rng *rand.Rand

	features uint32

	minElection, maxElection int
	election                 int

	id     NodeID
	leader NodeID

	nodes         map[NodeID]*ConfNode
	configuration *Configuration

	state.State
	stateStore *state.Store

	log *raftlog.Storage

	commitIndex uint64
}

func (s *raftState) majority() int {
	return len(s.configuration.Nodes)/2 + 1
}

func (s *raftState) must(err error, msg string) {
	if err != nil {
		s.logger.Panicw(msg, "error", err)
	}
}

func (s *raftState) commit(u *Update, commited uint64) {
	if commited <= s.commitIndex {
		return
	}
	s.logger.Debugw("entry commited", "index", commited, "term", s.Term)
	s.commitIndex = commited
	u.Commit = commited
	u.Updated = true
}

func (s *raftState) cmpLogs(term, index uint64) int {
	if s.log.IsEmpty() {
		if term == 0 && index == 0 {
			return 0
		}
		return -1
	}
	last, err := s.log.Last()
	s.must(err, "can't get last log")
	if last.Term > term {
		return 1
	} else if last.Term < term {
		return -1
	}
	if last.Index > index {
		return 1
	} else if last.Index < index {
		return -1
	}
	return 0
}

func (s *raftState) send(u *Update, msg interface{}, to ...NodeID) {
	if len(to) == 0 {
		for i := range s.configuration.Nodes {
			id := s.configuration.Nodes[i].ID
			if id == s.id {
				continue
			}
			u.Msgs = append(u.Msgs, MessageTo{To: id, Message: msg})
		}
	}
	for _, id := range to {
		u.Msgs = append(u.Msgs, MessageTo{To: id, Message: msg})
	}
	u.Updated = true
}

func (s *raftState) resetElectionTimeout() {
	// jitter = [1, maxElection-minElection]
	jitter := s.rng.Intn(1 + s.maxElection - s.minElection)
	if jitter == 0 {
		jitter = 1
	}
	s.election = s.minElection + jitter
}

type role interface {
	tick(int, *Update) role
	next(interface{}, *Update) role
	applied(uint64, *Update)
}

func newStateMachine(
	logger *zap.Logger,
	id NodeID,
	features uint32,
	minTicks, maxTicks int,
	conf *Configuration,
	log *raftlog.Storage,
	stateStore *state.Store,
) *stateMachine {
	update := &Update{}
	nodes := map[NodeID]*ConfNode{}
	for i := range conf.Nodes {
		node := &conf.Nodes[i]
		nodes[node.ID] = node
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	rstate := &raftState{
		rng:           rng,
		stateStore:    stateStore,
		logger:        logger.With(zap.Uint64("ID", uint64(id))).Sugar(),
		minElection:   minTicks,
		maxElection:   maxTicks,
		id:            id,
		features:      features,
		nodes:         nodes,
		configuration: conf,
		log:           log,
	}
	rstate.must(stateStore.Load(&rstate.State), "failed to load persistent state")
	return &stateMachine{
		update: update,
		role:   toFollower(rstate, 0, update),
	}
}

type stateMachine struct {
	role   role
	update *Update
}

func (s *stateMachine) Tick(n int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v\n%s", ErrUnexpected, r, debug.Stack())
		}
	}()
	r := s.role.tick(n, s.update)
	if r != nil {
		s.role = r
	}
	return
}

func (s *stateMachine) Next(msg interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v\n%s", ErrUnexpected, r, debug.Stack())
		}
	}()
	r := s.role.next(msg, s.update)
	for r != nil {
		s.role = r
		// technically it is enough to iterate only once.
		// it is never the case when that the role will be changed
		// immediatly after the previous change
		r = s.role.next(msg, s.update)
	}
	return
}

func (s *stateMachine) Applied(applied uint64) {
	s.role.applied(applied, s.update)
}

func (s *stateMachine) Update() *Update {
	if !s.update.Updated {
		return nil
	}
	u := s.update
	s.update = &Update{}
	if u.State > 0 {
		s.update.State = u.State
	}
	return u
}

func toFollower(s *raftState, term uint64, u *Update) *follower {
	f := &follower{raftState: s}
	f.resetElectionTimeout()
	f.leader = None
	if term > s.Term {
		f.Term = term
		f.VotedFor = None
		f.must(f.stateStore.Save(&s.State), "failed to sync durable state")
	}

	u.State = RaftFollower
	u.LeaderState = LeaderUnknown
	u.Updated = true
	return f
}

type follower struct {
	*raftState

	// linkTimeout is equal to the min election timeout.
	// if RequestVote in PreVote mode is received before min election timeout has passed it will be rejected.
	// prevents partially connected replica with suficiently new log from livelocking a cluster
	//
	// described in 6. Cluster membership changes
	linkTimeout int
}

// resetLinkTimeout must be reset only after hearing from a current leader.
func (f *follower) resetLinkTimeout() {
	f.linkTimeout = f.minElection
}

func (f *follower) applied(uint64, *Update) {}

func (f *follower) tick(n int, u *Update) role {
	f.election -= n
	if f.election <= 0 {
		f.logger.Debugw("election timeout elapsed. transitioning to candidate",
			"id", f.id,
		)
		return toCandidate(f.raftState, u)
	}
	if IsPreVoteEnabled(f.features) {
		f.linkTimeout -= n
	}
	return nil
}

func (f *follower) next(msg interface{}, u *Update) role {
	switch m := msg.(type) {
	case *RequestVote:
		return f.onRequestVote(m, u)
	case *AppendEntries:
		return f.onAppendEntries(m, u)
	case []*request:
		if f.leader == None {
			f.logger.Panicw("proposals can't be sent while leader is not elected")
		}
		redirect := &ErrRedirect{Leader: f.nodes[f.leader]}
		for _, proposal := range m {
			proposal.Complete(redirect)
		}
	}
	return nil
}

func (f *follower) onAppendEntries(msg *AppendEntries, u *Update) role {
	if f.Term > msg.Term {
		f.send(u, &AppendEntriesResponse{
			Term:     f.Term,
			Follower: f.id,
		}, msg.Leader)
		return nil
	} else if f.Term < msg.Term {
		f.Term = msg.Term
		f.VotedFor = None
		f.must(f.stateStore.Save(&f.State), "failed to sync durable state")
	}
	if f.leader != msg.Leader {
		f.leader = msg.Leader
		u.LeaderState = LeaderKnown
		u.Updated = true
	}
	f.resetElectionTimeout()
	f.resetLinkTimeout()

	// TODO i need trace level
	if len(msg.Entries) > 0 {
		f.logger.Debugw("append entries", "msg", msg)
	}
	empty := f.log.IsEmpty()
	if empty && msg.PrevLog.Index > 0 {
		f.send(u, &AppendEntriesResponse{
			Term:     f.Term,
			Follower: f.id,
		}, msg.Leader)
		return nil
	} else if !empty && msg.PrevLog.Index > 0 {
		entry, err := f.log.Get(msg.PrevLog.Index)
		if errors.Is(err, raftlog.ErrEntryFromFuture) {
			f.send(u, &AppendEntriesResponse{
				Term:     f.Term,
				Follower: f.id,
			}, msg.Leader)
			return nil
		} else if err != nil {
			f.must(err, "failed to get log entry")
		}
		if entry.Term != msg.PrevLog.Term {
			f.send(u, &AppendEntriesResponse{
				Term:     f.Term,
				Follower: f.id,
			}, msg.Leader)
			return nil
		}
	}

	var last *types.Entry
	for i := range msg.Entries {
		last = msg.Entries[i]
		f.must(f.log.Append(last), "failed to append log")
	}
	if len(msg.Entries) == 0 {
		entry, err := f.log.Last()
		f.must(err, "error loading last log entry")
		last = entry
	} else {
		f.logger.Debugw("last appended entry",
			"index", last.Index,
			"term", last.Term,
		)
		f.must(f.log.Sync(), "failed to persist the log on disk")
	}
	f.commit(u, min(msg.Commited, last.Index))
	resp := &AppendEntriesResponse{
		Term:      f.Term,
		Follower:  f.id,
		Success:   true,
		ReadIndex: msg.ReadIndex,
		LastLog: types.LogHeader{
			Index: last.Index,
			Term:  last.Term,
		},
	}
	f.send(u, resp, msg.Leader)
	return nil
}

func min(i, j uint64) uint64 {
	if i > j {
		return j
	}
	return i
}

func (f *follower) onRequestVote(msg *RequestVote, u *Update) role {
	var (
		grant   bool
		context = ""
		sync    bool
	)
	if msg.Term > f.Term {
		f.Term = msg.Term
		// updating to None here is crucial as it invalidates "already voted" condition
		f.VotedFor = None
		sync = true
	}
	// ignoring link timeout if it is no a pre-vote phase, if next candidate
	// term is higher then the current leader term it will disrupt current leader
	// so it is better to switch to a new leader immediatly
	if msg.PreVote && f.linkTimeout > 0 {
		grant = false
		context = "link timeout has not elapsed"
	} else if msg.Term < f.Term {
		grant = false
		context = "msg term is less than the local term"
	} else if !msg.PreVote && (msg.Term == f.Term && f.VotedFor != None) {
		grant = f.VotedFor == msg.Candidate
		if grant {
			context = "already voted for the candidate in this term"
		} else {
			context = "voted for a different candidate in this term"
		}
	} else {
		grant = f.cmpLogs(msg.LastLog.Term, msg.LastLog.Index) <= 0
		context = "log is outdated"
		if grant {
			context = "log is sufficiently new"
			if !msg.PreVote {
				f.VotedFor = msg.Candidate
				sync = true
			}
		}
	}

	if sync {
		f.must(f.stateStore.Save(&f.State), "failed to sync durable state")
	}
	if grant {
		// do we reset timer if it is a pre-vote?
		f.resetElectionTimeout()
	}
	f.logger.Debugw(context,
		"term", msg.Term,
		"candidate", msg.Candidate,
		"voter", f.id,
		"last log index", msg.LastLog.Index,
		"last log term", msg.LastLog.Term,
		"pre vote", msg.PreVote,
	)
	f.send(u, &RequestVoteResponse{
		Term:        f.Term,
		Voter:       f.id,
		VoteGranted: grant,
		PreVote:     msg.PreVote,
	}, msg.Candidate)
	return nil
}

func toCandidate(s *raftState, u *Update) *candidate {
	c := candidate{
		raftState: s,
		votes:     map[NodeID]struct{}{},
	}
	c.campaign(IsPreVoteEnabled(c.features), u)
	return &c
}

type candidate struct {
	*raftState
	preVote bool
	votes   map[NodeID]struct{}
}

func (c *candidate) campaign(preVote bool, u *Update) {
	c.preVote = preVote
	for voter := range c.votes {
		delete(c.votes, voter)
	}
	c.votes[c.id] = struct{}{}

	if !c.preVote {
		c.Term++
		c.VotedFor = c.id
		c.must(c.stateStore.Save(&c.State), "failed to sync durable state")
	}

	c.resetElectionTimeout()

	last, err := c.log.Last()
	if errors.Is(err, raftlog.ErrEmptyLog) {
		c.logger.Debugw("log is empty")
	} else if err != nil {
		c.must(err, "failed to fetch last log entry")
	}

	var header types.LogHeader
	if last != nil {
		header.Term = last.Term
		header.Index = last.Index
	}

	request := &RequestVote{
		Term:      c.Term,
		Candidate: c.id,
		PreVote:   c.preVote,
		LastLog:   header,
	}

	c.logger.Debugw("starting an election campaign", "candidate", c.id, "term", c.Term, "pre-vote mode", c.preVote)
	c.send(u, request)

	u.LeaderState = LeaderUnknown
	u.State = RaftCandidate
	u.Updated = true
}

func (c *candidate) applied(uint64, *Update) {}

func (c *candidate) tick(n int, u *Update) role {
	c.election -= n
	if c.election <= 0 {
		c.logger.Debugw("election timeout elapsed. transitioning to candidate",
			"id", c.id,
		)
		return toCandidate(c.raftState, u)
	}
	return nil
}

func (c *candidate) next(msg interface{}, u *Update) role {
	switch m := msg.(type) {
	case *RequestVote:
		if m.Term > c.Term {
			return toFollower(c.raftState, m.Term, u)
		}
		var grant bool
		if m.Term == c.Term && m.PreVote {
			grant = c.cmpLogs(m.LastLog.Term, m.LastLog.Index) <= 0
			// no need to reset election timeout if pre-vote is granted in this case
			// if candidate times out in pre-vote mode it will restart pre-vote mode
			// with the same term.
			// so timeout will not prevent other candidate from succeeding
		}
		c.send(u, &RequestVoteResponse{
			Voter:       c.id,
			Term:        c.Term,
			PreVote:     m.PreVote,
			VoteGranted: grant,
		}, m.Candidate)
	case *AppendEntries:
		// leader might have been elected in the same term as the candidate
		if m.Term >= c.Term {
			return toFollower(c.raftState, m.Term, u)
		}
		c.send(u, &AppendEntriesResponse{
			Follower: c.id,
			Term:     c.Term,
		}, m.Leader)
	case *RequestVoteResponse:
		if m.Term < c.Term {
			return nil
		}
		if m.Term > c.Term {
			return toFollower(c.raftState, m.Term, u)
		}
		if !m.VoteGranted {
			return nil
		}
		if c.preVote != m.PreVote {
			return nil
		}
		c.logger.Debugw("received a vote", "voter", m.Voter, "candidate", c.id, "pre-vote", m.PreVote)
		c.votes[m.Voter] = struct{}{}
		if len(c.votes) < c.majority() {
			return nil
		}
		if c.preVote {
			c.campaign(false, u)
			return nil
		}
		return toLeader(c.raftState, u)
	case []*request:
		c.logger.Panicw("proposals can't be sent while leader is not elected")
	}
	return nil
}

type readReq struct {
	proposal *request
	// commitIndex at the time of request.
	commitIndex uint64
	// logical timestamp. once we got confirmation from majority
	// for index higher or equal to this one request is safe to execute.
	readIndex uint64
}

func toLeader(s *raftState, u *Update) *leader {
	s.logger.Infow("leader is elected", "id", s.id, "term", s.Term)
	l := leader{
		raftState:          s,
		matchIndex:         newMatch(len(s.configuration.Nodes)),
		checkQuorumTimeout: s.minElection,
		checkQuorum:        map[NodeID]struct{}{},
		inflight:           list.New(),

		ackedRead: newMatch(len(s.configuration.Nodes)),
		reads:     list.New(),
	}
	last, err := s.log.Last()
	if errors.Is(err, raftlog.ErrEmptyLog) {
		l.nextLogIndex = 1
	} else if err != nil {
		s.must(err, "failed fetching last log")
	} else {
		l.nextLogIndex = last.Index + 1
		l.prevLog.Index = last.Index
		l.prevLog.Term = last.Term
	}
	// replicate noop in order to commit entries from previous terms
	l.sendProposals(u, newWriteRequest(nil, &types.Entry{
		Type: types.Entry_NOOP,
	}))
	u.State = RaftLeader
	u.LeaderState = LeaderKnown
	u.Updated = true
	return &l
}

type leader struct {
	*raftState
	// recentlyCommited is true if leader commited in this term.
	recentlyCommited bool
	nextLogIndex     uint64
	prevLog          LogHeader
	matchIndex       *match

	checkQuorumTimeout int
	checkQuorum        map[NodeID]struct{}

	inflight *list.List

	// last confirmed applied index
	appliedIndex uint64
	// logical clock. incremented each time raft receives next batch of read requests
	// required to establish causality between AppendEntries and AppendEntriesResponse
	// because communication is generally asynchronous
	readIndex uint64
	// last acknowledged read index
	acked uint64
	// acknolwdged read index by each node
	ackedRead *match
	// list of pending read requsts.similar to inflight list.
	reads *list.List
}

func (l *leader) sendProposals(u *Update, proposals ...*request) {
	msg := &AppendEntries{
		Term:     l.Term,
		Leader:   l.id,
		Commited: l.commitIndex,
		PrevLog: types.LogHeader{
			Term:  l.prevLog.Term,
			Index: l.prevLog.Index,
		},
	}
	for _, proposal := range proposals {
		if proposal.Read() {
			// readIndex is the same for all read requests submitted in a batch
			// zero is a null value, which is invalid as it is always returned
			// by the follower by default
			if msg.ReadIndex == 0 {
				l.readIndex++
				// overflow if leader remains stable for more than 1<<64-2 reads
				if l.readIndex == 0 {
					panic("read index overflow")
				}
				msg.ReadIndex = l.readIndex
			}
			l.reads.PushBack(&readReq{
				proposal:    proposal,
				commitIndex: l.commitIndex,
				readIndex:   l.readIndex,
			})

			readCounter.Inc()
			continue
		}
		entry := proposal.Entry
		entry.Index = l.nextLogIndex
		entry.Term = l.Term
		msg.Entries = append(msg.Entries, entry)
		l.nextLogIndex++
		l.logger.Debugw("append entry on a leader and send proposal",
			"index", entry.Index, "term", entry.Term)
		l.must(l.log.Append(entry), "failed to append a record")
		_ = l.inflight.PushBack(proposal)

		writeCounter.Inc()
	}
	if msg.ReadIndex == 0 {
		msg.ReadIndex = l.readIndex
	}
	l.updateReadIndex(l.id, l.readIndex, u)
	if len(msg.Entries) > 0 {
		l.prevLog.Index = msg.Entries[len(msg.Entries)-1].Index
		l.prevLog.Term = l.Term
		l.matchIndex.update(l.id, l.prevLog.Index)
		l.must(l.log.Sync(), "failed to sync the log")
	}
	l.send(u, msg)
}

func (l *leader) applied(applied uint64, u *Update) {
	if applied > l.appliedIndex {
		l.logger.Debugw("applied index is updated", "applied", applied)
		l.appliedIndex = applied
		l.completePendingReads(u)
	}
}

func (l *leader) tick(n int, u *Update) role {
	// NOTE state machine is not responsible for sending heartbeats.
	// each peer will have a separate goroutine that will send heartbeats in
	// the idle periods.
	// technically it doesn't affect the protocol as long as heartbeats timeout
	// is lower than the electiom timeout
	l.checkQuorumTimeout -= n
	if l.checkQuorumTimeout <= 0 {
		// don't count leader itself
		expect := l.majority() - 1
		if n := len(l.checkQuorum); n < expect {
			l.logger.Debugw("CheckQuorum failed", "received", n, "expected", expect)
			return l.stepdown(l.Term, u)
		}
		for id := range l.checkQuorum {
			delete(l.checkQuorum, id)
		}
		l.checkQuorumTimeout = l.minElection
	}
	return nil
}

// notifyPending notifies both write and read requests submitters.
func (l *leader) notifyPending(err error) {
	for front := l.inflight.Front(); front != nil; front = front.Next() {
		front.Value.(*request).Complete(err)
	}
	for front := l.reads.Front(); front != nil; front = front.Next() {
		front.Value.(*readReq).proposal.Complete(err)
	}
}

func (l *leader) stepdown(term uint64, u *Update) *follower {
	l.notifyPending(ErrLeaderStepdown)
	return toFollower(l.raftState, term, u)
}

func (l *leader) commitInflight(u *Update, idx uint64) {
	l.logger.Debugw("commiting proposals on a leader",
		"commit", idx, "proposals count", l.inflight.Len())
	if !l.recentlyCommited {
		l.recentlyCommited = true
		l.completePendingReads(u)
	}
	for front := l.inflight.Front(); front != nil; {
		proposal := front.Value.(*request)
		if proposal.Entry.Index > idx {
			break
		} else {
			l.logger.Debugw("proposal is commited", "proposal", proposal)
			u.Updated = true

			proposal.Complete(nil)

			commitedWriteCounter.Inc()
			commitSec.Observe(time.Since(proposal.Created).Seconds())

			u.Proposals = append(u.Proposals, proposal)
			prev := front
			front = front.Next()
			l.inflight.Remove(prev)
		}
	}
	// update commit index everywhere
	l.sendProposals(u)
}

func (l *leader) next(msg interface{}, u *Update) role {
	switch m := msg.(type) {
	case *RequestVote:
		if m.Term > l.Term {
			return l.stepdown(m.Term, u)
		}
		l.send(u, &RequestVoteResponse{
			Voter:   l.id,
			Term:    l.Term,
			PreVote: m.PreVote,
		}, m.Candidate)
	case *AppendEntries:
		if m.Term > l.Term {
			return l.stepdown(m.Term, u)
		}
		l.send(u, &AppendEntriesResponse{
			Follower: l.id,
			Term:     l.Term,
		}, m.Leader)
	case *RequestVoteResponse:
		if m.Term > l.Term {
			return l.stepdown(m.Term, u)
		}
	case *AppendEntriesResponse:
		if m.Term < l.Term {
			return nil
		} else if m.Term > l.Term {
			return l.stepdown(m.Term, u)
		}
		return l.onAppendEntriesResponse(m, u)
	case *request:
		l.sendProposals(u, m)
	case []*request:
		l.sendProposals(u, m...)
	}
	return nil
}

func (l *leader) completePendingReads(u *Update) {
	if !l.recentlyCommited {
		return
	}
	for front := l.reads.Front(); front != nil; {
		rr := front.Value.(*readReq)
		if l.appliedIndex >= rr.commitIndex && rr.readIndex <= l.acked {
			rr.proposal.Complete(nil)

			completedReadCounter.Inc()
			readSec.Observe(time.Since(rr.proposal.Created).Seconds())

			prev := front
			front = front.Next()
			l.reads.Remove(prev)
		} else {
			return
		}
	}
}

func (l *leader) updateReadIndex(follower NodeID, index uint64, u *Update) {
	if !l.ackedRead.update(follower, index) {
		return
	}
	acked := l.ackedRead.commited()
	if acked > l.acked {
		l.logger.Debugw("update acked read index", "prev", l.acked, "next", acked)
		l.acked = acked
		l.completePendingReads(u)
	}
}

func (l *leader) onAppendEntriesResponse(m *AppendEntriesResponse, u *Update) role {
	l.checkQuorum[m.Follower] = struct{}{}
	if !m.Success {
		// peer replication channel will take care of conflicts
		return nil
	}

	l.updateReadIndex(m.Follower, m.ReadIndex, u)
	if !l.matchIndex.update(m.Follower, m.LastLog.Index) {
		return nil
	}
	if m.LastLog.Index <= l.commitIndex {
		// oudated server is catching up
		return nil
	}
	idx := l.matchIndex.commited()
	if idx == 0 {
		return nil
	}
	l.logger.Debugw("ready to update commit idx", "index", idx, "set", l.matchIndex.index)
	// FIXME we are storing index starting at 0. but in the state machine
	// first valid index starts with 1.
	entry, err := l.log.Get(idx)
	l.must(err, "failed to get entry")
	if entry.Term != l.Term {
		return nil
	}
	l.commit(u, idx)
	l.commitInflight(u, idx)
	return nil
}

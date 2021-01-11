package raft

import (
	"container/list"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/dshulyak/raft/types"
	"github.com/dshulyak/raftlog"
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
	Message               = types.Message
	RequestVote           = types.RequestVote
	RequestVoteResponse   = types.RequestVoteResponse
	AppendEntries         = types.AppendEntries
	AppendEntriesResponse = types.AppendEntriesResponse
	Proposal              = types.Proposal
)

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
	Proposals   []*Proposal
	LeaderState int
	State       RaftState
	Commit      uint64
}

type state struct {
	logger *zap.SugaredLogger

	rng *rand.Rand

	minElection, maxElection int
	election                 int

	id     NodeID
	leader NodeID

	nodes         map[NodeID]*Node
	configuration *Configuration

	*DurableState
	log *raftlog.Storage

	commitIndex uint64
}

func (s *state) majority() int {
	return len(s.configuration.Nodes)/2 + 1
}

func (s *state) must(err error, msg string) {
	if err != nil {
		s.logger.Panicw(msg, "error", err)
	}
}

func (s *state) commit(u *Update, commited uint64) {
	if commited <= s.commitIndex {
		return
	}
	s.logger.Debugw("entry commited", "index", commited, "term", s.Term)
	s.commitIndex = commited
	u.Commit = commited
	u.Updated = true
}

func (s *state) cmpLogs(term, index uint64) int {
	if s.log.IsEmpty() {
		if term == 0 && index == 0 {
			return 0
		}
		return -1
	}
	last, err := s.log.Last()
	s.must(err, "failed fetching last log")
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

func (s *state) send(u *Update, msg interface{}, to ...NodeID) {
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

func (s *state) resetTicks() {
	s.election = s.rng.Intn(s.maxElection-s.minElection) + s.minElection
}

type role interface {
	tick(int, *Update) role
	next(interface{}, *Update) role
}

func newStateMachine(logger *zap.Logger,
	id NodeID,
	minTicks, maxTicks int,
	conf *Configuration,
	log *raftlog.Storage,
	ds *DurableState,
) *stateMachine {
	update := &Update{}
	nodes := map[NodeID]*Node{}
	for i := range conf.Nodes {
		node := &conf.Nodes[i]
		nodes[node.ID] = node
	}
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &stateMachine{
		update: update,
		role: toFollower(&state{
			rng:           rng,
			DurableState:  ds,
			logger:        logger.With(zap.Uint64("ID", uint64(id))).Sugar(),
			minElection:   minTicks,
			maxElection:   maxTicks,
			id:            id,
			nodes:         nodes,
			configuration: conf,
			log:           log,
		}, 0, update),
	}
}

type stateMachine struct {
	role   role
	update *Update
}

func (s *stateMachine) Tick(n int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrUnexpected, r)
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
			err = fmt.Errorf("%w: %v", ErrUnexpected, r)
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

func toFollower(s *state, term uint64, u *Update) *follower {
	s.resetTicks()
	s.leader = None
	if term > s.Term {
		s.Term = term
		s.VotedFor = None
		s.must(s.Sync(), "failed to sync durable state")
	}
	u.State = RaftFollower
	u.LeaderState = LeaderUnknown
	u.Updated = true
	return &follower{state: s}
}

type follower struct {
	*state
}

func (f *follower) tick(n int, u *Update) role {
	f.election -= n
	if f.election <= 0 {
		f.logger.Debugw("election timeout elapsed. transitioning to candidate",
			"id", f.id,
		)
		return toCandidate(f.state, u)
	}
	return nil
}

func (f *follower) next(msg interface{}, u *Update) role {
	switch m := msg.(type) {
	case *RequestVote:
		return f.onRequestVote(m, u)
	case *AppendEntries:
		return f.onAppendEntries(m, u)
	case []*Proposal:
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
		f.must(f.Sync(), "failed to sync durable state")
	}
	if f.leader != msg.Leader {
		f.leader = msg.Leader
		u.LeaderState = LeaderKnown
		u.Updated = true
	}
	f.resetTicks()
	f.logger.Debugw("append entries",
		"leader", msg.Leader,
		"count", len(msg.Entries),
		"replica", f.id,
		"prev log term", msg.PrevLog.Term,
		"prev log index", msg.PrevLog.Index,
		"heartbeat", len(msg.Entries) == 0,
	)
	empty := f.log.IsEmpty()
	if empty && msg.PrevLog.Index > 0 {
		f.send(u, &AppendEntriesResponse{
			Term:     f.Term,
			Follower: f.id,
		}, msg.Leader)
		return nil
	}
	if !empty && msg.PrevLog.Index == 0 {
		f.logger.Debugw("cleaning local log")
		f.must(f.log.DeleteFrom(0), "failed to delete a log")
	} else if !empty {
		entry, err := f.log.Get(int(msg.PrevLog.Index) - 1)
		if errors.Is(err, raftlog.ErrEntryNotFound) {
			f.send(u, &AppendEntriesResponse{
				Term:     f.Term,
				Follower: f.id,
			}, msg.Leader)
			return nil
		} else if err != nil {
			f.must(err, "failed to get log entry")
		}
		if entry.Term != msg.PrevLog.Term {
			f.logger.Debugw("deleting log file",
				"at index", msg.PrevLog.Index,
				"local prev term", entry.Term,
				"new term", msg.PrevLog.Term,
			)
			f.must(f.log.DeleteFrom(int(msg.PrevLog.Index)-1), "failed to delete a log")
			f.send(u, &AppendEntriesResponse{
				Term:     f.Term,
				Follower: f.id,
			}, msg.Leader)
			return nil
		}
	}

	var last *raftlog.LogEntry
	for i := range msg.Entries {
		last = msg.Entries[i]
		f.must(f.log.Append(last), "failed to append log")
	}
	if len(msg.Entries) == 0 {
		entry, err := f.log.Last()
		f.must(err, "error loading last log entry")
		last = &entry
	} else {
		f.logger.Debugw("last appended entry",
			"index", last.Index,
			"term", last.Term,
		)
		f.must(f.log.Sync(), "failed to persist the log on disk")
		f.commit(u, min(msg.Commited, last.Index))
	}
	resp := &AppendEntriesResponse{
		Term:     f.Term,
		Follower: f.id,
		Success:  true,
	}
	resp.LastLog.Index = last.Index
	resp.LastLog.Term = last.Term
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
	var grant bool
	if msg.Term < f.Term {
		grant = false
	} else if msg.Term == f.Term && f.VotedFor != None {
		// this is kind of an optimization to allow faster recovery
		// if candidate crashed (connection timed out) before persisting new term.
		// can be removed to simplify things a bit.
		grant = f.VotedFor == msg.Candidate
	} else {
		grant = f.cmpLogs(msg.LastLog.Term, msg.LastLog.Index) <= 0
		if grant {
			f.VotedFor = msg.Candidate
			f.Term = msg.Term
			f.must(f.Sync(), "failed to sync durable state")
		}
	}
	context := "vote is not granted"
	if grant {
		context = "granted a vote"
	}
	f.logger.Debugw(context,
		"term", msg.Term,
		"candidate", msg.Candidate,
		"voter", f.id,
		"last log index", msg.LastLog.Index,
		"last log term", msg.LastLog.Term,
	)
	f.send(u, &RequestVoteResponse{
		Term:        f.Term,
		Voter:       f.id,
		VoteGranted: grant,
	}, msg.Candidate)
	return nil
}

func toCandidate(s *state, u *Update) *candidate {
	s.resetTicks()
	s.Term++
	s.VotedFor = s.id
	s.must(s.Sync(), "failed to sync durable state")

	c := candidate{state: s, votes: map[NodeID]struct{}{
		s.id: {},
	}}

	last, err := s.log.Last()
	if errors.Is(err, raftlog.ErrEmptyLog) {
		c.logger.Debugw("log is empty")
	} else if err != nil {
		s.must(err, "failed to fetch last log entry")
	}

	request := &RequestVote{
		Term:      s.Term,
		Candidate: s.id,
	}
	request.LastLog.Term = last.Term
	request.LastLog.Index = last.Index
	s.logger.Debugw("starting an election campaign", "candidate", s.id, "term", s.Term)
	s.send(u, request)
	u.State = RaftCandidate
	u.Updated = true
	return &c
}

type candidate struct {
	*state
	votes map[NodeID]struct{}
}

func (c *candidate) tick(n int, u *Update) role {
	c.election -= n
	if c.election <= 0 {
		c.logger.Debugw("election timeout elapsed. transitioning to candidate",
			"id", c.id,
		)
		return toCandidate(c.state, u)
	}
	return nil
}

func (c *candidate) next(msg interface{}, u *Update) role {
	switch m := msg.(type) {
	case *RequestVote:
		if m.Term > c.Term {
			return toFollower(c.state, m.Term, u)
		}
		c.send(u, &RequestVoteResponse{
			Voter: c.id,
			Term:  c.Term,
		}, m.Candidate)
	case *AppendEntries:
		// leader might have been elected in the same term as the candidate
		if m.Term >= c.Term {
			return toFollower(c.state, m.Term, u)
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
			return toFollower(c.state, m.Term, u)
		}
		if !m.VoteGranted {
			return nil
		}
		c.logger.Debugw("received a vote", "voter", m.Voter, "candidate", c.id)
		c.votes[m.Voter] = struct{}{}
		if len(c.votes) < c.majority() {
			return nil
		}
		return toLeader(c.state, u)
	case []*Proposal:
		c.logger.Panicw("proposals can't be sent while leader is not elected")
	}
	return nil
}

func toLeader(s *state, u *Update) *leader {
	s.logger.Debugw("leader is elected", "id", s.id, "term", s.Term)
	l := leader{
		state:              s,
		matchIndex:         map[NodeID]uint64{},
		checkQuorumTimeout: s.minElection,
		checkQuorum:        map[NodeID]struct{}{},
		inflight:           list.New()}
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
	l.sendProposals(u, &Proposal{Entry: &raftlog.LogEntry{
		OpType: raftlog.LogNoop,
	}})
	u.State = RaftLeader
	u.LeaderState = LeaderKnown
	u.Updated = true
	return &l
}

type leader struct {
	*state
	nextLogIndex uint64
	prevLog      LogHeader
	matchIndex   map[NodeID]uint64

	checkQuorumTimeout int
	checkQuorum        map[NodeID]struct{}

	inflight *list.List
}

func (l *leader) sendProposals(u *Update, proposals ...*Proposal) {
	msg := &AppendEntries{
		Term:     l.Term,
		Leader:   l.id,
		Commited: l.commitIndex,
		Entries:  make([]*raftlog.LogEntry, len(proposals)),
	}
	msg.PrevLog.Index = l.prevLog.Index
	msg.PrevLog.Term = l.prevLog.Term
	for i := range msg.Entries {
		msg.Entries[i] = proposals[i].Entry
		msg.Entries[i].Index = l.nextLogIndex
		msg.Entries[i].Term = l.Term
		l.nextLogIndex++
		l.logger.Debugw("append entry on a leader and send proposal",
			"index", msg.Entries[i].Index, "term", msg.Entries[i].Term)
		l.must(l.log.Append(msg.Entries[i]), "failed to append a record")
		_ = l.inflight.PushBack(proposals[i])
	}
	if len(msg.Entries) > 0 {
		l.prevLog.Index = msg.Entries[len(msg.Entries)-1].Index
		l.prevLog.Term = l.Term
	}
	l.send(u, msg)
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
			return l.stepdown(0, u)
		}
		for id := range l.checkQuorum {
			delete(l.checkQuorum, id)
		}
		l.checkQuorumTimeout = l.minElection
	}
	return nil
}

func (l *leader) completeProposals(err error) {
	for front := l.inflight.Front(); front != nil; front = front.Next() {
		proposal := front.Value.(*Proposal)
		proposal.Complete(err)
	}
}

func (l *leader) stepdown(term uint64, u *Update) *follower {
	l.completeProposals(ErrLeaderStepdown)
	return toFollower(l.state, term, u)
}

func (l *leader) commitInflight(u *Update, idx uint64) {
	l.logger.Debugw("commiting proposals on a leader",
		"commit", idx, "proposals count", l.inflight.Len())
	for front := l.inflight.Front(); front != nil; {
		proposal := front.Value.(*Proposal)
		if proposal.Entry.Index > idx {
			return
		} else {
			u.Updated = true
			u.Proposals = append(u.Proposals, proposal)
			prev := front
			front = front.Next()
			l.inflight.Remove(prev)
		}
	}
}

func (l *leader) next(msg interface{}, u *Update) role {
	switch m := msg.(type) {
	case *RequestVote:
		if m.Term > l.Term {
			return l.stepdown(m.Term, u)
		}
		l.send(u, &RequestVoteResponse{
			Voter: l.id,
			Term:  l.Term,
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
		}
		if m.Term > l.Term {
			return l.stepdown(m.Term, u)
		}
		return l.onAppendEntriesResponse(m, u)
	case *Proposal:
		l.sendProposals(u, m)
	case []*Proposal:
		l.sendProposals(u, m...)
	}
	return nil
}

func (l *leader) onAppendEntriesResponse(m *AppendEntriesResponse, u *Update) role {
	l.checkQuorum[m.Follower] = struct{}{}
	if !m.Success {
		// peer replication channel will take care of conflicts
		return nil
	}
	l.logger.Debugw("leader received response", "msg", m)
	current := l.matchIndex[m.Follower]
	if current >= m.LastLog.Index {
		return nil
	}
	l.matchIndex[m.Follower] = m.LastLog.Index
	if m.LastLog.Index <= l.commitIndex {
		// oudated server is catching up
		return nil
	}
	// we received an actual update. time to check if we can commit new entries
	indexes := make([]uint64, len(l.configuration.Nodes)-1)
	i := 0
	for _, index := range l.matchIndex {
		indexes[i] = index
		i++
	}
	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i] < indexes[j]
	})
	// leader is excluded from the matchIndex slice. so the size is always N-1
	idx := indexes[l.majority()-1]
	if idx == 0 {
		return nil
	}
	l.logger.Debugw("ready to update commit idx", "index", idx)
	// FIXME we are storing index starting at 0. but in the state machine
	// first valid index starts with 1.
	entry, err := l.log.Get(int(idx) - 1)
	l.must(err, "failed to get entry")
	if entry.Term != l.Term {
		return nil
	}
	l.must(l.log.Sync(), "failed to sync the log")
	l.commit(u, idx)
	l.commitInflight(u, idx)
	return nil
}

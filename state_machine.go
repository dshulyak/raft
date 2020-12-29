package raft

import (
	"container/list"
	"context"
	"errors"
	"math/rand"
	"sort"

	"github.com/dshulyak/raftlog"
	"go.uber.org/zap"
)

var (
	ErrLeaderStepdown = errors.New("leader stepdown")
)

type NodeID uint64

var None NodeID

type RequestVote struct {
	Term      uint64
	Candidate NodeID
	LastLog   struct {
		Term  uint64
		Index uint64
	}
}

type RequestVoteResponse struct {
	Term        uint64
	Voter       NodeID
	VoteGranted bool
}

type AppendEntries struct {
	Term    uint64
	Leader  NodeID
	PrevLog struct {
		Term  uint64
		Index uint64
	}
	Commited uint64
	Entries  []*raftlog.LogEntry
}

type AppendEntriesResponse struct {
	Term     uint64
	Follower NodeID
	Success  bool
	LastLog  struct {
		Index uint64
		Term  uint64
	}
}

type Proposal struct {
	ctx    context.Context
	cancel func()
	result chan error
	Entry  *raftlog.LogEntry
}

func (p *Proposal) Complete(err error) {
	if p.ctx == nil {
		return
	}
	select {
	case <-p.ctx.Done():
	case p.result <- err:
	}
}

func (p *Proposal) Wait() error {
	if p.ctx == nil {
		panic("not waitable proposal")
	}
	select {
	case <-p.ctx.Done():
		return context.Canceled
	case err := <-p.result:
		return err
	}
}

func (p *Proposal) Cancel() {
	if p.ctx == nil {
		panic("not waitable proposal")
	}
	p.cancel()
}

type MessageTo struct {
	To      NodeID
	Message interface{}
}

type Update struct {
	Updated   bool
	Msgs      []MessageTo
	Proposals []*Proposal
	CommitLog struct {
		Index, Term uint64
	}
}

type state struct {
	logger *zap.SugaredLogger

	minTicks, maxTicks int
	ticks              int

	id     NodeID
	leader NodeID

	configuration *Configuration

	// FIXME term and votedFor must be durable
	term     uint64
	votedFor NodeID
	log      *raftlog.Storage

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
	s.logger.Debugw("entry commited", "index", commited, "term", s.term)
	s.commitIndex = commited
	u.CommitLog.Index = commited
	u.CommitLog.Term = s.term
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
	// TODO allow to set custom rand function?
	s.ticks = rand.Intn(s.minTicks+s.maxTicks) - s.minTicks
}

type role interface {
	tick(int, *Update) role
	next(interface{}, *Update) role
}

type StateMachineConfig struct {
	ID                 NodeID
	Configuration      *Configuration
	MinTicks, MaxTicks int
}

func NewStateMachine(logger *zap.Logger, config StateMachineConfig, log *raftlog.Storage) *StateMachine {
	return &StateMachine{
		update: &Update{},
		role: toFollower(&state{
			logger:        logger.Sugar(),
			minTicks:      config.MinTicks,
			maxTicks:      config.MaxTicks,
			id:            config.ID,
			configuration: config.Configuration,
			log:           log,
		}, 0),
	}
}

type StateMachine struct {
	role   role
	update *Update
}

func (s *StateMachine) Tick(n int) *Update {
	r := s.role.tick(n, s.update)
	if r != nil {
		s.role = r
	}
	if s.update.Updated {
		u := s.update
		s.update = &Update{}
		return u
	}
	return nil
}

func (s *StateMachine) Next(msg interface{}) *Update {
	r := s.role.next(msg, s.update)
	for r != nil {
		s.role = r
		// technically it is enough to iterate only once.
		// it is never the case when that the role will be changed
		// immediatly after the previous change
		r = s.role.next(msg, s.update)
	}
	if s.update.Updated {
		u := s.update
		s.update = &Update{}
		return u
	}
	return nil
}

func toFollower(s *state, term uint64) *follower {
	// NOTE not sure if it can be harmful.
	// spec doesn't specify that election timeout must be reset
	// when candidate or leader transitions to follower because newer term
	// was observed.
	s.resetTicks()
	if term > s.term {
		s.term = term
		s.votedFor = None
	}
	return &follower{state: s}
}

type follower struct {
	*state
}

func (f *follower) tick(n int, u *Update) role {
	f.ticks -= n
	if f.ticks <= 0 {
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
	}
	return nil
}

func (f *follower) onAppendEntries(msg *AppendEntries, u *Update) role {
	if f.term > msg.Term {
		f.send(u, &AppendEntriesResponse{
			Term:     f.term,
			Follower: f.id,
		}, msg.Leader)
		return nil
	} else if f.term < msg.Term {
		f.term = msg.Term
		f.votedFor = None
	}
	f.logger.Debugw("append entries",
		"leader", msg.Leader,
		"count", len(msg.Entries),
		"replica", f.id,
	)
	f.resetTicks()
	if !(f.log.IsEmpty() && msg.PrevLog.Term == 0) {
		entry, err := f.log.Get(int(msg.PrevLog.Index) - 1)
		if errors.Is(err, raftlog.ErrEntryNotFound) {
			f.send(u, &AppendEntriesResponse{
				Term:     f.term,
				Follower: f.id,
			}, msg.Leader)
			return nil
		} else if err != nil {
			f.must(err, "failed log get")
		}
		if entry.Term != msg.PrevLog.Term {
			f.logger.Debugw("deleting log file", "starting at index", msg.PrevLog.Index)
			f.must(f.log.DeleteFrom(int(msg.PrevLog.Index)), "failed log deletion")
			f.send(u, &AppendEntriesResponse{
				Term:     f.term,
				Follower: f.id,
			}, msg.Leader)
			return nil
		}
	}
	if len(msg.Entries) == 0 {
		f.logger.Debugw("received heartbeat", "from", msg.Leader)
		f.send(u, &AppendEntriesResponse{
			Term:     f.term,
			Follower: f.id,
			Success:  true,
		}, msg.Leader)
		return nil
	}

	var last *raftlog.LogEntry
	for i := range msg.Entries {
		last = msg.Entries[i]
		f.must(f.log.Append(last), "failed to append log")
	}
	// TODO as an optimization we don't need to fsync in state machine
	// we only need to fsync before replying to the leader or before sending
	// logs to the Application
	// fsync as late as possible will allow to batch multiple writes into
	// single write syscall, followed by a single fsync
	f.must(f.log.Sync(), "failed to persist the log on disk")
	f.commit(u, min(msg.Commited, last.Index))
	resp := &AppendEntriesResponse{
		Term:     f.term,
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
	if msg.Term < f.term {
		grant = false
	} else if msg.Term == f.term {
		// this is kind of an optimization to allow faster recovery
		// if candidate crashed (connection timed out) before persisting new term.
		// can be removed to simplify things a bit.
		grant = f.votedFor == msg.Candidate
	} else {
		grant = f.cmpLogs(msg.LastLog.Term, msg.LastLog.Index) <= 0
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
	f.votedFor = msg.Candidate
	f.term = msg.Term
	f.send(u, &RequestVoteResponse{
		Term:        f.term,
		Voter:       f.id,
		VoteGranted: grant,
	}, msg.Candidate)
	return nil
}

func toCandidate(s *state, u *Update) *candidate {
	s.resetTicks()
	s.term++
	s.votedFor = s.id

	c := candidate{state: s, votes: map[NodeID]struct{}{
		s.id: struct{}{},
	}}

	last, err := s.log.Last()
	if errors.Is(err, raftlog.ErrEmptyLog) {
		c.logger.Debugw("log is empty")
	} else if err != nil {
		s.must(err, "failed to fetch last log entry")
	}

	request := &RequestVote{
		Term:      s.term,
		Candidate: s.id,
	}
	request.LastLog.Term = last.Term
	request.LastLog.Index = last.Index
	s.logger.Debugw("starting an election campaign", "candidate", s.id, "term", s.term)
	s.send(u, request)
	return &c
}

type candidate struct {
	*state
	votes map[NodeID]struct{}
}

func (c *candidate) tick(n int, u *Update) role {
	c.ticks -= n
	if c.ticks <= 0 {
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
		if m.Term > c.term {
			return toFollower(c.state, m.Term)
		}
		c.send(u, &RequestVoteResponse{
			Voter: c.id,
			Term:  c.term,
		}, m.Candidate)
	case *AppendEntries:
		// leader might have been elected in the same term as this candidate
		if m.Term >= c.term {
			return toFollower(c.state, m.Term)
		}
		c.send(u, &AppendEntriesResponse{
			Follower: c.id,
			Term:     c.term,
		}, m.Leader)
	case *RequestVoteResponse:
		if m.Term > c.term {
			return toFollower(c.state, m.Term)
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
	}
	return nil
}

func toLeader(s *state, u *Update) *leader {
	s.logger.Debugw("leader is elected", "id", s.id, "term", s.term)
	l := leader{state: s, matchIndex: map[NodeID]uint64{}, inflight: list.New()}
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
	return &l

}

type leader struct {
	*state
	nextLogIndex uint64
	prevLog      struct {
		Index uint64
		Term  uint64
	}
	matchIndex map[NodeID]uint64
	inflight   *list.List
}

func (l *leader) sendProposals(u *Update, proposals ...*Proposal) {
	msg := &AppendEntries{
		Term:     l.term,
		Leader:   l.id,
		Commited: l.commitIndex,
		Entries:  make([]*raftlog.LogEntry, len(proposals)),
	}
	msg.PrevLog.Index = l.prevLog.Index
	msg.PrevLog.Term = l.prevLog.Term
	for i := range msg.Entries {
		msg.Entries[i] = proposals[i].Entry
		msg.Entries[i].Index = l.nextLogIndex
		msg.Entries[i].Term = l.term
		l.nextLogIndex++
		l.must(l.log.Append(msg.Entries[i]), "failed to append a record")
		_ = l.inflight.PushBack(proposals[i])
	}
	if len(msg.Entries) > 0 {
		l.prevLog.Index = msg.Entries[len(msg.Entries)-1].Index
		l.prevLog.Term = l.term
	}
	l.send(u, msg)
}

func (l *leader) tick(n int, u *Update) role {
	// NOTE state machine is not responsible for sending heartbeats.
	// each peer will have a separate goroutine that will send heartbeats in
	// the idle periods.
	// technically it doesn't affect the protocol as long as heartbeats timeout
	// is lower than the electiom timeout
	return nil
}

func (l *leader) stepdown(term uint64) *follower {
	for front := l.inflight.Front(); front != nil; front = front.Next() {
		proposal := front.Value.(*Proposal)
		proposal.Complete(ErrLeaderStepdown)
	}
	return toFollower(l.state, term)
}

func (l *leader) commitInflight(u *Update, idx uint64) {
	for front := l.inflight.Front(); front != nil; front = front.Next() {
		proposal := front.Value.(*Proposal)
		if proposal.Entry.Index > idx {
			break
		} else {
			u.Updated = true
			u.Proposals = append(u.Proposals, proposal)
			l.inflight.Remove(front)
		}
	}
}

func (l *leader) next(msg interface{}, u *Update) role {
	switch m := msg.(type) {
	case *RequestVote:
		if m.Term > l.term {
			return l.stepdown(m.Term)
		}
		l.send(u, &RequestVoteResponse{
			Voter: l.id,
			Term:  l.term,
		}, m.Candidate)
	case *AppendEntries:
		if m.Term > l.term {
			return l.stepdown(m.Term)
		}
		l.send(u, &AppendEntriesResponse{
			Follower: l.id,
			Term:     l.term,
		}, m.Leader)
	case *RequestVoteResponse:
		if m.Term > l.term {
			return l.stepdown(m.Term)
		}
	case *AppendEntriesResponse:
		if m.Term > l.term {
			return l.stepdown(m.Term)
		}
		return l.onAppendEntriesResponse(m, u)
	case *Proposal:
		l.sendProposals(u, m)
	}
	return nil
}

func (l *leader) onAppendEntriesResponse(m *AppendEntriesResponse, u *Update) role {
	if !m.Success {
		// peer replication component will take care of conflicts
		return nil
	}
	if m.LastLog.Term == 0 {
		// do nothing. this a response for the heartbeat
		return nil
	}
	current := l.matchIndex[m.Follower]
	if current >= m.LastLog.Index {
		return nil
	}
	l.matchIndex[m.Follower] = m.LastLog.Index
	if m.LastLog.Index <= l.commitIndex {
		// oudated is catching up
		return nil
	}
	// we received an actual update. time to **maybe** commit new entries
	indexes := make([]uint64, len(l.configuration.Nodes)-1)
	i := 0
	for _, index := range l.matchIndex {
		indexes[i] = index
		i++
	}
	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i] < indexes[j]
	})
	// leader is excluded from the matchIndex slice.
	// so the size is always N-1
	idx := indexes[l.majority()-1]
	entry, err := l.log.Get(int(idx) - 1)
	l.must(err, "failed to get entry")
	if entry.Term != l.term {
		return nil
	}
	l.commit(u, idx)
	l.commitInflight(u, idx)
	return nil
}

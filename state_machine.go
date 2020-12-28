package raft

import (
	"errors"
	"math/rand"

	"github.com/dshulyak/raftlog"
	"go.uber.org/zap"
)

type NodeID uint64

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
	Entries  []raftlog.LogEntry
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

const (
	ActionTypeSend = iota + 1
	ActionTypeCommit
	ActionTypeSyncStart
	ActionTypeSyncStop
)

type Action struct {
	Type   int
	Action interface{}
}

type ActionSend struct {
	To      NodeID
	Message interface{}
}

type ActionSyncStart struct {
	To NodeID
}

type state struct {
	logger *zap.SugaredLogger

	minTicks, maxTicks int
	ticks              int

	id NodeID

	configuration *Configuration

	// FIXME term and votedFor must be durable
	// add mapped file with 16 bytes
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

func (s *state) commit(actions []Action, commited uint64) []Action {
	if commited <= s.commitIndex {
		s.commitIndex = commited
		return nil
	}
	s.logger.Debugw("entry commited", "index", commited)
	s.commitIndex = commited
	return append(actions, Action{Type: ActionTypeCommit, Action: s.commitIndex})
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

func (s *state) send(actions []Action, msg interface{}, to ...NodeID) []Action {
	if len(to) == 0 {
		for i := range s.configuration.Nodes {
			id := s.configuration.Nodes[i].ID
			if id == s.id {
				continue
			}
			actions = append(actions, Action{Type: ActionTypeSend, Action: ActionSend{
				To:      id,
				Message: msg,
			}})
		}
	}
	for _, id := range to {
		actions = append(actions, Action{Type: ActionTypeSend, Action: ActionSend{
			To:      id,
			Message: msg,
		}})
	}
	return actions
}

func (s *state) resetTicks() {
	// TODO allow to set custom rand fucntion ?
	s.ticks = rand.Intn(s.minTicks+s.maxTicks) - s.minTicks
}

type role interface {
	tick(int) (role, []Action)
	next(interface{}) (role, []Action)
}

type StateMachine struct {
	role role
}

func (s *StateMachine) Tick(n int) []Action {
	update, actions := s.role.tick(n)
	if update != nil {
		s.role = update
	}
	return actions
}

func (s *StateMachine) Next(msg interface{}) []Action {
	update, actions := s.role.next(msg)
	for update != nil {
		s.role = update
		// technically it is enough to iterate only once.
		// it is never the case when that the role will be changed
		// immediatly after the previous change
		update1, actions1 := s.role.next(msg)
		actions = append(actions, actions1...)
		update = update1
	}
	return actions
}

type follower struct {
	*state
}

func (f *follower) tick(n int) (role, []Action) {
	f.ticks -= n
	if f.ticks <= 0 {
		f.logger.Debugw("election timeout elapsed. transitioning to candidate",
			"id", f.id,
		)
		return toCandidate(f.state)
	}
	return nil, nil
}

func (f *follower) next(msg interface{}) (role, []Action) {
	switch m := msg.(type) {
	case *RequestVote:
		return f.onRequestVote(m)
	case *AppendEntries:
		return f.onAppendEntries(m)
	}
	return nil, nil
}

func (f *follower) onAppendEntries(msg *AppendEntries) (role, []Action) {
	if f.term > msg.Term {
		return nil, f.send(nil, &AppendEntriesResponse{
			Term:     f.term,
			Follower: f.id,
		}, msg.Leader)
	} else if f.term < msg.Term {
		f.term = msg.Term
		f.votedFor = 0
	}
	f.resetTicks()
	entry, err := f.log.Get(int(msg.PrevLog.Index))
	if errors.Is(err, raftlog.ErrEntryNotFound) {
		return nil, f.send(nil, &AppendEntriesResponse{
			Term:     f.term,
			Follower: f.id,
		})
	} else if err != nil {
		f.must(err, "failed log get")
	}
	if entry.Term != msg.PrevLog.Term {
		f.logger.Debugw("deleting log file", "starting at index", msg.PrevLog.Index)
		f.must(f.log.DeleteFrom(int(msg.PrevLog.Index)), "failed log deletion")
		return nil, f.send(nil, &AppendEntriesResponse{
			Term:     f.term,
			Follower: f.id,
		})
	}
	if len(msg.Entries) == 0 {
		f.logger.Debugw("received heartbeat", "from", msg.Leader)
		// NOTE response is expected due to reads optimizations bypassing
		// the log. but checking that leader wasn't replaced by looking
		// at response timing
		return nil, f.send(nil, &AppendEntriesResponse{
			Term:     f.term,
			Follower: f.id,
			Success:  true,
		}, msg.Leader)
	}

	f.logger.Debugw("appending entries",
		"from", msg.Leader,
		"count", len(msg.Entries),
	)
	var last *raftlog.LogEntry
	for i := range msg.Entries {
		last = &msg.Entries[i]
		f.must(f.log.Append(last), "failed to append log")
	}
	// TODO as an optimization we don't need to fsync in state machine
	// we only need to do it before replying to the leader or before sending
	// logs to the Application
	// fsync as late as possible will allow to batch multiple writes into
	// single write syscall, followed by a single fsync
	f.must(f.log.Sync(), "failed to persist the log on disk")
	actions := f.commit(nil, min(msg.Commited, last.Index))
	resp := &AppendEntriesResponse{
		Term:     f.term,
		Follower: f.id,
		Success:  true,
	}
	resp.LastLog.Index = last.Index
	resp.LastLog.Term = last.Term
	return nil, f.send(actions, resp, msg.Leader)
}

func min(i, j uint64) uint64 {
	if i > j {
		return j
	}
	return i
}

func (f *follower) onRequestVote(msg *RequestVote) (role, []Action) {
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
	if grant {
		f.logger.Debugw("granted a vote",
			"term", msg.Term,
			"candidate", msg.Candidate,
			"voter", f.id,
			"last log index", msg.LastLog.Index,
			"last log term", msg.LastLog.Term,
		)
		f.votedFor = msg.Candidate
		f.term = msg.Term
	}
	return nil, f.send(nil, &RequestVoteResponse{
		Term:        f.term,
		Voter:       f.id,
		VoteGranted: grant,
	}, msg.Candidate)
}

func toFollower(s *state) (*follower, []Action) {
	// NOTE not sure if it can be harmful.
	// spec doesn't specify that election timeout must be reset
	// when candidate or leader transition to follower because newer term
	// was observed.
	s.resetTicks()
	return &follower{state: s}, nil
}

func toCandidate(s *state) (*candidate, []Action) {
	s.resetTicks()
	s.term++
	s.votedFor = s.id
	c := candidate{state: s, votes: map[NodeID]struct{}{}}
	actions := make([]Action, 0, len(s.configuration.Nodes)-1)
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
	s.logger.Debugw("started an election", "candidate", s.id, "term", s.term)
	return &c, s.send(actions, request)
}

type candidate struct {
	*state
	votes map[NodeID]struct{}
}

func (c *candidate) tick(n int) (role, []Action) {
	return nil, nil
}

func (c *candidate) next(msg interface{}) (role, []Action) {
	switch m := msg.(type) {
	case *RequestVote:
		if m.Term > c.term {
			return toFollower(c.state)
		}
		return nil, c.send(nil, &RequestVoteResponse{
			Voter: c.id,
			Term:  c.term,
		}, m.Candidate)
	case *AppendEntries:
		// leader might be elected in the same term as candidate
		if m.Term >= c.term {
			return toFollower(c.state)
		}
		return nil, c.send(nil, &AppendEntriesResponse{
			Follower: c.id,
			Term:     c.term,
		}, m.Leader)
	case *RequestVoteResponse:
		if m.Term > c.term {
			c.term = m.Term
			c.votedFor = 0
			return toFollower(c.state)
		}
		if !m.VoteGranted {
			return nil, nil
		}
		c.votes[m.Voter] = struct{}{}
		if len(c.votes) < c.majority() {
			return nil, nil
		}
		return toLeader(c.state)
	}
	return nil, nil
}

func toLeader(s *state) (*leader, []Action) {
	s.logger.Debugw("leader is elected", "id", s.id, "term", s.term)
	l := leader{state: s, matchIndex: map[NodeID]uint64{}}
	last, err := s.log.Last()
	if errors.Is(err, raftlog.ErrEmptyLog) {
	} else if err != nil {
		s.must(err, "failed fetching last log")
	} else {
		l.nextLogIndex = last.Index + 1
		l.prevLog.Index = last.Index
		l.prevLog.Term = last.Term
	}
	// replicate noop in order to commit entries from previous terms
	return &l, l.sendEntries(nil, raftlog.LogEntry{
		Term:   s.term,
		OpType: raftlog.LogNoop,
	})

}

type leader struct {
	*state
	nextLogIndex uint64
	prevLog      struct {
		Index uint64
		Term  uint64
	}
	matchIndex map[NodeID]uint64
}

func (l *leader) sendEntries(actions []Action, entries ...raftlog.LogEntry) []Action {
	msg := AppendEntries{
		Term:     l.term,
		Leader:   l.id,
		Commited: l.commitIndex,
		Entries:  entries,
	}
	msg.PrevLog.Index = l.prevLog.Index
	msg.PrevLog.Term = l.prevLog.Term
	for i := range entries {
		entries[i].Index = l.nextLogIndex
		l.nextLogIndex++
		l.must(l.log.Append(&entries[i]), "failed to append a record")
	}
	if len(entries) > 0 {
		l.prevLog.Index = entries[len(entries)-1].Index
		l.prevLog.Term = l.term
	}
	return l.send(actions, msg)
}

func (l *leader) tick(n int) (role, []Action) {
	// NOTE state machine is not responsible for sending heartbeats.
	// each peer will have a separate goroutine that will send heartbeats in
	// the idle periods.
	// technically it doesn't affect the protocol as long as heartbeats timeout
	// is lower than the electiom timeout
	return nil, nil
}

func (l *leader) next(msg interface{}) (role, []Action) {
	switch m := msg.(type) {
	case *RequestVote:
		if m.Term > l.term {
			return toFollower(l.state)
		}
		return nil, l.send(nil, &RequestVoteResponse{
			Voter: l.id,
			Term:  l.term,
		}, m.Candidate)
	case *AppendEntries:
		if m.Term > l.term {
			return toFollower(l.state)
		}
		return nil, l.send(nil, &AppendEntriesResponse{
			Follower: l.id,
			Term:     l.term,
		}, m.Leader)
	case *RequestVoteResponse:
		if m.Term > l.term {
			l.term = m.Term
			l.votedFor = 0
			return toFollower(l.state)
		}
		return nil, nil
	case *AppendEntriesResponse:
		if m.Term > l.term {
			l.term = m.Term
			l.votedFor = 0
			return toFollower(l.state)
		}
		return l.onAppendEntriesResponse(m)
	}
	return nil, nil
}

func (l *leader) onAppendEntriesResponse(m *AppendEntriesResponse) (role, []Action) {
	if m.LastLog.Term == 0 {
		// do nothing. this a response for a heartbeat
		return nil, nil
	}
	current := l.matchIndex[m.Follower]
	if current >= m.LastLog.Index {
		return nil, nil
	}
	l.matchIndex[m.Follower] = current
	if current <= l.commitIndex {
		return nil, nil
	}
	n := l.majority()
	for _, match := range l.matchIndex {
		if match >= current {
			n--
			if n == 0 {
				return nil, l.commit(nil, current)
			}
		}
	}
	return nil, nil
}

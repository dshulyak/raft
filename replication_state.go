package raft

import (
	"errors"

	"github.com/dshulyak/raftlog"
	"go.uber.org/zap"
)

func newReplicationState(
	logger *zap.SugaredLogger,
	maxEntries uint64,
	heartbeatTimeout int,
	log *raftlog.Storage) *replicationState {
	peer := &replicationState{
		logger:           logger,
		maxEntries:       maxEntries,
		heartbeat:        heartbeatTimeout,
		heartbeatTimeout: heartbeatTimeout,
		log:              log,
	}
	last := peer.lastLog()
	peer.prevLog.Index = last.Index
	peer.prevLog.Term = last.Term
	peer.sentLog = peer.prevLog
	return peer
}

// replicationState maintans state required for the replication with a single peer.
// the idea is to handle heartbeats and log replication in parallel with the
// main raft state machine.
// for example finding out correct log offset doesn't require serialized access
// to the state maintained by the main raft state machine.
// reading bext batch of entries to replicate doesn't require access to the
// state maintained by raft state machine as well.
//
// AppendEntriesResponse's must be sent to the main state machine anyway
// for updating commited index log and some other features (CheckQuorum)
// but reading from log can be done in parallel, it might be especially
// meaningfull
type replicationState struct {
	logger *zap.SugaredLogger
	id     NodeID

	// if pipeline is true next chunk of entries will be sent without
	// waiting for succesfull response.
	// it will be set to true after first succesfull ApendEntriesResponse
	//
	// it works this way because peer log may differ from leaders log
	// and require changes
	pipeline bool

	maxEntries       uint64
	heartbeatTimeout int
	heartbeat        int

	term           uint64
	commitLogIndex uint64
	readIndex      uint64

	prevLog, sentLog LogHeader
	log              *raftlog.Storage
}

func (p *replicationState) lastLog() raftlog.LogEntry {
	// FIXME make sure that fetching last log doesn't hit the disk
	entry, err := p.log.Last()
	if errors.Is(err, raftlog.ErrEmptyLog) {
		return entry
	}
	if err != nil {
		p.logger.Panicw("failed to get last log", "error", err)
	}
	return entry
}

// tick updates heartbeat timeout.
func (p *replicationState) tick(n int) {
	p.heartbeat -= n
}

// sendHeartbeat and reset heartbeat timer.
func (p *replicationState) sendHeartbeat() *AppendEntries {
	p.heartbeat = p.heartbeatTimeout
	return &AppendEntries{
		Term:     p.term,
		Leader:   p.id,
		PrevLog:  p.sentLog,
		Commited: p.commitLogIndex,
	}
}

// next selects next in-order batch of entries to append or heartbeat if logs are up to date.
func (p *replicationState) next() *AppendEntries {
	last := p.lastLog()
	if p.sentLog.Index >= last.Index {
		if p.heartbeat <= 0 {
			return p.sendHeartbeat()
		}
		return nil
	}
	if p.sentLog.Index > p.prevLog.Index && !p.pipeline {
		return nil
	}
	p.heartbeat = p.heartbeatTimeout

	var (
		d         = min(last.Index-p.sentLog.Index, p.maxEntries)
		entries   = make([]*raftlog.LogEntry, d)
		prevLog   = p.sentLog
		nextIndex = int(p.sentLog.Index) + 1
	)

	for i := 0; i < int(d); i++ {
		entry, err := p.log.Get(nextIndex - 1)
		if err != nil {
			p.logger.Panicw("failed to get entry", "index", nextIndex, "error", err)
		}
		entries[i] = &entry
		nextIndex++
	}

	if d > 0 {
		p.sentLog.Index = entries[d-1].Index
		p.sentLog.Term = entries[d-1].Term
	}
	return &AppendEntries{
		Term:      p.term,
		Leader:    p.id,
		PrevLog:   prevLog,
		Commited:  p.commitLogIndex,
		Entries:   entries,
		ReadIndex: p.readIndex,
	}
}

// onResponse receives AppendEntriesResponse's from the peer and adjusts
// leader view of the replica in order for AppendEntries RPC to succeed eventually.
func (p *replicationState) onResponse(m *AppendEntriesResponse) {
	if m.Term != p.term {
		return
	}
	if m.Success {
		p.pipeline = true
		p.prevLog = m.LastLog
		return
	}
	p.pipeline = false
	p.prevLog.Index -= 1
	if p.prevLog.Index == 0 {
		p.prevLog.Term = 0
	} else {
		// TODO add term to index structures and provide a method
		// to read LogHeader (Index and Term)
		entry, err := p.log.Get(int(p.prevLog.Index) - 1)
		if err != nil {
			p.logger.Panicw("failed to get log", "index", p.prevLog.Index, "error", err)
		}
		p.prevLog.Term = entry.Term
	}
	p.sentLog = p.prevLog
}

func (p *replicationState) init(m *AppendEntries) {
	p.id = m.Leader
	p.term = m.Term
	p.prevLog = m.PrevLog
	p.sentLog = m.PrevLog
	p.commitLogIndex = m.Commited
	p.readIndex = m.ReadIndex
}

// update from message generated by calling next with a message sent by the consensus layer.
// generally consensus will be replicating new entries, and may update commited
// m1 is generated by replicationState on the call to `next`.
// m2 sent by the consensus state machine.
func (p *replicationState) update(m1, m2 *AppendEntries) *AppendEntries {
	if m2.Term != p.term {
		return nil
	}
	p.commitLogIndex = m2.Commited
	p.readIndex = m2.ReadIndex
	if m1 != nil {
		// never modify m1 without copying it first
		return m1
	}
	if len(m2.Entries) > 0 {
		last := m2.Entries[len(m2.Entries)-1]
		p.sentLog.Index = last.Index
		p.sentLog.Term = last.Term
	}
	return m2
}

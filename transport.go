package raft

import (
	"context"
	"sync"

	"github.com/dshulyak/raft/types"
	"go.uber.org/zap"
)

type (
	Transport = types.Transport
	MsgStream = types.MsgStream
)

func newLastMessageSender(
	ctx context.Context,
	logger *zap.SugaredLogger,
	out chan<- Message,
) *lastMessageChannel {
	ctx, cancel := context.WithCancel(ctx)
	sender := &lastMessageChannel{
		ctx:    ctx,
		cancel: cancel,
		logger: logger,
		out:    out,
		in:     make(chan Message, 1),
	}
	go sender.run()
	return sender
}

// lastMessageChannel buffers last message, dropping all previous messages.
type lastMessageChannel struct {
	ctx    context.Context
	cancel func()

	logger *zap.SugaredLogger

	in  chan Message
	out chan<- Message
}

func (s *lastMessageChannel) Send(msg Message) {
	select {
	case <-s.ctx.Done():
		return
	case s.in <- msg:
	}
}

func (s *lastMessageChannel) Close() {
	s.cancel()
}

func (s *lastMessageChannel) run() {
	var (
		msg Message
		out chan<- Message
	)
	for {
		if msg != nil {
			out = s.out
		} else {
			out = nil
		}
		select {
		case <-s.ctx.Done():
			return
		case next := <-s.in:
			if msg != nil {
				// FIXME log that message is dropped
			}
			msg = next
		case out <- msg:
			msg = nil
		}
	}
}

type peerDeliveryChannel interface {
	Send(Message)
	Close()
}

// peerMailbox can be in one of two states:
// - in leader state it will run replication channel
//   that will either restore follower logs or send append entries in pipelined mode
// - in follower and candidate state it will run channel that buffers last sent message
//   and delivers it once the stream is opened/restored
// in both cases peerMailbox is a non-blocking layer before the network
type peerMailbox struct {
	mu      sync.Mutex
	state   RaftState
	mail    chan<- Message
	current peerDeliveryChannel
}

func (m *peerMailbox) Update(global *Context, state RaftState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// mailbox type changes only on transitions:
	// from candidate to leader - 1st type
	// from leader or empty to not leader  - 2nd type
	toLeader := m.state == RaftCandidate && state == RaftLeader
	toNotLeader := (m.state == RaftLeader || m.state == 0) || state != RaftLeader
	if !(toLeader || toNotLeader) {
		return
	}
	if m.current != nil {
		m.current.Close()
	}
	logger := global.Logger.Sugar()
	if toLeader {
		m.current = newReplicationChannel(
			global,
			logger,
			global.TickInterval,
			m.mail,
			newReplicationState(
				logger,
				uint64(global.EntriesPerAppend),
				global.HeartbeatTimeout,
				global.Storage,
			),
		)
		return
	}
	m.current = newLastMessageSender(global, logger, m.mail)
}

func (m *peerMailbox) Response(msg Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == RaftLeader {
		m.current.Send(msg)
	}
	return
}

func (m *peerMailbox) Send(msg Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.current.Send(msg)
}

func (m *peerMailbox) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.current == nil {
		return
	}
	m.current.Close()
}

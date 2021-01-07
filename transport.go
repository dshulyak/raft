package raft

import (
	"context"

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
) *lastMessageSender {
	ctx, cancel := context.WithCancel(ctx)
	sender := &lastMessageSender{
		ctx:    ctx,
		cancel: cancel,
		logger: logger,
		out:    out,
		in:     make(chan Message, 1),
	}
	go sender.run()
	return sender
}

// lastMessageSender buffers last message and ensures that it will be sent over the wire.
type lastMessageSender struct {
	ctx    context.Context
	cancel func()

	logger *zap.SugaredLogger

	in  chan Message
	out chan<- Message
}

func (s *lastMessageSender) Send(msg Message) {
	select {
	case <-s.ctx.Done():
		return
	case s.in <- msg:
	}
}

func (s *lastMessageSender) Close() {
	s.cancel()
}

func (s *lastMessageSender) run() {
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

type peerDelivery interface {
	Send(Message)
	Close()
}

// peerMailbox can be in one of two states:
// - in leader state it will try run peerReactor as a delivery service
// - in follower and candidate state it run delivery service that buffers
//   last sent message
// in both cases peerMailbox is a non-blocking layer before the network
type peerMailbox struct {
	state   RaftState
	mail    chan<- Message
	current peerDelivery
}

func (m *peerMailbox) Update(global *Context, state RaftState) {
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
		m.current = newPeerReactor(
			global,
			logger,
			global.TickInterval,
			m.mail,
			newPeerState(
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

func (m *peerMailbox) Send(msg Message) {
	m.current.Send(msg)
}

func (m *peerMailbox) Close() {
	if m.current == nil {
		return
	}
	m.current.Close()
}

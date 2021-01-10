package raft

import (
	"context"
	"sync"

	"github.com/dshulyak/raft/types"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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
	return &lastMessageChannel{
		ctx:    ctx,
		cancel: cancel,
		logger: logger,
		out:    out,
		in:     make(chan Message, 1),
	}
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

func (s *lastMessageChannel) Run() (err error) {
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
				s.logger.Debugw("message is dropped", "body", msg)
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
	Run() error
}

// peerMailbox can be in one of two states:
// - in leader state it will run replication channel
//   that will either restore follower logs or send append entries in pipelined mode
// - in follower and candidate state it will run channel that buffers last sent message
//   and delivers it once the stream is opened/restored
// in both cases peerMailbox is a non-blocking layer before the network
type peerMailbox struct {
	ctx     context.Context
	group   *errgroup.Group
	mu      sync.Mutex
	state   RaftState
	mail    chan<- Message
	current peerDeliveryChannel
}

func (m *peerMailbox) Update(global *Context, state RaftState) {
	if state == 0 || m.state == state {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	toLeader := m.state == RaftCandidate && state == RaftLeader
	toNotLeader := (m.state == RaftLeader || m.state == 0) || state != RaftLeader
	if !(toLeader || toNotLeader) {
		return
	}
	if m.current != nil {
		m.current.Close()
	}
	logger := global.Logger.Sugar()
	logger.Debugw("update peer mailbox type", "to leader", toLeader, "not leader", toNotLeader, "current", m.state, "next", state)
	m.state = state
	if toLeader {
		m.current = newReplicationChannel(
			m.ctx,
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
		m.group.Go(m.current.Run)
		return
	}
	m.current = newLastMessageSender(m.ctx, logger, m.mail)
	m.group.Go(m.current.Run)
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

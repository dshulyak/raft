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

// lastMessageSender buffers last consensus message and ensures that it will be sent over the wire.
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

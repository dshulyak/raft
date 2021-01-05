package raft

import (
	"context"
	"time"

	"go.uber.org/zap"
)

type Transport interface {
	Dial(context.Context, *Node) (MsgStream, error)
	HandleStream(func(MsgStream))
	Close() error
}

type MsgStream interface {
	ID() NodeID
	Send(context.Context, Message) error
	Receive(context.Context) (Message, error)
	Close() error
}

func newEgressStream(
	ctx context.Context,
	logger *zap.SugaredLogger,
	backoff time.Duration,
	tr Transport,
	node *Node) *egressStream {
	ctx, cancel := context.WithCancel(ctx)
	return &egressStream{
		logger:      logger,
		ctx:         ctx,
		cancel:      cancel,
		backoffTime: backoff,
		tr:          tr,
		node:        node,
		in:          make(chan Message),
	}
}

type egressStream struct {
	logger      *zap.SugaredLogger
	ctx         context.Context
	cancel      func()
	backoffTime time.Duration
	tr          Transport
	node        *Node
	stream      MsgStream
	in          chan Message
}

func (e *egressStream) Close() {
	e.cancel()
}

func (e *egressStream) In() chan<- Message {
	return e.in
}

func (e *egressStream) run() {
	var (
		in      = e.in
		timer   *time.Timer
		timerC  <-chan time.Time
		err     error
		backoff time.Duration
		last    Message
	)
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		if err == nil {
			last = nil
			backoff = 0
		} else {
			timer = time.NewTimer(e.backoffTime << backoff)
			timerC = timer.C
			backoff++
		}

		select {
		case <-e.ctx.Done():
			e.logger.Debugw("egress stream closed", "peer", e.node)
			if e.stream != nil {
				if err := e.stream.Close(); err != nil {
					e.logger.Debugw("error while closing a stream",
						"peer", e.node, "error", err)
				}
			}
			return
		case <-timerC:
			in = e.in
			timerC = nil
		case last = <-in:
		}

		if e.stream == nil {
			err = e.dial()
			if err != nil {
				continue
			}
		}
		err = e.send(last)

	}
}

func (e *egressStream) send(msg Message) error {
	return e.stream.Send(e.ctx, msg)
}

func (e *egressStream) dial() error {
	stream, err := e.tr.Dial(e.ctx, e.node)
	if err != nil {
		return err
	}
	e.stream = stream
	return nil
}

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

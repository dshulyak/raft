package raft

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
)

func newPeerReactor(parent context.Context,
	logger *zap.SugaredLogger,
	tick time.Duration,
	out chan<- Message,
	peer *peerState,
) *peerReactor {
	ctx, cancel := context.WithCancel(parent)
	pr := &peerReactor{
		ctx:    ctx,
		cancel: cancel,
		tick:   tick,
		out:    out,
		in:     make(chan Message, 1),
		peer:   peer,
	}
	// FIXME send error from run to some error handler
	// if it exits with error application must shutdown
	go pr.run()
	return pr
}

type peerReactor struct {
	ctx    context.Context
	cancel func()
	logger *zap.SugaredLogger

	tick time.Duration

	in  chan Message
	out chan<- Message

	peer *peerState
}

func (r *peerReactor) Send(msg Message) {
	select {
	case <-r.ctx.Done():
		return
	case r.in <- msg:
	}
}

func (r *peerReactor) Close() {
	r.cancel()
}

func (r *peerReactor) run() (err error) {
	defer func() {
		// disk IO error will panic
		rec := recover()
		if rec != nil {
			err = fmt.Errorf("%w: %v", ErrUnexpected, rec)
			r.cancel()
		}
	}()
	var (
		timeout     = make(chan int)
		out         chan<- Message
		next        *AppendEntries
		initialized bool
	)
	runTicker(r.ctx, timeout, r.tick)
	for {
		if next == nil {
			out = nil
		} else {
			out = r.out
		}
		select {
		case <-r.ctx.Done():
			return
		case n := <-timeout:
			r.peer.tick(n)
			if next == nil {
				next = r.peer.next()
			}
		case msg := <-r.in:
			switch m := msg.(type) {
			case *AppendEntriesResponse:
				r.peer.onResponse(m)
				next = r.peer.next()
			case *AppendEntries:
				if !initialized {
					r.peer.init(m)
					next = m
				}
				next = r.peer.update(next, m)
			}
		case out <- next:
			next = r.peer.next()
		}
	}
}

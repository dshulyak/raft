package raft

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
)

func newReplicationChannel(parent context.Context,
	logger *zap.SugaredLogger,
	tick time.Duration,
	out chan<- Message,
	peer *replicationState,
) *replicationChannel {
	ctx, cancel := context.WithCancel(parent)
	return &replicationChannel{
		ctx:    ctx,
		cancel: cancel,
		logger: logger,
		tick:   tick,
		out:    out,
		in:     make(chan Message, 1),
		peer:   peer,
	}
}

type replicationChannel struct {
	ctx    context.Context
	cancel func()
	logger *zap.SugaredLogger

	tick time.Duration

	in  chan Message
	out chan<- Message

	peer *replicationState
}

func (r *replicationChannel) Send(msg Message) {
	select {
	case <-r.ctx.Done():
		return
	case r.in <- msg:
	}
}

func (r *replicationChannel) Close() {
	r.cancel()
}

func (r *replicationChannel) Run() (err error) {
	r.logger.Debugw("started replication channel")
	defer func() {
		// disk IO error will panic
		rec := recover()
		if rec != nil {
			err = fmt.Errorf("%w: %v", ErrUnexpected, rec)
			r.cancel()
		}
		if err != nil {
			r.logger.Errorw("replication channel exited", "error", err)
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
					r.logger.Debugw("init", "ptr", m)
					r.peer.init(m)
					next = m
					initialized = true
				} else {
					r.logger.Debugw("update", "next", next, "ptr", m)
					next = r.peer.update(next, m)
				}
			}
		case out <- next:
			next = r.peer.next()
		}
	}
}

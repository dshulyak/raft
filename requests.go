package raft

import (
	"context"
	"time"

	"github.com/dshulyak/raft/raftlog"
)

func newReadRequest(ctx context.Context) *request {
	return &request{
		ctx:     ctx,
		errc:    make(chan error, 1),
		read:    true,
		Created: time.Now(),
	}
}

func newWriteRequest(ctx context.Context, entry *raftlog.Entry) *request {
	return &request{
		ctx:     ctx,
		errc:    make(chan error, 1),
		resultc: make(chan interface{}, 1),
		Entry:   entry,
		Created: time.Now(),
	}
}

type request struct {
	ctx     context.Context
	errc    chan error
	read    bool
	resultc chan interface{}
	Entry   *raftlog.Entry

	// Created is set when request reaches raft node.
	// Used for tracking latency for read/write requests.
	Created time.Time
}

func (p *request) Read() bool {
	return p.read
}

func (p *request) WaitResult(ctx context.Context) (interface{}, error) {
	if p.ctx == nil {
		panic("proposal is noop")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-p.resultc:
		return result, nil
	case <-p.ctx.Done():
		return nil, p.ctx.Err()
	}
}

func (p *request) Apply(result interface{}) {
	if p.ctx == nil {
		return
	}
	select {
	case <-p.ctx.Done():
	case p.resultc <- result:
	}
}

func (p *request) Complete(err error) {
	if p.ctx == nil {
		return
	}
	select {
	case <-p.ctx.Done():
	case p.errc <- err:
	}
}

func (p *request) Wait(ctx context.Context) error {
	if p.ctx == nil {
		panic("proposal is noop")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-p.errc:
		return err
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
}

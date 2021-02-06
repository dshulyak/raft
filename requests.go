package raft

import (
	"context"
	"sync"
	"time"

	"github.com/dshulyak/raft/raftlog"
)

var requestPool = sync.Pool{
	New: func() interface{} {
		return &request{
			errc:    make(chan error, 1),
			resultc: make(chan interface{}, 1),
		}
	},
}

type Releasable interface {
	Release()
}

type WriteRequest interface {
	Releasable
	// Wait completes when the entry is commited.
	// Or with error if:
	// - leader has stepped down
	// - queue overflow
	// - redirect to a new leader
	Wait(context.Context) error
	// WaitResult completes when the entry is applied.
	// Will not complete succesfully if Wait failed.
	WaitResult(context.Context) (interface{}, error)
}

type ReadRequest interface {
	Releasable
	// Wait completes when read is safe to execute.
	// Otherwise may fail with same reasons as WriteRequest.
	Wait(context.Context) error
}

func newReadRequest(ctx context.Context) *request {
	req := requestPool.Get().(*request)
	req.ctx = ctx
	req.Created = time.Now()
	req.read = true
	return req
}

func newWriteRequest(ctx context.Context, entry *raftlog.Entry) *request {
	req := requestPool.Get().(*request)
	req.ctx = ctx
	req.Created = time.Now()
	req.Entry = entry
	return req
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

// Release object. After calling this method object should not be used.
func (p *request) Release() {
	p.ctx = nil
	p.Entry = nil
	p.read = false
	requestPool.Put(p)
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

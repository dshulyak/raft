package raft

import (
	"context"

	"github.com/dshulyak/raft/raftlog"
)

func NewReadRequest(ctx context.Context) *Proposal {
	return &Proposal{
		ctx:  ctx,
		errc: make(chan error, 1),
		read: true,
	}
}

func NewProposal(ctx context.Context, entry *raftlog.Entry) *Proposal {
	return &Proposal{
		ctx:     ctx,
		errc:    make(chan error, 1),
		resultc: make(chan interface{}, 1),
		Entry:   entry,
	}
}

type Proposal struct {
	ctx     context.Context
	errc    chan error
	read    bool
	resultc chan interface{}
	Entry   *raftlog.Entry
}

func (p *Proposal) Read() bool {
	return p.read
}

func (p *Proposal) WaitResult(ctx context.Context) (interface{}, error) {
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

func (p *Proposal) Apply(result interface{}) {
	if p.ctx == nil {
		return
	}
	select {
	case <-p.ctx.Done():
	case p.resultc <- result:
	}
}

func (p *Proposal) Complete(err error) {
	if p.ctx == nil {
		return
	}
	select {
	case <-p.ctx.Done():
	case p.errc <- err:
	}
}

func (p *Proposal) Wait(ctx context.Context) error {
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

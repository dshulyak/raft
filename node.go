package raft

import (
	"context"
	"errors"
	"time"

	"github.com/dshulyak/raftlog"
)

var (
	ErrProposalsOverflow = errors.New("proposals queue overflow")
	ErrStopped           = errors.New("node is stopped")
)

type bufferedProposals struct {
	max int
	in  chan *Proposal
	out chan []*Proposal
}

func (b bufferedProposals) run(ctx context.Context) {
	var (
		batch []*Proposal
		out   chan []*Proposal
	)
	for {
		if len(batch) == 0 {
			out = nil
		} else {
			out = b.out
		}
		select {
		case <-ctx.Done():
			return
		case proposal := <-b.in:
			if len(batch) == b.max {
				proposal.Complete(ErrProposalsOverflow)
			}
			batch = append(batch, proposal)
		case out <- batch:
			batch = nil
		}
	}
}

type appUpdate struct {
	Commit    uint64
	Proposals []*Proposal
}

type node struct {
	global       *Context
	ctx          context.Context
	cancel       func()
	tick         time.Duration
	maxProposals int

	raft    *stateMachine
	streams *streamHandler
	server  *server

	inbound     chan Message
	proposals   chan *Proposal
	application chan *appUpdate
}

func (s *node) Propose(ctx context.Context, data []byte) (*Proposal, error) {
	proposal := &Proposal{
		ctx:    s.ctx,
		result: make(chan error, 1),
		Entry: &raftlog.LogEntry{
			OpType: raftlog.LogNoop,
			Op:     data,
		},
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.ctx.Done():
		return nil, ErrStopped
	case s.proposals <- proposal:
		return proposal, nil
	}
}

func (s *node) Push(ctx context.Context, msg Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.inbound <- msg:
		return nil
	case <-s.ctx.Done():
		return ErrStopped
	}
}

func (s *node) run() (err error) {
	var (
		proposals = make(chan []*Proposal, 1)
		timeout   = make(chan int)
		app       *appUpdate
		appC      chan *appUpdate
	)
	runTicker(s.ctx, timeout, s.tick)
	go bufferedProposals{
		out: proposals,
		in:  s.proposals,
		max: s.maxProposals,
	}.run(s.ctx)

	for {
		if err != nil {
			s.cancel()
			return err
		}
		update := s.raft.Update()
		if update != nil {
			if update.CommitLog.Index != 0 {
				if app != nil {
					app.Proposals = append(app.Proposals, update.Proposals...)
					app.Commit = update.CommitLog.Index
				}
				app = &appUpdate{
					Commit:    update.CommitLog.Index,
					Proposals: update.Proposals,
				}
			}

		}

		if app != nil {
			appC = s.application
		} else {
			appC = nil
		}
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case batch := <-s.proposals:
			err = s.raft.Next(batch)
		case msg := <-s.inbound:
			err = s.raft.Next(msg)
		case n := <-timeout:
			err = s.raft.Tick(n)
		case appC <- app:
			app = nil
		}
	}
}

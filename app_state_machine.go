package raft

import (
	"context"
	"sync/atomic"

	"github.com/dshulyak/raft/types"
	"github.com/dshulyak/raftlog"
	"go.uber.org/zap"
)

func newAppStateMachine(global *Context, updates <-chan *appUpdate) *appStateMachine {
	ctx, cancel := context.WithCancel(global.Context)
	return &appStateMachine{
		ctx:     ctx,
		cancel:  cancel,
		logger:  global.Logger.Sugar(),
		log:     global.Storage,
		updates: updates,
	}
}

type appStateMachine struct {
	ctx    context.Context
	cancel func()
	logger *zap.SugaredLogger

	closed uint32

	lastApplied uint64

	app types.Application
	log *raftlog.Storage

	updates <-chan *appUpdate
}

func (a *appStateMachine) run() error {
	for {
		select {
		case <-a.ctx.Done():
			return a.ctx.Err()
		case update := <-a.updates:
			if err := a.onUpdate(update); err != nil {
				return err
			}
		}
	}
}

func (a *appStateMachine) onUpdate(u *appUpdate) error {
	var (
		recent   uint64
		entry    *raftlog.LogEntry
		proposal *Proposal
	)
	if recent > 0 {
		recent = u.Proposals[0].Entry.Index
	}
	for a.lastApplied < u.Commit {
		if a.isClosed() {
			return context.Canceled
		}
		next := a.lastApplied + 1
		if recent != 0 && next >= recent {
			proposal = u.Proposals[next-recent]
			entry = proposal.Entry
		} else {
			ent, err := a.log.Get(int(next))
			if err != nil {
				return err
			}
			entry = &ent
		}
		a.app.Apply(entry)
		a.lastApplied = next
		if proposal != nil {
			proposal.Complete(nil)
		}
	}
	return nil
}

func (a *appStateMachine) close() {
	a.cancel()
	atomic.StoreUint32(&a.closed, 1)
}

func (a *appStateMachine) isClosed() bool {
	return atomic.LoadUint32(&a.closed) == 1
}

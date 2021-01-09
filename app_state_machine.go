package raft

import (
	"context"
	"sync/atomic"

	"github.com/dshulyak/raft/types"
	"github.com/dshulyak/raftlog"
	"go.uber.org/zap"
)

func newAppStateMachine(global *Context) *appStateMachine {
	ctx, cancel := context.WithCancel(global.Context)
	sm := &appStateMachine{
		ctx:      ctx,
		cancel:   cancel,
		logger:   global.Logger.Sugar(),
		log:      global.Storage,
		app:      global.App,
		appliedC: make(chan uint64),
		updatesC: make(chan *appUpdate),
	}
	go sm.run()
	return sm
}

type appStateMachine struct {
	ctx    context.Context
	cancel func()
	logger *zap.SugaredLogger

	closed uint32

	lastApplied uint64

	app types.Application
	log *raftlog.Storage

	appliedC chan uint64
	updatesC chan *appUpdate
}

func (a *appStateMachine) updates() chan<- *appUpdate {
	return a.updatesC
}

func (a *appStateMachine) applied() <-chan uint64 {
	return a.appliedC
}

func (a *appStateMachine) run() error {
	var (
		learned uint64
		applied chan uint64
	)
	for {
		select {
		case <-a.ctx.Done():
			return a.ctx.Err()
		case applied <- learned:
			applied = nil
		case update := <-a.updatesC:
			a.logger.Debugw("app received update", "commit", update.Commit, "proposals", len(update.Proposals))
			if err := a.onUpdate(update); err != nil {
				a.logger.Debugw("app state macine closed with error", "error", err)
				return err
			}
			a.logger.Debugw("app processed update", "commit", update.Commit)
			if a.lastApplied != learned {
				learned = a.lastApplied
				applied = a.appliedC
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
	if len(u.Proposals) > 0 {
		recent = u.Proposals[0].Entry.Index
	}
	for a.lastApplied < u.Commit {
		if a.isClosed() {
			return context.Canceled
		}
		next := a.lastApplied + 1
		if recent > 0 && next >= recent {
			proposal = u.Proposals[next-recent]
			entry = proposal.Entry
		} else {
			ent, err := a.log.Get(int(next) - 1)
			if err != nil {
				return err
			}
			entry = &ent
		}
		if entry.OpType == raftlog.LogApplication {
			a.logger.Debugw("applying entry", "index", entry.Index, "term", entry.Term, "proposed", proposal != nil)
			a.app.Apply(entry, proposal)
		}
		a.lastApplied = next
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

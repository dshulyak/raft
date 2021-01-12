package raft

import (
	"context"
	"sync/atomic"

	"github.com/dshulyak/raft/types"
	"github.com/dshulyak/raftlog"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func newAppStateMachine(ctx context.Context, global *Context, group *errgroup.Group) *appStateMachine {
	ctx, cancel := context.WithCancel(ctx)
	sm := &appStateMachine{
		ctx:      ctx,
		cancel:   cancel,
		logger:   global.Logger.Sugar(),
		log:      global.Storage,
		app:      global.App,
		appliedC: make(chan uint64),
		updatesC: make(chan *appUpdate),
	}
	group.Go(sm.run)
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

func (a *appStateMachine) run() (err error) {
	defer a.logger.Debugw("app exited", "error", err)
	var (
		learned uint64
		applied chan uint64
	)
	for {
		select {
		case <-a.ctx.Done():
			err = a.ctx.Err()
			return
		case applied <- learned:
			applied = nil
		case update := <-a.updatesC:
			a.logger.Debugw("app received update", "commit", update.Commit, "proposals", update.Proposals)
			if err = a.onUpdate(update); err != nil {
				return
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
			result := a.app.Apply(entry)
			if proposal != nil {
				proposal.UpdateResult(result)
				proposal.Complete(nil)
			}
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

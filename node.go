package raft

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/dshulyak/raftlog"
	"go.uber.org/zap"
)

var (
	// ErrProposalsOverflow raised in case nodes proposal queue overflows.
	ErrProposalsOverflow = errors.New("proposals queue overflow")
	// ErrStopped raised if node is shutting down due to a crash or a request.
	ErrStopped = errors.New("node is stopped")
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
			for _, proposal := range batch {
				proposal.Complete(ErrStopped)
			}
			return
		case proposal := <-b.in:
			if len(batch) == b.max {
				proposal.Complete(ErrProposalsOverflow)
				continue
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

func newNode(global *Context) *node {
	ctx, cancel := context.WithCancel(global)
	n := &node{
		global:       global,
		logger:       global.Logger.Sugar(),
		ctx:          ctx,
		cancel:       cancel,
		tick:         global.TickInterval,
		maxProposals: global.ProposalsBuffer,
		mailboxes:    map[NodeID]*peerMailbox{},
		inbound:      make(chan Message, 1),
		// proposals are buffered until node is ready to process them
		proposals: make(chan *Proposal),
		// logs are buffered until application is ready to process them
		application: make(chan *appUpdate),
	}
	n.raft = newStateMachine(
		global.Logger,
		global.ID,
		global.ElectionTimeoutMin, global.ElectionTimeoutMax,
		global.Configuration,
		global.Storage,
		global.State,
	)
	n.streams = newStreamHandler(global.Logger, n.msgPipeline)
	n.server = newServer(global, n.streams.handle)
	// FIXME report error from run up the stack
	go n.run()
	return n
}

type node struct {
	global       *Context
	logger       *zap.SugaredLogger
	ctx          context.Context
	cancel       func()
	tick         time.Duration
	maxProposals int

	raft    *stateMachine
	streams *streamHandler
	server  *server

	bmu       sync.RWMutex
	mailboxes map[NodeID]*peerMailbox

	inbound     chan Message
	proposals   chan *Proposal
	application chan *appUpdate
}

func (n *node) getMailbox(id NodeID) *peerMailbox {
	n.bmu.Lock()
	defer n.bmu.Unlock()
	return n.mailboxes[id]
}

func (n *node) sendMessages(u *Update) {
	if len(u.Msgs) == 0 {
		return
	}
	n.bmu.Lock()
	defer n.bmu.Unlock()
	for _, msg := range u.Msgs {
		box, exist := n.mailboxes[msg.To]
		if !exist {
			box = &peerMailbox{
				mail: n.streams.getSender(msg.To),
			}
			n.mailboxes[msg.To] = box
		}
		box.Update(n.global, u.State)
		box.Send(msg.Message)
	}
}

func (n *node) manageConnections(conf *Configuration, u *Update) {
	if u.State == RaftCandidate {
		for i := range conf.Nodes {
			n.server.Add(&conf.Nodes[i])
		}

	} else if u.State != 0 {
		for i := range conf.Nodes {
			n.server.Remove(conf.Nodes[i].ID)
		}
	}
}

func (n *node) msgPipeline(id NodeID, msg Message) {
	if mailbox := n.getMailbox(id); mailbox != nil {
		mailbox.Response(msg)
	}
	_ = n.Push(msg)
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

func (s *node) Push(msg Message) error {
	select {
	case s.inbound <- msg:
		return nil
	case <-s.ctx.Done():
		return ErrStopped
	}
}

func (n *node) run() (err error) {
	var (
		proposals = make(chan []*Proposal, 1)
		timeout   = make(chan int)
		app       *appUpdate
		appC      chan *appUpdate
	)
	runTicker(n.ctx, timeout, n.tick)
	go bufferedProposals{
		out: proposals,
		in:  n.proposals,
		max: n.maxProposals,
	}.run(n.ctx)

	for {
		if app != nil {
			appC = n.application
		} else {
			appC = nil
		}
		select {
		case <-n.ctx.Done():
			_ = n.raft.Next(ErrStopped)
			err = n.ctx.Err()
		case batch := <-n.proposals:
			err = n.raft.Next(batch)
		case msg := <-n.inbound:
			err = n.raft.Next(msg)
		case count := <-timeout:
			err = n.raft.Tick(count)
		case appC <- app:
			app = nil
		}
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				n.logger.Errorw("node will be terminated", "error", err)
				n.Stop()
			}
			return err
		}

		update := n.raft.Update()
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
			n.manageConnections(n.global.Configuration, update)
			n.sendMessages(update)
		}
	}
}

func (n *node) Stop() {
	n.cancel()
	n.server.Close()
	n.streams.close()
}

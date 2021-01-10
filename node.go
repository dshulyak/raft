package raft

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/dshulyak/raft/types"
	"github.com/dshulyak/raftlog"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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
	group, ctx := errgroup.WithContext(ctx)
	n := &node{
		global:       global,
		logger:       global.Logger.Sugar(),
		ctx:          ctx,
		cancel:       cancel,
		group:        group,
		closeC:       make(chan error, 1),
		tick:         global.TickInterval,
		maxProposals: global.ProposalsBuffer,
		mailboxes:    map[NodeID]*peerMailbox{},
		messages:     make(chan Message, 1),
		// proposals are buffered until node is ready to process them
		proposals: make(chan *Proposal),
	}
	n.raft = newStateMachine(
		global.Logger,
		global.ID,
		global.ElectionTimeoutMin, global.ElectionTimeoutMax,
		global.Configuration,
		global.Storage,
		global.State,
	)
	n.app = newAppStateMachine(global, n.group)
	n.streams = newStreamHandler(ctx, global.Logger, n.msgPipeline)
	n.server = newServer(global, n.streams.handle)
	n.group.Go(n.run)
	go func() {
		<-ctx.Done()
		err := n.group.Wait()
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		n.closeC <- err
	}()
	return n
}

type node struct {
	global *Context
	logger *zap.SugaredLogger

	ctx    context.Context
	cancel func()
	group  *errgroup.Group
	closeC chan error

	tick         time.Duration
	maxProposals int

	app     *appStateMachine
	raft    *stateMachine
	streams *streamHandler
	server  *server

	bmu       sync.RWMutex
	mailboxes map[NodeID]*peerMailbox

	messages  chan Message
	proposals chan *Proposal
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
				group: n.group,
				mail:  n.streams.getSender(msg.To),
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
			if conf.Nodes[i].ID != n.global.ID {
				n.server.Add(&conf.Nodes[i])
			}
		}
	} else if u.State == RaftFollower {
		for i := range conf.Nodes {
			if conf.Nodes[i].ID != n.global.ID {
				n.server.Remove(conf.Nodes[i].ID)
			}
		}
	}
}

func (n *node) msgPipeline(id NodeID, msg Message) {
	if mailbox := n.getMailbox(id); mailbox != nil {
		mailbox.Response(msg)
	}
	_ = n.Push(msg)
}

func (n *node) Propose(ctx context.Context, data []byte) (*types.Proposal, error) {
	proposal := types.NewProposal(n.ctx,
		&raftlog.LogEntry{
			OpType: raftlog.LogApplication,
			Op:     data,
		})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.ctx.Done():
		return nil, ErrStopped
	case n.proposals <- proposal:
		return proposal, nil
	}
}

func (n *node) Push(msg Message) error {
	select {
	case n.messages <- msg:
		return nil
	case <-n.ctx.Done():
		return ErrStopped
	}
}

func (n *node) run() (err error) {
	var (
		// node shouldn't consume from buffer until the leader is known
		proposals        = make(chan []*Proposal)
		blockedProposals chan []*Proposal

		timeout = make(chan int)
		app     *appUpdate
		appC    chan<- *appUpdate
	)
	runTicker(n.ctx, timeout, n.tick)
	go bufferedProposals{
		out: proposals,
		in:  n.proposals,
		max: n.maxProposals,
	}.run(n.ctx)

	for {
		if app != nil {
			appC = n.app.updates()
		} else {
			appC = nil
		}
		select {
		case <-n.ctx.Done():
			err = n.ctx.Err()
		case batch := <-blockedProposals:
			err = n.raft.Next(batch)
		case msg := <-n.messages:
			err = n.raft.Next(msg)
		case count := <-timeout:
			err = n.raft.Tick(count)
		case appC <- app:
			app = nil
		}
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				n.logger.Errorw("node will be terminated", "error", err)
				n.Close()
			}
			return err
		}

		update := n.raft.Update()
		if update != nil {
			if update.Commit != 0 {
				if app != nil {
					app.Proposals = append(app.Proposals, update.Proposals...)
					app.Commit = update.Commit
				}
				app = &appUpdate{
					Commit:    update.Commit,
					Proposals: update.Proposals,
				}
			}
			n.manageConnections(n.global.Configuration, update)
			n.sendMessages(update)
			switch update.LeaderState {
			case LeaderKnown:
				n.logger.Debugw("leader was elected. accepting proposals")
				blockedProposals = proposals
			case LeaderUnknown:
				n.logger.Debugw("leader is unknown. stopped accepting proposals")
				blockedProposals = nil
			}
		}
	}
}

func (n *node) Close() {
	n.cancel()
	n.server.Close()
}

func (n *node) Wait() error {
	return nil
}

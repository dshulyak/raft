package raft

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/dshulyak/raft/raftlog"
	"github.com/dshulyak/raft/types"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	// ErrProposalsOverflow raised in case nodes proposal queue overflows.
	ErrProposalsOverflow = errors.New("proposals queue overflow")
	// ErrProposalsEvicted raised if proposals are not consumed and not potentially
	// in the limbo.
	ErrProposalsEvicted = errors.New("proposals are evicted")
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

// NewNode creates instance of the raft node.
// After node is instantiated the caller must watch for error.
//
// err := node.Wait()
//
// It will be blocking until either Node is closed with node.Close(), and in such case
// error will be nil, or node crashes with irrecoverable error (only disk IO currently).
//
// To submit new operations use:
//
// wr, err := node.Propose(ctx, []byte("user command"))
//
// it will return future-like object that will be either failed or completed without error.
// err := wr.Wait(ctx)
// If error is nil wr.Result() will return an opaque interface{}. It is the same object
// returned by the Apply method of the App interface.
func NewNode(conf *Config) *Node {
	ctx, cancel := context.WithCancel(context.Background())
	group, ctx := errgroup.WithContext(ctx)
	n := &Node{
		global:       conf,
		logger:       conf.Logger.Sugar(),
		ctx:          ctx,
		cancel:       cancel,
		group:        group,
		closeC:       make(chan error, 1),
		tick:         conf.TickInterval,
		maxProposals: conf.ProposalsBuffer,
		mailboxes:    map[NodeID]*peerMailbox{},
		messages:     make(chan Message, 1),
		// proposals are buffered until node is ready to process them
		proposals: make(chan *Proposal),
	}
	n.raft = newStateMachine(
		conf.Logger,
		conf.ID,
		conf.ElectionTimeoutMin, conf.ElectionTimeoutMax,
		conf.Configuration,
		conf.Storage,
		conf.State,
	)
	n.app = newAppStateMachine(ctx, conf, n.group)
	n.streams = newStreamHandler(ctx, conf.Logger, n.msgPipeline)
	n.server = newServer(conf, ctx, n.streams.handle)
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

// Node ...
type Node struct {
	global *Config
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

func (n *Node) getMailbox(id NodeID) *peerMailbox {
	n.bmu.Lock()
	defer n.bmu.Unlock()
	return n.mailboxes[id]
}

func (n *Node) sendMessages(u *Update) {
	if len(u.Msgs) == 0 {
		return
	}
	n.bmu.Lock()
	defer n.bmu.Unlock()
	for _, msg := range u.Msgs {
		box, exist := n.mailboxes[msg.To]
		if !exist {
			box = &peerMailbox{
				ctx:   n.ctx,
				group: n.group,
				mail:  n.streams.getOutbound(msg.To),
			}
			n.mailboxes[msg.To] = box
		}
		box.Update(n.global, u.State)
		box.Send(msg.Message)
	}
}

func (n *Node) manageConnections(conf *Configuration, u *Update) {
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

func (n *Node) msgPipeline(id NodeID, msg Message) {
	if mailbox := n.getMailbox(id); mailbox != nil {
		mailbox.Response(msg)
	}
	_ = n.push(msg)
}

type WriteRequest interface {
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
	// Wait completes when read is safe to execute.
	// Otherwise may fail with same reasons as WriteRequest.
	Wait(context.Context) error
}

func (n *Node) Propose(ctx context.Context, data []byte) (WriteRequest, error) {
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

// Read returns future-like object that receives notification when read is safe to execute.
// Simplified client code:
//
// rr, err := n.Read(ctx)
// must(err)
// err = rr.Wait()
// must(err)
// app.Get(key)
//
// Read, unlike Proposal, bypasses raftlog but still requires coordination with followers to guarantee linearizability. If reading state data is not a problem client may ommit performing a Read on a raft node and go straight to the app.
func (n *Node) Read(ctx context.Context) (ReadRequest, error) {
	rr := types.NewReadRequest(n.ctx)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.ctx.Done():
		return nil, ErrStopped
	case n.proposals <- rr:
		return rr, nil
	}
}

func (n *Node) push(msg Message) error {
	select {
	case n.messages <- msg:
		return nil
	case <-n.ctx.Done():
		return ErrStopped
	}
}

func (n *Node) run() (err error) {
	defer n.logger.Debugw("node exited", "error", err)
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
		case applied := <-n.app.applied():
			n.raft.Applied(applied)
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
				} else {
					app = &appUpdate{
						Commit:    update.Commit,
						Proposals: update.Proposals,
					}
				}
			}
			n.manageConnections(n.global.Configuration, update)
			n.sendMessages(update)
			switch update.LeaderState {
			case LeaderKnown:
				n.logger.Debugw("leader was elected. accepting proposals")
				blockedProposals = proposals
			case LeaderUnknown:
				n.logger.Debugw("leader is unknown. not accepting proposals")
				blockedProposals = nil
			}
		}
	}
}

func (n *Node) Close() {
	n.cancel()
	n.server.Close()
}

func (n *Node) Wait() error {
	return <-n.closeC
}

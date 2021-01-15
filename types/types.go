package types

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/dshulyak/raft/raftlog"
)

type NodeID uint64

func (id NodeID) String() string {
	return strconv.Itoa(int(id))
}

func NodeIDFromString(id string) (NodeID, error) {
	nid, err := strconv.Atoi(id)
	if err != nil {
		return 0, err
	}
	return NodeID(nid), nil
}

type LogHeader struct {
	Term, Index uint64
}

type Message interface{}

type RequestVote struct {
	Term      uint64
	Candidate NodeID
	PreVote   bool
	LastLog   LogHeader
}

type RequestVoteResponse struct {
	Term        uint64
	Voter       NodeID
	PreVote     bool
	VoteGranted bool
}

type AppendEntries struct {
	Term      uint64
	Leader    NodeID
	PrevLog   LogHeader
	Commited  uint64
	ReadIndex uint64
	Entries   []*raftlog.LogEntry
}

type AppendEntriesResponse struct {
	Term      uint64
	Follower  NodeID
	Success   bool
	LastLog   LogHeader
	ReadIndex uint64
}

type Node struct {
	ID   NodeID
	IP   net.IP
	Port int
}

func (n *Node) String() string {
	return fmt.Sprintf("Node(id=%d,ip=%s,port=%d)", n.ID, n.IP, n.Port)
}

type Configuration struct {
	Nodes []Node
}

type ConfChangeType uint8

func (c ConfChangeType) String() string {
	return confChangeString[c]
}

const (
	ConfAdd = iota + 1
	ConfDelete
)

var confChangeString = [...]string{"Empty", "Add", "Delete"}

type ConfChange struct {
	Type ConfChangeType
	Node Node
}

func NewReadRequest(ctx context.Context) *Proposal {
	return &Proposal{
		ctx:  ctx,
		errc: make(chan error, 1),
		read: true,
	}
}

func NewProposal(ctx context.Context, entry *raftlog.LogEntry) *Proposal {
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
	Entry   *raftlog.LogEntry
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

package types

import (
	"fmt"
	"net"

	"github.com/dshulyak/raftlog"
)

type NodeID uint64

type LogHeader struct {
	Term, Index uint64
}

type Message interface{}

type RequestVote struct {
	Term      uint64
	Candidate NodeID
	LastLog   LogHeader
}

type RequestVoteResponse struct {
	Term        uint64
	Voter       NodeID
	VoteGranted bool
}

type AppendEntries struct {
	Term     uint64
	Leader   NodeID
	PrevLog  LogHeader
	Commited uint64
	Entries  []*raftlog.LogEntry
}

type AppendEntriesResponse struct {
	Term     uint64
	Follower NodeID
	Success  bool
	LastLog  LogHeader
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

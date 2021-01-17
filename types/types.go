package types

import (
	"fmt"
	"strconv"
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

type Node struct {
	ID      NodeID
	Address string
}

func (n *Node) String() string {
	return fmt.Sprintf("Node(id=%d,address=%s)", n.ID, n.Address)
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

package raft

import (
	"fmt"
	"net"
)

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

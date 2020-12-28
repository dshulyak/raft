package raft

import "net"

type Node struct {
	ID NodeID
	IP net.IP
}

type Configuration struct {
	Nodes []Node
}

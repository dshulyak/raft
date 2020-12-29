package raft

import "net"

type Node struct {
	ID        NodeID
	Protocols []Protocol
}

type Protocol struct {
	Name string
	IP   net.IP
	Port int
}

type Configuration struct {
	Nodes []Node
}

package types

import (
	"context"

	"github.com/dshulyak/raft/raftlog"
)

type Transport interface {
	Dial(context.Context, *Node) (MsgStream, error)
	// TODO refactor it to provide API such as
	// Accept(context.Context) (MsgStream, error)
	HandleStream(func(MsgStream))
	Close() error
}

type MsgStream interface {
	ID() NodeID
	Send(Message) error
	Receive() (Message, error)
	Close() error
}

type Application interface {
	Apply(*raftlog.LogEntry) interface{}
}

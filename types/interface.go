package types

import (
	"context"

	"github.com/dshulyak/raftlog"
)

type Transport interface {
	Dial(context.Context, *Node) (MsgStream, error)
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

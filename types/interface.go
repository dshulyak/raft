package types

import "context"

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

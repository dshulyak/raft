package transport

import (
	"context"

	"github.com/dshulyak/raft/types"
)

type (
	Transport interface {
		Dial(context.Context, *types.Node) (MsgStream, error)
		// TODO refactor it to provide API such as
		// Accept(context.Context) (MsgStream, error)
		HandleStream(func(MsgStream))
		Close() error
	}

	MsgStream interface {
		ID() types.NodeID
		Send(types.Message) error
		Receive() (types.Message, error)
		Close() error
	}
)

package raft

type egress struct {
	ctx  *Context
	errc chan error

	// NOTE this will change when cluster membership changes will be merged.
	configuration map[NodeID]*Node

	// all connections are in the one of two states:
	// - leader state. pipeline for replicating append entries.
	//   AppendEntries are sent in order.
	// - follower/candidate state. buffering last sent message
	conns map[NodeID]*egressConnection
}

func (e *egress) getConn(id NodeID) *egressConnection {
	conn, exist := e.conns[id]
	if !exist {
		node, exist := e.configuration[id]
		if !exist {
			e.ctx.Logger().Errorw("node is not in the cluster", "id", id)
			return nil
		}
		logger := e.ctx.Logger().With("peer", id)
		stream := newEgressStream(e.ctx,
			logger,
			e.ctx.Backoff(),
			e.ctx.Transport(),
			node)
		conn = &egressConnection{
			ctx:    e.ctx,
			id:     id,
			buf:    newLastMessageSender(e.ctx, logger, stream.In()),
			stream: stream,
		}
		e.conns[id] = conn
	}
	return conn
}

func (e *egress) Send(state RaftState, msgs []MessageTo) {
	for i := range msgs {
		msg := &msgs[i]
		conn := e.getConn(msg.To)
		if conn == nil {
			// panic here because it signals programming error
			e.ctx.Logger().Panicw(
				"sending message to the peer outside of the cluster",
				"peer", msg.To)
		}
		conn.ChangeState(state)
		conn.Send(msg.Message)
	}
}

type egressBuffer interface {
	// Send must not block for IO.
	Send(Message)
	// Close ensures that all requested resources are released, without blocking.
	Close()
}

// egressConnection switches buf between two possible states.
type egressConnection struct {
	ctx *Context

	id     NodeID
	state  RaftState
	buf    egressBuffer
	stream *egressStream
}

func (e *egressConnection) ChangeState(state RaftState) {
	if state == 0 {
		return
	} else if state == RaftLeader {
		e.buf.Close()
		logger := e.ctx.Logger().With("peer", e.id)
		e.buf = newPeerReactor(e.ctx,
			logger,
			e.ctx.TickInterval(),
			e.stream.In(),
			newPeerState(
				logger,
				uint64(e.ctx.EntriesPerAppend()),
				e.ctx.HeartbeatTimeout(),
				e.ctx.Storage(),
			),
		)
	} else if (e.state == RaftLeader) && state != RaftLeader {
		e.buf.Close()
		e.buf = newLastMessageSender(e.ctx,
			e.ctx.Logger().With("peer", e.id),
			e.stream.In())
	}
	e.state = state
}

func (e *egressConnection) Send(msg Message) {
	e.buf.Send(msg)
}

func (e *egressConnection) Close() {
	if e.buf != nil {
		e.buf.Close()
	}
	if e.stream != nil {
		e.stream.Close()
	}
}

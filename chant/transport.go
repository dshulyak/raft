package chant

import (
	"context"
	"io"
	"sync"
	"syscall"

	"github.com/dshulyak/raft"
)

func New() *Network {
	ctx, cancel := context.WithCancel(context.Background())
	return &Network{
		ctx:        ctx,
		cancel:     cancel,
		transports: map[raft.NodeID]*Transport{},
	}
}

type Network struct {
	ctx        context.Context
	cancel     func()
	mu         sync.Mutex
	transports map[raft.NodeID]*Transport
}

func (n *Network) Transport(id raft.NodeID) *Transport {
	n.mu.Lock()
	defer n.mu.Unlock()
	tr, exist := n.transports[id]
	if !exist {
		ctx, cancel := context.WithCancel(n.ctx)
		tr = &Transport{
			network:     n,
			ctx:         ctx,
			cancel:      cancel,
			id:          id,
			connections: map[raft.NodeID]*Connection{},
		}
		n.transports[id] = tr
	}
	return tr
}

func (n *Network) Accept(to raft.NodeID, stream *Stream) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	tr, exist := n.transports[to]
	if !exist {
		return error(syscall.EHOSTDOWN)
	}
	return tr.Accept(stream)
}

func (n *Network) Close() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cancel()
}

var _ raft.Transport = (*Transport)(nil)

type Transport struct {
	network *Network
	ctx     context.Context
	cancel  func()
	id      raft.NodeID

	handler func(raft.MsgStream)

	mu          sync.Mutex
	connections map[raft.NodeID]*Connection
}

func (t *Transport) Dial(ctx context.Context, n *raft.Node) (raft.MsgStream, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if n.ID == t.id {
		return nil, error(syscall.EINVAL)
	}
	conn, exist := t.connections[n.ID]
	if !exist {
		ctx, cancel := context.WithCancel(t.ctx)
		conn = &Connection{
			ctx:    ctx,
			cancel: cancel,

			p1: t.id,
			p2: n.ID,
			r1: make(chan raft.Message),
			r2: make(chan raft.Message),
			w1: make(chan raft.Message),
			w2: make(chan raft.Message),
		}
		t.connections[n.ID] = conn
		go func(id raft.NodeID) {
			conn.run()
			t.mu.Lock()
			delete(t.connections, id)
			t.mu.Unlock()
		}(n.ID)
	}
	s1, s2 := conn.Pair()
	if err := t.network.Accept(n.ID, s2); err != nil {
		conn.Close()
		delete(t.connections, n.ID)
		return nil, err
	}
	return s1, nil
}

func (t *Transport) Accept(stream raft.MsgStream) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.handler == nil {
		return error(syscall.EHOSTDOWN)
	}
	t.handler(stream)
	return nil
}

func (t *Transport) RemoveConnection(id raft.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.connections, id)
}

func (t *Transport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cancel()
	return nil
}

func (t *Transport) HandleStream(handler func(raft.MsgStream)) {
	t.handler = handler
}

// Connection acts as typical os level TCP connection.
// All messages are delivered in order, and sender receives success right
// after message is accepted into internal OS buffer, for simplicity
// internal buffer always is a single item
type Connection struct {
	p1, p2         raft.NodeID
	ctx            context.Context
	cancel         func()
	r1, w1, r2, w2 chan raft.Message
}

func (c *Connection) Pair() (s1, s2 *Stream) {
	s1 = &Stream{
		conn: c,
		id:   c.p2,
		r:    c.r1,
		w:    c.w2,
	}
	s2 = &Stream{
		conn: c,
		id:   c.p1,
		r:    c.r2,
		w:    c.w1,
	}
	return
}

func (c *Connection) Close() {
	c.cancel()
}

func (c *Connection) run() {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		deliver(c.ctx, c.r1, c.w1)
		wg.Done()
	}()
	go func() {
		deliver(c.ctx, c.r2, c.w2)
		wg.Done()
	}()
	wg.Wait()

}

func deliver(ctx context.Context, r, w chan raft.Message) {
	var (
		msg     raft.Message
		out, in chan raft.Message
	)
	for {
		if msg != nil {
			out = r
			in = nil
		} else {
			out = nil
			in = w
		}
		select {
		case <-ctx.Done():
			return
		case msg = <-in:
		case out <- msg:
			msg = nil
		}
	}
}

var _ raft.MsgStream = (*Stream)(nil)

type Stream struct {
	conn *Connection
	id   raft.NodeID
	r, w chan raft.Message
}

func (s *Stream) ID() raft.NodeID {
	return s.id
}

func (s *Stream) Send(ctx context.Context, msg raft.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.conn.ctx.Done():
		return io.EOF
	case s.w <- msg:
		return nil
	}
}

func (s *Stream) Receive(ctx context.Context) (raft.Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.conn.ctx.Done():
		return nil, io.EOF
	case msg := <-s.r:
		return msg, nil
	}
}

func (s *Stream) Close() error {
	s.conn.Close()
	return nil
}

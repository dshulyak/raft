package chant

import (
	"context"
	"io"
	"sync"
	"syscall"

	"github.com/dshulyak/raft/types"
)

func New() *Network {
	ctx, cancel := context.WithCancel(context.Background())
	return &Network{
		ctx:        ctx,
		cancel:     cancel,
		transports: map[types.NodeID]*Transport{},
	}
}

type Network struct {
	ctx        context.Context
	cancel     func()
	mu         sync.Mutex
	transports map[types.NodeID]*Transport
}

func (n *Network) Transport(id types.NodeID) *Transport {
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
			connections: map[types.NodeID]*Connection{},
		}
		n.transports[id] = tr
	}
	return tr
}

func (n *Network) Accept(to types.NodeID, stream *Stream) error {
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

var _ types.Transport = (*Transport)(nil)

type Transport struct {
	network *Network
	ctx     context.Context
	cancel  func()
	id      types.NodeID

	handler func(types.MsgStream)

	mu          sync.Mutex
	connections map[types.NodeID]*Connection
}

func (t *Transport) Dial(ctx context.Context, n *types.Node) (types.MsgStream, error) {
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
			r1: make(chan types.Message),
			r2: make(chan types.Message),
			w1: make(chan types.Message),
			w2: make(chan types.Message),
		}
		t.connections[n.ID] = conn
		go func(id types.NodeID) {
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

func (t *Transport) Accept(stream types.MsgStream) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.handler == nil {
		return error(syscall.EHOSTDOWN)
	}
	t.handler(stream)
	return nil
}

func (t *Transport) RemoveConnection(id types.NodeID) {
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

func (t *Transport) HandleStream(handler func(types.MsgStream)) {
	t.handler = handler
}

// Connection acts as typical os level TCP connection.
// All messages are delivered in order, and sender receives success right
// after message is accepted into internal OS buffer, for simplicity
// internal buffer always is a single item
type Connection struct {
	p1, p2         types.NodeID
	ctx            context.Context
	cancel         func()
	r1, w1, r2, w2 chan types.Message
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

func deliver(ctx context.Context, r, w chan types.Message) {
	var (
		msg     types.Message
		out, in chan types.Message
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

var _ types.MsgStream = (*Stream)(nil)

type Stream struct {
	conn *Connection
	id   types.NodeID
	r, w chan types.Message
}

func (s *Stream) ID() types.NodeID {
	return s.id
}

func (s *Stream) Send(ctx context.Context, msg types.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.conn.ctx.Done():
		return io.EOF
	case s.w <- msg:
		return nil
	}
}

func (s *Stream) Receive(ctx context.Context) (types.Message, error) {
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

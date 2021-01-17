package grpcstream

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/dshulyak/raft/transport"
	stream "github.com/dshulyak/raft/transport/grpcstream/pb"
	"github.com/dshulyak/raft/types"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var defaultDialOpts = [...]grpc.DialOption{
	grpc.WithInsecure(),
	grpc.WithBlock(),
}

func NewTransport(id types.NodeID, srv *grpc.Server, opts ...grpc.DialOption) *Transport {
	tr := &Transport{
		id:       id,
		srv:      srv,
		dialOpts: opts,
	}
	stream.RegisterRaftServer(srv, tr)
	return tr
}

var _ transport.Transport = (*Transport)(nil)

type Transport struct {
	id types.NodeID

	srv      *grpc.Server
	dialOpts []grpc.DialOption

	mu      sync.Mutex
	handler func(transport.MsgStream)
}

func (tr *Transport) Dial(ctx context.Context, node *types.Node) (transport.MsgStream, error) {
	opts := tr.dialOpts
	if opts == nil {
		opts = defaultDialOpts[:]
	}
	conn, err := grpc.DialContext(ctx, node.Address, opts...)
	if err != nil {
		return nil, err
	}

	st := stream.NewRaftClient(conn)

	ctx, cancel := context.WithCancel(context.Background())
	ctx = metadata.AppendToOutgoingContext(ctx, "peerID", tr.id.String())
	clientStream, err := st.Pipe(ctx)
	if err != nil {
		cancel()
		return nil, err
	}

	return &Stream{
		peer:   node.ID,
		st:     clientStream,
		closer: cancel,
	}, nil
}

func (tr *Transport) HandleStream(f func(transport.MsgStream)) {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.handler != nil {
		panic("handler already registered")
	}
	tr.handler = f
}

func (tr *Transport) Pipe(st stream.Raft_PipeServer) error {
	tr.mu.Lock()
	handler := tr.handler
	tr.mu.Unlock()

	if handler == nil {
		return errors.New("grpc server is not ready")
	}

	md, ok := metadata.FromIncomingContext(st.Context())
	if !ok {
		return errors.New("metadata with a peerID must be available")
	}
	ids := md.Get("peerID")
	if len(ids) != 1 {
		return errors.New("invalid peerID")
	}
	nid, err := types.NodeIDFromString(ids[0])
	if err != nil {
		return fmt.Errorf("can't parse peerID %v", err)
	}

	ctx, cancel := context.WithCancel(st.Context())
	s := &Stream{
		peer:   nid,
		st:     st,
		closer: cancel,
	}
	handler(s)
	<-ctx.Done()
	return nil
}

type sendReceiver interface {
	Send(*types.Message) error
	Recv() (*types.Message, error)
}

var _ transport.MsgStream = (*Stream)(nil)

type Stream struct {
	peer types.NodeID

	st     sendReceiver
	closer func()
}

func (s *Stream) ID() types.NodeID {
	return s.peer
}

func (s *Stream) Send(msg *types.Message) error {
	return s.st.Send(msg)
}

func (s *Stream) Receive() (*types.Message, error) {
	return s.st.Recv()
}

func (s *Stream) Close() error {
	s.closer()
	return nil
}

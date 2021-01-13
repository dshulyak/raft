package raft

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/dshulyak/raft/types"
	"go.uber.org/zap"
)

var ErrConnected = errors.New("already connected")

type msgPipeline func(NodeID, Message)

func newStreamHandler(ctx context.Context, logger *zap.Logger, push msgPipeline) *streamHandler {
	return &streamHandler{
		ctx:      ctx,
		logger:   logger.Sugar(),
		push:     push,
		messages: map[NodeID]chan Message{},
		sema:     map[NodeID]chan struct{}{},
	}
}

type streamHandler struct {
	ctx    context.Context
	logger *zap.SugaredLogger
	push   msgPipeline

	mu       sync.Mutex
	messages map[NodeID]chan Message
	sema     map[NodeID]chan struct{}
}

func (p *streamHandler) reader(stream MsgStream) error {
	for {
		msg, err := stream.Receive()
		if err != nil {
			return err
		}
		p.push(stream.ID(), msg)
	}
}

func (p *streamHandler) getSema(id NodeID) chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	sema, exist := p.sema[id]
	if !exist {
		sema = make(chan struct{}, 1)
		p.sema[id] = sema
	}
	return sema
}

func (p *streamHandler) getOutbound(id NodeID) chan Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	sender, exist := p.messages[id]
	if !exist {
		sender = make(chan Message)
		p.messages[id] = sender
	}
	return sender
}

func (p *streamHandler) removeOutbound(id NodeID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.messages, id)
	delete(p.sema, id)
}

// writer prevents multiple streams from accessing outbound message channel
// at the same time. while it is fine for multiple readers to exist:
// - one stream may read RequestVote/AppendEntries requests
// - another will download snapshot
// multiple writers are not order preserving which won't break but will
// negatively affect protocol.
func (p *streamHandler) writer(stream MsgStream, closer chan struct{}) error {
	var (
		outbound chan types.Message
		sema     = p.getSema(stream.ID())
	)
	defer func() {
		if outbound != nil {
			<-sema
		}
	}()
	for {
		select {
		case sema <- struct{}{}:
			outbound = p.getOutbound(stream.ID())
		case <-closer:
			return nil
		case <-p.ctx.Done():
			return ErrStopped
		case msg := <-outbound:
			err := stream.Send(msg)
			if err != nil {
				return err
			}
		}
	}
}

func (p *streamHandler) handle(stream MsgStream) {
	var wg sync.WaitGroup
	wg.Add(2)
	errc := make(chan error, 2)
	closer := make(chan struct{})
	go func() {
		err := p.reader(stream)
		errc <- err
		wg.Done()
	}()
	go func() {
		err := p.writer(stream, closer)
		errc <- err
		wg.Done()
	}()

	for err := range errc {
		if err != nil {
			if !errors.Is(err, io.EOF) {
				stream.Close()
			}
			close(closer)
			break
		}
	}
	wg.Wait()
}

package raft

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"
)

var ErrConnected = errors.New("already connected")

type streamHandler struct {
	logger *zap.SugaredLogger
	push   func(context.Context, Message) error

	readTimeout, writeTimeout time.Duration

	mu     sync.Mutex
	sender map[NodeID]chan Message
	// in current protocol there is no need for more than 1 stream
	// if two peers will concurrently initiate connections we will end up with more
	// if connection already exists in this map abandon it.
	connected map[NodeID]struct{}
}

func (p *streamHandler) registerConnection(id NodeID) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, exist := p.connected[id]
	if exist {
		return false
	}
	p.connected[id] = struct{}{}
	return true
}

func (p *streamHandler) unregisterConnection(id NodeID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.connected, id)
}

func (p *streamHandler) reader(parent context.Context, stream MsgStream) error {
	for {
		ctx, cancel := context.WithTimeout(parent, p.readTimeout)
		msg, err := stream.Receive(ctx)
		cancel()
		if err != nil {
			return err
		}
		if err := p.push(parent, msg); err != nil {
			return err
		}
	}
}

func (p *streamHandler) getSender(id NodeID) chan Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	sender, exist := p.sender[id]
	if !exist {
		sender = make(chan Message)
		p.sender[id] = sender
	}
	return sender
}

func (p *streamHandler) writer(parent context.Context, stream MsgStream) error {
	sender := p.getSender(stream.ID())
	for msg := range sender {
		ctx, cancel := context.WithTimeout(parent, p.writeTimeout)
		err := stream.Send(ctx, msg)
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *streamHandler) handle(ctx context.Context, stream MsgStream) error {
	if !p.registerConnection(stream.ID()) {
		return ErrConnected
	}
	defer p.unregisterConnection(stream.ID())
	var (
		wg   sync.WaitGroup
		errc = make(chan error, 2)
	)
	wg.Add(2)
	go func() {
		errc <- p.reader(ctx, stream)
		wg.Done()
	}()
	go func() {
		errc <- p.writer(ctx, stream)
		wg.Done()
	}()
	wg.Wait()
	close(errc)
	for err := range errc {
		if err != nil {
			return err
		}
	}
	return nil
}

package raft

import (
	"context"
	"sync"
	"time"
)

type streamHandler struct {
	state *stateReactor

	readTimeout, writeTimeout time.Duration
	mu                        sync.Mutex
	sender                    map[NodeID]chan Message
}

func (p *streamHandler) reader(parent context.Context, stream MsgStream) error {
	for {
		ctx, cancel := context.WithTimeout(parent, p.readTimeout)
		msg, err := stream.Receive(ctx)
		cancel()
		if err != nil {
			return err
		}
		if err := p.state.Push(parent, msg); err != nil {
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

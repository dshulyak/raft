package raft

import (
	"errors"
	"io"
	"sync"

	"go.uber.org/zap"
)

var ErrConnected = errors.New("already connected")

type msgPipeline func(NodeID, Message)

func newStreamHandler(logger *zap.Logger, push msgPipeline) *streamHandler {
	return &streamHandler{
		logger:    logger.Sugar(),
		push:      push,
		sender:    map[NodeID]chan Message{},
		connected: map[NodeID]struct{}{},
	}
}

type streamHandler struct {
	logger *zap.SugaredLogger
	push   msgPipeline

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

func (p *streamHandler) reader(stream MsgStream) error {
	for {
		msg, err := stream.Receive()
		if err != nil {
			return err
		}
		p.push(stream.ID(), msg)
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

func (p *streamHandler) closeSender(id NodeID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	sender, exist := p.sender[id]
	if !exist {
		return
	}
	close(sender)
}

func (p *streamHandler) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, sender := range p.sender {
		close(sender)
	}
}

func (p *streamHandler) writer(stream MsgStream) error {
	sender := p.getSender(stream.ID())
	for msg := range sender {
		err := stream.Send(msg)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *streamHandler) handle(stream MsgStream) {
	if !p.registerConnection(stream.ID()) {
		stream.Close()
		return
	}
	defer p.unregisterConnection(stream.ID())

	errc := make(chan error, 2)
	go func() {
		err := p.reader(stream)
		p.logger.Debugw("reader finished", "peer", stream.ID(), "error", err)
		errc <- err
	}()
	go func() {
		err := p.writer(stream)
		p.logger.Debugw("writer finished", "peer", stream.ID(), "error", err)
		errc <- err
	}()

	n := 0
	for err := range errc {
		if err != nil && !errors.Is(err, io.EOF) {
			stream.Close()
			return
		}
		n++
		if n == 2 {
			stream.Close()
			return
		}
	}
	panic("unreachable!")
}

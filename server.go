package raft

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

var (
	ErrNotInCluster = errors.New("node not in the cluster")
)

type handler func(context.Context, MsgStream) error

type server struct {
	ctx    context.Context
	cancel func()
	tr     Transport

	backoff     time.Duration
	dialTimeout time.Duration
	protocol    handler

	mu      sync.Mutex
	dialers map[NodeID]*dialer
}

func (s *server) dial(node *Node) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exist := s.dialers[node.ID]
	if !exist {
		ctx, cancel := context.WithCancel(s.ctx)
		d := &dialer{
			ctx:         ctx,
			cancel:      cancel,
			dialTimeout: s.dialTimeout,
			backoff:     s.backoff,
			tr:          s.tr,
			node:        node,
		}
		s.dialers[node.ID] = d
		go func() {
			for {
				stream, err := d.dialWithBackoff()
				if err == nil {
					err = s.protocol(s.ctx, stream)
				}
				if !errors.Is(err, io.EOF) {
					stream.Close()
				}
				if errors.Is(err, context.Canceled) {
					return
				}
			}
		}()
	}
}

func (s *server) accept(stream MsgStream) {
	go func() {
		// i need a way to notify dialer that the connection is restored
		// if dialer is in the deep backoff
		// if connection is restored handoff this stream to the dialer
		// and let him manage connections as it was previously
		err := s.protocol(s.ctx, stream)
		if errors.Is(err, io.EOF) {
			stream.Close()
		}
	}()
}

func (s *server) closeDialers() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, d := range s.dialers {
		delete(s.dialers, id)
		d.close()
	}
}

func (s *server) close() {
	s.cancel()
	s.closeDialers()
}

type dialer struct {
	ctx    context.Context
	cancel func()

	dialTimeout time.Duration
	tr          Transport
	node        *Node

	backoff      time.Duration
	backoffCount int
}

func (d *dialer) dial() (MsgStream, error) {
	ctx, cancel := context.WithTimeout(d.ctx, d.dialTimeout)
	defer cancel()
	return d.tr.Dial(ctx, d.node)
}

func (d *dialer) dialWithBackoff() (MsgStream, error) {
	if d.backoffCount > 0 {
		timer := time.NewTimer(d.backoff << (d.backoffCount - 1))
		defer timer.Stop()
		select {
		case <-d.ctx.Done():
			return nil, d.ctx.Err()
		case <-timer.C:
		}
	}
	stream, err := d.dial()
	if err != nil {
		d.backoffCount++
	} else {
		d.backoffCount = 0
	}
	return stream, err
}

func (d *dialer) close() {
	d.cancel()
}

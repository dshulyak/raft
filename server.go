package raft

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"
)

var (
	ErrNotInCluster = errors.New("node not in the cluster")
)

type handler func(MsgStream)

func newServer(global *Context, protocol handler) *server {
	ctx, cancel := context.WithCancel(global)
	srv := &server{
		logger:      global.Logger.Sugar(),
		ctx:         ctx,
		cancel:      cancel,
		tr:          global.Transport,
		backoff:     global.Backoff,
		dialTimeout: global.DialTimeout,
		protocol:    protocol,
		connectors:  map[NodeID]*connector{},
		connected:   map[NodeID]struct{}{},
	}
	global.Transport.HandleStream(func(stream MsgStream) {
		srv.accept(stream.ID(), stream)
	})
	return srv
}

type server struct {
	logger *zap.SugaredLogger
	ctx    context.Context
	cancel func()
	tr     Transport

	backoff     time.Duration
	dialTimeout time.Duration
	protocol    handler

	mu         sync.RWMutex
	connectors map[NodeID]*connector
	connected  map[NodeID]struct{}
}

// setConnected marks node id as connected if it wasn't connected.
func (s *server) setConnected(id NodeID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exist := s.connected[id]
	if exist {
		return false
	}
	s.connected[id] = struct{}{}
	return true
}

func (s *server) removeConnected(id NodeID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.connected, id)
}

func (s *server) getConnector(id NodeID) *connector {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.connectors[id]
}

// Add will ensure that there is one opened stream at every point of time.
// If node is not reachable it will be dialed in the background, according to the backoff
// policy.
// In case if two nodes will connect to each it is the protocol responsibility to
// close redundant streams or make use of them.
func (s *server) Add(node *Node) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exist := s.connectors[node.ID]
	if !exist {
		ctx, cancel := context.WithCancel(s.ctx)
		d := &connector{
			ctx:         ctx,
			cancel:      cancel,
			dialTimeout: s.dialTimeout,
			backoff:     s.backoff,
			tr:          s.tr,
			node:        node,
		}
		s.connectors[node.ID] = d
		_, exist := s.connected[node.ID]
		// if stream is not opened spawn a connector goroutine
		// otherwise when an already accepted stream will be closing
		// it will check if connector exists or not
		// this is done in order for dialed and accepted streams to
		// have shared codepath
		if !exist {
			s.accept(node.ID, nil)
		}
	}
}

// Remove persistent connector to a node. It doesn't close existing connections.
func (s *server) Remove(id NodeID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, exist := s.connectors[id]
	if !exist {
		return
	}
	delete(s.connectors, id)
	c.close()
}

func (s *server) accept(id NodeID, stream MsgStream) {
	go func() {
		var (
			connected bool
			conn      *connector
			err       error
		)
		for {
			if conn != nil {
				stream, err = conn.dialWithBackoff()
			}
			if err == nil && stream != nil {
				s.protocol(stream)
			}
			if stream != nil {
				s.removeConnected(id)
				stream = nil
			}
			connected = s.setConnected(id)
			if !connected {
				return
			}
			c1 := s.getConnector(id)
			if c1 == nil {
				return
			}
			if conn == nil {
				// need to reset the backoff if peer dialed to us
				// while this node connector might be in the deep backoff
				c1.resetBackoff()
			}
			conn = c1
		}
	}()
}

func (s *server) closeConnectors() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, d := range s.connectors {
		delete(s.connectors, id)
		d.close()
	}
}

func (s *server) Close() error {
	s.cancel()
	s.closeConnectors()
	return s.tr.Close()
}

type connector struct {
	ctx    context.Context
	cancel func()

	dialTimeout time.Duration
	tr          Transport
	node        *Node

	backoff      time.Duration
	backoffCount int
}

func (d *connector) dial() (MsgStream, error) {
	ctx := d.ctx
	if d.dialTimeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(d.ctx, d.dialTimeout)
		defer cancel()
	}
	return d.tr.Dial(ctx, d.node)
}

func (d *connector) resetBackoff() {
	d.backoff = 0
}

func (d *connector) dialWithBackoff() (MsgStream, error) {
	if d.backoffCount > 0 && d.backoff > 0 {
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

func (d *connector) close() {
	d.cancel()
}

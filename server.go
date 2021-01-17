package raft

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

var (
	ErrNotInCluster = errors.New("node not in the cluster")
)

type handler func(MsgStream)

func newServer(global *Config, ctx context.Context, protocol handler) *server {
	ctx, cancel := context.WithCancel(ctx)
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
	if srv.backoff == 0 {
		srv.backoff = 200 * time.Millisecond
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
func (s *server) Add(node *ConfNode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exist := s.connectors[node.ID]
	if !exist {
		s.logger.Debugw("adding dialer for a peer", "peer", node.ID)
		ctx, cancel := context.WithCancel(s.ctx)
		d := &connector{
			ctx:         ctx,
			cancel:      cancel,
			dialTimeout: s.dialTimeout,
			backoff:     rate.NewLimiter(rate.Every(s.backoff), 1),
			tr:          s.tr,
			node:        node,
		}
		s.connectors[node.ID] = d
		_, exist := s.connected[node.ID]
		// if stream is not opened spawn a connector goroutine
		// otherwise when an already accepted stream will be closing
		// it will check if connector exists or not
		// this is done in order for dialed and accepted streams to
		// have a shared codepath
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
	s.logger.Debugw("removing dialer for a peer", "peer", id)
	delete(s.connectors, id)
	c.close()
}

func (s *server) accept(id NodeID, stream MsgStream) {
	go func() {
		// only one goroutine is suppose to dial
		// this is enforced by an owner boolean, the first g that updates
		// the connection is an owner and it shouldn't exit until a connector
		// is removed from a server
		for {
			owner := s.setConnected(id)
			if stream != nil {
				s.protocol(stream)
			}
			if owner {
				s.removeConnected(id)
			} else {
				return
			}

			conn := s.getConnector(id)
			if conn != nil {
				var err error
				stream, err = conn.dial()
				if errors.Is(err, context.Canceled) {
					return
				}
			} else {
				return
			}
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
	return nil
}

type connector struct {
	ctx    context.Context
	cancel func()

	dialTimeout time.Duration
	tr          Transport
	node        *ConfNode

	backoff *rate.Limiter
}

func (d *connector) dial() (MsgStream, error) {
	if err := d.backoff.Wait(d.ctx); err != nil {
		return nil, err
	}
	ctx := d.ctx
	if d.dialTimeout > 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(d.ctx, d.dialTimeout)
		defer cancel()
	}
	return d.tr.Dial(ctx, d.node)
}

func (d *connector) close() {
	d.cancel()
}

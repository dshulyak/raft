package raft

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/dshulyak/raft/chant"
	"github.com/stretchr/testify/require"
)

func TestServerConnect(t *testing.T) {
	net := chant.New()
	tr1 := net.Transport(1)
	tr2 := net.Transport(2)
	connected1 := make(chan NodeID)
	connected2 := make(chan NodeID)
	ctx1 := &Context{
		Context:   context.Background(),
		Logger:    testLogger(t),
		Transport: tr1,
	}
	srv1 := newServer(ctx1, func(_ context.Context, stream MsgStream) error {
		connected1 <- stream.ID()
		return nil
	})
	defer srv1.Close()
	ctx2 := &Context{
		Context:   context.Background(),
		Logger:    testLogger(t),
		Transport: tr2,
	}
	errc := make(chan error)
	srv2 := newServer(ctx2, func(ctx context.Context, stream MsgStream) error {
		connected2 <- stream.ID()
		_, err := stream.Receive(ctx)
		if err != nil {
			errc <- err

		}
		return nil
	})
	defer srv2.Close()
	srv1.Connect(&Node{ID: 2})
	select {
	case n := <-connected1:
		require.Equal(t, NodeID(2), n)
	case <-time.After(time.Millisecond):
		require.FailNow(t, "timed out waiting for connection")
	}

	select {
	case n := <-connected2:
		require.Equal(t, NodeID(1), n)
	case <-time.After(time.Millisecond):
		require.FailNow(t, "timed out waiting for connection")
	}
	srv1.Disconnect(2)
	select {
	case err := <-errc:
		require.True(t, errors.Is(err, io.EOF), "error %v", err)
	case <-time.After(time.Millisecond):
		require.FailNow(t, "timed out waiting for disconnect")
	}
}

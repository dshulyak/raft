package channel

import (
	"context"
	"errors"
	"io"
	"syscall"
	"testing"
	"time"

	"github.com/dshulyak/raft/transport"
	"github.com/dshulyak/raft/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getTestMessage(i int) *types.Message {
	return &types.Message{
		Type: &types.Message_ReqVote{
			ReqVote: &types.RequestVote{Term: uint64(i)},
		},
	}
}

func TestHostDown(t *testing.T) {
	net := New()
	t.Cleanup(func() {
		net.Close()
	})
	first := net.Transport(1)

	stream, err := first.Dial(context.TODO(), &types.Node{ID: 2})
	assert.Nil(t, stream)
	assert.Error(t, err, error(syscall.EHOSTDOWN).Error())
}

func TestMsgStream(t *testing.T) {
	net := New()
	t.Cleanup(func() {
		net.Close()
	})
	first := net.Transport(1)
	second := net.Transport(2)
	received := make(chan *types.Message, 1)
	second.HandleStream(func(stream transport.MsgStream) {
		go func() {
			for {
				msg, err := stream.Receive()
				if errors.Is(err, io.EOF) {
					return
				}
				received <- msg
			}
		}()
	})

	stream, err := first.Dial(context.TODO(), &types.Node{ID: 2})
	require.NoError(t, err)

	n := 3333
	for i := 0; i < n; i++ {
		msg := getTestMessage(i)
		require.NoError(t, stream.Send(msg))
		select {
		case received := <-received:
			require.Equal(t, msg, received)
		case <-time.After(500 * time.Millisecond):
			require.FailNow(t, "timed out waiting for a message %v", i)
		}
	}
}

func TestMsgReverse(t *testing.T) {
	net := New()
	t.Cleanup(func() {
		net.Close()
	})
	first := net.Transport(1)
	second := net.Transport(2)

	n := 3333
	second.HandleStream(func(stream transport.MsgStream) {
		go func() {
			for i := 0; i < n; i++ {
				if err := stream.Send(getTestMessage(i)); err == io.EOF {
					return
				}
			}
		}()
	})

	stream, err := first.Dial(context.TODO(), &types.Node{ID: 2})
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		msg, err := stream.Receive()
		require.NoError(t, err)
		require.Equal(t, getTestMessage(i), msg)
	}
}

func TestStreamsBlocked(t *testing.T) {
	net := New()
	t.Cleanup(func() {
		net.Close()
	})
	first := net.Transport(1)
	second := net.Transport(2)

	received := make(chan *types.Message, 1)
	errc := make(chan error, 1)
	second.HandleStream(func(stream transport.MsgStream) {
		go func() {
			for {
				msg, err := stream.Receive()
				if err != nil {
					errc <- err
					return
				}
				received <- msg
			}
		}()
	})

	stream, err := first.Dial(context.TODO(), &types.Node{ID: 2})
	require.NoError(t, err)

	expect := getTestMessage(10)
	require.NoError(t, stream.Send(expect))
	select {
	case <-time.After(time.Second):
		require.FailNow(t, "timed out waiting for a message")
	case msg := <-received:
		require.Equal(t, expect, msg)
	}
	net.Block(1, 2)
	require.NoError(t, stream.Send(expect))
	select {
	case <-time.After(time.Second):
		require.FailNow(t, "timed out waiting for an error")
	case err := <-errc:
		require.Equal(t, io.EOF, err)
	}
}

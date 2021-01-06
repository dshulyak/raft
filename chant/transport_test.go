package chant

import (
	"context"
	"errors"
	"io"
	"syscall"
	"testing"
	"time"

	"github.com/dshulyak/raft/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	received := make(chan types.Message, 1)
	second.HandleStream(func(stream types.MsgStream) {
		go func() {
			for {
				msg, err := stream.Receive(context.TODO())
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
		require.NoError(t, stream.Send(context.TODO(), i))
		select {
		case received := <-received:
			require.Equal(t, i, received)
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
	second.HandleStream(func(stream types.MsgStream) {
		go func() {
			for i := 0; i < n; i++ {
				if err := stream.Send(context.TODO(), i); err == io.EOF {
					return
				}
			}
		}()
	})

	stream, err := first.Dial(context.TODO(), &types.Node{ID: 2})
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		msg, err := stream.Receive(ctx)
		cancel()
		require.NoError(t, err)
		require.Equal(t, i, msg)
	}
}

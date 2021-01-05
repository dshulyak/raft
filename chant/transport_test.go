package chant

import (
	"context"
	"errors"
	"io"
	"syscall"
	"testing"
	"time"

	"github.com/dshulyak/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHostDown(t *testing.T) {
	net := New()
	t.Cleanup(func() {
		net.Close()
	})
	first := net.Transport(1)

	stream, err := first.Dial(context.TODO(), &raft.Node{ID: 2})
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
	received := make(chan raft.Message, 1)
	second.HandleStream(func(stream raft.MsgStream) {
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

	stream, err := first.Dial(context.TODO(), &raft.Node{ID: 2})
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

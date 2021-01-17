package grpcstream

import (
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/dshulyak/raft/transport"
	"github.com/dshulyak/raft/types"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func createTransport(t *testing.T, id types.NodeID) (*Transport, *types.Node) {
	t.Helper()

	f, err := ioutil.TempFile("", "testing-grpc-*.sock")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.NoError(t, os.Remove(f.Name()))

	srv := grpc.NewServer(grpc.KeepaliveParams(
		keepalive.ServerParameters{
			MaxConnectionIdle: 10 * time.Second,
		},
	))

	ls, err := net.ListenUnix("unix", &net.UnixAddr{
		Name: f.Name(),
		Net:  "unix",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		srv.Stop()
		ls.Close()
	})
	tr := NewTransport(id, srv)
	go func() {
		srv.Serve(ls)
	}()
	return tr, &types.Node{ID: id, Address: fmt.Sprintf("unix://%s", f.Name())}
}

func TestMessageExchange(t *testing.T) {
	tr1, _ := createTransport(t, 1)

	tr2, n2 := createTransport(t, 2)
	messagesc := make(chan *types.Message, 1)
	errc := make(chan error, 1)
	tr2.HandleStream(func(st transport.MsgStream) {
		msg, err := st.Receive()
		if err != nil {
			errc <- err
			return
		}
		messagesc <- msg
	})

	_, err := tr1.Dial(context.Background(), n2)
	require.NoError(t, err)

	//require.NoError(t, st.Send(&types.Message{}))

	select {
	case received := <-messagesc:
		require.NotNil(t, received)
	case err := <-errc:
		require.FailNow(t, "unexpected error %v", err)
	case <-time.After(20 * time.Second):
		require.FailNow(t, "timed out waiting for GoAway")
	}
}

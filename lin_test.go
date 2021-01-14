package raft

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	printHistory = flag.Bool("history", false, "if true visualized history will be saved to file. by default history is saved only if linerizability test failed.")
)

const (
	regReadOp = iota + 1
	regWriteOp
)

type registerInput struct {
	op    int
	value int
}

var atomicRegister = porcupine.Model{
	Init: func() interface{} {
		return 0
	},
	Step: func(state, input, output interface{}) (bool, interface{}) {
		value := state.(int)
		inp := input.(registerInput)
		switch inp.op {
		case regReadOp:
			return output.(int) == value, state
		case regWriteOp:
			return true, inp.value
		}
		panic("unkown operation")
	},
	DescribeOperation: func(input, output interface{}) string {
		inp := input.(registerInput)
		switch inp.op {
		case regReadOp:
			return fmt.Sprintf("Read=%d", output.(int))
		case regWriteOp:
			return fmt.Sprintf("Write=%d", inp.value)
		}
		panic("unkown operation")
	},
}

func TestLinearizableRegister(t *testing.T) {
	cluster := newNodeCluster(t, 3)
	var (
		wg sync.WaitGroup

		mu      sync.Mutex
		history []porcupine.Event

		id   int64
		iter = 1_000
		key  = uint64(1)
	)
	client := func(reader bool, clientID int) {
		for i := 1; i <= iter; i++ {
			cid := int(atomic.AddInt64(&id, 1))
			mu.Lock()
			var value interface{}
			if reader {
				value = registerInput{op: regReadOp}
			} else {
				value = registerInput{op: regWriteOp, value: cid}
			}
			history = append(history, porcupine.Event{
				ClientId: clientID,
				Kind:     porcupine.CallEvent,
				Id:       cid,
				Value:    value,
			})
			mu.Unlock()

			var op []byte
			if !reader {
				var err error
				op, err = cluster.encoder.Insert(key, cid)
				require.NoError(t, err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			require.NoError(t, cluster.propose(ctx, op))

			var out interface{}
			if reader {
				rst, set := cluster.leaderApp().Get(key)
				if !set {
					out = 0
				} else {
					out = rst
				}
			}
			mu.Lock()
			history = append(history, porcupine.Event{
				ClientId: clientID,
				Kind:     porcupine.ReturnEvent,
				Id:       cid,
				Value:    out,
			})
			mu.Unlock()
		}
	}
	wg.Add(4)
	go func() {
		client(false, 1)
		wg.Done()
	}()
	go func() {
		client(false, 2)
		wg.Done()
	}()
	go func() {
		client(true, 3)
		wg.Done()
	}()
	go func() {
		client(true, 4)
		wg.Done()
	}()
	wg.Wait()

	result, info := porcupine.CheckEventsVerbose(atomicRegister, history, 0)
	assert.True(t, porcupine.Ok == result, "history is not linearizable")
	if *printHistory {
		f, err := ioutil.TempFile("", "lintest-register-*.html")
		defer f.Close()
		require.NoError(t, err)
		require.NoError(t, porcupine.Visualize(atomicRegister, info, f))
		t.Logf("history is written to %v", f.Name())
	}
}

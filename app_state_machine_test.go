package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dshulyak/raft/types"
	"github.com/dshulyak/raftlog"
	"github.com/stretchr/testify/require"
)

const (
	insertOp uint16 = iota + 1
	deleteOp
)

type encodedOp struct {
	Kind  uint16
	Key   uint64
	Value interface{}
}

func newKeyValueApp() *keyValueApp {
	return &keyValueApp{
		vals: map[uint64]interface{}{},
	}
}

type keyValueOpEncoder interface {
	Insert(key uint64, value interface{}) ([]byte, error)
	Delete(key uint64) ([]byte, error)
}

type keyValueApp struct {
	mu   sync.RWMutex
	vals map[uint64]interface{}
}

func (a *keyValueApp) encode(op *encodedOp) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(op); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (a *keyValueApp) Get(key uint64) (interface{}, bool) {
	// NOTE for linearizable reads synchronization with consensus is required
	a.mu.RLock()
	defer a.mu.RUnlock()
	val, exist := a.vals[key]
	return val, exist
}

func (a *keyValueApp) Insert(key uint64, value interface{}) ([]byte, error) {
	return a.encode(&encodedOp{Kind: insertOp, Key: key, Value: value})
}

func (a *keyValueApp) Delete(key uint64) ([]byte, error) {
	return a.encode(&encodedOp{Kind: deleteOp, Key: key})
}

func (a *keyValueApp) decode(buf []byte, op *encodedOp) error {
	b := bytes.NewBuffer(buf)
	return gob.NewDecoder(b).Decode(op)
}

func (a *keyValueApp) Apply(entry *raftlog.LogEntry, proposal *Proposal) {
	var op encodedOp
	err := a.decode(entry.Op, &op)
	if err == nil {
		a.mu.Lock()
		switch op.Kind {
		case insertOp:
			a.vals[op.Key] = op.Value
		case deleteOp:
			delete(a.vals, op.Key)
		default:
			err = fmt.Errorf("unknown operation code %d", op.Kind)
		}
		a.mu.Unlock()
	}
	if proposal != nil {
		proposal.Complete(err)
	}
}

func TestApplyLogs(t *testing.T) {
	logger := testLogger(t)
	storage, err := raftlog.New(logger, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		storage.Delete()
	})
	app := newKeyValueApp()
	global := &Context{
		Context: context.Background(),
		Storage: storage,
		App:     app,
		Logger:  logger,
	}
	appSM := newAppStateMachine(global)
	t.Cleanup(func() {
		appSM.close()
	})

	commit := uint64(33)
	for i := uint64(1); i <= commit; i++ {
		op, err := app.Insert(i, i)
		require.NoError(t, err)
		require.NoError(t, storage.Append(
			&raftlog.LogEntry{
				Index:  i,
				Term:   1,
				OpType: raftlog.LogApplication,
				Op:     op,
			},
		))
	}

	select {
	case <-time.After(time.Second):
		require.FailNow(t, "timed out sending commit update")
	case appSM.updates() <- &appUpdate{Commit: commit}:
	}
	select {
	case <-time.After(time.Second):
		require.FailNow(t, "timed out waiting for applied")
	case applied := <-appSM.applied():
		require.Equal(t, commit, applied)
	}

	rst, exists := app.Get(1)
	require.True(t, exists)
	require.EqualValues(t, 1, rst)

	op, err := app.Delete(1)
	require.NoError(t, err)
	proposal := types.NewProposal(context.TODO(), &raftlog.LogEntry{
		Index:  commit + 1,
		Term:   1,
		OpType: raftlog.LogApplication,
		Op:     op,
	})
	commit++
	select {
	case <-time.After(time.Second):
		require.FailNow(t, "timed out sending commit update")
	case appSM.updates() <- &appUpdate{Commit: commit, Proposals: []*types.Proposal{proposal}}:
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, proposal.Wait(ctx))
}

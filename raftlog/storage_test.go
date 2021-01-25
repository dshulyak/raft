package raftlog

import (
	"errors"
	"testing"

	"github.com/dshulyak/raft/types"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

func TestStorageLastEmpty(t *testing.T) {
	store, err := New(testLogger(t), nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Delete()) })
	_, err = store.Last()
	require.True(t, errors.Is(err, ErrEmptyLog), "expected empty log error")
}

func TestStorageDeleteAppend(t *testing.T) {
	store, err := New(testLogger(t), nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Delete()) })

	entries := 2
	for i := 0; i < entries; i++ {
		require.NoError(t, store.Append(&Entry{Index: uint64(i)}))
	}
	require.NoError(t, store.Sync())

	last, err := store.Last()
	require.NoError(t, err)

	require.NoError(t, store.DeleteFrom(entries-1))
	for i := entries; i < entries*2; i++ {
		require.NoError(t, store.Append(&Entry{Index: uint64(i)}))
	}
	require.NoError(t, store.Sync())
	last, err = store.Last()
	require.NoError(t, err)
	require.Equal(t, entries*2-1, int(last.Index))
}

func TestStorageLogDeletion(t *testing.T) {
	store, err := New(testLogger(t), nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Delete()) })
	entries := 1000
	for i := 0; i < entries; i++ {
		require.NoError(t, store.Append(&Entry{Index: uint64(i)}))
	}
	require.NoError(t, store.Sync())
	from := 100
	require.NoError(t, store.DeleteFrom(from))

	for i := 0; i < from; i++ {
		entry, err := store.Get(i)
		require.NoError(t, err)
		require.Equal(t, i, int(entry.Index))
	}

	require.NoError(t, store.DeleteFrom(0))
	require.True(t, store.IsEmpty())
}

type storageMachine struct {
	storage     *Storage
	term, index uint64
	logs        []*Entry
}

func (s *storageMachine) Init(t *rapid.T) {
	storage, err := New(testLogger(t), nil, nil)
	require.NoError(t, err)
	s.storage = storage
}

func (s *storageMachine) Last(t *rapid.T) {
	if s.storage.IsEmpty() {
		t.Skip("empty storage")
	}
	last, err := s.storage.Last()
	require.NoError(t, err)
	lastLog := s.logs[len(s.logs)-1]
	require.Equal(t, lastLog, last)
}

func (s *storageMachine) Get(t *rapid.T) {
	if s.storage.IsEmpty() {
		t.Skip("empty storage")
	}
	i := rapid.IntRange(0, len(s.logs)-1).Draw(t, "i").(int)
	entry, err := s.storage.Get(i)
	require.NoError(t, err)
	require.Equal(t, s.logs[i], entry)
}

func (s *storageMachine) Append(t *rapid.T) {
	size := rapid.IntRange(16, 1024).Draw(t, "size").(int)
	buf := make([]byte, size)
	if rapid.Bool().Draw(t, "term").(bool) {
		s.term++
	}
	entry := &types.Entry{Index: s.index, Term: s.term, Op: buf}
	require.NoError(t, s.storage.Append(entry))
	s.logs = append(s.logs, entry)
	s.index++
	require.NoError(t, s.storage.Sync())
}

func (s *storageMachine) Sync(t *rapid.T) {
	require.NoError(t, s.storage.Sync())
}

func (s *storageMachine) Check(t *rapid.T) {
	for i := range s.logs {

		entry, err := s.storage.Get(int(s.logs[i].Index))
		require.NoError(t, err)
		require.Equal(t, s.logs[i], entry)
	}
}

func (s *storageMachine) Cleanup() {
	_ = s.storage.Delete()
}

func TestStorageProperties(t *testing.T) {
	if testing.Short() {
		t.Skip("property based testing is skipped")
	}
	rapid.Check(t, rapid.Run(new(storageMachine)))
}

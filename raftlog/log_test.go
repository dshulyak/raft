package raftlog

import (
	"errors"
	"io/ioutil"
	"os"
	"testing"

	"github.com/dshulyak/raft/types"
	"github.com/stretchr/testify/require"
)

func TestLogAppendGet(t *testing.T) {
	log, err := NewLog(testLogger(t), nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, log.Delete())
	})

	entries := 100000
	offsets := make([]IndexEntry, entries)
	offset := log.HeaderSize()
	for i := 0; i < entries; i++ {
		size, err := log.Append(&Entry{
			Index: uint64(i),
		})
		offsets[i] = IndexEntry{Offset: offset, Length: size}
		require.NoError(t, err)
		offset += size
	}
	require.NoError(t, log.Flush())
	var entry Entry
	for i := range offsets {
		require.NoError(t, log.Get(&offsets[i], &entry), "entry at index %d", i)
		require.Equal(t, i, int(entry.Index))
	}
}

func TestLogCRCVerification(t *testing.T) {
	f, err := ioutil.TempFile("", "log-test-file-")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, f.Close())
	})
	log, err := NewLog(testLogger(t), nil, &LogOptions{File: f.Name()})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, log.Delete())
	})

	size, err := log.Append(&Entry{Index: 1})
	require.NoError(t, err)
	require.NoError(t, log.Flush())

	offset := log.HeaderSize()
	_, err = f.WriteAt([]byte{1, 2, 3}, int64(offset+size/2))
	require.NoError(t, err)

	var entry Entry
	require.True(t, errors.Is(log.Get(&IndexEntry{Offset: offset, Length: size}, &entry), ErrLogCorrupted))
}

func TestLogReopen(t *testing.T) {
	f, err := ioutil.TempFile("", "log-test-file-")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	t.Cleanup(func() {
		require.NoError(t, os.Remove(f.Name()))
	})

	log, err := NewLog(testLogger(t), nil, &LogOptions{File: f.Name()})
	require.NoError(t, err)

	total := 100
	for i := 0; i < total; i++ {
		_, err := log.Append(&types.Entry{})
		require.NoError(t, err)
	}

	require.NoError(t, log.Close())

	log, err = NewLog(testLogger(t), nil, &LogOptions{File: f.Name()})
	require.NoError(t, err)
	_, err = log.Append(&types.Entry{})
	require.NoError(t, err)
}

func BenchmarkLogAppend(b *testing.B) {
	log, err := NewLog(testLogger(b), nil, nil)
	require.NoError(b, err)
	b.Cleanup(func() {
		require.NoError(b, log.Delete())
	})

	buf := make([]byte, 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := log.Append(&Entry{
			Index: 1,
			Term:  1,
			Type:  types.Entry_NOOP,
			Op:    buf,
		})
		if err != nil {
			require.NoError(b, err)
		}
		if err := log.Sync(); err != nil {
			require.NoError(b, err)
		}
	}
}

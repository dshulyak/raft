package raftlog

import (
	"errors"
	"io/ioutil"
	"os"
	"testing"

	"github.com/dshulyak/raft/types"
	"github.com/stretchr/testify/require"
)

func makeTestLog(t testing.TB) (*log, *os.File) {
	t.Helper()

	f, err := ioutil.TempFile("", "log-test-file-")
	require.NoError(t, err)

	log, err := openLog(testLogger(t).Sugar(), f.Name())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, log.Delete())
	})
	return log, f
}

func TestLogAppendGet(t *testing.T) {
	log, _ := makeTestLog(t)

	entries := 10_000
	offsets := make([]indexEntry, entries)
	offset := uint64(0)
	for i := 1; i <= entries; i++ {
		size, err := log.Append(&Entry{
			Index: uint64(i),
		})
		require.NoError(t, err)
		offsets[i-1] = indexEntry{Offset: offset, Length: size}
		offset += size
	}
	require.NoError(t, log.Flush())

	for i := range offsets {
		entry, err := log.Get(&offsets[i])
		require.NoError(t, err, "entry at index %d", i)
		require.Equal(t, i+1, int(entry.Index))
	}
}

func TestLogCRCVerification(t *testing.T) {
	log, f := makeTestLog(t)

	size, err := log.Append(&Entry{Index: 1})
	require.NoError(t, err)

	require.NoError(t, log.Flush())

	offset := uint64(0)
	_, err = f.WriteAt([]byte{1, 2, 3}, int64(offset+size/2))
	require.NoError(t, err)

	_, err = log.Get(&indexEntry{Offset: offset, Length: size})
	require.True(t, errors.Is(err, ErrLogCorrupted),
		"log is expected to be corrupted")
}

func TestLogReopen(t *testing.T) {
	log, _ := makeTestLog(t)

	total := 100
	for i := 0; i < total; i++ {
		_, err := log.Append(&types.Entry{})
		require.NoError(t, err)
	}
	require.NoError(t, log.Flush())

	log, err := openLog(testLogger(t).Sugar(), log.f.Name())
	require.NoError(t, err)
	_, err = log.Append(&types.Entry{})
	require.NoError(t, err)
}

func BenchmarkLogAppend(b *testing.B) {
	log, _ := makeTestLog(b)

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

package raftlog

import (
	"errors"
	"flag"
	"os"
	"testing"

	"github.com/dshulyak/raft/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"pgregory.net/rapid"
)

var logLevel = flag.String("log-level", "panic", "test environment log level")

type testingHelper interface {
	Helper()
	zaptest.TestingT
}

func testLogger(t testingHelper) *zap.Logger {
	t.Helper()
	var level zapcore.Level
	require.NoError(t, level.Set(*logLevel))
	return zaptest.NewLogger(t, zaptest.Level(level), zaptest.WrapOptions(zap.AddCaller()))
}

func makeTestStorage(t testing.TB, opts ...Option) *Storage {
	t.Helper()
	opts = append([]Option{WithLogger(testLogger(t)), WithTempDir()}, opts...)
	store, err := New(opts...)
	require.NoError(t, err)
	t.Cleanup(func() { store.Delete() })
	return store
}

func TestStorageLastEmpty(t *testing.T) {
	store := makeTestStorage(t)

	_, err := store.Last()
	require.True(t, errors.Is(err, ErrEmptyLog), "expected empty log error")
}

func TestStorageRescan(t *testing.T) {
	opts := []Option{WithSegmentSize(512), WithCache(2)}
	store := makeTestStorage(t, opts...)

	total := 200
	for i := 1; i <= total; i++ {
		require.NoError(t, store.Append(&types.Entry{Index: uint64(i)}))
	}

	require.NoError(t, store.Sync())
	require.NoError(t, store.Close())

	store = makeTestStorage(t, WithDir(store.Location()))

	for i := 1; i <= total; i++ {
		entry, err := store.Get(uint64(i))
		require.NoError(t, err, "entry num %d", i)
		require.Equal(t, i, int(entry.Index))
	}
}

func TestStorageRewrite(t *testing.T) {
	store := makeTestStorage(t)

	total := 200
	terms := 2
	for term := 1; term <= terms; term++ {
		for i := 1; i <= total; i++ {
			require.NoError(t, store.Append(&types.Entry{
				Index: uint64(i),
				Term:  uint64(term),
			}))
		}
	}

	require.NoError(t, store.Sync())

	for i := 1; i <= total; i++ {
		entry, err := store.Get(uint64(i))
		require.NoError(t, err)
		require.Equal(t, i, int(entry.Index), "index")
		require.Equal(t, terms, int(entry.Term), "term")
	}
}

func TestStorageCorrupted(t *testing.T) {
	store := makeTestStorage(t, WithCache(1))
	for i := 1; i <= 2; i++ {
		require.NoError(t, store.Append(&types.Entry{
			Index: uint64(i),
			Op:    make([]byte, 10),
		}))
	}
	require.NoError(t, store.Sync())

	f, err := os.OpenFile(segmentPath(store.Location(), 0), os.O_WRONLY, 0)
	require.NoError(t, err)
	t.Cleanup(func() {
		f.Close()
	})

	_, err = f.WriteAt([]byte("abc"), 5)
	require.NoError(t, err)

	_, err = store.Get(1)
	require.True(t, errors.Is(err, ErrLogCorrupted), "log is not corrupted")
}

func TestStorageRepair(t *testing.T) {
	store := makeTestStorage(t)
	valid := 5
	offset := 0
	for i := 1; i <= 10; i++ {
		entry := &types.Entry{
			Index: uint64(i),
			Op:    make([]byte, 10),
		}
		if i <= valid {
			offset += onDiskSize(entry)
		}
		require.NoError(t, store.Append(entry))
	}

	require.NoError(t, store.Sync())
	require.NoError(t, store.Close())

	f, err := os.OpenFile(segmentPath(store.Location(), 0), os.O_WRONLY, 0)
	require.NoError(t, err)
	t.Cleanup(func() {
		f.Close()
	})

	// valid offset plus arbitrary number of bytes to cause unexpected eof
	require.NoError(t, f.Truncate(int64(offset)+4))

	store = makeTestStorage(t, WithDir(store.Location()))

	for i := 1; i <= valid; i++ {
		entry, err := store.Get(uint64(i))
		require.NoError(t, err)
		require.Equal(t, i, int(entry.Index))
	}
	_, err = store.Get(uint64(valid + 1))
	require.True(t, errors.Is(err, ErrEntryFromFuture), "entry must not be in the log")
}

type storageMachine struct {
	storage *Storage
	index   uint64
	logs    []*types.Entry
}

func (s *storageMachine) Init(t *rapid.T) {
	storage, err := New(WithLogger(testLogger(t)), WithTempDir())
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
	i := rapid.Uint64Range(1, uint64(len(s.logs))).Draw(t, "i").(uint64)
	entry, err := s.storage.Get(i)
	require.NoError(t, err)
	require.Equal(t, s.logs[i-1], entry)
}

func (s *storageMachine) Append(t *rapid.T) {
	size := rapid.IntRange(16, 1024).Draw(t, "size").(int)
	buf := make([]byte, size)
	s.index++
	entry := &types.Entry{Index: s.index, Op: buf}
	require.NoError(t, s.storage.Append(entry))
	s.logs = append(s.logs, entry)
}

func (s *storageMachine) Sync(t *rapid.T) {
	require.NoError(t, s.storage.Sync())
}

func (s *storageMachine) Check(t *rapid.T) {
	for i := range s.logs {
		entry, err := s.storage.Get(s.logs[i].Index)
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


func BenchmarkStorageAppend(b *testing.B) {
	store := makeTestStorage(b)
	entry := types.Entry{Index: 1, Op: make([]byte, 100)}
	
	b.SetBytes(int64(onDiskSize(&entry)))
	
	b.ResetTimer()
	
	for i := 1; i <= b.N; i++ {
		e := entry
		e.Index = uint64(i)
		if err := store.Append(&e); err != nil {
			require.NoError(b, err)
		}
	}


}

package raftlog

import (
	"flag"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logLevel = flag.String("log-level", "panic", "test environment log level")

type TestingI interface {
	Helper()
	require.TestingT
}

func testLogger(t TestingI) *zap.Logger {
	t.Helper()
	var level zapcore.Level
	require.NoError(t, level.Set(*logLevel))
	log, _ := zap.NewDevelopment(zap.IncreaseLevel(level))
	return log
}

func TestIndexGetAppend(t *testing.T) {
	items := 1200
	initial := items/2 - items/4
	index, err := NewIndex(testLogger(t), &IndexOptions{DefaultSize: initial * int(entryWidth)})
	require.NoError(t, err)
	t.Cleanup(func() {
		index.Delete()
	})
	for i := 0; i < 1000; i++ {
		require.NoError(t, index.Append(uint64(1)))
	}
	for i := 0; i < 1000; i++ {
		require.Equal(t, i, int(index.Get(uint64(i)).Offset))
	}
}

func TestIndexReopen(t *testing.T) {
	f, err := ioutil.TempFile("", "test-index-")
	require.NoError(t, err)
	require.NoError(t, f.Close())

	t.Cleanup(func() {
		require.NoError(t, os.Remove(f.Name()))
	})

	index, err := NewIndex(testLogger(t), &IndexOptions{File: f.Name()})
	require.NoError(t, err)
	n := 100
	size := 1
	for i := 0; i < n; i++ {
		require.NoError(t, index.Append(uint64(size)))
	}

	require.NoError(t, index.Close())

	index, err = NewIndex(testLogger(t), &IndexOptions{File: f.Name()})
	require.NoError(t, err)

	ie := index.LastIndex()
	require.Equal(t, (n-1)*size, int(ie.Offset))
}

func BenchmarkIndexTruncate(b *testing.B) {
	index, err := NewIndex(testLogger(b), &IndexOptions{DefaultSize: 0xffff})
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {

		require.NoError(b, index.Append(100))
		require.NoError(b, index.Sync())

		_ = index.Truncate(0)
	}
}

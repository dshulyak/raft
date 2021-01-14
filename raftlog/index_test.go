package raftlog

import (
	"flag"
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
	index, err := NewIndex(testLogger(t), 0, &IndexOptions{DefaultSize: initial * int(entryWidth)})
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

func TestIndexInit(t *testing.T) {
	index, err := NewIndex(testLogger(t), 0, &IndexOptions{DefaultSize: 16})
	require.NoError(t, err)
	require.NoError(t, index.Append(10))
	index.init()
}

func BenchmarkIndexTruncate(b *testing.B) {
	index, err := NewIndex(testLogger(b), 0, &IndexOptions{DefaultSize: 0xffff})
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {

		require.NoError(b, index.Append(100))
		require.NoError(b, index.Sync())

		_ = index.Truncate(0)
	}
}

package raftlog

import (
	"flag"
	"io/ioutil"
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

func makeTestIndex(t testing.TB, defaultSize ...int) *index {
	t.Helper()
	require.True(t, len(defaultSize) <= 1, "should not be larger then one")

	size := defaultIndexSize
	if len(defaultSize) == 1 {
		size = defaultSize[0]
	}

	f, err := ioutil.TempFile("", "index-test-file-")
	require.NoError(t, err)

	idx, err := openIndex(testLogger(t).Sugar(), f.Name(), size)
	require.NoError(t, err)
	t.Cleanup(func() {
		idx.Delete()
	})
	return idx
}

func TestIndexGetAppend(t *testing.T) {
	items := 1200
	initial := items/2 - items/4
	idx := makeTestIndex(t, initial*int(entryWidth))

	for i := 0; i < 1000; i++ {
		require.NoError(t, idx.Append(uint64(1)))
	}
	for i := 0; i < 1000; i++ {
		require.Equal(t, i, int(idx.Get(uint64(i)).Offset))
	}
}

func TestIndexReopen(t *testing.T) {
	idx := makeTestIndex(t)

	n := 100
	size := 1
	for i := 0; i < n; i++ {
		require.NoError(t, idx.Append(uint64(size)))
	}

	require.NoError(t, idx.Close())

	idx, err := openIndex(testLogger(t).Sugar(), idx.f.Name(), defaultIndexSize)
	require.NoError(t, err)

	require.Equal(t, (n-1)*size, int(idx.LastIndex().Offset))
}

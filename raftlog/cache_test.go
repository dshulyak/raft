package raftlog

import (
	"testing"

	"github.com/dshulyak/raft/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCacheTruncate(t *testing.T) {
	size := 8
	total := size + size/2
	cache := newCache(size)
	for i := 1; i <= total; i++ {
		cache.Append(&types.Entry{Index: uint64(i)})
	}

	entries := cache.SliceFrom(0)
	require.Len(t, entries, size)

	start := uint64(total - (size - 1))
	for _, entry := range entries {
		assert.Equal(t, int(start), int(entry.Index))
		start++
	}

	cache.DeleteFrom(uint64(total - size/2))

	entries = cache.SliceFrom(0)
	require.Len(t, entries, size/2)
}

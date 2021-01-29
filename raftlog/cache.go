package raftlog

import (
	"github.com/dshulyak/raft/types"
)

func newCache(size int) *entriesCache {
	return &entriesCache{
		cache: make(map[uint64]*types.Entry, size),
	}
}

type entriesCache struct {
	cache map[uint64]*types.Entry
}

func (e *entriesCache) Preload(entries []*types.Entry) {
	if len(entries) == 0 {
		return
	}
	for _, entry := range entries {
		e.Add(entry)
	}
}

func (e *entriesCache) Capacity() uint64 {
	return uint64(len(e.cache))
}

func (e *entriesCache) Iterate(from, to uint64, f func(*types.Entry) bool) {
	for ; from <= to; from++ {
		if !f(e.cache[from]) {
			return
		}
	}
}

func (e *entriesCache) Slice(from, to uint64) []*types.Entry {
	rst := make([]*types.Entry, 0, to-from+1)
	e.Iterate(from, to, func(entry *types.Entry) bool {
		rst = append(rst, entry)
		return true
	})
	return rst
}

func (e *entriesCache) Add(entry *types.Entry) {
	e.cache[entry.Index] = entry
}

func (e *entriesCache) Get(index uint64) *types.Entry {
	return e.cache[index]
}

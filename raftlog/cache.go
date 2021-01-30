package raftlog

import (
	"github.com/dshulyak/raft/types"
)

func newCache(size int) *entriesCache {
	return &entriesCache{
		ring: make([]*types.Entry, size),
	}
}

// entriesCacehe is a ring buffer that keeps N last entries in memory.
type entriesCache struct {
	head, tail uint64
	ring       []*types.Entry
}

func (e *entriesCache) Preload(entries ...*types.Entry) {
	if len(entries) == 0 {
		return
	}
	e.head = entries[0].Index
	e.tail = e.head
	for _, entry := range entries {
		e.Add(entry)
	}
}

func (e *entriesCache) Capacity() uint64 {
	return uint64(len(e.ring))
}

func (e *entriesCache) Empty() bool {
	return e.head == e.tail
}

func (e *entriesCache) IterateFrom(start uint64, f func(*types.Entry) bool) {
	if start < e.head {
		start = e.head
	}
	for start < e.tail {
		if !f(e.ring[start%e.Capacity()]) {
			return
		}
		start++
	}
}

func (e *entriesCache) Add(entry *types.Entry) {
	if entry.Index <= e.tail {
		e.tail = entry.Index - 1
	}

	pos := e.tail % e.Capacity()
	e.ring[pos] = entry
	e.tail++
	if e.tail-e.head > e.Capacity() {
		e.head++
	}
}

func (e *entriesCache) Get(index uint64) *types.Entry {
	if index < e.head || index > e.tail {
		return nil
	}
	return e.ring[index%e.Capacity()]
}

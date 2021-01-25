package raftlog

import (
	"fmt"

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

func (e *entriesCache) Warmup(entries ...*types.Entry) {
	if len(entries) == 0 {
		return
	}
	e.head = entries[0].Index
	e.tail = e.head
	for _, entry := range entries {
		e.Append(entry)
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

func (e *entriesCache) SliceFrom(start uint64) []*types.Entry {
	if start > e.tail {
		panic("unexpected state: requested index not in the log")
	}
	rst := make([]*types.Entry, 0, e.tail-start)
	e.IterateFrom(start, func(entry *types.Entry) bool {
		rst = append(rst, entry)
		return true
	})
	return rst
}

func (e *entriesCache) Append(entry *types.Entry) {
	pos := e.tail % e.Capacity()
	e.ring[pos] = entry
	e.tail++
	if e.tail-e.head > e.Capacity() {
		e.head++
	}
}

func (e *entriesCache) Get(index uint64) *types.Entry {
	if index < e.head {
		return nil
	}
	if index > e.tail {
		panic(fmt.Sprintf("unexpected state: requested index %d not in the log", index))
	}
	return e.ring[index%e.Capacity()]
}

// DeleteFrom purges entries starting from the specified index.
func (e *entriesCache) DeleteFrom(start uint64) {
	if e.Empty() {
		return
	}
	if start > e.tail {
		panic(fmt.Sprintf("unexpected state: log is smaller is then assumed"))
	}
	if start < e.head {
		start = e.head
	}
	for e.tail > start {
		e.ring[e.tail%e.Capacity()] = nil
		e.tail--
	}
	if start == e.head {
		e.head--
	}
}

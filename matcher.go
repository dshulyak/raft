package raft

import (
	"sort"
)

func newMatch(n int) *match {
	return &match{
		n:     n,
		index: map[NodeID]uint64{},
	}
}

type match struct {
	n     int
	index map[NodeID]uint64
}

// update will update the index for id if it larger then the current one.
func (m *match) update(id NodeID, index uint64) bool {
	prev := m.index[id]
	if prev >= index {
		return false
	}
	m.index[id] = index
	return true
}

func (m *match) reset() {
	for replica := range m.index {
		m.index[replica] = 0
	}
}

// commited returns index that is confirmed by the majority of nodes.
func (m *match) commited() uint64 {
	indexes := make([]uint64, m.n)
	// padded with zero in place of missing indexes
	i := 0
	for _, index := range m.index {
		indexes[i] = index
		i++
	}
	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i] < indexes[j]
	})
	return indexes[(m.n+1)/2]
}

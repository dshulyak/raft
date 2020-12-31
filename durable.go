package raft

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/tysonmote/gommap"
)

const (
	termWidth     = 8
	votedForWidth = 8
	stateWidth    = termWidth + votedForWidth
)

func NewDurableState(f *os.File) (*DurableState, error) {
	if err := f.Truncate(int64(stateWidth)); err != nil {
		return nil, fmt.Errorf("failed to truncate %v: %w", f.Name(), err)
	}
	mmap, err := gommap.Map(f.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED|gommap.MAP_POPULATE)
	if err != nil {
		return nil, fmt.Errorf("failed to mmap %v: %w", f.Name(), err)
	}
	ds := &DurableState{mmap: mmap}
	return ds, ds.Load()
}

type DurableState struct {
	term     uint64
	votedFor NodeID

	// serialize to buf first and copy to mmap to avoid incomplete writes
	buf [stateWidth]byte

	mmap gommap.MMap
}

func (ds *DurableState) Sync() error {
	buf := ds.buf[:]
	binary.LittleEndian.PutUint64(buf[:termWidth], ds.term)
	binary.LittleEndian.PutUint64(buf[termWidth:stateWidth], uint64(ds.votedFor))
	_ = copy(ds.mmap, buf)
	return ds.mmap.Sync(gommap.MS_SYNC)
}

func (ds *DurableState) Load() error {
	ds.term = binary.LittleEndian.Uint64(ds.mmap[:termWidth])
	ds.votedFor = NodeID(binary.LittleEndian.Uint64(ds.mmap[termWidth:stateWidth]))
	return nil
}

func (ds *DurableState) Close() error {
	_ = ds.Sync()
	return ds.mmap.UnsafeUnmap()
}

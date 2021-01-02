package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	ErrStateCorrupted = errors.New("durable state is corrupted")
)

const (
	crcWidth      = 4
	termWidth     = 8
	votedForWidth = 8
	stateWidth    = crcWidth + termWidth + votedForWidth
)

var (
	table = crc32.MakeTable(crc32.Castagnoli)
	empty = [stateWidth]byte{}
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
	binary.LittleEndian.PutUint64(buf[crcWidth:], ds.term)
	binary.LittleEndian.PutUint64(buf[crcWidth+termWidth:], uint64(ds.votedFor))
	code := crc32.Update(0, table, buf[crcWidth:])
	binary.LittleEndian.PutUint32(buf, code)
	_ = copy(ds.mmap, buf)
	return ds.mmap.Sync(gommap.MS_SYNC)
}

func (ds *DurableState) Load() error {
	buf := ds.buf[:]
	copy(buf, ds.mmap)
	if bytes.Compare(buf, empty[:]) == 0 {
		return nil
	}

	code := binary.LittleEndian.Uint32(buf)
	if code != crc32.Update(0, table, buf[crcWidth:]) {
		return ErrStateCorrupted
	}
	ds.term = binary.LittleEndian.Uint64(buf[crcWidth:])
	ds.votedFor = NodeID(binary.LittleEndian.Uint64(buf[crcWidth+termWidth:]))
	return nil
}

func (ds *DurableState) Close() error {
	_ = ds.Sync()
	return ds.mmap.UnsafeUnmap()
}

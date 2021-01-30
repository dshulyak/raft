package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

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
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if stat.Size() == 0 {
		dir, err := os.OpenFile(filepath.Dir(f.Name()), 0, os.ModeDir)
		if err != nil {
			return nil, fmt.Errorf("%w: can't open dir %s", err, dir.Name())
		}
		defer dir.Close()
		if err := dir.Sync(); err != nil {
			return nil, fmt.Errorf("%w: can't sync dir %s", err, dir.Name())
		}

		if err := f.Truncate(int64(stateWidth)); err != nil {
			return nil, fmt.Errorf("%w: failed to truncate %s", err, f.Name())
		}
		if err := f.Sync(); err != nil {
			return nil, fmt.Errorf("%w: can't sync durable state file %s", err, f.Name())
		}
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
	Term     uint64
	VotedFor NodeID

	// serialize to buf first and copy to mmap to avoid incomplete writes
	buf [stateWidth]byte

	mmap gommap.MMap
}

func (ds *DurableState) Sync() error {
	buf := ds.buf[:]
	binary.LittleEndian.PutUint64(buf[crcWidth:], ds.Term)
	binary.LittleEndian.PutUint64(buf[crcWidth+termWidth:], uint64(ds.VotedFor))
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
	ds.Term = binary.LittleEndian.Uint64(buf[crcWidth:])
	ds.VotedFor = NodeID(binary.LittleEndian.Uint64(buf[crcWidth+termWidth:]))
	return nil
}

func (ds *DurableState) Close() error {
	_ = ds.Sync()
	return ds.mmap.UnsafeUnmap()
}

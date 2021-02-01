package state

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/dshulyak/raft/types"
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

func NewStore(f *os.File) (*Store, error) {
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
	return &Store{mmap: mmap}, nil
}

type State struct {
	Term     uint64
	VotedFor types.NodeID
}

type Store struct {
	buf [stateWidth]byte

	mmap gommap.MMap
}

func (ds *Store) Save(state *State) error {
	buf := ds.buf[:]
	binary.LittleEndian.PutUint64(buf[crcWidth:], state.Term)
	binary.LittleEndian.PutUint64(buf[crcWidth+termWidth:], uint64(state.VotedFor))
	code := crc32.Update(0, table, buf[crcWidth:])
	binary.LittleEndian.PutUint32(buf, code)
	_ = copy(ds.mmap, buf)
	return ds.mmap.Sync(gommap.MS_SYNC)
}

func (ds *Store) Load(state *State) error {
	buf := ds.buf[:]
	copy(buf, ds.mmap)
	if bytes.Compare(buf, empty[:]) == 0 {
		return nil
	}

	code := binary.LittleEndian.Uint32(buf)
	if code != crc32.Update(0, table, buf[crcWidth:]) {
		return ErrStateCorrupted
	}

	state.Term = binary.LittleEndian.Uint64(buf[crcWidth:])
	state.VotedFor = types.NodeID(binary.LittleEndian.Uint64(buf[crcWidth+termWidth:]))
	return nil
}

func (ds *Store) Close() error {
	return ds.mmap.UnsafeUnmap()
}

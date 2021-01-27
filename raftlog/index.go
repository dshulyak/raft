package raftlog

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"

	"github.com/tysonmote/gommap"
	"go.uber.org/zap"
)

const (
	defaultIndexSize = 10 << 20 // 10mb
)

const (
	offsetWidth uint64 = 8
	lengthWidth uint64 = 8
	entryWidth         = offsetWidth + lengthWidth
)

type indexEntry struct {
	Offset uint64
	Length uint64
}

func (i *indexEntry) Encode(to []byte) error {
	binary.BigEndian.PutUint64(to, i.Offset)
	binary.BigEndian.PutUint64(to[offsetWidth:], i.Length)
	return nil
}

func (i *indexEntry) Decode(from []byte) error {
	i.Offset = binary.BigEndian.Uint64(from)
	i.Length = binary.BigEndian.Uint64(from[offsetWidth:])
	return nil
}

func (i *indexEntry) Size() uint64 {
	return entryWidth
}

func (i indexEntry) String() string {
	return fmt.Sprintf("offset=%d length=%d", i.Offset, i.Length)
}

func openIndex(logger *zap.SugaredLogger, name string, defaultIndexSize int) (*index, error) {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := int(stat.Size())
	if stat.Size() == 0 {
		size = defaultIndexSize
	}

	idx := &index{logger: logger, f: f}
	if err := idx.setup(size); err != nil {
		return nil, err
	}
	return idx, nil
}

// index is a mapping from log index to a log offset in the file.
type index struct {
	logger *zap.SugaredLogger

	nextOffset uint64
	// configured at the time of setup. might not be 0 if snapshot exists
	firstLogIndex uint64
	// incremented on every insertion
	nextLogIndex uint64
	// grow a mmap if nextLogIndex overflows maxLogIndex
	maxLogIndex uint64
	// pointer to the mmaped region
	mmap gommap.MMap
	// file handle
	f *os.File
}

func (i *index) setup(size int) error {
	if err := i.f.Truncate(int64(size)); err != nil {
		return fmt.Errorf("can't truncate file %s: %w", i.f.Name(), err)
	}

	mmap, err := gommap.Map(i.f.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED|gommap.MAP_POPULATE)
	if err != nil {
		return fmt.Errorf("failed to mmap: %w", err)
	}
	if err := mmap.Advise(gommap.MADV_SEQUENTIAL); err != nil {
		return fmt.Errorf("syscall MADVISE failed: %w", err)
	}
	i.mmap = mmap

	if size != 0 {
		i.maxLogIndex = uint64(size) / entryWidth
	}
	idx := sort.Search(int(i.maxLogIndex), func(j int) bool {
		entry := i.get(uint64(j))
		return entry.Offset == 0 && entry.Length == 0
	})
	i.nextLogIndex = uint64(idx)
	if idx > 0 {
		last := i.get(i.nextLogIndex - 1)
		i.nextOffset = last.Offset + last.Length
	}
	return nil
}

func (i *index) grow() error {
	size := len(i.mmap) * 2
	if err := i.mmap.UnsafeUnmap(); err != nil {
		return fmt.Errorf("failed to unmap: %w", err)
	}
	if err := i.setup(size); err != nil {
		return err
	}
	return nil
}

func (i *index) position(li uint64) uint64 {
	return (li - i.firstLogIndex) * entryWidth
}

func (i *index) Get(li uint64) indexEntry {
	return i.get(li)
}

func (i *index) get(li uint64) (entry indexEntry) {
	if li < i.firstLogIndex {
		return
	}
	pos := i.mmap[i.position(li):]
	_ = entry.Decode(pos[:entryWidth])
	return
}

func (i *index) IsEmpty() bool {
	return i.nextLogIndex == 0
}

func (i *index) LastIndex() indexEntry {
	if i.nextLogIndex == 0 {
		return indexEntry{}
	}
	return i.get(i.nextLogIndex - 1)
}

func (i *index) Append(size uint64) error {
	if i.nextLogIndex == i.maxLogIndex {
		if err := i.grow(); err != nil {
			return err
		}
	}
	pos := i.mmap[i.position(i.nextLogIndex):]
	if err := (&indexEntry{Offset: i.nextOffset, Length: size}).Encode(pos[:entryWidth]); err != nil {
		return err
	}
	i.nextLogIndex++
	i.nextOffset += size
	return nil
}

func (i *index) Truncate(start uint64) uint64 {
	idx := i.Get(start)
	nextOffset := idx.Offset

	offset := i.position(start)
	end := i.position(i.nextLogIndex)
	for off := offset; off < end; off++ {
		i.mmap[off] = 0
	}
	i.nextLogIndex = start
	i.nextOffset = nextOffset
	return i.nextOffset
}

func (i *index) Close() error {
	i.logger.Debugw("closing index file")
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return fmt.Errorf("syscall MSYNC failed: %w", err)
	}
	if err := i.mmap.UnsafeUnmap(); err != nil {
		return fmt.Errorf("failed to unmap: %w", err)
	}
	i.mmap = nil
	if err := i.f.Sync(); err != nil {
		return fmt.Errorf("failed to fsync %v: %w", i.f.Name(), err)
	}
	return nil
}

func (i *index) Delete() error {
	if err := i.Close(); err != nil {
		return err
	}
	return os.Remove(i.f.Name())
}

func (i *index) Sync() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return fmt.Errorf("syscall MSYNC failed: %w", err)
	}
	return nil
}

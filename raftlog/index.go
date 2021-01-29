package raftlog

import (
	"encoding/binary"
	"fmt"
	"os"

	"github.com/tysonmote/gommap"
	"go.uber.org/zap"
)

const (
	defaultIndexSize = 10 << 20 // 10mb
)

const (
	segmentWidth = 2
	offsetWidth  = 4
	entryWidth   = segmentWidth + offsetWidth
)

var (
	emptyEntry = [entryWidth]byte{}
)

type indexEntry struct {
	Segment uint16
	Offset  uint32
}

func (i *indexEntry) Encode(to []byte) error {
	binary.BigEndian.PutUint16(to, i.Segment)
	binary.BigEndian.PutUint32(to[segmentWidth:], i.Offset)
	return nil
}

func (i *indexEntry) Decode(from []byte) error {
	i.Segment = binary.BigEndian.Uint16(from)
	i.Offset = binary.BigEndian.Uint32(from[segmentWidth:])
	return nil
}

func (i indexEntry) String() string {
	return fmt.Sprintf("segment%d offset=%d", i.Segment, i.Offset)
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
	size := stat.Size()
	if stat.Size() == 0 {
		size = int64(defaultIndexSize)
	} else {
		if err := f.Truncate(0); err != nil {
			return nil, err
		}
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

	// pointer to the mmaped region
	mmap gommap.MMap
	// file handle
	f *os.File
}

func (i *index) setup(size int64) error {
	if err := i.f.Truncate(int64(size)); err != nil {
		return fmt.Errorf("can't truncate file %s: %w", i.f.Name(), err)
	}

	mmap, err := gommap.Map(i.f.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED|gommap.MAP_POPULATE,
	)
	if err != nil {
		return fmt.Errorf("failed to mmap: %w", err)
	}
	if err := mmap.Advise(gommap.MADV_SEQUENTIAL); err != nil {
		return fmt.Errorf("syscall MADVISE failed: %w", err)
	}

	i.mmap = mmap
	return nil
}

func (i *index) grow() error {
	size := len(i.mmap) * 2
	if err := i.mmap.UnsafeUnmap(); err != nil {
		return fmt.Errorf("failed to unmap: %w", err)
	}
	if err := i.setup(int64(size)); err != nil {
		return err
	}
	return nil
}

func (i *index) position(index uint64) int64 {
	return int64((index - 1) * entryWidth)
}

func (i *index) Get(index uint64) (entry indexEntry) {
	pos := i.position(index)
	if pos+int64(entryWidth) > int64(len(i.mmap)) {
		i.logger.Panicw("invalid index request", "index", index)
	}
	_ = entry.Decode(i.mmap[pos:])
	return
}

func (i *index) Set(index uint64, entry *indexEntry) error {
	pos := i.position(index)
	if pos+int64(entryWidth) > int64(len(i.mmap)) {
		if err := i.grow(); err != nil {
			return err
		}
	}
	if err := entry.Encode(i.mmap[pos:]); err != nil {
		return err
	}
	return nil
}

func (i *index) Close() error {
	if err := i.mmap.UnsafeUnmap(); err != nil {
		return fmt.Errorf("failed to unmap: %w", err)
	}
	i.mmap = nil
	return nil
}

func (i *index) Delete() error {
	if err := i.Close(); err != nil {
		return err
	}
	return os.Remove(i.f.Name())
}

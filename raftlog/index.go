package raftlog

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"sync"

	"github.com/tysonmote/gommap"
	"go.uber.org/zap"
)

const (
	initialIndexSize = 1 << 20
)

const (
	offsetWidth uint64 = 8
	lengthWidth uint64 = 8
	entryWidth         = offsetWidth + lengthWidth
)

type IndexEntry struct {
	Offset uint64
	Length uint64
}

func (i *IndexEntry) Encode(to []byte) error {
	binary.BigEndian.PutUint64(to, i.Offset)
	binary.BigEndian.PutUint64(to[offsetWidth:], i.Length)
	return nil
}

func (i *IndexEntry) Decode(from []byte) error {
	i.Offset = binary.BigEndian.Uint64(from)
	i.Length = binary.BigEndian.Uint64(from[offsetWidth:])
	return nil
}

func (i *IndexEntry) Size() uint64 {
	return entryWidth
}

func (i IndexEntry) String() string {
	return fmt.Sprintf("offset=%d length=%d", i.Offset, i.Length)
}

type IndexOptions struct {
	File        string
	DefaultSize int
}

func NewIndex(log *zap.Logger, nextOffset uint64, opts *IndexOptions) (*Index, error) {
	var (
		f      *os.File
		size   int
		err    error
		logger = log.Sugar()
	)
	if opts == nil || len(opts.File) == 0 {
		f, err = ioutil.TempFile("", "log-index-file-XXX")
	} else {
		f, err = os.OpenFile(opts.File, os.O_CREATE|os.O_RDWR, 0o644)
	}
	if err != nil {
		return nil, err
	}
	logger.Debugw("opened index file", "path", f.Name())
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if stat.Size() == 0 {
		if opts == nil || opts.DefaultSize == 0 {
			size = initialIndexSize
		} else {
			size = opts.DefaultSize
		}
	}
	index := &Index{logger: logger, f: f, initialOffset: nextOffset, nextOffset: nextOffset}
	if err := index.setup(size); err != nil {
		return nil, err
	}
	index.init()
	return index, nil
}

// Index is a mapping from log index to a log offset in the file.
type Index struct {
	logger *zap.SugaredLogger

	mu            sync.RWMutex
	initialOffset uint64
	nextOffset    uint64
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

func (i *Index) setup(size int) error {
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
	return nil
}

func (i *Index) init() {
	idx := sort.Search(int(i.maxLogIndex), func(j int) bool {
		entry := i.Get(uint64(j))
		return entry.Offset == 0 && entry.Length == 0
	})
	i.nextLogIndex = uint64(idx)
}

func (i *Index) grow() error {
	size := len(i.mmap) * 2
	if err := i.mmap.UnsafeUnmap(); err != nil {
		return fmt.Errorf("failed to unmap: %w", err)
	}
	if err := i.setup(size); err != nil {
		return err
	}
	return nil
}

func (i *Index) position(li uint64) uint64 {
	return (li - i.firstLogIndex) * entryWidth
}

func (i *Index) Get(li uint64) IndexEntry {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.get(li)
}

func (i *Index) get(li uint64) (entry IndexEntry) {
	if li < i.firstLogIndex {
		return
	}
	pos := i.mmap[i.position(li):]
	_ = entry.Decode(pos[:entryWidth])
	return
}

func (i *Index) IsEmpty() bool {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.nextLogIndex == 0
}

func (i *Index) LastIndex() IndexEntry {
	i.mu.RLock()
	defer i.mu.RUnlock()
	if i.nextLogIndex == 0 {
		return IndexEntry{}
	}
	return i.get(i.nextLogIndex - 1)
}

func (i *Index) Append(entrySize uint64) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.nextLogIndex == i.maxLogIndex {
		if err := i.grow(); err != nil {
			return err
		}
	}
	pos := i.mmap[i.position(i.nextLogIndex):]
	if err := (&IndexEntry{Offset: i.nextOffset, Length: entrySize}).Encode(pos[:entryWidth]); err != nil {
		return err
	}
	i.nextLogIndex++
	i.nextOffset += entrySize
	return nil
}

func (i *Index) Truncate(start uint64) uint64 {
	idx := i.Get(start)
	nextOffset := idx.Offset

	i.mu.Lock()
	defer i.mu.Unlock()

	offset := i.position(start)
	end := i.position(i.nextLogIndex)
	for off := offset; off < end; off++ {
		i.mmap[off] = 0
	}
	i.nextLogIndex = start
	i.nextOffset = nextOffset
	return i.nextOffset
}

func (i *Index) Close() error {
	i.logger.Debugw("closing index file")
	i.mu.Lock()
	defer i.mu.Unlock()
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

func (i *Index) Delete() error {
	if err := i.Close(); err != nil {
		return err
	}
	return os.Remove(i.f.Name())
}

func (i *Index) Sync() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return fmt.Errorf("syscall MSYNC failed: %w", err)
	}
	return nil
}

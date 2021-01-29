package raftlog

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/dshulyak/raft/types"
	"go.uber.org/zap"
)

var (
	ErrEmptyLog        = errors.New("log is empty")
	ErrEntryTooLarge   = errors.New("entry too large")
	ErrEntryFromFuture = errors.New("entry from future")
)

const (
	// number of entries stored in memory
	defaultCacheEntries = 4096
	// number of in-memory entries that are not flushed on disk
	// can't be larger then defaultCacheEntries
	defaultMaxDirtyEntries = 4096

	maxSegmentSize = math.MaxUint32
)

func notZero(i uint64) {
	if i == 0 {
		panic("zero refers to an empty log")
	}
}

type Entry = types.Entry

type Option func(*Storage) error

func WithCache(size int) Option {
	return func(s *Storage) error {
		s.cache = newCache(size)
		return nil
	}
}

func WithTempDir() Option {
	return func(s *Storage) error {
		dir, err := ioutil.TempDir("", "raft-store-")
		if err != nil {
			return err
		}
		s.conf.dataDir = dir
		return nil
	}
}

func WithDir(dir string) Option {
	return func(s *Storage) error {
		s.conf.dataDir = dir
		return nil
	}
}

func WithIndexSize(size int) Option {
	return func(s *Storage) error {
		s.conf.defaultIndexSize = size
		return nil
	}
}

func WithSegmentSize(size int) Option {
	return func(s *Storage) error {
		s.conf.maxSegmentSize = size
		return nil
	}
}

func WithLogger(log *zap.Logger) Option {
	return func(s *Storage) error {
		s.logger = log.Sugar()
		return nil
	}
}

type config struct {
	dataDir          string
	defaultIndexSize int
	maxSegmentSize   int
	maxDirtyEntries  int
	maxEntrySize     int
	cacheSize        int
}

var defaultConfig = config{
	maxSegmentSize:   512 << 20,
	defaultIndexSize: defaultIndexSize,
	maxDirtyEntries:  defaultMaxDirtyEntries,
	maxEntrySize:     4096 - int(crcSize),
	cacheSize:        defaultMaxDirtyEntries,
}

func New(opts ...Option) (*Storage, error) {
	st := &Storage{logger: zap.NewNop().Sugar(), conf: defaultConfig}

	for _, opt := range opts {
		if err := opt(st); err != nil {
			return nil, err
		}
	}
	if st.conf.maxSegmentSize > maxSegmentSize {
		return nil, fmt.Errorf("segment size can't be larger then %v", maxSegmentSize)
	}

	if st.cache == nil {
		st.cache = newCache(st.conf.cacheSize)
	}
	segs, err := scanSegments(st.logger, st.conf)
	if err != nil {
		return nil, err
	}
	st.segs = segs

	idx, err := openIndex(st.logger,
		filepath.Join(st.conf.dataDir, "raft.idx"),
		st.conf.defaultIndexSize,
	)
	if err != nil {
		return nil, err
	}
	st.idx = idx

	e1 := st.segs.scan(func(n int, offset int64, entry *types.Entry) bool {
		st.lastIndex = entry.Index
		return st.idx.Set(entry.Index, &indexEntry{
			Segment: uint16(n),
			Offset:  uint32(offset),
		}) == nil
	})
	if e1 != nil {
		return nil, e1
	}
	return st, err
}

type Storage struct {
	logger *zap.SugaredLogger

	conf config

	mu sync.RWMutex

	// last appended index
	lastIndex uint64
	flushed   uint64

	// cache is required for buffering uncommited entries.
	cache *entriesCache

	idx  *index
	segs *segments
}

func (s *Storage) Get(i uint64) (*types.Entry, error) {
	notZero(i)
	s.mu.RLock()
	defer s.mu.RUnlock()

	if i > s.lastIndex {
		return nil, ErrEntryFromFuture
	}

	entry := s.cache.Get(i)
	if entry != nil {
		return entry, nil
	}

	ie := s.idx.Get(i)
	return s.segs.get(&ie)
}

func (s *Storage) IsEmpty() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastIndex == 0
}

func (s *Storage) Last() (entry *types.Entry, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.lastIndex == 0 {
		err = ErrEmptyLog
		return
	}
	entry = s.cache.Get(s.lastIndex)
	if entry != nil {
		return
	}

	ie := s.idx.Get(s.lastIndex)
	return s.segs.get(&ie)
}

func (s *Storage) Append(entry *types.Entry) error {
	if entry.Size() > s.conf.maxEntrySize {
		return fmt.Errorf("%w: encoded size should not be larger then %d",
			ErrEntryTooLarge, s.conf.maxEntrySize)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.lastIndex = entry.Index

	s.cache.Add(entry)
	if s.lastIndex-s.flushed == uint64(s.conf.maxDirtyEntries) {
		return s.flush()
	}
	return nil
}

func (s *Storage) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sync()
}

func (s *Storage) flush() (err error) {
	if s.flushed == s.lastIndex {
		return nil
	}
	s.cache.Iterate(s.flushed+1, s.lastIndex, func(entry *types.Entry) bool {
		var (
			offset int
		)
		offset, _, err = s.segs.append(entry)
		if err != nil {
			return false
		}
		err = s.idx.Set(entry.Index, &indexEntry{
			Segment: uint16(s.segs.active.n),
			Offset:  uint32(offset),
		})
		return err == nil
	})
	if err != nil {
		return err
	}
	return
}

func (s *Storage) sync() (err error) {
	if err := s.flush(); err != nil {
		return err
	}
	return s.segs.sync()
}

func (s *Storage) Location() string {
	return s.segs.dir.Name()
}

func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Debug("closing storage")
	return s.segs.close()
}

func (s *Storage) Delete() error {
	s.Close()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Debug("deleting storage", "directory", s.conf.dataDir)

	return os.RemoveAll(s.conf.dataDir)
}

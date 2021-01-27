package raftlog

import (
	"errors"
	"io/ioutil"
	"os"
	"sync"

	"github.com/dshulyak/raft/types"
	"go.uber.org/zap"
)

var (
	ErrEmptyLog = errors.New("log is empty")
)

const (
	// number of entries stored in memory
	defaultCacheEntries = 4096
	// number of in-memory entries that are not flushed on disk
	// can't be larger then defaultCacheEntries
	defaultMaxDirtyEntries = 4096
)

func notZero(i uint64) {
	if i == 0 {
		panic("zero refers to an empty log")
	}
}

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
}

var defaultConfig = config{
	maxSegmentSize:   512 << 20,
	defaultIndexSize: defaultIndexSize,
	maxDirtyEntries:  defaultMaxDirtyEntries,
}

func New(opts ...Option) (*Storage, error) {
	st := &Storage{logger: zap.NewNop().Sugar(), conf: defaultConfig}

	for _, opt := range opts {
		if err := opt(st); err != nil {
			return nil, err
		}
	}

	if st.cache == nil {
		st.cache = newCache(defaultCacheEntries)
	}
	seg, err := openSegment(st.logger, st.conf, 0)
	if err != nil {
		return nil, err
	}
	st.active = seg
	st.dirty = st.active.lastIndex

	if st.active.lastIndex > 0 {
		var (
			start uint64
			num   = st.cache.Capacity()
		)
		if st.active.lastIndex > st.cache.Capacity() {
			start = st.active.lastIndex - st.cache.Capacity()
		} else {
			num = st.active.lastIndex
		}
		entries := make([]*types.Entry, num)
		for i := range entries {
			entries[i], err = st.active.get(start + uint64(i) - 1)
			if err != nil {
				return nil, err
			}
		}
		st.cache.Preload(entries)
	}
	return st, nil
}

type Storage struct {
	logger *zap.SugaredLogger

	conf config

	mu sync.RWMutex

	// last flushed index. starts at 1
	flushed uint64
	// last appended index. starts at 1. never lower than flushed.
	dirty uint64
	cache *entriesCache

	active *segment
}

func (s *Storage) Get(i uint64) (*types.Entry, error) {
	notZero(i)
	s.mu.RLock()
	defer s.mu.RUnlock()

	if i > s.dirty {
		return nil, ErrEntryNotFound
	}

	entry := s.cache.Get(i - 1)
	if entry != nil {
		return entry, nil
	}

	// TODO there must be an API to load many entries with one system call
	// probably with a range query
	return s.active.get(i - 1)
}

func (s *Storage) IsEmpty() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.dirty == 0
}

func (s *Storage) Last() (entry *types.Entry, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.dirty == 0 {
		err = ErrEmptyLog
		return
	}
	return s.cache.Get(s.dirty - 1), nil
}

func (s *Storage) Append(entry *Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.dirty++
	s.cache.Append(entry)
	if s.dirty-s.active.lastIndex == uint64(s.conf.maxDirtyEntries) {
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
	s.cache.IterateFrom(s.flushed+1, func(entry *types.Entry) bool {
		err = s.active.append(entry)
		return err == nil
	})
	return
}

func (s *Storage) sync() (err error) {
	if err := s.flush(); err != nil {
		return err
	}
	return s.active.sync()
}

func (s *Storage) DeleteFrom(start uint64) error {
	notZero(start)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.cache.DeleteFrom(start - 1)

	if err := s.active.deleteFrom(start - 1); err != nil {
		return err
	}

	s.dirty = start - 1
	return nil
}

func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Debug("closing storage")
	return s.active.close()
}

func (s *Storage) Delete() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Debug("deleting storage", "directory", s.conf.dataDir)

	return os.RemoveAll(s.conf.dataDir)
}

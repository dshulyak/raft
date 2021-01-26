package raftlog

import (
	"errors"
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

func New(logger *zap.Logger, iopts *IndexOptions, lopts *LogOptions, opts ...Option) (*Storage, error) {
	index, err := NewIndex(logger, iopts)
	if err != nil {
		return nil, err
	}
	var last *IndexEntry
	if !index.IsEmpty() {
		lastIndex := index.LastIndex()
		last = &lastIndex
	}
	// TODO index might have more recent data then the log
	// in such case we need to find last **fully** written log
	// and truncate both index and log file after that log
	log, err := NewLog(logger, last, lopts)
	if err != nil {
		return nil, err
	}

	st := &Storage{logger: logger.Sugar(), index: index, log: log}

	for _, opt := range opts {
		if err := opt(st); err != nil {
			return nil, err
		}
	}

	if st.dirtyLimit == 0 {
		st.dirtyLimit = defaultMaxDirtyEntries
	}
	if st.cache == nil {
		st.cache = newCache(defaultCacheEntries)
	}

	var entry types.Entry
	if last != nil {
		if err := st.log.Get(last, &entry); err != nil {
			return nil, err
		}
	}
	st.flushed = entry.Index
	st.dirty = st.flushed

	if st.flushed > 0 {
		var start uint64
		d := st.flushed - st.cache.Capacity()
		if d >= 0 {
			start = d
		}
		entries := make([]*types.Entry, d)
		for i := range entries {
			entries[i], err = st.getFlushed(start + uint64(i) - 1)
			if err != nil {
				return nil, err
			}
		}
		st.cache.Warmup(entries)
	}
	return st, nil
}

type Storage struct {
	logger *zap.SugaredLogger

	mu sync.RWMutex

	dirtyLimit uint64
	// last flushed index. starts at 1
	flushed uint64
	// last appended index. starts at 1. never lower than flushed.
	dirty uint64
	cache *entriesCache

	index *Index
	log   *Log
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
	return s.getFlushed(i - 1)
}

func (s *Storage) getFlushed(i uint64) (entry *types.Entry, err error) {
	idx := s.index.Get(i)
	if idx.Length == 0 && idx.Offset == 0 {
		err = ErrEntryNotFound
		return
	}
	entry = &types.Entry{}
	err = s.log.Get(&idx, entry)
	return
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
	if s.dirty-s.flushed == s.dirtyLimit {
		return s.sync()
	}
	return nil
}

func (s *Storage) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sync()
}

func (s *Storage) sync() (err error) {
	s.cache.IterateFrom(s.flushed+1, func(entry *types.Entry) bool {
		var size uint64
		size, err = s.log.Append(entry)
		if err != nil {
			return false
		}
		err = s.index.Append(size)
		if err != nil {
			return false
		}
		return true
	})
	if err != nil {
		return err
	}

	if err := s.log.Sync(); err != nil {
		return err
	}
	if err := s.index.Sync(); err != nil {
		return err
	}
	s.flushed = s.dirty
	return nil
}

func (s *Storage) DeleteFrom(start uint64) error {
	notZero(start)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.cache.DeleteFrom(start - 1)

	offset := s.index.Truncate(start - 1)
	if err := s.log.Truncate(offset); err != nil {
		return err
	}

	s.flushed = uint64(start - 1)
	s.dirty = s.flushed

	return nil
}

func (s *Storage) Close() error {
	s.logger.Debug("closing storage")

	s.mu.Lock()
	defer s.mu.Unlock()

	// use multierr package
	err1 := s.index.Close()
	err2 := s.log.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func (s *Storage) Delete() error {
	s.logger.Debug("deleting storage")

	s.mu.Lock()
	defer s.mu.Unlock()

	err1 := s.index.Delete()
	err2 := s.log.Delete()
	if err1 != nil {
		return err1
	}
	return err2
}

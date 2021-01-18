package raftlog

import (
	"errors"
	"sync"

	"go.uber.org/zap"
)

var (
	ErrEmptyLog = errors.New("log is empty")
)

func New(logger *zap.Logger, iopts *IndexOptions, lopts *LogOptions) (*Storage, error) {
	index, err := NewIndex(logger, logHeaderWidth, iopts)
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
	return &Storage{logger: logger.Sugar(), index: index, log: log}, nil
}

type Storage struct {
	logger *zap.SugaredLogger

	// TODO the only method that need to be atomic is Append
	// change index.Append to index.Set(int, IndexEntry)
	mu sync.Mutex

	index *Index
	log   *Log
}

func (s *Storage) Get(i int) (entry Entry, err error) {
	// TODO change index to int
	idx := s.index.Get(uint64(i))
	if idx.Length == 0 && idx.Offset == 0 {
		err = ErrEntryNotFound
		return
	}
	err = s.log.Get(&idx, &entry)
	return
}

func (s *Storage) IsEmpty() bool {
	return s.index.IsEmpty()
}

func (s *Storage) Last() (entry Entry, err error) {
	if s.index.IsEmpty() {
		err = ErrEmptyLog
		return
	}
	idx := s.index.LastIndex()
	err = s.log.Get(&idx, &entry)
	return
}

func (s *Storage) Append(entry *Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	size, err := s.log.Append(entry)
	if err != nil {
		return err
	}
	return s.index.Append(size)
}

func (s *Storage) Sync() error {
	if err := s.log.Sync(); err != nil {
		return err
	}
	return s.index.Sync()
}

func (s *Storage) DeleteFrom(start int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	offset := s.index.Truncate(uint64(start))
	return s.log.Truncate(offset)
}

// Iterate from start to the end. If end is 0 iterator will iterate til the last indexed log. Both boundaries are inclusive.
func (s *Storage) Iterate(from, to int) *Iterator {
	idx := s.index.Get(uint64(from))
	return &Iterator{
		index:   s.index,
		scanner: s.log.Scanner(&idx),
		from:    from,
		to:      to,
		current: from,
	}
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

type Iterator struct {
	index    *Index
	scanner  *LogScanner
	from, to int

	current int
	err     error
	entry   *Entry
}

func (i *Iterator) Error() error {
	return i.err
}

func (i *Iterator) Next() bool {
	if i.current > i.to && i.to != 0 || i.err != nil {
		return false
	}
	idx := i.index.Get(uint64(i.current))
	if idx.Offset == 0 {
		i.to = -1
		return false
	}
	i.entry, i.err = i.scanner.Scan(idx.Length)
	if errors.Is(i.err, ErrLogEnd) {
		i.to = -1
		i.err = nil
		return false
	} else if i.err != nil {
		return false
	}
	i.current++
	return true
}

func (i *Iterator) Entry() *Entry {
	return i.entry
}

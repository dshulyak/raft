package raftlog

import (
	"fmt"
	"path/filepath"

	"github.com/dshulyak/raft/types"
	"go.uber.org/zap"
)

const (
	// segmentCreated is set only once when segment is created.
	// removed when directory is fsynced.
	segmentCreated uint = 1 << iota
	// segnentUpdated is set every time new log item is appended.
	// remove on log and index fsync.
	segmentUpdated
)

func openSegment(logger *zap.SugaredLogger, conf config, index uint64) (*segment, error) {
	seg := &segment{
		logger:    logger,
		maxSize:   conf.maxSegmentSize,
		prevIndex: index,
		lastIndex: index,
	}
	var err error
	seg.idx, err = openIndex(logger,
		filepath.Join(
			conf.dataDir,
			fmt.Sprintf("%s-%d.%s", indexPrefix, index, indexExt),
		),
		conf.defaultIndexSize,
	)
	if err != nil {
		return nil, err
	}
	seg.log, err = openLog(logger,
		filepath.Join(conf.dataDir,
			fmt.Sprintf("%s-%d.%s", logPrefix, index, logExt),
		),
	)
	if err != nil {
		return nil, err
	}
	if !seg.idx.IsEmpty() {
		last := seg.idx.LastIndex()
		entry, err := seg.log.Get(&last)
		if err != nil {
			return nil, err
		}
		seg.lastIndex = entry.Index
	}
	return seg, nil
}

type segment struct {
	logger *zap.SugaredLogger

	maxSize int
	size    int

	state uint

	// prevIndex is the last index in the previous segment.
	// 0 for the first segment.
	prevIndex uint64
	// lastIndex is the last index in the current segment.
	// initialized to prevIndex.
	// number of appended entries is equal to lastIndex - prevIndex
	lastIndex uint64

	idx *index
	log *log
}

func (s *segment) hasSpace(entry *types.Entry) bool {
	return s.size+onDiskSize(entry) <= s.maxSize
}

func (s *segment) isCreated() bool {
	return s.state&segmentCreated > 0
}

func (s *segment) isUpdated() bool {
	return s.state&segmentUpdated > 0
}

func (s *segment) isEmpty() bool {
	return s.lastIndex == s.prevIndex
}

func (s *segment) append(entry *types.Entry) error {
	if !s.hasSpace(entry) {
		panic("segment is full")
	}

	esize, err := s.log.Append(entry)
	if err != nil {
		return err
	}
	err = s.idx.Append(esize)
	if err != nil {
		return err
	}

	s.lastIndex++
	s.state |= segmentUpdated
	s.size += int(esize)
	return nil
}

func (s *segment) entries() uint64 {
	return s.lastIndex - s.prevIndex
}

func (s *segment) get(i uint64) (*types.Entry, error) {
	idx := s.idx.Get(i - s.prevIndex)
	if idx.Length == 0 && idx.Offset == 0 {
		return nil, ErrEntryNotFound
	}
	return s.log.Get(&idx)
}

func (s *segment) sync() error {
	if !s.isUpdated() {
		return nil
	}
	if err := s.log.Sync(); err != nil {
		return err
	}
	if err := s.idx.Sync(); err != nil {
		return err
	}
	s.state ^= segmentUpdated
	s.state &= ^(segmentCreated) // do not flip, it is set only once
	return nil
}

func (s *segment) deleteFrom(start uint64) error {
	from := start
	if start < s.prevIndex {
		from = 0
	} else {
		from -= s.prevIndex
	}

	offset := s.idx.Truncate(from)
	if err := s.log.Truncate(offset); err != nil {
		return err
	}

	s.size -= int(uint64(s.size) - offset)
	if start < s.prevIndex {
		s.lastIndex = s.prevIndex
	} else {
		s.lastIndex = start - 1
	}
	return nil
}

func (s *segment) close() error {
	// use multierr package
	err1 := s.idx.Close()
	err2 := s.log.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

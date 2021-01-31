package raftlog

import (
	"errors"
	"fmt"

	"github.com/dshulyak/raft/types"
	"go.uber.org/zap"
)

var (
	// ScanCanceled returned if scan was terminated.
	ScanCanceled = errors.New("scan canceled")
)

const (
	// segmentCreated is set only once when segment is created.
	// removed when directory is fsynced.
	segmentCreated uint = 1 << iota
	// segnentUpdated is set every time new log item is appended.
	// remove on log and index fsync.
	segmentUpdated
)

func openSegment(logger *zap.SugaredLogger, conf config, n int) (*segment, error) {
	seg := &segment{
		n:       n,
		logger:  logger,
		maxSize: conf.maxSegmentSize,
	}
	var err error
	seg.log, err = openLog(logger, segmentPath(conf.dataDir, n), int64(conf.maxSegmentSize))
	if err != nil {
		return nil, err
	}

	size, err := seg.log.Size()
	if err != nil {
		return nil, err
	}
	if size == 0 {
		seg.state |= segmentCreated
	}
	seg.size = int(size)
	return seg, nil
}

type segment struct {
	n int

	logger *zap.SugaredLogger

	maxSize int
	size    int

	state uint

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

func (s *segment) String() string {
	return fmt.Sprintf("segment %d", s.n)
}

func (s *segment) append(entry *types.Entry) (int, int, error) {
	if !s.hasSpace(entry) {
		panic("segment is full")
	}

	esize, err := s.log.Append(entry)
	if err != nil {
		return 0, 0, err
	}

	s.state |= segmentUpdated
	offset := s.size
	s.size += esize
	return offset, esize, nil
}

func (s *segment) get(offset uint32) (*types.Entry, error) {
	return s.log.Get(offset)
}

func (s *segment) truncate(offset int64) error {
	s.state &= ^(segmentUpdated)
	return s.log.truncate(offset)
}

func (s *segment) scanner(offset uint32) *scanner {
	return s.log.scanner(offset)
}

func (s *segment) sync() error {
	if err := s.log.Sync(); err != nil {
		return err
	}
	s.state &= ^(segmentUpdated)
	return nil
}

func (s *segment) close() error {
	return s.log.Close()
}

func (s *segment) delete() error {
	s.logger.Debugw("deleting segment", "segment", s)
	if err := s.close(); err != nil {
		return err
	}
	return s.log.Delete()
}

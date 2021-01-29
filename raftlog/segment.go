package raftlog

import (
	"errors"
	"fmt"
	"io"
	"path/filepath"

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
	seg.log, err = openLog(logger,
		filepath.Join(conf.dataDir,
			fmt.Sprintf("%s%d%s", logPrefix, n, logExt),
		),
	)
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

func (s *segment) scan(f func(int, int64, *types.Entry) bool) error {
	scanner := s.log.scanner(0)
	defer scanner.close()
	for {
		offset := scanner.offset
		entry, err := scanner.next()

		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			// TODO unexpected EOF
			return err
		}
		if !f(s.n, offset, entry) {
			return ScanCanceled
		}
	}
}

func (s *segment) sync() error {
	if !s.isUpdated() {
		return nil
	}
	if err := s.log.Sync(); err != nil {
		return err
	}
	s.state ^= segmentUpdated
	return nil
}

func (s *segment) close() error {
	return s.log.Close()
}

func (s *segment) delete() error {
	return s.log.Delete()
}

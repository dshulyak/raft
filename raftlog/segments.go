package raftlog

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/dshulyak/raft/types"
	"go.uber.org/zap"
)

const (
	logPrefix = "segment-"
	logExt    = ".log"
	indexExt  = "idx"
)

func scanSegments(logger *zap.SugaredLogger, conf config) (*segments, error) {
	err := os.MkdirAll(conf.dataDir, 0o700)
	if err != nil && !os.IsExist(err) {
		return nil, err
	}
	dir, err := os.OpenFile(conf.dataDir, 0, os.ModeDir)
	if err != nil {
		return nil, err
	}

	files, err := filepath.Glob(filepath.Join(conf.dataDir, logPrefix+"*"+logExt))
	if err != nil {
		return nil, err
	}
	segs := &segments{
		logger: logger,
		conf:   conf,
		dir:    dir,
	}

	for _, fullpath := range files {
		_, file := filepath.Split(fullpath)
		num := strings.TrimSuffix(strings.TrimPrefix(file, logPrefix), logExt)
		if len(num) > 0 {
			n, err := strconv.Atoi(num)
			if err != nil {
				break
			}
			seg, err := openSegment(logger, conf, n)
			if err != nil {
				return nil, err
			}
			segs.list = append(segs.list, seg)
			continue
		}
		return nil, fmt.Errorf("invalid log file name %s", fullpath)
	}

	if len(segs.list) > 0 {
		sort.Slice(segs.list, func(i, j int) bool {
			return segs.list[i].n < segs.list[j].n
		})
		segs.active = segs.list[len(segs.list)-1]
	} else {
		segs.active, err = openSegment(logger, conf, 0)
		if err != nil {
			return nil, err
		}
		segs.list = append(segs.list, segs.active)
	}

	return segs, nil
}

type segments struct {
	logger *zap.SugaredLogger

	conf config

	// initialized to the last valid segment or to the initial segment-0
	active *segment
	// all segments including active. in ascending order.
	// TODO number of opened segments must be limited
	list []*segment

	// directory handle
	dir *os.File
}

func (s *segments) rescan(idx *index) (uint64, error) {
	var last uint64
	for i, seg := range s.list {
		scan := seg.scanner(0)

		for {
			offset := scan.offset
			entry, err := scan.next()

			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				if errors.Is(err, io.ErrUnexpectedEOF) {
					s.logger.Infow("repairing unexpected eof", "segment", seg, "offset", offset)
					if err := s.repairFrom(i, offset); err != nil {
						s.logger.Errorw("repair failed", "error", err)
						return 0, err
					}
					s.logger.Infow("repair complete", "active segment", s.active)
					return last, nil
				}
				return 0, err
			}

			if err := idx.Set(entry.Index, &indexEntry{
				Segment: uint16(seg.n),
				Offset:  uint32(offset),
			}); err != nil {
				return 0, err
			}
			last = entry.Index
		}
	}
	return last, nil
}

func (s *segments) repairFrom(segPos int, offset int64) error {
	for i, seg := range s.list[segPos+1:] {
		if err := seg.delete(); err != nil {
			return err
		}
		s.list[i] = nil
	}
	s.active = s.list[segPos]
	s.list = s.list[:segPos+1]
	if err := s.active.truncate(offset); err != nil {
		return err
	}
	if err := s.active.sync(); err != nil {
		return err
	}
	// need to flush deleted inodes in case we actually removed files
	if err := s.dir.Sync(); err != nil {
		return err
	}
	return nil
}

func (s *segments) append(entry *types.Entry) (int, int, error) {
	if !s.active.hasSpace(entry) {
		seg, err := openSegment(s.logger, s.conf, s.active.n+1)
		if err != nil {
			return 0, 0, err
		}
		s.list = append(s.list, seg)
		s.active = seg
	}
	return s.active.append(entry)
}

func (s *segments) get(ie *indexEntry) (*types.Entry, error) {
	return s.list[ie.Segment].get(ie.Offset)
}

func (s *segments) close() (err error) {
	for _, seg := range s.list {
		err1 := seg.close()
		if err1 != nil && err == nil {
			err = err1
		}
	}
	return
}

func (s *segments) delete() error {
	for _, seg := range s.list {
		if err := seg.delete(); err != nil {
			return err
		}
	}
	s.list = nil
	s.active = nil
	return nil
}

func (s *segments) sync() error {
	// sync dir once for all recently created segments
	sync := false
	for _, seg := range s.list {
		if seg.isCreated() {
			sync = true
			seg.state &= ^(segmentCreated)
		}
		if seg.isUpdated() {
			if err := seg.sync(); err != nil {
				return err
			}
		}
	}
	if sync {
		return s.dir.Sync()
	}
	return nil
}

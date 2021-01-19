package raftlog

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"

	"go.uber.org/zap"
	"golang.org/x/sys/unix"

	"github.com/dshulyak/raft/types"
)

const (
	opTypeSize      = 1
	writeBufferSize = 4096 * 16
	scanBufferSize  = writeBufferSize
)

var (
	ErrLogCorrupted  = errors.New("log is corrupted")
	ErrEntryNotFound = errors.New("entry not found")
	ErrLogEnd        = errors.New("log end")
)

type Entry = types.Entry

type LogOptions struct {
	File       string
	BufferSize int64
}

func NewLog(zlog *zap.Logger, last *IndexEntry, opts *LogOptions) (*Log, error) {
	var (
		file   string
		logger = zlog.Sugar()
	)
	if opts == nil || len(opts.File) == 0 {
		f, err := ioutil.TempFile("", "log-file-XXX")
		if err != nil {
			return nil, err
		}
		if err := f.Close(); err != nil {
			return nil, err
		}
		file = f.Name()
	} else {
		file = opts.File
	}
	f, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	if err := unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_SEQUENTIAL); err != nil {
		return nil, err
	}
	log := &Log{
		logger: logger,
		f:      f,
	}
	if err := log.init(last, opts); err != nil {
		return nil, err
	}

	return log, nil
}

type fileBackend interface {
	io.Writer
	Flush() error
}

type Log struct {
	logger *zap.SugaredLogger

	mu sync.RWMutex
	f  *os.File
	w  fileBackend
}

func (l *Log) init(lastIndex *IndexEntry, opts *LogOptions) error {
	if opts == nil || opts.BufferSize == 0 {
		l.w = bufio.NewWriter(l.f)
	} else {
		l.w = bufio.NewWriterSize(l.f, int(opts.BufferSize))
	}
	stat, err := l.f.Stat()
	if err != nil {
		return err
	}
	offset := uint64(stat.Size())
	if lastIndex != nil {
		var entry Entry
		if err := l.Get(lastIndex, &entry); err != nil {
			return err
		}
		validSize := lastIndex.Offset + uint64(entry.Size()) + crcSize
		if uint64(stat.Size()) == validSize {
			return nil
		}

		l.logger.Infow("log file will be truncated",
			"path", l.f.Name(),
			"current size", stat.Size(),
			"valid size", validSize,
		)
		offset = lastIndex.Offset + uint64(entry.Size())
		if err := l.Truncate(offset); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) Truncate(offset uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.w.Flush(); err != nil {
		return err
	}

	if err := l.f.Truncate(int64(offset)); err != nil {
		return fmt.Errorf("failed to truncate log file %v up to %d: %w", l.f.Name(), offset, err)
	}
	return nil
}

func (l *Log) Append(entry *Entry) (uint64, error) {
	size := uint64(entry.Size()) + crcSize
	buf := make([]byte, size)
	if _, err := entry.MarshalTo(buf[crcSize:]); err != nil {
		return 0, err
	}
	_ = putCrc32(buf, buf[crcSize:])
	l.mu.Lock()
	defer l.mu.Unlock()
	n, err := l.w.Write(buf)
	if err != nil {
		return uint64(n), err
	}
	if uint64(n) != size {
		return uint64(n), io.ErrShortWrite
	}
	return size, nil
}

func (l *Log) Get(index *IndexEntry, entry *Entry) error {
	buf := make([]byte, index.Length)
	l.mu.RLock()
	defer l.mu.RUnlock()
	_, err := l.f.ReadAt(buf, int64(index.Offset))
	if err != nil && errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: record offset %d", ErrEntryNotFound, index.Offset)
	} else if err != nil {
		return fmt.Errorf("failed to read log file %v at %d: %w", l.f.Name(), index.Offset, err)
	}
	if !cmpCrc32(buf, buf[crcSize:]) {
		return ErrLogCorrupted
	}
	return entry.Unmarshal(buf[crcSize:])
}

func (l *Log) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Flush()
}

func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.w.Flush(); err != nil {
		return err
	}
	err := l.f.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync the log: %v", err)
	}
	return nil
}

func (l *Log) Close() error {
	if err := l.Sync(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.f.Close()
}

func (l *Log) Delete() error {
	if err := l.f.Close(); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return os.Remove(l.f.Name())
}

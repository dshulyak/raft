package raftlog

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"

	"go.uber.org/zap"
	"golang.org/x/sys/unix"

	"github.com/dshulyak/raft/types"
)

const (
	opTypeSize = 1
)

var (
	ErrLogCorrupted  = errors.New("log is corrupted")
	ErrEntryNotFound = errors.New("entry not found")
	ErrLogEnd        = errors.New("log end")
)

type Entry = types.Entry

func onDiskSize(entry *types.Entry) int {
	return entry.Size() + int(crcSize)
}

func openLog(logger *zap.SugaredLogger, file string) (*log, error) {
	l := &log{logger: logger}
	if err := l.openAt(file); err != nil {
		return nil, err
	}
	return l, nil
}

type fileBackend interface {
	io.Writer
	Flush() error
}

type log struct {
	logger *zap.SugaredLogger

	f *os.File
	w fileBackend
}

func (l *log) openAt(file string) error {
	f, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	if err := unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_SEQUENTIAL); err != nil {
		return err
	}
	l.f = f
	l.w = bufio.NewWriter(f)
	return nil
}

func (l *log) Truncate(offset uint64) error {
	if err := l.w.Flush(); err != nil {
		return err
	}

	if err := l.f.Truncate(int64(offset)); err != nil {
		return fmt.Errorf("failed to truncate log file %v up to %d: %w", l.f.Name(), offset, err)
	}
	return nil
}

func (l *log) Append(entry *Entry) (uint64, error) {
	size := uint64(onDiskSize(entry))
	buf := make([]byte, size)
	if _, err := entry.MarshalTo(buf[crcSize:]); err != nil {
		return 0, err
	}
	_ = putCrc32(buf, buf[crcSize:])

	n, err := l.w.Write(buf)
	if err != nil {
		return uint64(n), err
	}
	if uint64(n) != size {
		return uint64(n), io.ErrShortWrite
	}
	return size, nil
}

func (l *log) Get(index *indexEntry) (*types.Entry, error) {
	buf := make([]byte, index.Length)
	n, err := l.f.ReadAt(buf, int64(index.Offset))

	if err != nil && errors.Is(err, io.EOF) && uint64(n) < index.Length {
		return nil, fmt.Errorf("%w: record offset %d", ErrEntryNotFound, index.Offset)
	} else if err != nil {
		return nil, fmt.Errorf("failed to read log file %v at %d: %w", l.f.Name(), index.Offset, err)
	}

	if !cmpCrc32(buf, buf[crcSize:]) {
		return nil, ErrLogCorrupted
	}

	entry := &types.Entry{}
	return entry, entry.Unmarshal(buf[crcSize:])
}

func (l *log) Flush() error {
	return l.w.Flush()
}

func (l *log) Sync() error {
	if err := l.Flush(); err != nil {
		return err
	}
	err := l.f.Sync()
	if err != nil {
		return fmt.Errorf("failed to sync the log: %v", err)
	}
	return nil
}

func (l *log) Close() error {
	if err := l.Sync(); err != nil {
		return err
	}
	return l.f.Close()
}

func (l *log) Delete() error {
	if err := l.f.Close(); err != nil {
		return err
	}
	return os.Remove(l.f.Name())
}

func (l *log) Size() (int64, error) {
	stat, err := l.f.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

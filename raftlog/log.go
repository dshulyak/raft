package raftlog

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"go.uber.org/zap"
	"golang.org/x/sys/unix"

	"github.com/dshulyak/raft/types"
)

var (
	// ErrLogCorrupted returned if computed crc doesn't match stored.
	ErrLogCorrupted = errors.New("log is corrupted")
)

var (
	readerPool = sync.Pool{
		New: func() interface{} {
			return &offsetReader{
				R: bufio.NewReader(nil),
			}
		},
	}
)

func onDiskSize(entry *types.Entry) int {
	size := entry.Size()
	return uvarintSize(uint64(size)) + size + crcSize
}

func openLog(logger *zap.SugaredLogger, file string) (*log, error) {
	l := &log{logger: logger}
	if err := l.openAt(file); err != nil {
		return nil, err
	}
	return l, nil
}

type logWriter interface {
	io.Writer
	io.ByteWriter
	Flush() error
}

type log struct {
	logger *zap.SugaredLogger

	f *os.File
	w logWriter
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
	l.logger.Debugw("opened log file", "path", file)
	l.w = bufio.NewWriter(f)
	return nil
}

func (l *log) Append(entry *types.Entry) (int, error) {
	size := entry.Size() + crcSize

	total := 0
	n, err := writeUvarint(l.w, uint64(size))
	if err != nil {
		return 0, err
	}
	total += n

	buf := make([]byte, size)
	if _, err := entry.MarshalTo(buf[crcSize:]); err != nil {
		return 0, err
	}
	putCrc32(buf, buf[crcSize:])

	n, err = l.w.Write(buf)
	if err != nil {
		return n, err
	}
	if n != size {
		return n, io.ErrShortWrite
	}
	total += n
	return total, nil
}

func (l *log) Get(offset uint32) (*types.Entry, error) {
	r := readerPool.Get().(*offsetReader)
	r.Reset(l.f, int64(offset))
	defer readerPool.Put(r)

	entry := &types.Entry{}
	_, err := readEntry(r.R, entry)
	return entry, err
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

func (l *log) scanner(offset uint32) *scanner {
	r := readerPool.Get().(*offsetReader)
	r.Reset(l.f, int64(offset))
	return &scanner{r: r}
}

func readEntry(r *bufio.Reader, entry *types.Entry) (int, error) {
	size, n, err := readUvarint(r)
	if err != nil {
		if n > 0 && errors.Is(err, io.EOF) {
			return 0, io.ErrUnexpectedEOF
		}
		return 0, err
	}
	total := n

	buf := make([]byte, size)

	n, err = io.ReadFull(r, buf)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return total + n, io.ErrUnexpectedEOF
		} else {
			return 0, err
		}
	}

	total += n

	if !cmpCrc32(buf, buf[crcSize:]) {
		return 0, ErrLogCorrupted
	}

	if err := entry.Unmarshal(buf[crcSize:]); err != nil {
		return 0, err
	}
	return total, nil
}

type scanner struct {
	r *offsetReader

	offset int64
}

func (s *scanner) next() (*types.Entry, error) {
	entry := &types.Entry{}
	n, err := readEntry(s.r.R, entry)
	s.offset += int64(n)
	return entry, err
}

func (s *scanner) close() {
	readerPool.Put(s.r)
	s.r = nil
}

package raftlog

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"syscall"

	"go.uber.org/zap"
	"golang.org/x/sys/unix"

	"github.com/dshulyak/raft/types"
)

var (
	// ErrLogCorrupted returned if computed crc doesn't match stored.
	ErrLogCorrupted = errors.New("log is corrupted")
)

const (
	lenFieldSize = 4 // uint32
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

func onDiskSize(size int) int {
	return lenFieldSize + size + crcSize*2
}

func openLog(logger *zap.SugaredLogger, file string, size int64) (*log, error) {
	l := &log{logger: logger, encBuf: make([]byte, 128<<10 /* 128kb */)}
	if err := l.openAt(file, size); err != nil {
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

	// buffer used for encoding entries
	encBuf []byte
}

func (l *log) openAt(file string, size int64) error {
	f, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	if err := unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_SEQUENTIAL); err != nil {
		return err
	}

	// syscall.Fallocate is not portable.
	// posix_fallocate doesn't support unix.FALLOC_FL_KEEP_SIZE and will fail if called on a file opened with O_APPEND
	if err := syscall.Fallocate(int(f.Fd()), unix.FALLOC_FL_KEEP_SIZE, 0, size); err != nil {
		return err
	}

	l.f = f
	l.logger.Debugw("opened log file", "path", file)
	l.w = bufio.NewWriter(f)
	return nil
}

func (l *log) Append(entry *types.Entry) (int, error) {
	var (
		size     = entry.Size()
		offset   = 0
		diskSize = onDiskSize(size)
		buf      []byte
	)

	if diskSize <= len(l.encBuf) {
		buf = l.encBuf[:diskSize]
	} else {
		buf = make([]byte, diskSize)
	}

	binary.BigEndian.PutUint32(buf, uint32(size))
	offset += lenFieldSize

	// why crc for size? see reader
	putCrc32(buf[offset:], buf[:offset])
	offset += crcSize

	w, err := entry.MarshalTo(buf[offset:])
	if err != nil {
		return 0, err
	}
	offset += w

	putCrc32(buf[offset:], buf[offset-w:offset])
	offset += crcSize

	n, err := l.w.Write(buf)
	if err != nil {
		return n, err
	}
	if n != offset {
		return n, io.ErrShortWrite
	}
	return n, nil
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

func (l *log) truncate(offset int64) error {
	return l.f.Truncate(offset)
}

func (l *log) scanner(offset uint32) *scanner {
	r := readerPool.Get().(*offsetReader)
	r.Reset(l.f, int64(offset))
	return &scanner{r: r}
}

func readEntry(r io.Reader, entry *types.Entry) (int, error) {
	meta := make([]byte, lenFieldSize+crcSize)

	n, err := io.ReadFull(r, meta)
	if err != nil {
		return n, err
	}

	total := n

	// if lenField bytes are corrupted it will result in io.ErrUnexpectedEOF
	// that is handled by truncating log at the valid offset during recovery.
	// however this is definitely should be ErrLogCorrupted, which is non-recoverable
	//
	// another way to deal with it is to ensure that read payload never returns io.EOF unless
	// size is larger then the max entry size. in that case we can fail early if size got corrupted
	// beyond largest possible, or fail after computing crc.
	// for the above method to work file must be zero filled (currently only disk space is allocated)

	if !cmpCrc32(meta[lenFieldSize:], meta[:lenFieldSize]) {
		return total, ErrLogCorrupted
	}

	size := binary.BigEndian.Uint32(meta[:lenFieldSize])
	payload := make([]byte, size+crcSize)

	n, err = io.ReadFull(r, payload)
	if err != nil {
		return total + n, err
	}

	total += n

	if !cmpCrc32(payload[size:], payload[:size]) {
		return total, ErrLogCorrupted
	}

	if err := entry.Unmarshal(payload[:size]); err != nil {
		return total, err
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

package raftlog

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sync"

	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

const (
	logHeaderWidth  uint64 = 64
	opTypeSize             = 1
	writeBufferSize        = 4096 * 16
	scanBufferSize         = writeBufferSize
)

var (
	ErrLogCorrupted  = errors.New("log is corrupted")
	ErrEntryNotFound = errors.New("entry not found")
	ErrLogEnd        = errors.New("log end")
)

func uvarintSize(x uint64) uint64 {
	i := uint64(0)
	for x >= 0x80 {
		x >>= 7
		i++
	}
	return i + 1
}

type LogType uint8

func (t LogType) String() string {
	return logString[t]
}

const (
	LogNoop LogType = iota + 1
	LogApplication
	LogConfiguration
)

var logString = [...]string{
	"Empty", "Noop", "App", "Conf",
}

type LogHeader struct {
	Version    [10]byte
	FirstIndex uint64
	_          [46]byte
}

type LogEntry struct {
	Term   uint64
	Index  uint64
	OpType LogType
	Op     []byte
}

func (e *LogEntry) String() string {
	return fmt.Sprintf("term=%d index=%d type=%s", e.Term, e.Index, e.OpType.String())
}

func (l *LogEntry) Size() uint64 {
	opSize := uint64(len(l.Op))
	return uvarintSize(l.Term) + uvarintSize(l.Index) +
		uint64(opTypeSize) + uvarintSize(opSize) + opSize
}

func (l *LogEntry) Encode(to []byte) error {
	offset := 0
	offset += binary.PutUvarint(to[offset:], l.Term)
	offset += binary.PutUvarint(to[offset:], l.Index)
	to[offset] = byte(l.OpType)
	offset++
	offset += binary.PutUvarint(to[offset:], uint64(len(l.Op)))
	copy(to[offset:], l.Op)
	return nil
}

func (l *LogEntry) Decode(from []byte) error {
	var offset, n int
	l.Term, n = binary.Uvarint(from[offset:])
	offset += n
	l.Index, n = binary.Uvarint(from[offset:])
	offset += n
	l.OpType = LogType(from[offset])
	offset++
	_, n = binary.Uvarint(from[offset:])
	offset += n
	l.Op = append(l.Op, from[offset:]...)
	return nil
}

// Reset LogEntry for reuse.
func (l *LogEntry) Reset() {
	l.Index = 0
	l.Term = 0
	l.OpType = 0
	l.Op = nil
}

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
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	var header LogHeader
	if stat.Size() == 0 {
		if err := binary.Write(f, binary.BigEndian, header); err != nil {
			return nil, err
		}
	} else {
		if err := binary.Read(f, binary.BigEndian, &header); err != nil {
			return nil, err
		}
	}
	if err := unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_SEQUENTIAL); err != nil {
		return nil, err
	}
	log := &Log{
		logger: logger,
		f:      f,
		header: header,
	}
	if err := log.init(last, opts); err != nil {
		return nil, err
	}

	return log, nil
}

type FileBackend interface {
	io.Writer
	Flush() error
}

type Log struct {
	logger *zap.SugaredLogger

	header LogHeader

	mu sync.RWMutex
	f  *os.File
	w  FileBackend
}

func (l *Log) init(lastIndex *IndexEntry, opts *LogOptions) error {
	stat, err := l.f.Stat()
	if err != nil {
		return err
	}
	offset := uint64(stat.Size())
	if lastIndex != nil {
		var entry LogEntry
		if err := l.Get(lastIndex, &entry); err != nil {
			return err
		}
		validSize := lastIndex.Offset + entry.Size() + crcSize
		if uint64(stat.Size()) == validSize {
			return nil
		}

		l.logger.Infow("log file will be truncated",
			"path", l.f.Name(),
			"current size", stat.Size(),
			"valid size", validSize,
		)
		offset = lastIndex.Offset + entry.Size()
		if err := l.Truncate(offset); err != nil {
			return err
		}
	}
	if opts == nil || opts.BufferSize == 0 {
		l.w = bufio.NewWriter(l.f)
	} else {
		l.w = bufio.NewWriterSize(l.f, int(opts.BufferSize))
	}
	return nil
}

func (l *Log) HeaderSize() uint64 {
	return logHeaderWidth
}

func (l *Log) Version() [10]byte {
	return l.header.Version
}

func (l *Log) FirstIndex() uint64 {
	return l.header.FirstIndex
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

func (l *Log) Append(entry *LogEntry) (uint64, error) {
	size := entry.Size() + crcSize
	buf := make([]byte, size)
	if err := entry.Encode(buf[crcSize:]); err != nil {
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

func (l *Log) Get(index *IndexEntry, entry *LogEntry) error {
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
	return entry.Decode(buf[crcSize:])
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

func (l *Log) Scanner(from *IndexEntry) *LogScanner {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return &LogScanner{
		reader: bufio.NewReaderSize(&offsetReader{
			offset: int64(from.Offset),
			reader: l.f,
		}, scanBufferSize),
	}
}

type LogScanner struct {
	reader io.Reader
}

func (s *LogScanner) Scan(size uint64) (*LogEntry, error) {
	buf := make([]byte, size)
	total := 0
	for total < int(size) {
		n, err := s.reader.Read(buf[total:])
		if err != nil && errors.Is(err, io.EOF) {
			return nil, ErrLogEnd
		} else if err != nil {
			return nil, err
		}
		total += n
	}
	if !cmpCrc32(buf, buf[crcSize:]) {
		return nil, ErrLogCorrupted
	}
	var entry LogEntry
	err := entry.Decode(buf[crcSize:])
	if err != nil {
		return nil, err
	}
	return &entry, nil
}

type offsetReader struct {
	offset int64
	reader io.ReaderAt
}

func (r *offsetReader) Read(buf []byte) (int, error) {
	n, err := r.reader.ReadAt(buf, r.offset)
	r.offset += int64(n)
	return n, err
}

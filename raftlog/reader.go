package raftlog

import (
	"bufio"
	"io"
)

type offsetReader struct {
	R *bufio.Reader

	f      io.ReaderAt
	offset int64
}

func (r *offsetReader) Read(buf []byte) (int, error) {
	n, err := r.f.ReadAt(buf, r.offset)
	r.offset += int64(n)
	return n, err
}

func (r *offsetReader) Seek(offset int64, whence int) error {
	if whence != io.SeekCurrent {
		panic("only SeekCurrent is supported")
	}
	r.offset += offset
	return nil
}

func (r *offsetReader) Reset(f io.ReaderAt, offset int64) {
	r.f = f
	r.offset = offset
	r.R.Reset(r)
}

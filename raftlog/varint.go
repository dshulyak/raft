package raftlog

import (
	"encoding/binary"
	"errors"
	"io"
)

// Code is based on encoding.Binary.ReadVarint and PutVarint

func uvarintSize(x uint64) int {
	i := 0
	for x >= 0x80 {
		x >>= 7
		i++
	}
	return i + 1
}

// writeUvarint doesn't exist in binary module.
func writeUvarint(w io.ByteWriter, x uint64) (int, error) {
	i := 0
	for x >= 0x80 {
		if err := w.WriteByte(byte(x) | 0x80); err != nil {
			return 0, err
		}
		x >>= 7
		i++
	}
	if err := w.WriteByte(byte(x)); err != nil {
		return 0, err
	}
	return i + 1, nil
}

var overflow = errors.New("binary: varint overflows a 64-bit integer")

// readUvarint is modified to return number of read bytes.
func readUvarint(r io.ByteReader) (uint64, int, error) {
	var x uint64
	var s uint
	for i := 0; i < binary.MaxVarintLen64; i++ {
		b, err := r.ReadByte()
		if err != nil {
			return x, 0, err
		}
		if b < 0x80 {
			if i == 9 && b > 1 {
				return x, 0, overflow
			}
			return x | uint64(b)<<s, i + 1, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return x, 0, overflow
}

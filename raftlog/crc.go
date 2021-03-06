package raftlog

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	crcSize = 4
)

var (
	table = crc32.MakeTable(crc32.Castagnoli)
)

func putCrc32(buf []byte, data []byte) {
	binary.LittleEndian.PutUint32(buf, getCrc32(data))
}

func getCrc32(data []byte) uint32 {
	return crc32.Update(0, table, data)
}

func cmpCrc32(buf []byte, data []byte) bool {
	return binary.LittleEndian.Uint32(buf) == getCrc32(data)
}

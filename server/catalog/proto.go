package catalog

import (
	"encoding/binary"
)

const (
	dataCF  = "data"
	lockCF  = "lock"
	writeCF = "write"
)

var (
	infix       = []byte{'/'}
	inodeSuffix = []byte{'i'}
	raftSuffix  = []byte{'r'}
)

type Timestamp struct{}

// proto for storage encoding/decoding and function return value

type item struct {
	ino    uint64
	links  uint64
	fields []field
}

func (i *item) Marshal() ([]byte, error) {
	// TODO: we may use flatbuffers as internal encoding/decoding to avoid packing and unpacking data
	return nil, nil
}

func (i *item) Unmarshal(raw []byte) error {
	// TODO
	return nil
}

type link struct {
	parent uint64
	name   string
	child  uint64
	fields []field
}

func (l *link) Marshal() ([]byte, error) {
	// TODO
	return nil, nil
}

func (l *link) Unmarshal(raw []byte) error {
	// TODO
	return nil
}

type field struct {
	name  string
	value []byte
}

func shardDataPrefixSize(sid uint64, shardId uint32) int {
	return 8 + len(infix) + 4 + len(inodeSuffix)
}

func shardRaftPrefixSize(sid uint64, shardId uint32) int {
	return 8 + len(infix) + 4 + len(raftSuffix)
}

func encodeShardDataPrefix(sid uint64, shardId uint32, raw []byte) {
	if raw == nil || cap(raw) == 0 {
		panic("invalid raw input")
	}
	infixSize := len(infix)
	binary.BigEndian.PutUint64(raw[:8], sid)
	copy(raw[8:], infix)
	binary.BigEndian.PutUint32(raw[8+infixSize:], shardId)
	copy(raw[8+infixSize+4:], inodeSuffix)
}

func encodeShardRaftPrefix(sid uint64, shardId uint32, raw []byte) {
	if raw == nil || cap(raw) == 0 {
		panic("invalid raw input")
	}
	infixSize := len(infix)
	binary.BigEndian.PutUint64(raw[:8], sid)
	copy(raw[8:], infix)
	binary.BigEndian.PutUint32(raw[8+infixSize:], shardId)
	copy(raw[8+infixSize+4:], raftSuffix)
}

func encodeIno(ino uint64, raw []byte) {
	binary.BigEndian.PutUint64(raw, ino)
}

func decodeIno(raw []byte) uint64 {
	return binary.BigEndian.Uint64(raw)
}

func newTsKey(key []byte, ts *Timestamp) []byte {
	if ts == nil {
		return key
	}
	return nil
}

package catalog

import (
	"encoding/binary"

	"github.com/cubefs/inodedb/shardserver/catalog/persistent"
	pb "google.golang.org/protobuf/proto"
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

type item persistent.Item

// Marshal return marshaled data of item
// TODO: As the flatbuffer unpacking data is more faster than protobuffer(10X above),
// we may use flatbuffer for the internal encoding/decoding to avoid unpacking data time cost
func (i *item) Marshal() ([]byte, error) {
	return pb.Marshal((*persistent.Item)(i))
}

func (i *item) Unmarshal(raw []byte) error {
	return pb.Unmarshal(raw, (*persistent.Item)(i))
}

type link persistent.Link

func (l *link) Marshal() ([]byte, error) {
	return pb.Marshal((*persistent.Link)(l))
}

func (l *link) Unmarshal(raw []byte) error {
	return pb.Unmarshal(raw, (*persistent.Link)(l))
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

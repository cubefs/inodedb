package catalog

import (
	"encoding/binary"

	"github.com/cubefs/inodedb/proto"
	"github.com/cubefs/inodedb/shardserver/catalog/persistent"
)

const (
	dataCF  = "data"
	lockCF  = "lock"
	writeCF = "write"
)

var (
	spacePrefix     = []byte{'s'}
	shardInfoPrefix = []byte{'n'}

	shardSuffix  = []byte{'p'}
	inodeSuffix  = []byte{'i'}
	linkSuffix   = []byte{'l'}
	vectorSuffix = []byte{'v'}
)

type Timestamp struct{}

// proto for storage encoding/decoding and function return value

type shardInfo = persistent.ShardInfo

type item = persistent.Item

type link = persistent.Link

type embedding = persistent.Embedding

// todo: merge these encode and decode function into shard?
func spacePrefixSize() int {
	return 8 + len(spacePrefix) + len(shardSuffix)
}

func shardPrefixSize() int {
	return spacePrefixSize() + 4
}

func shardInfoPrefixSize() int {
	return 8 + len(shardInfoPrefix) + len(shardSuffix)
}

func shardInodePrefixSize() int {
	return shardPrefixSize() + len(inodeSuffix)
}

func shardLinkPrefixSize() int {
	return shardPrefixSize() + len(linkSuffix)
}

func shardVectorPrefixSize() int {
	return shardPrefixSize() + len(vectorSuffix)
}

func encodeSpacePrefix(sid proto.Sid, raw []byte) {
	if raw == nil || cap(raw) == 0 {
		panic("invalid raw input")
	}
	prefixSize := len(spacePrefix)
	copy(raw, spacePrefix)
	binary.BigEndian.PutUint64(raw[prefixSize:8], uint64(sid))
	copy(raw[8+prefixSize:], shardSuffix)
}

func encodeShardInfoListPrefix(raw []byte) {
	if raw == nil || cap(raw) == 0 {
		panic("invalid raw input")
	}
	copy(raw, shardInfoPrefix)
}

func encodeShardInfoPrefix(sid proto.Sid, shardID proto.ShardID, raw []byte) {
	if raw == nil || cap(raw) == 0 {
		panic("invalid raw input")
	}
	prefixSize := len(shardInfoPrefix)
	copy(raw, shardInfoPrefix)
	binary.BigEndian.PutUint64(raw[prefixSize:8], sid)
	copy(raw[8+prefixSize:], shardSuffix)
	binary.BigEndian.PutUint32(raw[8+prefixSize+len(shardSuffix):], shardID)
}

func encodeShardPrefix(sid proto.Sid, shardID proto.ShardID, raw []byte) {
	spacePrefixSize := spacePrefixSize()
	encodeSpacePrefix(sid, raw[:spacePrefixSize])
	binary.BigEndian.PutUint32(raw[spacePrefixSize:], uint32(shardID))
}

func encodeShardInodePrefix(sid proto.Sid, shardID proto.ShardID, raw []byte) {
	shardPrefixSize := shardPrefixSize()
	encodeShardPrefix(sid, shardID, raw[:shardPrefixSize])
	copy(raw[shardPrefixSize:], inodeSuffix)
}

func encodeShardLinkPrefix(sid proto.Sid, shardID proto.ShardID, raw []byte) {
	shardPrefixSize := shardPrefixSize()
	encodeShardPrefix(sid, shardID, raw[:shardPrefixSize])
	copy(raw[shardPrefixSize:], linkSuffix)
}

func encodeShardVectorPrefix(sid proto.Sid, shardID proto.ShardID, raw []byte) {
	shardPrefixSize := shardPrefixSize()
	encodeShardPrefix(sid, shardID, raw[:shardPrefixSize])
	copy(raw[shardPrefixSize:], vectorSuffix)
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

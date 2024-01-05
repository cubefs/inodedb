package catalog

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/trace"

	"github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/proto"
)

const (
	defaultSplitMapNum = 16
)

type Space struct {
	sid  uint64
	name string

	shards           *concurrentShards
	getCatalogChange getCatalogChangeFunc

	shardIDs []uint32
	lock     sync.RWMutex
}

type getCatalogChangeFunc func(ctx context.Context) error

func newSpace(ctx context.Context, spaceName string, sid uint64, f getCatalogChangeFunc) *Space {
	return &Space{
		sid:              sid,
		name:             spaceName,
		getCatalogChange: f,
		shards:           newConcurrentShards(defaultSplitMapNum),
	}
}

func (s *Space) AddShard(info *ShardInfo, tr *transport) {
	if get := s.shards.Get(info.ShardID); get != nil {
		return
	}
	newShard := &shard{
		epoch:   info.Epoch,
		shardID: info.ShardID,
		tr:      tr,
		info:    info,
	}
	s.shards.Put(newShard)

	s.lock.Lock()
	defer s.lock.Unlock()
	s.shardIDs = append(s.shardIDs, info.ShardID)
}

func (s *Space) GetShard(ctx context.Context, shardID proto.ShardID) *shard {
	return s.shards.Get(shardID)
}

func (s *Space) InsertItem(ctx context.Context, item proto.Item) (uint64, error) {
	shardID := s.getRandShardID()
	shard := s.GetShard(ctx, shardID)

	itemRequest := &proto.ShardInsertItemRequest{
		Header: proto.ShardOpHeader{
			ShardID:      shard.shardID,
			Sid:          s.sid,
			RouteVersion: 0,
		},
		Item: item,
	}
	ino, err := shard.InsertItem(ctx, itemRequest)
	if err != nil {
		return 0, err
	}

	return ino, nil
}

func (s *Space) UpdateItem(ctx context.Context, item proto.Item) error {
	shard := s.locateShard(ctx, item.Ino)
	if shard == nil {
		return errors.ErrInoRangeNotFound
	}

	itemRequest := &proto.ShardUpdateItemRequest{
		Header: proto.ShardOpHeader{
			ShardID:      shard.shardID,
			Sid:          s.sid,
			RouteVersion: 0,
		},
		Item: item,
	}
	return shard.UpdateItem(ctx, itemRequest)
}

func (s *Space) DeleteItem(ctx context.Context, ino uint64) error {
	shard := s.locateShard(ctx, ino)
	if shard == nil {
		return errors.ErrInoRangeNotFound
	}

	deleteRequest := &proto.ShardDeleteItemRequest{
		Header: proto.ShardOpHeader{
			ShardID:      shard.shardID,
			Sid:          s.sid,
			RouteVersion: 0,
		},
		Ino: ino,
	}
	return shard.DeleteItem(ctx, deleteRequest)
}

func (s *Space) GetItem(ctx context.Context, ino uint64) (proto.Item, error) {
	shard := s.locateShard(ctx, ino)
	if shard == nil {
		return proto.Item{}, errors.ErrInoRangeNotFound
	}

	getItemRequest := &proto.ShardGetItemRequest{
		Header: proto.ShardOpHeader{
			ShardID:      shard.shardID,
			Sid:          s.sid,
			RouteVersion: 0,
		},
		Ino: ino,
	}
	return shard.GetItem(ctx, getItemRequest)
}

func (s *Space) Link(ctx context.Context, link proto.Link) error {
	shard := s.locateShard(ctx, link.Parent)
	if shard == nil {
		return errors.ErrInoRangeNotFound
	}

	linkRequest := &proto.ShardLinkRequest{
		Header: proto.ShardOpHeader{
			ShardID:      shard.shardID,
			Sid:          s.sid,
			RouteVersion: 0,
		},
		Link: link,
	}

	return shard.Link(ctx, linkRequest)
}

func (s *Space) GetLink(ctx context.Context, get proto.GetLink) (proto.Link, error) {
	shard := s.locateShard(ctx, get.Parent)
	if shard == nil {
		return proto.Link{}, errors.ErrInoRangeNotFound
	}

	getLinkRequest := &proto.ShardGetLinkRequest{
		Header: proto.ShardOpHeader{
			ShardID:      shard.shardID,
			Sid:          s.sid,
			RouteVersion: 0,
		},
		Get: get,
	}

	return shard.GetLink(ctx, getLinkRequest)
}

func (s *Space) Unlink(ctx context.Context, unlink proto.Unlink) error {
	shard := s.locateShard(ctx, unlink.Parent)
	if shard == nil {
		return errors.ErrInoRangeNotFound
	}

	unlinkRequest := &proto.ShardUnlinkRequest{
		Header: proto.ShardOpHeader{
			ShardID:      shard.shardID,
			Sid:          s.sid,
			RouteVersion: 0,
		},
		Unlink: unlink,
	}

	return shard.Unlink(ctx, unlinkRequest)
}

func (s *Space) List(ctx context.Context, req *proto.ListRequest) ([]proto.Link, error) {
	shard := s.locateShard(ctx, req.Ino)
	if shard == nil {
		return nil, errors.ErrInoRangeNotFound
	}

	return shard.List(ctx, &proto.ShardListRequest{
		Header: proto.ShardOpHeader{
			ShardID:      shard.shardID,
			Sid:          s.sid,
			RouteVersion: 0,
		},
		Ino:   req.Ino,
		Start: req.Start,
		Num:   req.Num,
	})
}

func (s *Space) Search(ctx context.Context, req *proto.SearchRequest) (*proto.SearchResponse, error) {
	return nil, nil
}

func (s *Space) locateShard(ctx context.Context, ino uint64) *shard {
	span := trace.SpanFromContextSafe(ctx)
	shardId := uint32(ino/proto.ShardRangeStepSize) + 1
	get := s.shards.Get(shardId)
	if get == nil {
		if err := s.getCatalogChange(ctx); err != nil {
			span.Errorf("get catalog change from master failed, err: %s", err)
			return nil
		}
	}
	return s.shards.Get(shardId)
}

func (s *Space) getRandShardID() uint32 {
	s.lock.RLock()
	defer s.lock.RUnlock()

	num := len(s.shardIDs)
	rand.Seed(time.Now().UnixNano())
	idx := rand.Intn(num)

	return s.shardIDs[idx]
}

// concurrentShards is an effective data struct (concurrent map implements)
type concurrentShards struct {
	total uint32
	num   uint32
	m     map[uint32]map[uint32]*shard
	locks map[uint32]*sync.RWMutex
}

func newConcurrentShards(splitMapNum uint32) *concurrentShards {
	m := &concurrentShards{
		num:   splitMapNum,
		m:     make(map[uint32]map[uint32]*shard),
		locks: make(map[uint32]*sync.RWMutex),
	}
	for i := uint32(0); i < splitMapNum; i++ {
		m.locks[i] = &sync.RWMutex{}
		m.m[i] = make(map[uint32]*shard)
	}
	return m
}

// Get shard from concurrentShards
func (s *concurrentShards) Get(sid uint32) *shard {
	idx := sid % s.num
	s.locks[idx].RLock()
	defer s.locks[idx].RUnlock()
	return s.m[idx][sid]
}

// Put new shard into concurrentShards
func (s *concurrentShards) Put(v *shard) {
	id := v.shardID
	idx := id % s.num
	s.locks[idx].Lock()
	defer s.locks[idx].Unlock()
	atomic.AddUint32(&s.total, 1)
	s.m[idx][id] = v
}

// Range concurrentShards, it only use in flush atomic switch situation.
// in other situation, it may occupy the read lock for a long time
func (s *concurrentShards) Range(f func(v *shard) error) {
	for i := uint32(0); i < s.num; i++ {
		l := s.locks[i]
		l.RLock()
		for _, v := range s.m[i] {
			err := f(v)
			if err != nil {
				l.RUnlock()
				return
			}
		}
		l.RUnlock()
	}
}

func (s *concurrentShards) List() []*shard {
	res := make([]*shard, 0, atomic.LoadUint32(&s.total))
	s.Range(func(v *shard) error {
		res = append(res, v)
		return nil
	})
	return res
}

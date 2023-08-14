package catalog

import (
	"context"
	"encoding/binary"
	"github.com/cubefs/inodedb/master/store"
	"sync"
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/master/client"
	"github.com/cubefs/inodedb/master/cluster"
	"github.com/cubefs/inodedb/proto"
)

type Space interface {
	ID(ctx context.Context) uint64
	AddShard(ctx context.Context) error
	List(ctx context.Context) []*proto.Shard
	GetMeta(ctx context.Context) (*proto.SpaceMeta, error)
	Report(ctx context.Context, reports []*ShardReport) error
	SetCluster(ctx context.Context, cluster cluster.Cluster)
	Flush(ctx context.Context) error
	Load(ctx context.Context) error
}

type space struct {
	spaceID        uint64
	inoLimit       uint64
	currentShardID uint32
	threshold      float64
	used           uint64

	info      *SpaceInfo
	allShards *shardedShards
	cluster   cluster.Cluster
	routerMgr Router

	store store.Store

	serverClient client.Client
	closeChan    chan struct{}

	state   SpaceState
	expChan chan Space

	lock sync.RWMutex
}

type SpaceConfig struct {
	ID          uint64
	Name        string
	InoLimit    uint64
	Desire      int
	SliceMapNum uint32
	SpaceType   proto.SpaceType
	Meta        []*FieldMeta
}

func NewSpace(ctx context.Context, cfg *SpaceConfig, ch chan Space) (Space, error) {
	s := &space{
		spaceID: cfg.ID,
		info: &SpaceInfo{
			Sid:         cfg.ID,
			Name:        cfg.Name,
			FixedFields: cfg.Meta,
			Type:        cfg.SpaceType,
		},
		expChan:   ch,
		allShards: newShardedShards(cfg.SliceMapNum),
	}
	return s, nil
}

func (s *space) ID(ctx context.Context) uint64 {
	return s.spaceID
}

func (s *space) SetCluster(ctx context.Context, cluster cluster.Cluster) {
	s.cluster = cluster
}

func (s *space) AddShard(ctx context.Context) error {

	id := s.generateShardID()
	//todo 1. raft

	err := s.addShard(ctx, id)
	if err != nil {
		return err
	}
	return nil
}

func (s *space) GetMeta(ctx context.Context) (*proto.SpaceMeta, error) {

	s.lock.RLock()
	var fields []*proto.FieldMeta
	for _, field := range s.info.FixedFields {
		fm := &proto.FieldMeta{
			Name:    field.Name,
			Type:    field.Type,
			Indexed: field.Indexed,
		}
		fields = append(fields, fm)
	}
	meta := &proto.SpaceMeta{
		Sid:         s.info.Sid,
		Name:        s.info.Name,
		Type:        s.info.Type,
		FixedFields: fields,
		Shards:      s.list(),
	}
	s.lock.RUnlock()
	return meta, nil
}

func (s *space) List(ctx context.Context) []*proto.Shard {
	return s.list()
}

func (s *space) Load(ctx context.Context) error {
	return nil
}

func (s *space) Report(ctx context.Context, reports []*ShardReport) error {

	span := trace.SpanFromContext(ctx)
	increase := int64(0)
	for _, report := range reports {
		getShard := s.allShards.Get(report.Info.ShardId)
		if getShard == nil {
			span.Errorf("shard[%d] report failed, err: %s", report.Info.ShardId, errors.ErrShardNotExist)
			return errors.ErrShardNotExist
		}
		increase = increase + getShard.Update(report.Info, report.LeaderId)
	}
	if increase > 0 {
		s.lock.Lock()
		s.used += uint64(increase)
		s.expanding()
		s.lock.Unlock()
	}
	return nil
}

// Flush flush heartbeat shard info to db
func (s *space) Flush(ctx context.Context) error {
	shardInfos := s.allShards.List()
	kvStore := s.store.KVStore()
	batch := kvStore.NewWriteBatch()
	for _, info := range shardInfos {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, info.ShardId)
		marshal, err := info.Marshal()
		if err != nil {
			return err
		}
		batch.Put(shardCF, key, marshal)
	}
	return kvStore.Write(ctx, batch, nil)
}

func (s *space) expanding() {
	level := float64(s.used) / float64(uint64(s.currentShardID)*s.inoLimit)
	if level >= s.threshold && s.state == SpaceStateNormal {
		select {
		case s.expChan <- s:
		default:
		}
	}
}

func (s *space) generateShardID() uint32 {
	return atomic.AddUint32(&s.currentShardID, 1)
}

func (s *space) list() []*proto.Shard {
	var shards []*proto.Shard
	s.allShards.Range(func(v Shard) error {
		var nodes []*proto.Node
		info := v.Info()
		for _, n := range info.Replicates {
			nodes = append(nodes, n.ToProtoNode())
		}
		sh := &proto.Shard{
			RouteVersion: info.RouteVersion,
			Id:           info.ShardId,
			InoLimit:     info.InoLimit,
			InoUsed:      info.InoUsed,
			Nodes:        nodes,
		}
		shards = append(shards, sh)
		return nil
	})
	return shards
}

// shardedSpaces is an effective data struct (concurrent map implements)
type shardedSpaces struct {
	num   uint32
	m     map[uint32]map[uint64]Space
	locks map[uint32]*sync.RWMutex
}

func newShardSpaces(sliceMapNum uint32) *shardedSpaces {
	spaces := &shardedSpaces{
		num:   sliceMapNum,
		m:     make(map[uint32]map[uint64]Space),
		locks: make(map[uint32]*sync.RWMutex),
	}
	for i := uint32(0); i < sliceMapNum; i++ {
		spaces.locks[i] = &sync.RWMutex{}
		spaces.m[i] = make(map[uint64]Space)
	}
	return spaces
}

// get space from shardedSpaces
func (s *shardedSpaces) getSpace(sid uint64) Space {
	idx := uint32(sid) % s.num
	s.locks[idx].RLock()
	defer s.locks[idx].RUnlock()
	return s.m[idx][sid]
}

// put new space into shardedSpace
func (s *shardedSpaces) putSpace(ctx context.Context, v Space) {
	id := v.ID(ctx)
	idx := uint32(id) % s.num
	s.locks[idx].Lock()
	defer s.locks[idx].Unlock()
	_, ok := s.m[idx][id]
	// space already exist
	if ok {
		return
	}
	s.m[idx][id] = v
}

// delete space into shardedSpace
func (s *shardedSpaces) deleteSpace(id uint64) {
	idx := uint32(id) % s.num
	s.locks[idx].Lock()
	defer s.locks[idx].Unlock()
	delete(s.m[idx], id)
}

// range shardedSpaces, it only use in flush atomic switch situation.
// in other situation, it may occupy the read lock for a long time
func (s *shardedSpaces) rangeSpace(f func(v Space) error) {
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

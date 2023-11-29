package catalog

import (
	"context"
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
	"sync"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	apierrors "github.com/cubefs/inodedb/errors"
	"github.com/cubefs/inodedb/proto"
	"github.com/cubefs/inodedb/raft"
	"github.com/cubefs/inodedb/shardserver/catalog/persistent"
	"github.com/cubefs/inodedb/shardserver/store"
)

const (
	sysRawFSPath = "sys"
	diskMetaFile = "disk.meta"
)

type (
	shardHandler interface {
		GetNodeInfo() *proto.Node
		GetShardBaseConfig() *ShardBaseConfig
	}
	diskConfig struct {
		nodeID          proto.NodeID
		diskPath        string
		checkMountPoint bool
		storeConfig     store.Config
		raftConfig      raft.Config
		transport       *transport
		shardHandler    shardHandler
	}
)

func openDisk(ctx context.Context, cfg diskConfig) *disk {
	span := trace.SpanFromContext(ctx)

	if cfg.checkMountPoint {
		if !store.IsMountPoint(cfg.diskPath) {
			span.Fatalf("disk path[%s] is not mount point", cfg.diskPath)
		}
	}

	cfg.storeConfig.Path = cfg.diskPath
	store, err := store.NewStore(ctx, &cfg.storeConfig)
	if err != nil {
		span.Fatalf("new store instance failed: %s", err)
	}

	// load disk meta info
	stats, err := store.Stats()
	if err != nil {
		span.Fatalf("stats store info failed: %s", err)
	}
	diskInfo := persistent.DiskInfo{
		Path:   cfg.diskPath,
		NodeID: cfg.nodeID,
		Total:  stats.Total,
		Used:   stats.Used,
	}
	rawFS := store.NewRawFS(sysRawFSPath)
	f, err := rawFS.OpenRawFile(diskMetaFile)
	if err != nil && !os.IsNotExist(err) {
		span.Fatalf("open disk meta file failed : %s", err)
	}
	if err == nil {
		b, err := ioutil.ReadAll(f)
		if err != nil {
			span.Fatalf("read disk meta file failed: %s", err)
		}
		if err := diskInfo.Unmarshal(b); err != nil {
			span.Fatalf("unmarshal disk meta failed: %s, raw: %v", err, b)
		}
	}

	return &disk{
		DiskInfo:     diskInfo,
		shardHandler: cfg.shardHandler,
		raftManager:  nil,
		store:        store,
	}
}

type disk struct {
	persistent.DiskInfo

	shardsMu struct {
		sync.RWMutex
		// sid + shard id as the map key
		shards map[[12]byte]*shard
	}
	shardHandler shardHandler
	raftManager  raft.Manager
	store        *store.Store
	lock         sync.RWMutex
}

func (d *disk) Load(ctx context.Context) error {
	listKeyPrefix := make([]byte, len(shardInfoPrefix))
	encodeShardInfoListPrefix(listKeyPrefix)

	kvStore := d.store.KVStore()
	lr := kvStore.List(ctx, dataCF, listKeyPrefix, nil, nil)
	defer lr.Close()

	for {
		kg, vg, err := lr.ReadNext()
		if err != nil {
			return errors.Info(err, "read next shard kv failed")
		}
		if kg == nil || vg == nil {
			break
		}

		shardInfo := &shardInfo{}
		if err := shardInfo.Unmarshal(vg.Value()); err != nil {
			return errors.Info(err, "unmarshal shard info failed")
		}

		shard, err := newShard(ctx, shardConfig{
			diskID:          d.DiskID,
			ShardBaseConfig: d.shardHandler.GetShardBaseConfig(),
			shardInfo:       *shardInfo,
			nodeInfo:        d.shardHandler.GetNodeInfo(),
			store:           d.store,
			raftManager:     d.raftManager,
		})
		if err != nil {
			return errors.Info(err, "new shard failed")
		}

		shardKey := d.encodeShardKey(shardInfo.Sid, shardInfo.ShardID)
		d.shardsMu.Lock()
		d.shardsMu.shards[shardKey] = shard
		d.shardsMu.Unlock()

		shard.Start()
	}

	return nil
}

func (d *disk) AddShard(ctx context.Context, sid proto.Sid, shardID proto.ShardID, epoch uint64, inoLimit uint64, nodes []*proto.ShardNode) error {
	span := trace.SpanFromContext(ctx)

	d.shardsMu.Lock()
	defer d.shardsMu.Unlock()

	key := d.encodeShardKey(sid, shardID)
	if _, ok := d.shardsMu.shards[key]; ok {
		span.Warnf("shard[%d-%d] already exist", sid, shardID)
		return nil
	}

	shardNodes := make([]persistent.ShardNode, len(nodes))
	for i := range nodes {
		shardNodes[i] = persistent.ShardNode{
			DiskID:  nodes[i].DiskID,
			Learner: nodes[i].Learner,
		}
	}

	shardInfo := &shardInfo{
		ShardID:   shardID,
		Sid:       sid,
		InoCursor: calculateStartIno(shardID),
		InoLimit:  inoLimit,
		Epoch:     epoch,
		Nodes:     shardNodes,
	}

	shard, err := newShard(ctx, shardConfig{
		ShardBaseConfig: d.shardHandler.GetShardBaseConfig(),
		shardInfo:       *shardInfo,
		nodeInfo:        d.shardHandler.GetNodeInfo(),
		diskID:          d.DiskID,
		store:           d.store,
		raftManager:     d.raftManager,
	})
	if err != nil {
		return err
	}

	if err := shard.SaveShardInfo(ctx, false); err != nil {
		return err
	}

	d.shardsMu.shards[key] = shard
	shard.Start()
	return nil
}

func (d *disk) UpdateShard(ctx context.Context, sid proto.Sid, shardID proto.ShardID, epoch uint64) error {
	shard, err := d.GetShard(sid, shardID)
	if err != nil {
		return err
	}
	return shard.UpdateShard(ctx, &persistent.ShardInfo{
		Epoch: epoch,
	})
}

func (d *disk) GetShard(sid proto.Sid, shardID proto.ShardID) (*shard, error) {
	key := d.encodeShardKey(sid, shardID)
	d.shardsMu.RLock()
	s := d.shardsMu.shards[key]
	d.shardsMu.RUnlock()

	if s == nil {
		return nil, apierrors.ErrShardDoesNotExist
	}
	return s, nil
}

func (d *disk) DeleteShard(ctx context.Context, sid proto.Sid, shardID proto.ShardID) error {
	key := d.encodeShardKey(sid, shardID)

	d.shardsMu.Lock()
	shard := d.shardsMu.shards[key]
	delete(d.shardsMu.shards, key)
	d.shardsMu.Unlock()

	if shard != nil {
		shard.Stop()
		shard.Close()
		// todo: clear shard's data
	}

	return nil
}

func (d *disk) RangeShard(f func(s *shard) bool) {
	d.shardsMu.RLock()
	for _, shard := range d.shardsMu.shards {
		if !f(shard) {
			break
		}
	}
	d.shardsMu.RUnlock()
}

func (d *disk) GetDiskInfo() persistent.DiskInfo {
	d.lock.RLock()
	ret := d.DiskInfo
	d.lock.RUnlock()
	return ret
}

func (d *disk) SaveDiskInfo() error {
	rawFS := d.store.NewRawFS(sysRawFSPath)
	f, err := rawFS.OpenRawFile(diskMetaFile)
	if err != nil {
		return err
	}

	d.lock.Lock()
	b, err := d.DiskInfo.Marshal()
	if err != nil {
		return err
	}
	d.lock.Unlock()

	n, err := f.Write(b)
	if err != nil {
		return err
	}
	if n != len(b) {
		return io.ErrShortWrite
	}

	return f.Close()
}

func (d *disk) encodeShardKey(sid proto.Sid, shardID proto.ShardID) [12]byte {
	key := [12]byte{}
	binary.BigEndian.PutUint64(key[:8], uint64(sid))
	binary.BigEndian.PutUint32(key[8:], uint32(shardID))

	return key
}

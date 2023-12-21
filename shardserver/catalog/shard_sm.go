package catalog

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/util/log"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/inodedb/common/kvstore"
	"github.com/cubefs/inodedb/proto"
	"github.com/cubefs/inodedb/raft"
	"github.com/cubefs/inodedb/shardserver/catalog/persistent"
)

const (
	RaftOpInsertItem uint32 = iota + 1
	RaftOpUpdateItem
	RaftOpDeleteItem
	RaftOpLinkItem
	RaftOpUnlinkItem
	RaftOpAllocInoRange
)

type shardSM shard

func (s *shardSM) Apply(cxt context.Context, pd []raft.ProposalData, index uint64) (rets []interface{}, err error) {
	rets = make([]interface{}, len(pd))

	for i := range pd {
		_, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", string(pd[i].Context))
		switch pd[i].Op {
		case RaftOpInsertItem:
			if err = s.applyInsertItem(ctx, pd[i].Data); err != nil {
				return
			}
			rets[i] = nil
		case RaftOpUpdateItem:
			if err = s.applyUpdateItem(ctx, pd[i].Data); err != nil {
				return
			}
			rets[i] = nil
		case RaftOpDeleteItem:
			if err = s.applyDeleteItem(ctx, pd[i].Data); err != nil {
				return
			}
			rets[i] = nil
		case RaftOpLinkItem:
			if err = s.applyLink(ctx, pd[i].Data); err != nil {
				return
			}
			rets[i] = nil
		case RaftOpUnlinkItem:
			if err = s.applyUnlink(ctx, pd[i].Data); err != nil {
				return
			}
			rets[i] = nil
		case RaftOpAllocInoRange:
			if rets[i], err = s.applyAllocInoRange(ctx, pd[i].Data); err != nil {
				return
			}
		default:
			panic(fmt.Sprintf("unsupported operation type: %d", pd[i].Op))
		}
	}

	s.setAppliedIndex(index)
	return
}

func (s *shardSM) LeaderChange(peerID uint64) error {
	log.Info("shard receive leader change", peerID)
	// todo: report leader change to master
	s.shardMu.Lock()
	s.shardMu.leader = proto.DiskID(peerID)
	s.shardMu.Unlock()

	return nil
}

func (s *shardSM) ApplyMemberChange(cc *raft.Member, index uint64) error {
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	s.shardMu.Lock()
	defer s.shardMu.Unlock()

	switch cc.Type {
	case raft.MemberChangeType_AddMember:
		found := false
		for _, node := range s.shardMu.Nodes {
			if node.DiskID == uint32(cc.NodeID) {
				found = true
				break
			}
		}
		if !found {
			s.shardMu.Nodes = append(s.shardMu.Nodes, persistent.ShardNode{
				DiskID:  uint32(cc.NodeID),
				Learner: cc.Learner,
			})
		}
	case raft.MemberChangeType_RemoveMember:
		for i, node := range s.shardMu.Nodes {
			if node.DiskID == uint32(cc.NodeID) {
				s.shardMu.Nodes = append(s.shardMu.Nodes[:i], s.shardMu.Nodes[i+1:]...)
				break
			}
		}
	}

	return (*shard)(s).SaveShardInfo(ctx, false)
}

func (s *shardSM) Snapshot() raft.Snapshot {
	kvStore := s.store.KVStore()
	appliedIndex := s.getAppliedIndex()
	kvSnap := kvStore.NewSnapshot()
	readOpt := kvStore.NewReadOption()
	readOpt.SetSnapShot(kvSnap)

	// create cf list reader
	lrs := make([]kvstore.ListReader, 0)
	for _, cf := range []kvstore.CF{dataCF, lockCF, writeCF} {
		prefix := make([]byte, shardPrefixSize())
		encodeShardPrefix(s.shardMu.Sid, s.shardID, prefix)
		lrs = append(lrs, kvStore.List(context.Background(), cf, prefix, nil, readOpt))
	}

	return &raftSnapshot{
		appliedIndex:               appliedIndex,
		RaftSnapshotTransmitConfig: &s.cfg.RaftSnapTransmitConfig,
		st:                         kvSnap,
		ro:                         readOpt,
		lrs:                        lrs,
		kvStore:                    kvStore,
	}
}

func (s *shardSM) ApplySnapshot(snap raft.Snapshot) error {
	defer snap.Close()
	kvStore := s.store.KVStore()
	_, ctx := trace.StartSpanFromContext(context.Background(), "")

	// clear all data with shard prefix
	batch := kvStore.NewWriteBatch()
	batch.DeleteRange(dataCF, s.shardKeys.encodeShardPrefix(), s.shardKeys.encodeShardMaxPrefix())
	if err := kvStore.Write(ctx, batch, nil); err != nil {
		return err
	}

	for {
		batch, err := snap.ReadBatch()
		if err != nil && err != io.EOF {
			return err
		}

		if batch != nil {
			if err := kvStore.Write(ctx, batch.(raftBatch).batch, nil); err != nil {
				batch.Close()
				return err
			}
			batch.Close()
		}
		if err == io.EOF {
			break
		}
	}

	s.setAppliedIndex(snap.Index())

	return nil
}

func (s *shardSM) applyInsertItem(ctx context.Context, data []byte) error {
	protoItem := &proto.Item{}
	if err := protoItem.Unmarshal(data); err != nil {
		return err
	}

	// update inode cursor, so the follower node can keep up with leader's inode cursor
	s.updateInoCursor(protoItem.Ino)

	kvStore := s.store.KVStore()
	key := s.shardKeys.encodeInoKey(protoItem.Ino)
	vg, err := kvStore.Get(ctx, dataCF, key, nil)
	if err != nil && err != kvstore.ErrNotFound {
		s.increaseInoUsed()
		return err
	}
	// already insert, just return
	if err == nil {
		vg.Close()
		return nil
	}

	// transform into internal item
	fields := protoFieldsToInternalFields(protoItem.Fields)
	value, err := (&item{
		Ino:    protoItem.Ino,
		Links:  protoItem.Links,
		Fields: fields,
	}).Marshal()
	if err != nil {
		return err
	}
	// todo: add embedding type store with write batch

	if err := kvStore.SetRaw(ctx, dataCF, key, value, nil); err != nil {
		return err
	}

	s.increaseInoUsed()
	return nil
}

func (s *shardSM) applyUpdateItem(ctx context.Context, data []byte) error {
	span := trace.SpanFromContext(ctx)
	protoItem := &proto.Item{}
	if err := protoItem.Unmarshal(data); err != nil {
		return err
	}

	kvStore := s.store.KVStore()
	key := s.shardKeys.encodeInoKey(protoItem.Ino)
	data, err := kvStore.GetRaw(ctx, dataCF, key, nil)
	if err != nil {
		// replay raft wal log may meet with item deleted and replay update item operation
		if err == kvstore.ErrNotFound {
			span.Warnf("item[%v] has been deleted", protoItem)
			return nil
		}
		return err
	}
	item := &item{}
	if err := item.Unmarshal(data); err != nil {
		return err
	}

	fieldMap := make(map[string]int)
	for i := range item.Fields {
		fieldMap[item.Fields[i].Name] = i
	}
	for _, updateField := range protoItem.Fields {
		// update existed field or insert new field
		if idx, ok := fieldMap[updateField.Name]; ok {
			item.Fields[idx].Value = updateField.Value
			continue
		}
		item.Fields = append(item.Fields, persistent.Field{Name: updateField.Name, Value: updateField.Value})
	}
	// todo: add embedding type store with write batch

	data, err = item.Marshal()
	if err != nil {
		return err
	}
	return kvStore.SetRaw(ctx, dataCF, key, data, nil)
}

func (s *shardSM) applyDeleteItem(ctx context.Context, data []byte) error {
	ino := decodeIno(data)
	kvStore := s.store.KVStore()

	// independent check, avoiding decrease ino used repeatedly at raft log replay progress
	key := s.shardKeys.encodeInoKey(ino)
	vg, err := kvStore.Get(ctx, dataCF, key, nil)
	if err != nil {
		if err != kvstore.ErrNotFound {
			return err
		}
		s.decreaseInoUsed()
		return nil
	}
	vg.Close()

	if err := kvStore.Delete(ctx, dataCF, key, nil); err != nil {
		return err
	}

	s.decreaseInoUsed()
	return nil
}

func (s *shardSM) applyLink(ctx context.Context, data []byte) error {
	protoLink := &proto.Link{}
	if err := protoLink.Unmarshal(data); err != nil {
		return err
	}

	kvStore := s.store.KVStore()
	linkKey := s.shardKeys.encodeLinkKey(protoLink.Parent, protoLink.Name)
	vg, err := kvStore.Get(ctx, dataCF, linkKey, nil)
	if err != nil && err != kvstore.ErrNotFound {
		return errors.Info(err, "get link data failed")
	}
	// independent check
	if err == nil {
		vg.Close()
		return nil
	}

	// transform into internal link
	fields := protoFieldsToInternalFields(protoLink.Fields)
	link := &link{
		Parent: protoLink.Parent,
		Name:   protoLink.Name,
		Child:  protoLink.Child,
		Fields: fields,
	}
	linkData, err := link.Marshal()
	if err != nil {
		return errors.Info(err, "marshal link data failed")
	}

	// todo: add embedding type store with write batch

	pKey := s.shardKeys.encodeInoKey(protoLink.Parent)
	raw, err := kvStore.GetRaw(ctx, dataCF, pKey, nil)
	if err != nil {
		return errors.Info(err, "get parent item data failed")
	}
	pItem := &item{}
	if err := pItem.Unmarshal(raw); err != nil {
		return errors.Info(err, "unmarshal parent item data failed")
	}

	pItem.Links += 1
	pData, err := pItem.Marshal()
	if err != nil {
		return errors.Info(err, "marshal parent item data failed")
	}

	batch := kvStore.NewWriteBatch()
	batch.Put(dataCF, linkKey, linkData)
	batch.Put(dataCF, pKey, pData)
	return kvStore.Write(ctx, batch, nil)
}

func (s *shardSM) applyUnlink(ctx context.Context, data []byte) error {
	protoUnlink := &proto.Unlink{}
	if err := protoUnlink.Unmarshal(data); err != nil {
		return err
	}

	kvStore := s.store.KVStore()
	pKey := s.shardKeys.encodeInoKey(protoUnlink.Parent)
	linkKey := s.shardKeys.encodeLinkKey(protoUnlink.Parent, protoUnlink.Name)

	vg, err := kvStore.Get(ctx, dataCF, linkKey, nil)
	if err != nil && err != kvstore.ErrNotFound {
		return errors.Info(err, "get link data failed")
	}
	// independent check
	if err == kvstore.ErrNotFound {
		return nil
	}
	vg.Close()

	raw, err := kvStore.GetRaw(ctx, dataCF, pKey, nil)
	if err != nil {
		return errors.Info(err, "get parent item data failed")
	}
	pItem := &item{}
	if err := pItem.Unmarshal(raw); err != nil {
		return errors.Info(err, "unmarshal parent item data failed")
	}

	pItem.Links -= 1
	pData, err := pItem.Marshal()
	if err != nil {
		return errors.Info(err, "marshal parent item data failed")
	}

	batch := kvStore.NewWriteBatch()
	batch.Delete(dataCF, linkKey)
	batch.Put(dataCF, pKey, pData)
	return kvStore.Write(ctx, batch, nil)
}

func (s *shardSM) applyAllocInoRange(ctx context.Context, data []byte) (inodeRange, error) {
	if inoUsed := atomic.LoadUint64(&s.shardMu.InoUsed); inoUsed > s.shardMu.InoLimit {
		span := trace.SpanFromContext(ctx)
		span.Warnf("inode limit exceed[%d], can alloc inode range any more", inoUsed)
		return inodeRange{}, nil
	}

	count := binary.BigEndian.Uint64(data)
	s.shardMu.Lock()
	inoRange := inodeRange{
		start: s.shardMu.InoCursor,
		end:   s.shardMu.InoCursor + count,
	}
	s.shardMu.Unlock()

	return inoRange, nil
}

// todo: how to optimized the lock arena of shardMu and InoUsed modification
func (s *shardSM) increaseInoUsed() {
	atomic.AddUint64(&s.shardMu.InoUsed, 1)
}

// todo: how to optimized the lock arena of shardMu and InoUsed modification
func (s *shardSM) decreaseInoUsed() {
	for {
		cur := atomic.LoadUint64(&s.shardMu.InoUsed)
		new := cur - 1
		if atomic.CompareAndSwapUint64(&s.shardMu.InoUsed, cur, new) {
			return
		}
	}
}

func (s *shardSM) setAppliedIndex(index uint64) {
	atomic.StoreUint64(&s.shardMu.AppliedIndex, index)
}

func (s *shardSM) getAppliedIndex() uint64 {
	return atomic.LoadUint64(&s.shardMu.AppliedIndex)
}

func (s *shardSM) updateInoCursor(new uint64) {
	for {
		cur := atomic.LoadUint64(&s.shardMu.InoCursor)
		if cur >= new {
			return
		}

		if atomic.CompareAndSwapUint64(&s.shardMu.InoCursor, cur, new) {
			return
		}
	}
}

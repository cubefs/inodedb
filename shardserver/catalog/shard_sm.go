package catalog

import (
	"context"
	"fmt"

	"github.com/cubefs/inodedb/common/raft"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/inodedb/common/kvstore"
	"github.com/cubefs/inodedb/proto"
	"github.com/cubefs/inodedb/shardserver/catalog/persistent"
	pb "google.golang.org/protobuf/proto"
)

func (s *shard) Apply(ctx context.Context, op raft.Op, data []byte, index uint64) (result interface{}, err error) {
	switch op {
	case RaftOpInsertItem:
		err := s.applyInsertItem(ctx, data)
		return nil, err
	case RaftOpUpdateItem:
		err := s.applyUpdateItem(ctx, data)
		return nil, err
	case RaftOpDeleteItem:
		err := s.applyDeleteItem(ctx, data)
		return nil, err
	case RaftOpLinkItem:
		err := s.applyLink(ctx, data)
		return nil, err
	case RaftOpUnlinkItem:
		err := s.applyUnlink(ctx, data)
		return nil, err
	default:
		panic(fmt.Sprintf("unsupported operation type: %d", op))
	}
	return nil, nil
}

func (s *shard) ApplyMemberChange(cc raft.ConfChange, index uint64) error {
	return nil
}

func (s *shard) Snapshot() (raft.Snapshot, error) {
	return nil, nil
}

func (s *shard) ApplySnapshot(st raft.Snapshot) error {
	return nil
}

func (s *shard) LeaderChange(leader uint64, addr string) error {
	return nil
}

func (s *shard) applyInsertItem(ctx context.Context, data []byte) error {
	protoItem := &proto.Item{}
	if err := pb.Unmarshal(data, protoItem); err != nil {
		return err
	}

	kvStore := s.store.KVStore()
	key := s.shardKeys.encodeInoKey(protoItem.Ino)
	vg, err := kvStore.Get(ctx, dataCF, key, nil)
	if err != nil && err != kvstore.ErrNotFound {
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

	if err := kvStore.SetRaw(ctx, dataCF, key, value, nil); err != nil {
		return err
	}
	s.increaseInoUsed()

	// TODO: move this into raft flush progress
	return s.saveShardInfo(ctx)
}

func (s *shard) applyUpdateItem(ctx context.Context, data []byte) error {
	span := trace.SpanFromContext(ctx)
	protoItem := &proto.Item{}
	if err := pb.Unmarshal(data, protoItem); err != nil {
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
		item.Fields = append(item.Fields, &persistent.Field{Name: updateField.Name, Value: updateField.Value})
	}

	data, err = item.Marshal()
	if err != nil {
		return err
	}
	return kvStore.SetRaw(ctx, dataCF, key, data, nil)
}

func (s *shard) applyDeleteItem(ctx context.Context, data []byte) error {
	ino := decodeIno(data)
	kvStore := s.store.KVStore()
	if err := kvStore.Delete(ctx, dataCF, s.shardKeys.encodeInoKey(ino), nil); err != nil {
		return err
	}

	s.decreaseInoUsed()
	// TODO: move this into raft flush progress
	return s.saveShardInfo(ctx)
}

func (s *shard) applyLink(ctx context.Context, data []byte) error {
	protoLink := &proto.Link{}
	if err := pb.Unmarshal(data, protoLink); err != nil {
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

func (s *shard) applyUnlink(ctx context.Context, data []byte) error {
	protoUnlink := &proto.Unlink{}
	if err := pb.Unmarshal(data, protoUnlink); err != nil {
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

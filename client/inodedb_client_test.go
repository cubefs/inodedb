package client

import (
	"context"
	"testing"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/log"
	"github.com/cubefs/inodedb/proto"
)

var (
	spaceName  = "space5"
	fieldMetas = []*proto.FieldMeta{
		{Name: "f1", Type: proto.FieldType_Int},
		{Name: "f2", Type: proto.FieldType_String},
		{Name: "f3", Type: proto.FieldType_Bool},
	}
)

func TestInodeDBClient(t *testing.T) {
	inodeDBClient, err := NewInodeDBClient(&InodeDBConfig{
		ShardServerConfig: ShardServerConfig{
			MasterAddresses: "127.0.0.1:9021",
		},
	})
	if err != nil {
		log.Fatalf("new inodedb client failed: %s", err)
	}

	span, ctx := trace.StartSpanFromContextWithTraceID(context.Background(), "", "inodedb-client")
	resp, err := inodeDBClient.GetRoleNodes(ctx, &proto.GetRoleNodesRequest{Role: proto.NodeRole_ShardServer})
	if err != nil {
		span.Fatalf("get role nodes failed: %s", err)
	}
	span.Infof("role nodes: %+v", resp.Nodes)
	nodes := resp.Nodes

	func() {
		resp, err := inodeDBClient.CreateSpace(ctx, &proto.CreateSpaceRequest{
			Name:          spaceName,
			Type:          proto.SpaceType_Inode,
			DesiredShards: 10,
			FixedFields:   fieldMetas,
		})
		if err != nil {
			span.Fatalf("create space failed: %s", err)
		}
		span.Infof("space info: %+v", resp.Info)
	}()

	time.Sleep(2 * time.Second)
	func() {
		shardServerClient, err := inodeDBClient.GetClient(ctx, nodes[0].GetId())
		if err != nil {
			span.Fatalf("get shard server client failed: %s", err)
		}

		resp, err := shardServerClient.ShardInsertItem(ctx, &proto.InsertItemRequest{
			SpaceName:      spaceName,
			PreferredShard: 1,
			Item: &proto.Item{
				Fields: []*proto.Field{
					{Name: fieldMetas[0].Name, Value: []byte("v1")},
					{Name: fieldMetas[1].Name, Value: []byte("v2")},
					{Name: fieldMetas[2].Name, Value: []byte("v3")},
				},
			},
		})
		if err != nil {
			span.Fatalf("insert item failed: %s", err)
		}
		span.Infof("item ino: %d", resp.Ino)

		_, err = shardServerClient.ShardUpdateItem(ctx, &proto.UpdateItemRequest{
			SpaceName: spaceName,
			Item: &proto.Item{
				Ino: resp.Ino,
				Fields: []*proto.Field{
					{Name: fieldMetas[0].Name, Value: []byte("v11")},
					{Name: fieldMetas[1].Name, Value: []byte("v22")},
					{Name: fieldMetas[2].Name, Value: []byte("v33")},
				},
			},
		})
		if err != nil {
			span.Fatalf("update item failed: %s", err)
		}

		getResp, err := shardServerClient.ShardGetItem(ctx, &proto.GetItemRequest{
			SpaceName: spaceName,
			Ino:       resp.Ino,
		})
		if err != nil {
			span.Fatalf("get item failed: %s", err)
		}
		span.Infof("item info: %+v", getResp.Item)

		_, err = shardServerClient.ShardDeleteItem(ctx, &proto.DeleteItemRequest{
			SpaceName: spaceName,
			Ino:       resp.Ino,
		})
		if err != nil {
			span.Fatalf("delete item failed: %s", err)
		}
	}()

	func() {
		shardServerClient, err := inodeDBClient.GetClient(ctx, nodes[0].GetId())
		if err != nil {
			span.Fatalf("get shard server client failed: %s", err)
		}

		resp, err := shardServerClient.ShardInsertItem(ctx, &proto.InsertItemRequest{
			SpaceName:      spaceName,
			PreferredShard: 1,
			Item: &proto.Item{
				Fields: []*proto.Field{
					{Name: fieldMetas[0].Name, Value: []byte("v1")},
					{Name: fieldMetas[1].Name, Value: []byte("v2")},
					{Name: fieldMetas[2].Name, Value: []byte("v3")},
				},
			},
		})
		if err != nil {
			span.Fatalf("insert item failed: %s", err)
		}
		span.Infof("parent ino: %d", resp.Ino)

		parentIno := resp.Ino

		resp, err = shardServerClient.ShardInsertItem(ctx, &proto.InsertItemRequest{
			SpaceName:      spaceName,
			PreferredShard: 1,
			Item: &proto.Item{
				Fields: []*proto.Field{
					{Name: fieldMetas[0].Name, Value: []byte("v1")},
					{Name: fieldMetas[1].Name, Value: []byte("v2")},
					{Name: fieldMetas[2].Name, Value: []byte("v3")},
				},
			},
		})
		if err != nil {
			span.Fatalf("insert item failed: %s", err)
		}
		span.Infof("child ino: %d", resp.Ino)
		childIno := resp.Ino

		_, err = shardServerClient.ShardLink(ctx, &proto.LinkRequest{
			SpaceName: spaceName,
			Link: &proto.Link{
				Parent: parentIno,
				Name:   "file1",
				Child:  childIno,
			},
		})
		if err != nil {
			span.Fatalf("link failed: %s", err)
		}

		listResp, err := shardServerClient.ShardList(ctx, &proto.ListRequest{
			SpaceName: spaceName,
			Ino:       parentIno,
			Start:     "",
			Num:       10,
		})
		if err != nil {
			span.Fatalf("list item failed: %s", err)
		}
		for _, link := range listResp.Links {
			span.Infof("list info: %+v", link)
		}

		_, err = shardServerClient.ShardUnlink(ctx, &proto.UnlinkRequest{
			SpaceName: spaceName,
			Unlink: &proto.Unlink{
				Parent: parentIno,
				Name:   "file1",
			},
		})
		if err != nil {
			span.Fatalf("unlink failed: %s", err)
		}

		listResp, err = shardServerClient.ShardList(ctx, &proto.ListRequest{
			SpaceName: spaceName,
			Ino:       parentIno,
			Start:     "",
			Num:       10,
		})
		if err != nil {
			span.Fatalf("list item failed: %s", err)
		}
		for _, link := range listResp.Links {
			span.Infof("unlink list info: %+v", link)
		}

		for _, ino := range []uint64{parentIno, childIno} {
			_, err = shardServerClient.ShardDeleteItem(ctx, &proto.DeleteItemRequest{
				SpaceName: spaceName,
				Ino:       ino,
			})
			if err != nil {
				span.Fatalf("delete item failed: %s", err)
			}
		}
	}()
}

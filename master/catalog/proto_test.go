package catalog

import (
	"testing"

	"github.com/cubefs/inodedb/proto"
)

func TestProto(t *testing.T) {
	detail := &routeItemSpaceAdd{
		Sid: 1,
	}
	itemInfo := &routeItemInfo{
		RouteVersion: 1,
		Type:         proto.CatalogChangeType_AddSpace,
		ItemDetail:   detail,
	}

	data, _ := itemInfo.Marshal()
	t.Log(string(data))

	new := &routeItemInfo{}
	new.Unmarshal(data)
	t.Log(new.ItemDetail)
}

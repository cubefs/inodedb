package catalog

import (
	"encoding/json"
	"testing"
)

func TestProto(t *testing.T) {
	detail := &routeItemSpaceAdd{
		Sid:  1,
		Name: "space1",
	}
	itemInfo := &routeItemInfo{
		RouteVersion: 0,
		Type:         0,
		ItemDetail:   detail,
	}

	data, _ := json.Marshal(itemInfo)
	t.Log(string(data))

	new := &routeItemInfo{ItemDetail: &routeItemSpaceAdd{}}
	json.Unmarshal(data, new)
	t.Log(new.ItemDetail)

	new1 := &routeItemInfo{}
	json.Unmarshal(data, new1)
	t.Log(new1)
}

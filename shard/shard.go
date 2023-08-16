package shard

import "github.com/cubefs/inodedb/shard/catalog"

type Shard struct {
	Catalog *catalog.Catalog
}

func NewShard() *Shard {
	return &Shard{}
}

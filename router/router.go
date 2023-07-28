package router

import (
	"context"
	"github.com/cubefs/inodedb/client"
	"github.com/cubefs/inodedb/proto"
)

type Router struct {
	ctx context.Context
	cli *client.Client
}

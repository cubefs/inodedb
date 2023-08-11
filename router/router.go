package router

import (
	"context"

	"github.com/cubefs/inodedb/client"
)

type Router struct {
	ctx context.Context
	cli *client.Client
}

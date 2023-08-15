package master

import (
	"github.com/cubefs/inodedb/proto"
	"google.golang.org/grpc"
)

type Config struct {
	MasterAddr []string `json:"master_addr"`
}

type Client struct {
	proto.InodeDBClient
}

func NewClient(cfg *Config) (*Client, error) {
	conn, err := grpc.Dial(cfg.MasterAddr)
	return &Client{}
}

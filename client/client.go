package client

import (
	"math"
	"time"

	"github.com/cubefs/inodedb/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

type Client struct {
	proto.SpaceClient
	conn *grpc.ClientConn
}

func NewClient(address string) (*Client, error) {
	dialOpts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(math.MaxInt64),
			grpc.MaxCallRecvMsgSize(math.MaxInt64),
		),
		grpc.WithKeepaliveParams(
			keepalive.ClientParameters{
				Time:                1 * time.Second,
				Timeout:             5 * time.Second,
				PermitWithoutStream: true,
			},
		),
	}

	dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.Dial(address, dialOpts...)
	if err != nil {
		return nil, err
	}
	client := proto.NewSpaceClient(conn)

	return &Client{
		SpaceClient: client,
		conn:        conn,
	}, nil
}

func (c *Client) Address() string {
	return c.conn.Target()
}

func (c *Client) Close() error {
	return c.conn.Close()
}

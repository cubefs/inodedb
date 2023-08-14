package raft

import (
	"context"
)

type raftNode struct{}

type Raft interface {
	Propose(ctx context.Context, data []byte) error
	Stop()
}

type Applier interface {
	Apply(ctx context.Context, data []byte) error
}

type Config struct{}

func NewRaft(ctx context.Context, cfg Config) Raft {
	return nil
}

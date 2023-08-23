package raft

import (
	"context"
)

type Raft interface {
	Propose(ctx context.Context, data []byte) error
	Stop()
}

type Applier interface {
	Apply(ctx context.Context, data []byte) error
}

type raftNode struct{}

type Config struct{}

type RaftOp uint32

func NewRaft(ctx context.Context, cfg Config) Raft {
	return nil
}

package catalog

import (
	"github.com/cubefs/inodedb/common/raft"
)

const (
	RaftOpInsertItem raft.Op = iota + 1
	RaftOpUpdateItem
	RaftOpDeleteItem
	RaftOpLinkItem
	RaftOpUnlinkItem
)

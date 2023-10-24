package raft

const (
	ErrCodeGroupHandlerNotFound = 601 + iota
	ErrCodeReplicaTooOld
	ErrCodeRaftGroupDeleted
	ErrCodeGroupHandleRaftMessage
	ErrCodeGroupNotFound
)

var (
	ErrGroupHandlerNotFound   = newError(ErrCodeGroupHandlerNotFound, "group handler not found")
	ErrReplicaTooOld          = newError(ErrCodeReplicaTooOld, "replica too old")
	ErrRaftGroupDeleted       = newError(ErrCodeRaftGroupDeleted, "raft group has been deleted")
	ErrGroupHandleRaftMessage = newError(ErrCodeGroupHandleRaftMessage, "group handle raft message failed")
	ErrGroupNotFound          = newError(ErrCodeGroupNotFound, "group not found")
)

func newError(code uint32, err string) *Error {
	return &Error{
		ErrorCode: code,
		Error:     err,
	}
}

package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIdGenerator(t *testing.T) {
	generator := newIDGenerator(1, time.Now())

	id1 := generator.Next()
	id2 := generator.Next()
	require.Equal(t, id1+1, id2)
}

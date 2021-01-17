package raft

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMatch(t *testing.T) {
	m := newMatch(3)
	m.update(1, 21)
	m.update(3, 22)

	require.Equal(t, 21, int(m.commited()))
}

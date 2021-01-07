package raft

import (
	"context"
	"time"

	"github.com/dshulyak/raft/types"
	"github.com/dshulyak/raftlog"
	"go.uber.org/zap"
)

type Context struct {
	context.Context

	ID        NodeID
	Transport types.Transport

	// EntriesPerAppend max number of entries in a single AppendEntries.
	EntriesPerAppend int
	// Proposals that buffered by the node before theys are sent to raft.
	ProposalsBuffer int
	// Proposals that are waiting for a confirmation from a majority.
	PendingProposalsBuffer int

	Storage *raftlog.Storage
	State   *DurableState

	Logger *zap.Logger

	// Timeout for dialing to a peer.
	DialTimeout time.Duration
	// Backoff will be doubled on every error. If peer initiates
	// a stream Backoff is reset to 0.
	Backoff time.Duration

	// Heartbeat and Election timeouts expressed in TickInterval's.
	TickInterval     time.Duration
	HeartbeatTimeout int

	// ElectionTimeout lower and upper bounds.
	ElectionTimeoutMin int
	ElectionTimeoutMax int

	// FIXME configuration will change according to the consensus rules
	// when cluster membership will be implemented
	Configuration *Configuration
}

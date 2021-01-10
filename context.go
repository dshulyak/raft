package raft

import (
	"context"
	"time"

	"github.com/dshulyak/raft/types"
	"github.com/dshulyak/raftlog"
	"go.uber.org/zap"
)

type Context struct {
	// TODO remove context.Context from this object and rename it to Config
	context.Context

	ID        types.NodeID
	Transport types.Transport
	App       types.Application

	// EntriesPerAppend max number of entries in a single AppendEntries.
	EntriesPerAppend int
	// Proposals that are buffered by the node while state machine is busy.
	ProposalsBuffer int
	// Proposals that are waiting for a confirmation from a majority of nodes.
	PendingProposalsBuffer int

	Storage *raftlog.Storage
	State   *DurableState

	Logger *zap.Logger

	// Timeout for dialing to a peer.
	DialTimeout time.Duration
	// Backoff after dialer failed. Only a candidate and a leader are running
	// a dialer.
	Backoff time.Duration

	// Heartbeat and Election timeouts are expressed in TickInterval's.
	TickInterval     time.Duration
	HeartbeatTimeout int

	// ElectionTimeout lower and upper bounds.
	ElectionTimeoutMin int
	ElectionTimeoutMax int

	// FIXME configuration will change according to the consensus rules
	// when cluster membership will be implemented
	Configuration *types.Configuration
}

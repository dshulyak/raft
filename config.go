package raft

import (
	"time"

	"github.com/dshulyak/raft/raftlog"
	"github.com/dshulyak/raft/types"
	"go.uber.org/zap"
)

const (
	// FeaturePreVote if enabled candidate will gather pre-votes from a majority
	// before incrementing its own term and starting election.
	// pre-vote doesn't cause current leader and followers to change
	// its own term and therefore will not disrupt the cluster if candidate
	// doesn't have upto-date log.
	// PreVote mode will allow to tolerate partially disconnected nodes
	// that otherwise would start multiple campaigns and livelock the cluster.
	FeaturePreVote uint32 = 1 << iota
)

func IsPreVoteEnabled(flags uint32) bool {
	return flags&FeaturePreVote > 0
}

type Config struct {
	ID        types.NodeID
	Transport types.Transport
	App       types.Application

	FeatureFlags uint32

	// EntriesPerAppend max number of entries in a single AppendEntries.
	EntriesPerAppend int
	// Proposals that are buffered by the node while state machine is busy.
	ProposalsBuffer int
	// ProposalsEvictionTimeout period since last batch of proposals were consumed
	// by a raft state machine.
	ProposalsEvictionTimeout time.Duration
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

package raft

import (
	"os"
	"path/filepath"
	"time"

	"github.com/dshulyak/raft/raftlog"
	"github.com/dshulyak/raft/transport"
	"github.com/dshulyak/raft/types"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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

type ConfigOption func(conf *Config) error

func BuildConfig(conf *Config, opts ...ConfigOption) error {
	for _, opt := range opts {
		if err := opt(conf); err != nil {
			return err
		}
	}
	return nil
}

// WithStorageAt pass after creating a Logger.
// If logger is created in the other ConfigOption be sure that it precedes
// WithStorageAt when passed to the BuildConfig.
func WithStorageAt(dirPath string) ConfigOption {
	return func(conf *Config) error {
		storage, err := raftlog.New(conf.Logger, &raftlog.IndexOptions{
			File: filepath.Join(dirPath, "index.raft"),
		}, &raftlog.LogOptions{
			File: filepath.Join(dirPath, "wal.raft"),
		})
		if err != nil {
			return err
		}
		conf.Storage = storage
		return nil
	}
}

func WithStateAt(dirPath string) ConfigOption {
	return func(conf *Config) error {
		f, err := os.OpenFile(
			filepath.Join(dirPath, "state.raft"), os.O_CREATE|os.O_RDWR, 0o660,
		)
		if err != nil {
			return err
		}
		state, err := NewDurableState(f)
		if err != nil {
			return err
		}
		conf.State = state
		return nil
	}
}

func WithLogger(lvl string) ConfigOption {
	return func(conf *Config) error {
		var level zapcore.Level
		if err := level.Set(lvl); err != nil {
			return err
		}
		logger, err := zap.NewProduction(zap.IncreaseLevel(level))
		if err != nil {
			return err
		}
		conf.Logger = logger
		return nil
	}
}

var DefaultConfig = Config{
	FeatureFlags:           FeaturePreVote,
	EntriesPerAppend:       128,
	ProposalsBuffer:        2048,
	PendingProposalsBuffer: 2048,
	DialTimeout:            200 * time.Millisecond,
	Backoff:                200 * time.Millisecond,
	TickInterval:           100 * time.Millisecond,
	HeartbeatTimeout:       1,
	ElectionTimeoutMin:     4,
	ElectionTimeoutMax:     10,
	Logger:                 zap.NewNop(),
}

type Config struct {
	ID        types.NodeID
	Transport transport.Transport
	App       Application

	FeatureFlags uint32

	// EntriesPerAppend max number of entries in a single AppendEntries.
	// TODO changes this to bytes
	EntriesPerAppend int
	// Proposals that are buffered by the node while state machine is busy.
	// If this limit is reached proposals will fail with ErrProposalsOverflow.
	ProposalsBuffer int
	// Proposals that are waiting for a confirmation from a majority of nodes.
	// TODO if limit is reached node should stop accepting proposals.
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

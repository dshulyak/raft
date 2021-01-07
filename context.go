package raft

import (
	"context"
	"time"

	"github.com/dshulyak/raftlog"
	"go.uber.org/zap"
)

type Context struct {
	context.Context
	Transport        Transport
	EntriesPerAppend int
	Storage          *raftlog.Storage
	Logger           *zap.Logger
	DialTimeout      time.Duration
	Backoff          time.Duration
	TickInterval     time.Duration
	HeartbeatTimeout int
	ElectionTimeout  int
	// FIXME configuration will change according to the consensus rules
	// when cluster membership will be implemented
	Configuration *Configuration
}

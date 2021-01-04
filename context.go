package raft

import (
	"context"
	"time"

	"github.com/dshulyak/raftlog"
	"go.uber.org/zap"
)

type Context struct {
	context.Context
	transport        Transport
	entriesPerAppend int
	storage          *raftlog.Storage
	logger           *zap.SugaredLogger
	backoff          time.Duration
	tickInterval     time.Duration
	heartbeatTimeout int
	electionTimeout  int
}

func (c *Context) Transport() Transport {
	return c.transport
}

func (c *Context) Storage() *raftlog.Storage {
	return c.storage
}

func (c *Context) EntriesPerAppend() int {
	return c.entriesPerAppend
}

func (c *Context) Logger() *zap.SugaredLogger {
	return c.logger
}

func (c *Context) Backoff() time.Duration {
	return c.backoff
}

func (c *Context) TickInterval() time.Duration {
	return c.tickInterval
}

func (c *Context) HeartbeatTimeout() int {
	return c.heartbeatTimeout
}

func (c *Context) ElectionTimeout() int {
	return c.electionTimeout
}

package raft

import (
	"context"
	"time"
)

func runTicker(ctx context.Context, timeout chan int, tick time.Duration) {
	go func() {
		ticker := time.NewTicker(tick)
		defer ticker.Stop()
		n := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				n++
			case timeout <- n:
				n = 0
			}
		}
	}()
}

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
		var (
			blocked chan int
		)
		for {
			if n > 0 {
				blocked = timeout
			} else {
				blocked = nil
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				n++
			case blocked <- n:
				n = 0
			}
		}
	}()
}

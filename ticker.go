package raft

import (
	"context"
	"time"
)

// runTicker accumulates ticks as long as timeout channel is blocked.
// consider what happens if ticker.C is used direct in state machine loop, such as:
//   select {
//       case msg := <-messages:
//           handle(msg)
//       case <-ticker.C:
//           tick()
//   }
//
// handle(msg) may run for longer than the actual tick, but state machine
// will be notified only about a single tick.
// with runTicker we basically replace ticker.C with unbuffered timeout channel
// which will consume a number of ticks only when it is ready.
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

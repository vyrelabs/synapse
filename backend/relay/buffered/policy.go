// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package buffered

import "time"

// State provides a snapshot of current metrics to the [BufferPolicy] to decide:
//   - How many tasks to prefetch/flush from/to the underlying queue.
//   - When to prefetch/flush tasks.
//
// In the future, this can extend to include other relevant metrics without
// breaking changes. If no other metrics could be added, we can consider
// passing primitive types directly instead of a struct.
//
// # Note
//
// This is intentionally kept minimal to avoid computational overhead in the hot-path.
//
// Internal developers should NOT add compute-intensive runtime metrics like statistics-based
// metrics (queuing theory, control theory, etc), as the [State] will be computed only when
// prefetch/flush decisions are to be made.
//
// To support this, the current architecture can be extended via a `MetricsProvider`
// (basically instrumentation) in the [BufferedScheduler] itself.
//
// Specialized use cases requiring more robust statistical heuristics for
// making prefetch/flush decisions (that i can overthink of):
//   - Serverless queue backends for cost optimization, where every op is billable.
//   - Hyperscaler infra, where even tiny compute adds up to significant costs.
//   - Research experiments on scheduling algorithms, again to avoid overhead.
//
// Also, most practical use-cases can be addressed with the current design only.
type State struct {
	BufLen int
	BufCap int
}

func (state State) Usage() float64 {
	return float64(state.BufLen) / float64(state.BufCap)
}

// Defines the behavior for the dynamic scheduler.
// It controls how tasks are buffered for prefetching and flushing.
type BufferPolicy interface {
	// Number of tasks to prefetch from the queue at once.
	Prefetch(state State) int

	// Whichever condition is met first triggers the flush.
	//
	// int: Number of tasks to batch before flushing to the queue.
	// time.Duration: Maximum time to wait before flushing tasks to the queue, when set to zero, it means no time-based flush.
	Flush(state State) (int, time.Duration)
}

type ThresholdPolicy struct {
	// Prefetch when minimum-prefetch-threshold is reached
	MinPrefetchThresh float64

	// Flush when maximum-flush-threshold is reached
	MaxFlushThresh float64
}

func NewThresholdPolicy(minPrefetchThresh float64, maxFlushThresh float64) *ThresholdPolicy {
	return &ThresholdPolicy{
		MinPrefetchThresh: minPrefetchThresh,
		MaxFlushThresh:    maxFlushThresh,
	}
}

func (p *ThresholdPolicy) Prefetch(state State) int {
	if state.Usage() <= p.MinPrefetchThresh {
		return state.BufCap - state.BufLen
	}
	return 0
}

func (p *ThresholdPolicy) Flush(state State) (int, time.Duration) {
	if state.Usage() >= p.MaxFlushThresh {
		return state.BufCap - state.BufLen, 0 * time.Millisecond
	}
	return 0, 0 * time.Millisecond
}

var _ BufferPolicy = (*ThresholdPolicy)(nil)

// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package buffered

import "time"

var _ BufferPolicy = (*DefaultPolicy)(nil)

// Trigger prefetch and flush at 100% buffer usage.
type DefaultPolicy struct {
	threshold ThresholdPolicy
}

func NewDefaultPolicy() *DefaultPolicy {
	return &DefaultPolicy{
		threshold: *NewThresholdPolicy(0, 1),
	}
}

// Trigger a prefetch at 100% buffer usage
func (p *DefaultPolicy) Prefetch(state State) int {
	return p.threshold.Prefetch(state)
}

// Trigger a flush at 100% buffer usage
func (p *DefaultPolicy) Flush(state State) (int, time.Duration) {
	return p.threshold.Flush(state)
}

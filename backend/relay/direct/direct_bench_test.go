// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package direct_test

import (
	"context"
	"testing"

	"github.com/vyrelabs/synapse/backend/relay/direct"
	"github.com/vyrelabs/synapse/internal/testutil/modeltest"
	"github.com/vyrelabs/synapse/internal/testutil/queuetest"
)

func BenchmarkDirectRelayDequeue(b *testing.B) {
	mq := queuetest.NewManualFIFO[Task]()
	c := direct.NewDirectRelay(mq, mq)

	task := modeltest.NewMockTask()
	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		_ = c.Dispatch(ctx, task)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Next(ctx)
	}
}

func BenchmarkDirectRelayEnqueue(b *testing.B) {
	mq := queuetest.NewManualFIFO[Task]()
	c := direct.NewDirectRelay(mq, mq)

	task := modeltest.NewMockTask()
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.Dispatch(ctx, task)
	}
}

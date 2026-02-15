// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package sequential_test

import (
	"context"
	"testing"

	"github.com/vyrelabs/synapse/backend/relay/sequential"
	"github.com/vyrelabs/synapse/internal/testutil/modeltest"
	"github.com/vyrelabs/synapse/internal/testutil/queuetest"
)

func BenchmarkSequentialRelayEnqueue(b *testing.B) {
	mq := queuetest.NewManualFIFO[Task]()
	sr := sequential.NewSequentialRelay(mq, mq)

	task := modeltest.NewMockTask()
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sr.Dispatch(ctx, task)
	}
}

func BenchmarkSequentialRelayDequeue(b *testing.B) {
	mq := queuetest.NewManualFIFO[Task]()
	sr := sequential.NewSequentialRelay(mq, mq)

	task := modeltest.NewMockTask()
	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		_ = sr.Dispatch(ctx, task)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = sr.Next(ctx)
	}
}

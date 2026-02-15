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

type Task = *modeltest.MockTask

func TestSequentialRelayDequeue(t *testing.T) {
	mq := queuetest.NewManualFIFO[Task]()
	sr := sequential.NewSequentialRelay(mq, mq)

	task := modeltest.NewMockTask()
	ctx := context.Background()

	for i := 0; i < 1000; i++ {
		_ = sr.Dispatch(ctx, task)
	}

	for i := 0; i < 1000; i++ {
		_, err := sr.Next(ctx)
		if err != nil {
			t.Fatalf("Failed to dequeue task: %v", err)
		}
	}
}

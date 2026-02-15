// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package direct_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/vyrelabs/synapse/backend/relay/direct"
	"github.com/vyrelabs/synapse/internal/testutil/modeltest"
	"github.com/vyrelabs/synapse/internal/testutil/queuetest"
)

type Task = *modeltest.MockTask

func TestDirectRelayCorrectness(t *testing.T) {
	mq := queuetest.NewManualFIFO[Task]()
	relay := direct.NewDirectRelay(mq, mq)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	count := 10
	inputUrls := make([]string, count)

	for i := range count {
		url := fmt.Sprintf("http://example.com/%d", i)
		inputUrls[i] = url
		task := modeltest.NewMockTask(modeltest.WithURL(url))

		if err := relay.Dispatch(ctx, task); err != nil {
			t.Fatalf("Dispatch failed at index %d: %v", i, err)
		}
	}

	for i := range count {
		task, err := relay.Next(ctx)
		if err != nil {
			t.Fatalf("Next failed at index %d: %v", i, err)
		}

		if task.Url != inputUrls[i] {
			t.Errorf("out-of-order exec: expected %s, got %s", inputUrls[i], task.Url)
		}
	}

	len, _ := mq.Len(ctx)
	if len != 0 {
		t.Errorf("Expected queue to be empty post-processing, but it has %d items", len)
	}
}

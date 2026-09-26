// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package mem_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vyrelabs/synapse/frontier/filter/mem"
)

func TestCoreOperations(t *testing.T) {
	ctx := context.Background()
	seenSet := mem.NewInMemorySeenSet()

	t.Run("empty set", func(t *testing.T) {
		exists, err := seenSet.HasSeen(ctx, "https://example.com")
		assert.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("mark and check", func(t *testing.T) {
		urls := []string{
			"https://example.com/1",
			"https://example.com/2",
			"https://example.com/3",
		}

		for _, url := range urls {
			err := seenSet.MarkSeen(ctx, url)
			assert.NoError(t, err)
		}

		for _, url := range urls {
			exists, err := seenSet.HasSeen(ctx, url)
			assert.NoError(t, err)
			assert.True(t, exists)
		}

		exists, err := seenSet.HasSeen(ctx, "https://example.com/not-seen")
		assert.NoError(t, err)
		assert.False(t, exists)
	})
}

func TestIdempotency(t *testing.T) {
	ctx := context.Background()
	seenSet := mem.NewInMemorySeenSet()
	url := "https://example.com/test"

	for i := 0; i < 10; i++ {
		err := seenSet.MarkSeen(ctx, url)
		assert.NoError(t, err)
	}

	exists, err := seenSet.HasSeen(ctx, url)
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestConcurrency(t *testing.T) {
	ctx := context.Background()
	seenSet := mem.NewInMemorySeenSet()
	urls := make([]string, 100)
	for i := range urls {
		urls[i] = "https://example.com/" + string(rune(i))
	}

	var wg sync.WaitGroup
	for _, url := range urls {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			err := seenSet.MarkSeen(ctx, u)
			assert.NoError(t, err)
		}(url)
	}

	wg.Wait()

	for _, url := range urls {
		exists, err := seenSet.HasSeen(ctx, url)
		assert.NoError(t, err)
		assert.True(t, exists)
	}
}

func TestContextCancellation(t *testing.T) {
	seenSet := mem.NewInMemorySeenSet()

	// probably better naming, idk?
	t.Run("cancel followed by mark-seen()", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := seenSet.MarkSeen(ctx, "https://example.com/test")
		assert.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("cancel followed by has-seen()", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		exists, err := seenSet.HasSeen(ctx, "https://example.com/test")
		assert.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
		assert.False(t, exists)
	})

	t.Run("timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()

		time.Sleep(10 * time.Millisecond)

		err := seenSet.MarkSeen(ctx, "https://example.com/test")
		assert.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

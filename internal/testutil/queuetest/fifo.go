// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package queuetest

// NOTE: Super inefficient (wastes precious __compute__), but good enough for testing.
//       Consider using a more efficient one, if needed.

import (
	"context"
	"sync"

	"github.com/vyrelabs/synapse/backend"
)

var (
	_ backend.Consumer[any] = (*ManualFIFO[any])(nil)
	_ backend.Producer[any] = (*ManualFIFO[any])(nil)
	_ backend.Queue[any]    = (*ManualFIFO[any])(nil)
)

// ManualFIFO is a thread-safe in-memory FIFO queue implementation
type ManualFIFO[T any] struct {
	items []T
	mu    sync.RWMutex
}

func NewManualFIFO[T any]() *ManualFIFO[T] {
	return &ManualFIFO[T]{
		items: make([]T, 0),
	}
}

func (q *ManualFIFO[T]) Enqueue(ctx context.Context, batch backend.Batch[T]) (int, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	q.items = append(q.items, batch...)
	return len(batch), nil
}

func (q *ManualFIFO[T]) Dequeue(ctx context.Context, batch backend.Batch[T]) (int, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	n := min(len(batch), len(q.items))

	if n == 0 {
		return 0, nil
	}

	copy(batch, q.items[:n])
	q.items = q.items[n:]

	return n, nil
}

func (q *ManualFIFO[T]) Len(ctx context.Context) (int, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	return len(q.items), nil
}

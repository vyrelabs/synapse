// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package backend

// TODO:
// - Ack/Nack
// - Decide whether or not to enforce 'Codec'?

import (
	"context"
	"errors"
)

type Batch[T any] = []T

type Consumer[I any] interface {
	Dequeue(ctx context.Context, batch Batch[I]) (int, error)
	Len(ctx context.Context) (int, error)
}

type Producer[O any] interface {
	Enqueue(ctx context.Context, batch Batch[O]) (int, error)
}

type Queue[T any] interface {
	Consumer[T]
	Producer[T]
}

var ErrEmptyQueue = errors.New("empty queue")

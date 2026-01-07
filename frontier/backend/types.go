package backend

import (
	"context"
)

// Generic Queue interface
type Queue[T any] interface {
	// Insert items into the queue.
	Enqueue(ctx context.Context, items []T) error

	// Retrieve upto 'n' items from the queue.
	Dequeue(ctx context.Context, n int) <-chan T // todo: remove

	// Underlying dequeue channel
	DequeueCh(ctx context.Context) <-chan T

	// Number of pending items in the queue.
	Len(ctx context.Context) (int, error)
}

type Store[T any] interface {
	Put(ctx context.Context, key string, value T) error
	Get(ctx context.Context, key string) (T, error)
	Delete(ctx context.Context, key string) error
}

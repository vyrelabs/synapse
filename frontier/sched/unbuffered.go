// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package sched

import (
	"context"
)

var _ Scheduler[any] = (*UnbufferedScheduler[any])(nil)

// Directly enqueue/dequeue to/from the backend queue without any buffering.
type UnbufferedScheduler[T any] struct {
	queue     Queue[T]
	dequeueCh chan Task[T]
}

func NewUnbufferedScheduler[T any](queue Queue[T]) *UnbufferedScheduler[T] {
	return &UnbufferedScheduler[T]{
		queue:     queue,
		dequeueCh: make(chan Task[T], 1),
	}
}

func (s *UnbufferedScheduler[T]) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (s *UnbufferedScheduler[T]) Dequeue(ctx context.Context) (Task[T], error) {
	n, err := s.queue.Dequeue(ctx, 1, s.dequeueCh)
	if err != nil {
		// TODO: retry/backoff
		return nil, err
	}
	if n <= 0 {
		return nil, nil
	}

	select {
	case task := <-s.dequeueCh:
		return task, nil
	case <-ctx.Done():
		return nil, nil
	default:
	}

	return nil, nil
}

func (s *UnbufferedScheduler[T]) Enqueue(ctx context.Context, task Task[T]) error {
	return s.queue.Enqueue(ctx, []Task[T]{task})
}

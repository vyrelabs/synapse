// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package sched

import (
	"context"

	"github.com/vyrelabs/synapse/frontier/backend"
	"github.com/vyrelabs/synapse/lifecycle"
	"github.com/vyrelabs/synapse/model"
)

type (
	Task[T any]  = *model.Task[T]
	Queue[T any] = backend.Queue[Task[T]]
)

type Scheduler[T any] interface {
	lifecycle.Runner

	// Enqueues a task into the scheduler,
	// eventally to be flushed to the underlying queue.
	Enqueue(ctx context.Context, task Task[T]) error

	// Dequeues tasks from the underlying queue into the scheduler's buffer
	// (if any, based on underlying implementation) and returns the number of tasks dequeued.
	Dequeue(ctx context.Context) (Task[T], error)
}

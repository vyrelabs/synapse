package sched

import (
	"context"
	"fmt"
	"sync"

	"github.com/ritvikos/synapse/frontier/backend"
	"github.com/ritvikos/synapse/model"
)

type ScoredTask[T any] = *model.ScoredTask[T]
type Queue[T any] = backend.Queue[ScoredTask[T]]

type Scheduler[T any] interface {
	// TODO: Replace Start() and Stop() with lifecycle interface, once fixed :p
	Start(ctx context.Context) error
	Stop(ctx context.Context) error

	Schedule(task ScoredTask[T]) error
	// The dequeue channel
	Tasks() <-chan ScoredTask[T]
}

type StaticScheduler[T any] struct {
	queue Queue[T]

	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
}

func NewStaticScheduler[T any](queue Queue[T], prefetchBufSize uint) *StaticScheduler[T] {
	return &StaticScheduler[T]{
		queue: queue,
	}
}

func (s *StaticScheduler[T]) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		return fmt.Errorf("[blocking scheduler]: already started")
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	return nil
}

func (s *StaticScheduler[T]) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel == nil {
		return fmt.Errorf("[blocking scheduler]: not started")
	}
	s.cancel()
	s.cancel = nil
	return nil
}

func (s *StaticScheduler[T]) Tasks() <-chan ScoredTask[T] {
	return s.queue.DequeueCh(s.ctx)
}

func (s *StaticScheduler[T]) Schedule(task *model.ScoredTask[T]) error {
	return s.queue.Enqueue(s.ctx, []ScoredTask[T]{task})
}

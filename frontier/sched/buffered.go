// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package sched

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

// TODO: Support on-disk persistence (configurable) for prefetch/flush buffers
// for recovery and prevent task loss.

var _ Scheduler[any] = (*BufferedScheduler[any])(nil)

type BufferedScheduler[T any] struct {
	queue  Queue[T]
	policy BufferPolicy

	prefetchChan       chan ScoredTask[T]
	prefetchSignalChan chan struct{}

	flushChan       chan ScoredTask[T]
	flushSignalChan chan struct{}
	flushTimer      *time.Timer

	// Internal
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
}

func NewBufferedScheduler[T any](
	queue Queue[T],
	policy BufferPolicy,
	prefetchBufSize uint,
	flushBufSize uint,
) *BufferedScheduler[T] {
	return &BufferedScheduler[T]{
		queue:              queue,
		policy:             policy,
		prefetchChan:       make(chan ScoredTask[T], prefetchBufSize),
		prefetchSignalChan: make(chan struct{}, 1),
		flushChan:          make(chan ScoredTask[T], flushBufSize),
		flushSignalChan:    make(chan struct{}, 1),
	}
}

// Starts the buffered scheduler in blocking manner.
// The caller should manage its lifecycle in a separate goroutine.
// Note: Multiple calls to 'Run' will return an error.
func (s *BufferedScheduler[T]) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	s.mu.Lock()
	if s.cancel != nil {
		s.mu.Unlock()
		return fmt.Errorf("[buffered scheduler]: already started")
	}

	s.ctx, s.cancel = context.WithCancel(ctx)
	s.mu.Unlock()

	g.Go(func() error {
		s.prefetchWorker()
		return nil
	})

	s.triggerPrefetch()
	return g.Wait()
}

func (s *BufferedScheduler[T]) Dequeue(ctx context.Context) ScoredTask[T] {
	// fast path
	select {
	case task, ok := <-s.prefetchChan:
		if !ok {
			return nil
		}
		return task
	default:
	}

	// slow path
	s.triggerPrefetch()

	select {
	case <-ctx.Done():
		return nil
	case <-s.ctx.Done():
		return nil
	case task, ok := <-s.prefetchChan:
		if !ok {
			return nil
		}
		return task
	}
}

func (s *BufferedScheduler[T]) Enqueue(ctx context.Context, task ScoredTask[T]) error {
	// fast path
	select {
	case s.flushChan <- task:
		return nil
	default:
	}

	// slow path
	select {
	case s.flushSignalChan <- struct{}{}:
	default:
	}

	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case s.flushChan <- task:
		return nil
	}
}

func (s *BufferedScheduler[T]) prefetchWorker() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.prefetchSignalChan:
			for {
				state := State{
					BufLen: len(s.prefetchChan),
					BufCap: cap(s.prefetchChan),
				}

				count := s.policy.Prefetch(state)
				if count <= 0 {
					break
				}

				// TODO: backoff/retry on error
				n, err := s.queue.Dequeue(s.ctx, count, s.prefetchChan)
				if err != nil {
					log.Printf("[buffered scheduler]: prefetch dequeue error: %v", err)
					break
				}
				if n == 0 {
					break
				}
			}
		}
	}
}

func (s *BufferedScheduler[T]) triggerPrefetch() {
	select {
	case s.prefetchSignalChan <- struct{}{}:
	default:
	}
}

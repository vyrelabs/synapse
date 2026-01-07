package buffer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ritvikos/synapse/frontier/backend"
	"github.com/ritvikos/synapse/model"
)

// TODO: Track relevent runtime metrics for decision making in PrefetchState and FlushState, or combine them into a single State, if relevant.

type ScoredTask[T any] = *model.ScoredTask[T]
type Queue[T any] = backend.Queue[ScoredTask[T]]

type DynamicScheduler[T any] struct {
	queue        backend.Queue[ScoredTask[T]]
	policy       Policy
	prefetchBuf  chan ScoredTask[T]
	flushBuf     chan ScoredTask[T]
	tickInterval time.Duration

	// Internal
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	wg     sync.WaitGroup
}

func NewDynamicScheduler[T any](
	queue backend.Queue[ScoredTask[T]],
	policy Policy,
	prefetchBufSize uint,
	flushBufSize uint,
	tickInterval time.Duration,
) *DynamicScheduler[T] {
	return &DynamicScheduler[T]{
		queue:        queue,
		policy:       policy,
		prefetchBuf:  make(chan ScoredTask[T], prefetchBufSize),
		flushBuf:     make(chan ScoredTask[T], flushBufSize),
		tickInterval: tickInterval,
	}
}

func (s *DynamicScheduler[T]) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.cancel != nil {
		s.mu.Unlock()
		return fmt.Errorf("scheduler: already started")
	}

	s.ctx, s.cancel = context.WithCancel(ctx)
	s.mu.Unlock()

	s.wg.Add(2)
	go s.prefetchWorker()
	go s.flushWorker()

	return nil
}

func (s *DynamicScheduler[T]) Stop(ctx context.Context) error {
	s.mu.Lock()
	if s.cancel == nil {
		s.mu.Unlock()
		return fmt.Errorf("scheduler: not started")
	}

	s.cancel()
	s.cancel = nil
	s.mu.Unlock()

	s.wg.Wait()
	return nil
}

func (s *DynamicScheduler[T]) Tasks() <-chan ScoredTask[T] {
	return s.prefetchBuf
}

func (s *DynamicScheduler[T]) Schedule(task *model.ScoredTask[T]) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case s.flushBuf <- task:
		return nil
	}
}

func (s *DynamicScheduler[T]) prefetchWorker() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.checkAndPrefetch()
		}
	}
}

func (s *DynamicScheduler[T]) checkAndPrefetch() {
	state := PrefetchState{
		Capacity: cap(s.prefetchBuf),
		Size:     len(s.prefetchBuf),
	}

	decision := s.policy.ShouldPrefetch(s.ctx, state)
	shouldFetch := decision.ShouldFetch
	if !shouldFetch {
		return
	}

	if shouldFetch && decision.Delay > 0 {
		time.Sleep(decision.Delay)
	}

	fetchCount := decision.Count

	if err := s.prefetch(fetchCount); err != nil {
		fmt.Printf("prefetch error: %v\n", err)
	}
}

func (s *DynamicScheduler[T]) prefetch(count int) error {
	tasks := s.queue.Dequeue(s.ctx, count)
	for task := range tasks {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case s.prefetchBuf <- task:
		}
	}

	return nil
}

func (s *DynamicScheduler[T]) flushWorker() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			if err := s.flush(0); err != nil {
				fmt.Printf("final flush error: %v", err)
			}
			return
		case <-ticker.C:
			s.checkAndFlush()
		}
	}
}

func (s *DynamicScheduler[T]) checkAndFlush() {
	state := FlushState{
		Capacity: cap(s.flushBuf),
		Size:     len(s.flushBuf),
	}

	decision := s.policy.ShouldFlush(s.ctx, state)
	shouldFlush := decision.ShouldFlush

	if shouldFlush {
		return
	}

	if shouldFlush && decision.Delay > 0 {
		time.Sleep(decision.Delay)
	}

	flushCount := decision.Count

	if err := s.flush(flushCount); err != nil {
		fmt.Printf("flush error: %v", err)
	}
}

func (s *DynamicScheduler[T]) flush(count int) error {
	flushCount := count
	if flushCount == 0 {
		flushCount = len(s.flushBuf)
	}

	if flushCount == 0 {
		return nil
	}

	tasks := make([]ScoredTask[T], 0, flushCount)

LOOP:
	for range flushCount {
		select {
		case task := <-s.flushBuf:
			tasks = append(tasks, task)
		default:
			break LOOP
		}
	}

	if len(tasks) == 0 {
		return nil
	}

	if err := s.queue.Enqueue(s.ctx, tasks); err != nil {
		return fmt.Errorf("scheduler: flush enqueue error: %v", err)
	}

	return nil
}

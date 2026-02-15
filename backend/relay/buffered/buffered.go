// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package buffered

import (
	"context"
	"fmt"
	"sync"

	"github.com/gammazero/deque"
	"github.com/vyrelabs/synapse/backend"
	"github.com/vyrelabs/synapse/backend/relay"
	"golang.org/x/sync/errgroup"
)

// TODO: Support on-disk persistence (configurable) for prefetch/flush buffers
// for recovery and prevent task loss.

// TODO: Support multiple buffers: memory, mmap, disk.

var _ relay.Relay[any, any] = (*BufferedRelay[any, any])(nil)

// Asynchronous buffer that receives tasks from consumer and flushes into the producer, based on the buffer policy,
// which handles "when and how much to consume/flush" decisions.
type BufferedRelay[In any, Out any] struct {
	consumer backend.Consumer[In]
	producer backend.Producer[Out]
	policy   BufferPolicy

	rx buffer[In]
	tx buffer[Out]

	// Internal
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
}

// Starts the buffered scheduler in blocking manner.
// The caller should manage its lifecycle in a separate goroutine.
func (s *BufferedRelay[In, Out]) Connect(ctx context.Context) error {
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

	g.Go(func() error {
		s.flushWorker()
		return nil
	})

	s.rx.trigger()
	return g.Wait()
}

func (s *BufferedRelay[In, Out]) Next(ctx context.Context) (In, error) {
	for {
		// fast path
		s.mu.Lock()
		if !s.rx.isEmpty() {
			item, _ := s.rx.pop()
			if s.rx.isEmpty() {
				s.rx.trigger()
			}
			s.mu.Unlock()
			return item, nil
		}
		readyCh := s.rx.ready
		s.mu.Unlock()

		// slow path
		s.rx.trigger()

		select {
		case <-s.ctx.Done():
			var zero In
			return zero, s.ctx.Err()
		case <-ctx.Done():
			var zero In
			return zero, s.ctx.Err()
		case <-readyCh:
		}
	}
}

func (s *BufferedRelay[In, Out]) Dispatch(ctx context.Context, item Out) error {
	if s.tx.isFull() {
		s.tx.trigger()
	}

	s.mu.Lock()
	s.tx.push(item)

	state := State{
		BufLen: s.tx.len(),
		BufCap: s.tx.cap(),
	}
	count, interval := s.policy.Flush(state)
	s.mu.Unlock()

	if count > 0 || interval == 0 {
		s.tx.trigger()
	}

	return nil
}

func (s *BufferedRelay[In, Out]) prefetchWorker() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.rx.signal:
			for {
				s.mu.Lock()
				state := State{
					BufLen: s.rx.len(),
					BufCap: s.rx.cap(),
				}
				s.mu.Unlock()

				count := s.policy.Prefetch(state)
				if count <= 0 {
					break
				}

				n, err := s.consumer.Len(s.ctx)
				if err != nil {
					break
				}

				if n == 0 {
					break
				}

				tempBuf := make([]In, count)
				n, err = s.consumer.Dequeue(s.ctx, tempBuf)
				if err != nil {
					break
				}

				if n == 0 {
					break
				}

				s.mu.Lock()
				for i := 0; i < n; i++ {
					s.rx.push(tempBuf[i])
				}
				s.mu.Unlock()

				s.rx.broadcastReady()
			}
		}
	}
}

func (s *BufferedRelay[In, Out]) flushWorker() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.tx.signal:
			s.flush()
		}
	}
}

func (s *BufferedRelay[In, Out]) flush() {
	s.mu.Lock()
	state := State{
		BufLen: s.tx.len(),
		BufCap: s.tx.cap(),
	}
	count, interval := s.policy.Flush(state)

	if count <= 0 && interval == 0 {
		s.mu.Unlock()
		return
	}

	if s.tx.isEmpty() {
		s.mu.Unlock()
		return
	}

	tempBuf := make([]Out, 0, s.tx.len())
	for !s.tx.isEmpty() {
		item, _ := s.tx.pop()
		tempBuf = append(tempBuf, item)
	}
	s.mu.Unlock()

	_, err := s.producer.Enqueue(s.ctx, tempBuf)
	if err == nil {
		s.rx.trigger()
	}
}

type buffer[T any] struct {
	data   *deque.Deque[T]
	size   int
	signal chan struct{}
	ready  chan struct{}
}

func newBuffer[T any](size uint) buffer[T] {
	buf := new(deque.Deque[T])
	buf.Grow(int(size))
	return buffer[T]{
		data:   buf,
		size:   int(size),
		signal: make(chan struct{}, 1),
		ready:  make(chan struct{}),
	}
}

func (b *buffer[T]) push(item T) {
	b.data.PushBack(item)
}

func (b *buffer[T]) pop() (T, bool) {
	if b.data.Len() == 0 {
		var zero T
		return zero, false
	}

	item := b.data.PopFront()
	return item, true
}

func (b *buffer[T]) trigger() {
	select {
	case b.signal <- struct{}{}:
	default:
	}
}

func (b *buffer[T]) broadcastReady() {
	ch := b.ready
	b.ready = make(chan struct{})
	close(ch)
}

func (b *buffer[T]) len() int {
	return b.data.Len()
}

func (b *buffer[T]) cap() int {
	return b.size
}

func (b *buffer[T]) isFull() bool {
	return b.data.Len() >= b.size
}

func (b *buffer[T]) isEmpty() bool {
	return b.data.Len() == 0
}

func (b *buffer[T]) clear() {
	for b.data.Len() > 0 {
		b.data.PopFront()
	}
}

func (b *buffer[T]) isReady() bool {
	select {
	case <-b.ready:
		return true
	default:
		return false
	}
}

func (b *buffer[T]) isTriggered() bool {
	select {
	case <-b.signal:
		return true
	default:
		return false
	}
}

// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package sequential

import (
	"context"

	"github.com/vyrelabs/synapse/backend"
	"github.com/vyrelabs/synapse/backend/relay"
)

var _ relay.Relay[any, any] = (*SequentialRelay[any, any])(nil)

// SequentialRelay synchronously enqueue/dequeue to/from the underlying producer/consumer.
// It shouldn't be used for concurrent workloads.
type SequentialRelay[In any, Out any] struct {
	producer backend.Producer[Out]
	consumer backend.Consumer[In]
	rx       backend.Batch[In]
	tx       backend.Batch[Out]
}

func NewSequentialRelay[In any, Out any](producer backend.Producer[Out], consumer backend.Consumer[In]) *SequentialRelay[In, Out] {
	return &SequentialRelay[In, Out]{
		producer: producer,
		consumer: consumer,
		rx:       make(backend.Batch[In], 1),
		tx:       make(backend.Batch[Out], 1),
	}
}

func (s *SequentialRelay[In, Out]) Connect(ctx context.Context) error {
	return nil
}

func (s *SequentialRelay[In, Out]) Dispatch(ctx context.Context, item Out) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if s.tx == nil {
		s.tx = make(backend.Batch[Out], 1)
	}

	s.tx[0] = item
	_, err := s.producer.Enqueue(ctx, s.tx[:1])

	var zero Out
	s.tx[0] = zero

	return err
}

func (s *SequentialRelay[In, Out]) Next(ctx context.Context) (In, error) {
	var zero In
	if err := ctx.Err(); err != nil {
		return zero, err
	}

	if s.rx == nil {
		s.rx = make(backend.Batch[In], 1)
	}

	if len(s.rx) == 0 {
		return zero, backend.ErrEmptyQueue
	}

	n, err := s.consumer.Dequeue(ctx, s.rx[:1])
	if err != nil {
		return zero, err
	}

	if n == 0 {
		return zero, backend.ErrEmptyQueue
	}

	out := s.rx[0]
	s.rx[0] = zero

	return out, nil
}

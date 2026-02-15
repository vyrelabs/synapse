// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package direct

import (
	"context"

	"github.com/vyrelabs/synapse/backend"
	"github.com/vyrelabs/synapse/backend/relay"
)

var _ relay.Relay[any, any] = (*DirectRelay[any, any])(nil)

type DirectRelay[In any, Out any] struct {
	producer backend.Producer[Out]
	consumer backend.Consumer[In]
}

func NewDirectRelay[In any, Out any](producer backend.Producer[Out], consumer backend.Consumer[In]) *DirectRelay[In, Out] {
	return &DirectRelay[In, Out]{
		producer: producer,
		consumer: consumer,
	}
}

func (dc *DirectRelay[In, Out]) Connect(ctx context.Context) error {
	return nil
}

func (dc *DirectRelay[In, Out]) Dispatch(ctx context.Context, item Out) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	var buf [1]Out
	buf[0] = item
	_, err := dc.producer.Enqueue(ctx, buf[:])

	return err
}

func (dc *DirectRelay[In, Out]) Next(ctx context.Context) (In, error) {
	var zero In
	if err := ctx.Err(); err != nil {
		return zero, err
	}

	var buf [1]In
	n, err := dc.consumer.Dequeue(ctx, buf[:])
	if err != nil {
		return zero, err
	}

	if n == 0 {
		return zero, backend.ErrEmptyQueue
	}

	return buf[0], nil
}

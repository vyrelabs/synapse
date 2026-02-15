// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package storetest

import (
	"context"
	"sync"

	"github.com/vyrelabs/synapse/backend"
)

var _ backend.Store[any] = (*ManualStore[any])(nil)

// ManualStore is a thread-safe in-memory store implementation.
type ManualStore[T any] struct {
	data map[string]T
	mu   sync.RWMutex
}

func NewMemoryStore[T any]() *ManualStore[T] {
	return &ManualStore[T]{
		data: make(map[string]T),
	}
}

func (s *ManualStore[T]) Put(ctx context.Context, key string, value T) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.data[key] = value
	return nil
}

func (s *ManualStore[T]) Get(ctx context.Context, key string) (T, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	if !ok {
		var zero T
		return zero, nil
	}
	return val, nil
}

func (s *ManualStore[T]) Delete(ctx context.Context, key string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	delete(s.data, key)
	return nil
}

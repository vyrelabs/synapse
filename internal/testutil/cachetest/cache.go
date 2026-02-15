// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package cachetest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/vyrelabs/synapse/backend"
)

var _ backend.Cache[any] = (*ManualCache[any])(nil)

type cacheItem[T any] struct {
	value      T
	expiration int64
}

// ManualCache is a simple thread-safe in-memory cache implementation.
type ManualCache[T any] struct {
	data map[string]cacheItem[T]
	mu   sync.RWMutex
}

func NewMemoryCache[T any](t *testing.T) *ManualCache[T] {
	return &ManualCache[T]{
		data: make(map[string]cacheItem[T]),
	}
}

func (c *ManualCache[T]) Set(ctx context.Context, key string, value T, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ttl < 0 {
		return errors.New("TTL cannot be negative")
	}

	var exp int64
	if ttl > 0 {
		exp = time.Now().Add(ttl).UnixNano()
	}

	c.data[key] = cacheItem[T]{
		value:      value,
		expiration: exp,
	}
	return nil
}

func (c *ManualCache[T]) Get(ctx context.Context, key string) (T, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, ok := c.data[key]
	if !ok {
		var zero T
		return zero, nil
	}

	if item.expiration > 0 && time.Now().UnixNano() > item.expiration {
		var zero T
		return zero, nil
	}

	return item.value, nil
}

func (c *ManualCache[T]) Purge(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	clear(c.data)
	return nil
}

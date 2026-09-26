// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package mem

import (
	"context"
	"sync"

	"github.com/vyrelabs/synapse/frontier/filter"
)

type InMemorySeenSet struct {
	mu   sync.RWMutex
	seen map[string]struct{}
}

var _ filter.SeenSet = (*InMemorySeenSet)(nil)

func NewInMemorySeenSet() *InMemorySeenSet {
	return &InMemorySeenSet{
		seen: make(map[string]struct{}),
	}
}

func (s *InMemorySeenSet) HasSeen(ctx context.Context, url string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	_, exists := s.seen[url]
	return exists, nil
}

func (s *InMemorySeenSet) MarkSeen(ctx context.Context, url string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen[url] = struct{}{}
	return nil
}

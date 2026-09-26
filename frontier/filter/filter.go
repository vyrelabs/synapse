// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package filter

import (
	"context"
)

type SeenSet interface {
	HasSeen(ctx context.Context, url string) (bool, error)
	MarkSeen(ctx context.Context, url string) error
}

func New(seenSet SeenSet) Filter {
	return Filter{seenSet: seenSet}
}

type Filter struct {
	seenSet SeenSet
}

func (f *Filter) Allow(ctx context.Context, url string) (bool, error) {
	found, err := f.seenSet.HasSeen(ctx, url)
	if err != nil {
		return false, err
	}
	if found {
		return false, nil
	}

	err = f.seenSet.MarkSeen(ctx, url)
	if err != nil {
		return false, err
	}
	return true, nil
}

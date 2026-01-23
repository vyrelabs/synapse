// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package score

import (
	"context"

	model "github.com/vyrelabs/synapse/model"
)

type Score[T any] interface {
	Score(ctx context.Context, item *model.Task[T]) (float64, error)
}

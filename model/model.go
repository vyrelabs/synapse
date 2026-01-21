// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package model

import (
	"time"
)

type Task[T any] struct {
	ExecuteAt time.Time
	Metadata  T
	Url       string
	Score     float64
	// Fingerprint string
}

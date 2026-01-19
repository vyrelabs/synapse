// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package lifecycle

import "context"

type Runner interface {
	Run(ctx context.Context) error
}

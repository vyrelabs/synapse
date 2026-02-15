// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package relay

import (
	"context"
)

// TODO Ack/Nack

// Source abstracts the transport logic for moving data from source.
type Source[I any] interface {
	// Connect establishes connection to source.
	Connect(ctx context.Context) error

	// Next retrieves next available input from the source.
	// It depends on the implementation whether it's blocking/non-blocking
	Next(ctx context.Context) (I, error)
}

// Sink abstracts transport logic for moving data to sink.
type Sink[O any] interface {
	// Connect establishes connection to sink.
	Connect(ctx context.Context) error

	// Dispatch pushes the payload to the sink.
	// It depends on the implementation whether it's blocking/non-blocking.
	Dispatch(ctx context.Context, payload O) error
}

// Relay orchestrates the data exchange between source and sink.
type Relay[I any, O any] interface {
	Source[I]
	Sink[O]
}

// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package model

import (
	"fmt"
)

// NOTE
//
// I believe these methods aren't needed.
// They'll be added as per requirement.
//
// MethodPut, MethodDelete, MethodPatch,
// MethodOptions, MethodConnect, MethodTrace

type HTTPMethod uint8

const (
	MethodUnknown HTTPMethod = iota
	MethodGet
	MethodPost
	MethodHead
)

func (m HTTPMethod) String() string {
	switch m {
	case MethodGet:
		return "GET"
	case MethodPost:
		return "POST"
	case MethodHead:
		return "HEAD"
	default:
		return fmt.Sprintf("UNKNOWN METHOD: %d", m)
	}
}

// Task is a unit of work for crawling a URL,
// along with its associated metadata of generic type T.
//
// On 64-bit arch, originally it was ~120 bytes (aligned),
// now reduced to ~40 bytes
type Task[T any] struct {
	// Metadata holds user-defined auxiliary data of generic type T.
	// Ex: Custom headers, Cookies, Response, etc.
	Metadata T

	// Fingerprint is a unique identifier for the URL,
	// generated via a hashing algorithm.
	Fingerprint string

	// Url is the complete address fo the crawled resource.
	Url string

	// FIXME
	// Can we store last execution time and next crawl's offset packed?

	// ExecuteAt is a unix timestamp representing the
	// scheduled time for the next crawl.
	ExecuteAt int64

	// LastCrawlAt is a unix timestamp representing the
	// last time the URL was crawled.
	LastCrawlAt int64

	// FIXME
	// Decide how to store the 'score' efficiently (without wasting space)
	// The precision will be compromised, but how much would be acceptable?
	//
	// Possible approaches:
	// - store as 'float32'.
	// - scale 'float64' by 1000 and store as 'uint16'.
	// - offload it to the underlying storage.

	// Score is the priority assigned to the URL.
	Score float64

	// StatusCode is HTTP response code.
	StatusCode uint16

	// Method is the HTTP method used for the request.
	Method HTTPMethod
}

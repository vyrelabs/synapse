// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package modeltest

import (
	"fmt"
)

type HTTPMethod uint8

const (
	MethodUnknown HTTPMethod = iota
	MethodGet
	MethodPost
	MethodHead
	MethodPut
	MethodDelete
	MethodPatch
)

func (m HTTPMethod) String() string {
	switch m {
	case MethodGet:
		return "GET"
	case MethodPost:
		return "POST"
	case MethodHead:
		return "HEAD"
	case MethodPut:
		return "PUT"
	case MethodDelete:
		return "DELETE"
	case MethodPatch:
		return "PATCH"
	default:
		return fmt.Sprintf("UNKNOWN METHOD: %d", m)
	}
}

func httpMethodFromGofakeit(method string) HTTPMethod {
	switch method {
	case "GET":
		return MethodGet
	case "POST":
		return MethodPost
	case "HEAD":
		return MethodHead
	case "PUT":
		return MethodPut
	case "DELETE":
		return MethodDelete
	case "PATCH":
		return MethodPatch
	default:
		return MethodUnknown
	}
}

// Re-purposed from `model.Task`, until the __core__ structures are added [WIP]
// Meanwhile, it'll be used for testing the underlying infra components.
type MockTask struct {
	// Metadata holds user-defined auxiliary data of generic type T.
	// Ex: Custom headers, Cookies, Response, etc.
	Metadata map[string]any

	// Fingerprint is a unique identifier for the URL,
	// generated via a hashing algorithm.
	Fingerprint string

	// Url is the complete address fo the crawled resource.
	Url string

	// ExecuteAt is a unix timestamp representing the
	// scheduled time for the next crawl.
	ExecuteAt int64

	// LastCrawlAt is a unix timestamp representing the
	// last time the URL was crawled.
	LastCrawlAt int64

	// Score is the priority assigned to the URL.
	Score float64

	// StatusCode is HTTP response code.
	StatusCode uint16

	// Method is the HTTP method used for the request.
	Method HTTPMethod
}

type TaskOption func(*MockTask)

func WithMetadata(metadata map[string]any) TaskOption {
	return func(t *MockTask) {
		t.Metadata = metadata
	}
}

func WithStatusCode(code uint16) TaskOption {
	return func(t *MockTask) {
		t.StatusCode = code
	}
}

func WithURL(url string) TaskOption {
	return func(t *MockTask) {
		t.Url = url
	}
}

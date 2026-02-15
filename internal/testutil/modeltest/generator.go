// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package modeltest

import "github.com/brianvoe/gofakeit/v7"

func NewMockTask(opts ...TaskOption) *MockTask {
	task := MockTask{
		Metadata:    map[string]any{gofakeit.Word(): gofakeit.Sentence(3)},
		Fingerprint: gofakeit.UUID(),
		Url:         gofakeit.URL(),
		ExecuteAt:   gofakeit.Date().Unix(),
		Score:       gofakeit.Float64Range(0.1, 1),
		StatusCode:  uint16(gofakeit.HTTPStatusCode()),
		Method:      httpMethodFromGofakeit(gofakeit.HTTPMethod()),
	}

	for _, opt := range opts {
		opt(&task)
	}

	return &task
}

func NewMockTasks(count int, opts ...TaskOption) []*MockTask {
	tasks := make([]*MockTask, count)
	for i := range count {
		tasks[i] = NewMockTask(opts...)
	}
	return tasks
}

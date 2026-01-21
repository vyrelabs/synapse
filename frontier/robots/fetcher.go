// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package robots

import (
	"context"
	"net/http"
	"time"
)

type DefaultRobotsFetcher struct {
	client http.Client
}

func NewDefaultRobotsTxtFetcher() *DefaultRobotsFetcher {
	return &DefaultRobotsFetcher{
		client: http.Client{
			Timeout: 12 * time.Second,
		},
	}
}

func (r *DefaultRobotsFetcher) Fetch(ctx context.Context, host string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", host+"/robots.txt", nil)
	if err != nil {
		return nil, err
	}

	return r.client.Do(req)
}

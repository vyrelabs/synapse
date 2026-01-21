// Copyright 2025-2026 Ritvik Gupta
// SPDX-License-Identifier: Apache-2.0

package frontier

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/ritvikos/synapse/frontier/robots"
	"github.com/ritvikos/synapse/frontier/sched"
	"github.com/ritvikos/synapse/frontier/score"
	model "github.com/ritvikos/synapse/model"
	"golang.org/x/sync/errgroup"
)

type Config struct {
	IngressBufSize        int
	RobotsResolvedBufSize int
	ScoreBufSize          int
	DefaultCrawlDelay     time.Duration

	ScoreWorkerCount     uint
	RobotsWorkerCount    uint
	SchedulerWorkerCount uint
}

// 'T' is a generic parameter for the auxiliary
// metadata associated with each URL to-be-crawled.
//
// # NOTE
//
// 'T' might be removed if a better design is created.
// Currently, 'map[any]any' is into consideration as a replacement,
// but it'd be less type-safe and the caller must ensure serializability, when using remote backend.
type Frontier[T any] struct {
	// Channels
	ingressChan        chan *model.Task[T]
	robotsResolvedChan chan *model.Task[T]
	scoredChan         chan *model.Task[T]

	// Sub-components
	robotstxt *robots.RobotsResolver
	Scorer    score.Score[T]
	scheduler sched.Scheduler[T]

	config Config
}

func NewFrontier[T any](
	robotstxt *robots.RobotsResolver,
	scorer score.Score[T],
	scheduler sched.Scheduler[T],
	config Config,
) *Frontier[T] {
	return &Frontier[T]{
		robotstxt:          robotstxt,
		Scorer:             scorer,
		scheduler:          scheduler,
		config:             config,
		ingressChan:        make(chan *model.Task[T], config.IngressBufSize),
		robotsResolvedChan: make(chan *model.Task[T], config.RobotsResolvedBufSize),
		scoredChan:         make(chan *model.Task[T], config.ScoreBufSize),
	}
}

func (f *Frontier[T]) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return f.scheduler.Run(ctx)
	})

	for range f.config.RobotsWorkerCount {
		g.Go(func() error {
			f.robotsWorker(ctx)
			return nil
		})
	}

	for range f.config.ScoreWorkerCount {
		g.Go(func() error {
			f.scoreWorker(ctx)
			return nil
		})
	}

	for range f.config.SchedulerWorkerCount {
		g.Go(func() error {
			f.scheduleWorker(ctx)
			return nil
		})
	}

	return g.Wait()
}

func (f *Frontier[T]) Dequeue(ctx context.Context) (*model.Task[T], error) {
	task, err := f.scheduler.Dequeue(ctx)
	if err != nil {
		return nil, err
	}

	// TODO: Handle tasks mem-pool dealloc here (when added)
	return task, nil
}

func (f *Frontier[T]) Enqueue(ctx context.Context, endpoint string, metadata T) error {
	_, err := url.Parse(endpoint)
	if err != nil {
		return err
	}

	// TODO: Handle tasks mem-pool alloc here (when added)
	task := model.Task[T]{
		Url:      endpoint,
		Metadata: metadata,
	}

	select {
	case f.ingressChan <- &task:
	case <-ctx.Done():
	}

	return nil
}

func (f *Frontier[T]) robotsWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case task, ok := <-f.ingressChan:
			if !ok {
				log.Println("scored channel closed, stopping robots worker")
				return
			}

			url, err := url.Parse(task.Url)
			// This shouldn't happen
			if err != nil {
				return
			}

			entry, err := f.robotstxt.Resolve(ctx, url.Scheme+"://"+url.Host)
			if err != nil {
				log.Println("error resolving robots.txt for host", url.Host, ":", err)
			}

			if !entry.Test(task.Url) {
				log.Printf("disallowed by robots.txt: host=%s url=%s", url.Host, task.Url)
				continue
			}

			now := time.Now()
			crawlDelay := entry.CrawlDelay()

			if crawlDelay == 0 {
				now = now.Add(f.config.DefaultCrawlDelay)
			} else {
				now = now.Add(crawlDelay)
			}

			task.ExecuteAt = now
			fmt.Printf("Robots.txt entry for host %s: %+v\n", url.Host, entry)

			select {
			case f.robotsResolvedChan <- task:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (f *Frontier[T]) scoreWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case task, ok := <-f.robotsResolvedChan:
			if !ok {
				log.Println("ingress channel closed, stopping score worker")
				return
			}

			score, err := f.Scorer.Score(ctx, task)
			if err != nil {
				log.Printf("error scoring item: %v", err)
				continue
			}

			task.Score = score
			fmt.Printf("scored task: %+v\n", *task)

			select {
			case f.scoredChan <- task:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (f *Frontier[T]) scheduleWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case task, ok := <-f.scoredChan:
			if !ok {
				log.Println("robots resolved channel closed, stopping scheduler worker")
				return
			}

			err := f.scheduler.Enqueue(ctx, task)
			if err != nil {
				log.Printf("error scheduling task for url %s: %v", task.Url, err)
				continue
			}
		}
	}
}

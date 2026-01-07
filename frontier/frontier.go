package frontier

import (
	"context"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/ritvikos/synapse/frontier/robots"
	"github.com/ritvikos/synapse/frontier/sched"
	"github.com/ritvikos/synapse/frontier/score"
	model "github.com/ritvikos/synapse/model"
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

// T represents crawl metadata (e.g., URL, Request).
type Frontier[T any] struct {
	robotstxt *robots.RobotsResolver
	Scorer    score.Score[T]
	scheduler sched.Scheduler[T]
	config    Config

	// Channels
	ingressCh        chan *model.Task[T]
	robotsResolvedCh chan *model.Task[T]
	scoredCh         chan *model.ScoredTask[T]

	// Internal
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewFrontier[T any](
	robotstxt *robots.RobotsResolver,
	scorer score.Score[T],
	scheduler sched.Scheduler[T],
	config Config,
) *Frontier[T] {
	return &Frontier[T]{
		robotstxt: robotstxt,
		Scorer:    scorer,
		scheduler: scheduler,
		config:    config,
	}
}

func (f *Frontier[T]) Start(ctx context.Context) error {
	f.ctx, f.cancel = context.WithCancel(ctx)

	f.ingressCh = make(chan *model.Task[T], f.config.IngressBufSize)
	f.robotsResolvedCh = make(chan *model.Task[T], f.config.RobotsResolvedBufSize)
	f.scoredCh = make(chan *model.ScoredTask[T], f.config.ScoreBufSize)

	if err := f.scheduler.Start(f.ctx); err != nil {
		return err
	}

	for range f.config.RobotsWorkerCount {
		f.wg.Add(1)
		go f.robotsWorker()
	}

	for range f.config.ScoreWorkerCount {
		f.wg.Add(1)
		go f.scoreWorker()
	}

	for range f.config.SchedulerWorkerCount {
		f.wg.Add(1)
		go f.scheduleWorker()
	}

	return nil // f.scheduler.Start(ctx) was called above
}

func (f *Frontier[T]) Tasks() <-chan *model.ScoredTask[T] {
	return f.scheduler.Tasks()
}

func (f *Frontier[T]) Enqueue(ctx context.Context, endpoint string, metadata T) error {
	_, err := url.Parse(endpoint)
	if err != nil {
		return err
	}

	task := model.Task[T]{
		Url:      endpoint,
		Metadata: metadata,
	}

	select {
	case f.ingressCh <- &task:
	case <-ctx.Done():
	}

	return nil
}

func (f *Frontier[T]) robotsWorker() {
	defer f.wg.Done()

	for {
		select {
		case <-f.ctx.Done():
			return

		case task, ok := <-f.ingressCh:
			if !ok {
				log.Println("scored channel closed, stopping robots worker")
				return
			}

			url, err := url.Parse(task.Url)
			// This shouldn't happen
			if err != nil {
				return
			}

			entry, err := f.robotstxt.Resolve(f.ctx, url.Host)
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

			select {
			case f.robotsResolvedCh <- task:
			case <-f.ctx.Done():
				return
			}
		}
	}
}

func (f *Frontier[T]) scoreWorker() {
	defer f.wg.Done()

	for {
		select {
		case <-f.ctx.Done():
			return

		case task, ok := <-f.robotsResolvedCh:
			if !ok {
				log.Println("ingress channel closed, stopping score worker")
				return
			}

			score, err := f.Scorer.Score(f.ctx, task)
			if err != nil {
				log.Printf("error scoring item: %v", err)
				continue
			}

			scoredTask := &model.ScoredTask[T]{
				Task:  task,
				Score: score,
			}

			select {
			case f.scoredCh <- scoredTask:
			case <-f.ctx.Done():
				return
			}
		}
	}
}

func (f *Frontier[T]) scheduleWorker() {
	defer f.wg.Done()

	for {
		select {
		case <-f.ctx.Done():
			return

		case task, ok := <-f.scoredCh:
			if !ok {
				log.Println("robots resolved channel closed, stopping scheduler worker")
				return
			}

			err := f.scheduler.Schedule(task)
			if err != nil {
				log.Printf("error scheduling task for url %s: %v", task.Task.Url, err)
				continue
			}
		}
	}
}

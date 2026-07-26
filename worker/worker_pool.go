package worker

import (
	"context"
	"sync"
)

type WorkerPool[T any] struct {
	ctx     context.Context
	cancel  context.CancelFunc
	handler Handler[T]
	jobs    chan T
	wg      sync.WaitGroup
}

func NewWorkerPool[T any](ctx context.Context, handler Handler[T], workers, queueSize int) *WorkerPool[T] {
	ctx, cancel := context.WithCancel(ctx) //nolint:gosec // cancel is stored and called during shutdown

	p := &WorkerPool[T]{
		ctx:    ctx,
		cancel: cancel,
		jobs:   make(chan T, queueSize),
	}

	p.wg.Add(workers)

	for i := 0; i < workers; i++ {
		go p.worker()
	}

	return p
}

func (p *WorkerPool[T]) worker() {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			return

		case job := <-p.jobs:
			p.handler(p.ctx, job)
		}
	}
}

func (p *WorkerPool[T]) Submit(job T) error {
	select {
	case <-p.ctx.Done():
		return context.Canceled

	case p.jobs <- job:
		return nil
	}
}

func (p *WorkerPool[T]) Close() {
	p.cancel()
	p.wg.Wait()
}

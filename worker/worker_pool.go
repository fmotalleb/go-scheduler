package worker

import (
	"context"
	"sync"
)

// WorkerPool is a concurrent worker that fans tasks out to a fixed pool
// of goroutines. Tasks are submitted via a buffered channel; idle workers
// pick up tasks as they arrive.
//
// Create one via NewWorkerPool. The pool shuts down when its context is
// cancelled or Close is called.
type WorkerPool[T any] struct {
	ctx     context.Context
	cancel  context.CancelFunc
	handler Handler[T]
	jobs    chan T
	wg      sync.WaitGroup
}

// NewWorkerPool creates and starts a pool of worker goroutines.
//
//   - ctx controls the lifetime of the pool; cancellation signals all
//     workers to shut down.
//   - handler is called for each submitted task.
//   - workers is the number of goroutines to spawn.
//   - queueSize is the capacity of the internal buffered channel.
//
// The pool starts accepting tasks immediately.
func NewWorkerPool[T any](ctx context.Context, handler Handler[T], workers, queueSize int) *WorkerPool[T] {
	ctx, cancel := context.WithCancel(ctx) //nolint:gosec // cancel is stored and called during shutdown

	p := &WorkerPool[T]{
		ctx:     ctx,
		cancel:  cancel,
		handler: handler,
		jobs:    make(chan T, queueSize),
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

// Submit enqueues a task for processing by an available worker.
//
// Returns nil on success. If the pool's context has been cancelled
// (e.g. during shutdown), Submit returns context.Canceled.
func (p *WorkerPool[T]) Submit(job T) error {
	// Check cancellation before attempting to send so that a closed pool
	// always rejects new submissions, even when the job channel has room.
	select {
	case <-p.ctx.Done():
		return context.Canceled
	default:
	}

	select {
	case <-p.ctx.Done():
		return context.Canceled

	case p.jobs <- job:
		return nil
	}
}

// Close shuts down the worker pool by cancelling its context and waiting
// for all in-flight tasks to complete.
func (p *WorkerPool[T]) Close() {
	p.cancel()
	p.wg.Wait()
}

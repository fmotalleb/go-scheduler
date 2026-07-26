package scheduler

import (
	"context"
	"time"

	"github.com/fmotalleb/go-scheduler/storage"
	"github.com/fmotalleb/go-scheduler/worker"
)

const defaultTickCycle = time.Second

// Option configures a Scheduler[T].
//
// Options are applied in order during New or NewCallback. Use the
// With* functions to create Option values.
//
// Example:
//
//	s := scheduler.New(ctx, worker,
//	    scheduler.WithTickerCycle(500*time.Millisecond),
//	    scheduler.WithLogger(log.Default().Println),
//	)
type Option[T any] func(*Scheduler[T])

func defaultTickerCycle[T any](s *Scheduler[T]) {
	s.ticker = time.NewTicker(defaultTickCycle)
}

func defaultStorage[T any](s *Scheduler[T]) {
	s.storage = storage.NewBTreeStorage[T]()
}

func defaultWorker[T func(context.Context)](ctx context.Context) Option[T] {
	worker := worker.NewSync(
		ctx,
		func(ctx context.Context, t T) {
			t(ctx)
		},
	)
	return func(s *Scheduler[T]) {
		s.worker = worker
	}
}

// WithTickerCycle sets how often the scheduler checks for due tasks.
// By default the ticker fires every 1 second.
func WithTickerCycle[T any](d time.Duration) Option[T] {
	return func(s *Scheduler[T]) {
		s.ticker = time.NewTicker(d)
	}
}

// WithStorage sets a custom Storage backend on the scheduler.
// If not provided, a default in-memory storage is used.
func WithStorage[T any](storage Storage[T]) Option[T] {
	return func(s *Scheduler[T]) {
		s.storage = storage
	}
}

// WithWorker sets a custom Worker on the scheduler, overriding any
// worker passed directly to New or previously set by another option.
func WithWorker[T any](w Worker[T]) Option[T] {
	return func(s *Scheduler[T]) {
		s.worker = w
	}
}

// WithWorkerPool creates a worker.WorkerPool and sets it on the scheduler.
// This is a shorthand for creating a pool manually and passing it via
// WithWorker.
//
//   - handler is called for each task.
//   - workers is the number of concurrent goroutines.
//   - queueSize is the capacity of the internal job channel.
func WithWorkerPool[T any](ctx context.Context, h worker.Handler[T], workers, queueSize int) Option[T] {
	return func(s *Scheduler[T]) {
		s.worker = worker.NewWorkerPool[T](ctx, h, workers, queueSize)
	}
}

// WithLogger sets a custom error logger on the scheduler.
// The provided LogFn will be called whenever an error occurs in Start or
// runCycle. Pass nil to restore the default no-op behaviour.
func WithLogger[T any](fn LogFn) Option[T] {
	return func(s *Scheduler[T]) {
		s.logFn = fn
	}
}

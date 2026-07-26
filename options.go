package scheduler

import (
	"context"
	"time"

	st "github.com/fmotalleb/go-scheduler/storage"
	"github.com/fmotalleb/go-scheduler/worker"
)

const defaultTickCycle = time.Second

type Option[T any] func(*Scheduler[T])

func defaultTickerCycle[T any](s *Scheduler[T]) {
	s.ticker = time.NewTicker(defaultTickCycle)
}

func defaultStorage[T any](s *Scheduler[T]) {
	s.storage = st.NewMemoryStorage[T](0)
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

func WithTickerCycle[T any](d time.Duration) Option[T] {
	return func(s *Scheduler[T]) {
		s.ticker = time.NewTicker(d)
	}
}

func WithStorage[T any](storage Storage[T]) Option[T] {
	return func(s *Scheduler[T]) {
		s.storage = storage
	}
}

func WithWorker[T any](w Worker[T]) Option[T] {
	return func(s *Scheduler[T]) {
		s.worker = w
	}
}

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

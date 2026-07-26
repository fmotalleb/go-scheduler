package scheduler

import (
	"context"
	"time"
)

type Callback = func(context.Context)

type Scheduler[T any] struct {
	storage Storage[T]
	ticker  *time.Ticker
	worker  Worker[T]
}

func New[T any](worker Worker[T], opts ...Option[T]) *Scheduler[T] {
	sc := new(Scheduler[T])
	sc.worker = worker
	for _, o := range opts {
		o(sc)
	}
	if sc.storage == nil {
		defaultStorage(sc)
	}
	if sc.ticker == nil {
		defaultTickerCycle(sc)
	}
	return sc
}

func NewCallback(ctx context.Context, opts ...Option[Callback]) *Scheduler[Callback] {
	sc := new(Scheduler[Callback])
	for _, o := range opts {
		o(sc)
	}
	if sc.storage == nil {
		defaultStorage(sc)
	}
	if sc.ticker == nil {
		defaultTickerCycle(sc)
	}
	if sc.worker == nil {
		defaultWorker(ctx)(sc)
	}
	return sc
}

func (s *Scheduler[T]) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():

			return
		case tick := <-s.ticker.C:
			s.runCycle(tick)
		}
	}
}

func (s *Scheduler[T]) runCycle(t time.Time) {
	items, err := s.storage.PopBefore(t)
	if err != nil {
		// ?
	}
	for _, i := range items {
		s.worker.Submit(i)
	}
}

func (s *Scheduler[T]) Close() {
	s.worker.Close()
	s.storage.Close()
}

package scheduler

import (
	"context"
	"fmt"
	"time"
)

// LogFn is a pluggable function for handling errors that occur during
// scheduler operation (Start and runCycle). By default, errors are
// silently ignored. Provide a custom LogFn via WithLogger to log,
// monitor, or otherwise react to scheduler errors.
type LogFn func(error)

// Callback is a convenience alias for scheduler tasks that are simple
// context-aware functions.
type Callback = func(context.Context)

type Scheduler[T any] struct {
	storage Storage[T]
	ticker  *time.Ticker
	worker  Worker[T]
	logFn   LogFn
}

// defaultLogFn silently discards all errors.
func defaultLogFn(error) {}

func New[T any](ctx context.Context, worker Worker[T], opts ...Option[T]) *Scheduler[T] {
	sc := new(Scheduler[T])
	sc.worker = worker
	sc.logFn = defaultLogFn
	for _, o := range opts {
		o(sc)
	}
	if sc.storage == nil {
		defaultStorage(sc)
	}
	if sc.ticker == nil {
		defaultTickerCycle(sc)
	}
	go sc.Start(ctx)
	return sc
}

func NewCallback(ctx context.Context, opts ...Option[Callback]) *Scheduler[Callback] {
	sc := new(Scheduler[Callback])
	sc.logFn = defaultLogFn
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
	go sc.Start(ctx)
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
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				s.logFn(fmt.Errorf("runCycle panicked: %w", err))
			} else {
				s.logFn(fmt.Errorf("runCycle panicked: %v", r))
			}
		}
	}()
	items, err := s.storage.PopBefore(t)
	if err != nil {
		s.logFn(fmt.Errorf("failed to pop items before %v: %w", t, err))
	}
	for _, i := range items {
		if err := s.worker.Submit(i); err != nil {
			s.logFn(fmt.Errorf("failed to submit item %v: %w", i, err))
		}
	}
}

// Add stores a task T to be executed at (or after) the given time.
// It returns a unique ID that can be used with Remove to cancel the task.
func (s *Scheduler[T]) Add(when time.Time, task T) (int, error) {
	return s.storage.Add(when, task)
}

// Remove cancels a previously added task by ID, returning the original
// value. An error is returned if the ID does not exist.
func (s *Scheduler[T]) Remove(id int) (T, error) {
	return s.storage.Remove(id)
}

func (s *Scheduler[T]) Close() {
	s.ticker.Stop()
	s.storage.Close()
	s.worker.Close()
}

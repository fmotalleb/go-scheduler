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

// Scheduler is a generic task scheduler that periodically checks a storage
// backend for tasks whose time has elapsed and submits them to a worker for
// processing.
//
// Create one via New or NewCallback; the scheduler starts in a background
// goroutine immediately. Use context cancellation to shut it down.
type Scheduler[T any] struct {
	storage Storage[T]
	ticker  Ticker
	worker  Worker[T]
	logFn   LogFn
}

// defaultLogFn silently discards all errors.
func defaultLogFn(error) {}

// New creates and starts a Scheduler[T] in a background goroutine.
//
// The worker parameter is the default Worker[T] used to process tasks. It
// can be overridden by passing WithWorker or WithWorkerPool as an option.
//
// The scheduler calls Storage.PopBefore at each tick to find due tasks and
// hands them to the worker via Worker.Submit. The scheduler stops when ctx
// is cancelled.
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
	go sc.start(ctx)
	return sc
}

// NewCallback is a convenience constructor for schedulers that execute
// simple context-aware functions (Callback).
//
// Unlike New, NewCallback does not require an explicit Worker — if none
// is provided via WithWorker or WithWorkerPool, a default synchronous
// worker is created internally.
//
//	s := scheduler.NewCallback(ctx,
//	    scheduler.WithTickerCycle(250*time.Millisecond),
//	)
//	s.Add(time.Now().Add(5*time.Second), func(ctx context.Context) {
//	    fmt.Println("Hello!")
//	})
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
	go sc.start(ctx)
	return sc
}

// start begins the scheduler's main loop.
//
// It listens for tick events from the internal ticker and, on each tick,
// calls runCycle to pop and process all due tasks. The loop exits when ctx
// is cancelled.
//
// start is called automatically by New and NewCallback and normally does
// not need to be invoked directly.
func (s *Scheduler[T]) start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case tick := <-s.ticker.C():
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

// Close shuts down the scheduler by stopping the ticker and closing both
// the storage backend and the worker. After Close returns the scheduler
// can no longer be used.
func (s *Scheduler[T]) Close() {
	s.ticker.Close()
	s.storage.Close()
	s.worker.Close()
}

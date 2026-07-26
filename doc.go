// Package scheduler provides a generic, extensible task scheduling engine for Go.
//
// # Overview
//
// Scheduler lets you schedule arbitrary tasks (any type T) to run at specific
// times. At each tick of an internal time.Ticker the scheduler pops all items
// whose scheduled time has elapsed and hands them off to a pluggable worker.
//
// The two core abstractions are:
//
//   - Storage[T] — persists scheduled items and returns the ones ready to run.
//   - Worker[T]   — processes items submitted by the scheduler.
//
// A ready-to-use in-memory storage is provided in the subpackage at
// github.com/fmotalleb/go-scheduler/storage, and two worker implementations
// live in the subpackage github.com/fmotalleb/go-scheduler/worker:
//
//   - worker.Sync — runs each task synchronously, in the scheduler's own
//     goroutine, one at a time.
//   - worker.WorkerPool — fans tasks out to a fixed pool of goroutines for
//     concurrent execution.
//
// # Getting started
//
// The simplest usage is NewCallback, which accepts context-aware functions:
//
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//
//	s := scheduler.NewCallback(ctx,
//	    scheduler.WithTickerCycle(250*time.Millisecond),
//	)
//
//	id, _ := s.Add(time.Now().Add(5*time.Second), func(ctx context.Context) {
//	    fmt.Println("Hello, world!")
//	})
//
// For custom task types, use the generic New constructor:
//
//	type Email struct { To, Body string }
//
//	w := worker.NewSync(ctx, func(ctx context.Context, e Email) {
//	    sendEmail(e.To, e.Body)
//	})
//
//	s := scheduler.New(ctx, w)
//	s.Add(time.Now().Add(1*time.Hour), Email{"user@example.com", "Hi!"})
//
// # Functional options
//
// All configuration is done via functional options:
//
//   - WithTickerCycle — how often the scheduler checks for due items
//     (default: 1 second).
//   - WithStorage     — a custom Storage implementation.
//   - WithWorker      — a custom Worker implementation.
//   - WithWorkerPool  — shorthand to create a worker.WorkerPool inline.
//   - WithLogger      — an error hook called on internal failures.
//
// # Concurrency
//
// New and NewCallback start the scheduler loop in a background goroutine.
// The scheduler guarantees that each call to Storage.PopBefore and each
// Worker.Submit is serialised — only one tick is processed at a time. If a
// tick handler panics the scheduler recovers, logs the error, and continues.
//
// Use context cancellation to shut down the scheduler cleanly.
//
// # Sub-packages
//
//   - github.com/fmotalleb/go-scheduler/storage — MemoryStorage backed by a
//     B-tree.
//
//     store := storage.NewMemoryStorage[MyType](8)
//
//   - github.com/fmotalleb/go-scheduler/worker — Sync and WorkerPool
//     implementations, plus the Handler[T] type alias.
package scheduler

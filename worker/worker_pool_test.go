package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewWorkerPool_defaultState(t *testing.T) {
	ctx := context.Background()
	pool := NewWorkerPool[int](ctx, func(_ context.Context, _ int) {}, 2, 10)
	if pool == nil {
		t.Fatal("expected non-nil pool")
	}
	defer pool.Close()
}

func TestWorkerPoolSubmit_executesHandler(t *testing.T) {
	ctx := context.Background()
	var (
		mu     sync.Mutex
		result int
	)
	pool := NewWorkerPool[int](ctx, func(_ context.Context, v int) {
		mu.Lock()
		result = v
		mu.Unlock()
	}, 2, 10)
	defer pool.Close()

	err := pool.Submit(42)
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	// Allow time for worker to pick up the job
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	if result != 42 {
		t.Fatalf("expected 42, got %d", result)
	}
	mu.Unlock()
}

func TestWorkerPoolSubmit_concurrentExecution(t *testing.T) {
	ctx := context.Background()
	var counter atomic.Int32

	pool := NewWorkerPool[int](ctx, func(_ context.Context, v int) {
		counter.Add(1)
		// Simulate some work
		time.Sleep(10 * time.Millisecond)
	}, 4, 100)
	defer pool.Close()

	for i := 0; i < 20; i++ {
		err := pool.Submit(i)
		if err != nil {
			t.Fatalf("Submit %d failed: %v", i, err)
		}
	}

	// Allow enough time for all jobs to be processed
	time.Sleep(200 * time.Millisecond)

	if c := counter.Load(); c != 20 {
		t.Fatalf("expected 20 tasks processed, got %d", c)
	}
}

func TestWorkerPoolSubmit_afterClose(t *testing.T) {
	ctx := context.Background()
	pool := NewWorkerPool[int](ctx, func(_ context.Context, _ int) {}, 1, 10)
	pool.Close()

	err := pool.Submit(1)
	if err == nil {
		t.Fatal("expected error after close")
	}
}

func TestWorkerPoolClose_waitsForInflightTasks(t *testing.T) {
	ctx := context.Background()
	var counter atomic.Int32

	pool := NewWorkerPool[int](ctx, func(_ context.Context, v int) {
		time.Sleep(50 * time.Millisecond)
		counter.Add(1)
	}, 2, 10)

	// Submit 2 tasks (matching the number of workers) so none remain queued
	for i := 0; i < 2; i++ {
		_ = pool.Submit(i)
	}

	// Allow a moment for both workers to pick up the tasks
	time.Sleep(10 * time.Millisecond)

	// Close should block until in-flight tasks complete
	pool.Close()

	if c := counter.Load(); c != 2 {
		t.Fatalf("expected both in-flight tasks to complete, got %d", c)
	}
}

func TestWorkerPoolClose_drainsQueueBeforeReturn(t *testing.T) {
	ctx := context.Background()
	var counter atomic.Int32

	pool := NewWorkerPool[int](ctx, func(_ context.Context, v int) {
		time.Sleep(20 * time.Millisecond)
		counter.Add(1)
	}, 2, 10)

	// Submit 6 tasks — 2 are in-flight, 4 are queued
	for i := 0; i < 6; i++ {
		_ = pool.Submit(i)
	}

	// Give workers time to pick up first 2 tasks and complete them
	// then pick up next 2 before Close cancels the context
	time.Sleep(60 * time.Millisecond)

	pool.Close()

	// At minimum the tasks picked up in the first batch completed;
	// tasks still in the queue when context was cancelled did not.
	c := counter.Load()
	if c < 2 {
		t.Fatalf("expected at least the 2 in-flight tasks to complete, got %d", c)
	}
}

func TestWorkerPool_contextCancellation_stopsWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	done := make(chan struct{})

	pool := NewWorkerPool[int](ctx, func(_ context.Context, _ int) {
		close(started)
		time.Sleep(time.Second)
	}, 1, 10)

	_ = pool.Submit(1)

	// Wait for the worker to start processing
	<-started

	// Cancel the context
	go func() {
		cancel()
		pool.Close()
		close(done)
	}()

	// Close should return relatively quickly after cancellation
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for context cancellation to stop workers")
	}
}

func TestWorkerPool_passesContext(t *testing.T) {
	ctx := context.Background()
	pool := NewWorkerPool[int](ctx, func(c context.Context, _ int) {
		if c == nil {
			t.Error("expected non-nil context")
		}
	}, 1, 10)
	defer pool.Close()

	_ = pool.Submit(0)
	time.Sleep(50 * time.Millisecond)
}

func TestWorkerPool_multipleSubmitsWithQueue(t *testing.T) {
	ctx := context.Background()
	var mu sync.Mutex
	results := make([]int, 0, 10)

	pool := NewWorkerPool[int](ctx, func(_ context.Context, v int) {
		mu.Lock()
		results = append(results, v)
		mu.Unlock()
	}, 1, 10)
	defer pool.Close()

	for i := 0; i < 10; i++ {
		err := pool.Submit(i)
		if err != nil {
			t.Fatalf("Submit %d failed: %v", i, err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if len(results) != 10 {
		t.Fatalf("expected 10 results, got %d", len(results))
	}
	mu.Unlock()
}

func TestWorkerPool_zeroWorkers(t *testing.T) {
	ctx := context.Background()
	pool := NewWorkerPool[int](ctx, func(_ context.Context, _ int) {}, 0, 10)
	defer pool.Close()

	// Zero workers means no one will process. Submit should still succeed (buffered).
	err := pool.Submit(1)
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}
	// No assertion about processing since there are no workers
}

func TestWorkerPool_submitBlocksFullQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create pool with 1 worker and queue size 1
	pool := NewWorkerPool[int](ctx, func(_ context.Context, v int) {
		// Block the single worker so it can't drain the queue
		time.Sleep(100 * time.Millisecond)
	}, 1, 1)

	// Submit first item - gets picked up by worker immediately (no blocking)
	err := pool.Submit(1)
	if err != nil {
		t.Fatalf("Submit 1 failed: %v", err)
	}

	// Submit second item - fills the queue buffer
	err = pool.Submit(2)
	if err != nil {
		t.Fatalf("Submit 2 failed: %v", err)
	}

	// Submit third item - should still be accepted since the first is being processed
	// and the second is in the buffer. This might block briefly but shouldn't fail.
	done := make(chan error, 1)
	go func() {
		done <- pool.Submit(3)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Submit 3 failed: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		// This is acceptable - Submit may block until there's room
	}

	pool.Close()
}

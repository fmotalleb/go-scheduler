package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fmotalleb/go-scheduler/storage"
	"github.com/fmotalleb/go-scheduler/ticker"
	"github.com/fmotalleb/go-scheduler/worker"
)

// Compile-time check: *ticker.TimeTicker satisfies the Ticker interface.
var _ Ticker = (*ticker.TimeTicker)(nil)

// ---------------------------------------------------------------------------
// Helper types
// ---------------------------------------------------------------------------

// mockStorage implements Storage[int] for deterministic testing.
type mockStorage[T any] struct {
	mu        sync.Mutex
	items     []T
	popBefore func(time.Time) ([]T, error)
	add       func(time.Time, T) (int, error)
	remove    func(int) (T, error)
	closeFn   func()
	closed    bool
}

func (m *mockStorage[T]) Add(t time.Time, v T) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items = append(m.items, v)
	if m.add != nil {
		return m.add(t, v)
	}
	return len(m.items), nil
}

func (m *mockStorage[T]) Remove(id int) (T, error) {
	if m.remove != nil {
		return m.remove(id)
	}
	var zero T
	return zero, nil
}

func (m *mockStorage[T]) PopBefore(t time.Time) ([]T, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.popBefore != nil {
		return m.popBefore(t)
	}
	items := m.items
	m.items = nil
	return items, nil
}

func (m *mockStorage[T]) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	if m.closeFn != nil {
		m.closeFn()
	}
}

// mockWorker implements Worker[int] for deterministic testing.
type mockWorker[T any] struct {
	mu        sync.Mutex
	submitted []T
	submitFn  func(T) error
	closeFn   func()
	closed    bool
}

func (m *mockWorker[T]) Submit(t T) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.submitted = append(m.submitted, t)
	if m.submitFn != nil {
		return m.submitFn(t)
	}
	return nil
}

func (m *mockWorker[T]) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	if m.closeFn != nil {
		m.closeFn()
	}
}

// ---------------------------------------------------------------------------
// Constructor tests
// ---------------------------------------------------------------------------

func TestNew_createsAndStarts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := &mockWorker[int]{}
	s := New(ctx, w)

	if s == nil {
		t.Fatal("expected non-nil scheduler")
	}
	if s.storage == nil {
		t.Fatal("expected default storage")
	}
	if s.ticker == nil {
		t.Fatal("expected default ticker")
	}
	if s.worker == nil {
		t.Fatal("expected worker to be set")
	}
}

func TestNewCallback_createsWithDefaultWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := NewCallback(ctx)
	if s == nil {
		t.Fatal("expected non-nil scheduler")
	}
	if s.worker == nil {
		t.Fatal("expected default sync worker")
	}
}

func TestNewCallback_withCustomWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := &mockWorker[Callback]{}
	s := NewCallback(ctx, WithWorker[Callback](w))

	if s.worker != w {
		t.Fatal("expected custom worker")
	}
}

// ---------------------------------------------------------------------------
// Add / Remove tests
// ---------------------------------------------------------------------------

func TestScheduler_Add(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ms := &mockStorage[int]{}
	s := New(ctx, &mockWorker[int]{}, WithStorage[int](ms))

	id, err := s.Add(time.Now(), 42)
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if id != 1 {
		t.Fatalf("expected id 1 (first call returns len(items)), got %d", id)
	}
}

func TestScheduler_Remove(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ms := &mockStorage[int]{}
	ms.remove = func(id int) (int, error) {
		return 99, nil
	}

	s := New(ctx, &mockWorker[int]{}, WithStorage[int](ms))

	val, err := s.Remove(1)
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
	if val != 99 {
		t.Fatalf("expected 99, got %d", val)
	}
}

// ---------------------------------------------------------------------------
// runCycle tests
// ---------------------------------------------------------------------------

func TestRunCycle_popsAndSubmits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	now := time.Now()

	ms := &mockStorage[int]{
		popBefore: func(t time.Time) ([]int, error) {
			return []int{1, 2, 3}, nil
		},
	}
	mw := &mockWorker[int]{}

	s := New(ctx, mw, WithStorage[int](ms))
	s.ticker.Close() // prevent automatic ticks

	// Manually trigger a cycle
	s.runCycle(now)

	mw.mu.Lock()
	if len(mw.submitted) != 3 {
		t.Fatalf("expected 3 submitted items, got %d", len(mw.submitted))
	}
	if mw.submitted[0] != 1 || mw.submitted[1] != 2 || mw.submitted[2] != 3 {
		t.Fatalf("expected [1,2,3], got %v", mw.submitted)
	}
	mw.mu.Unlock()
}

func TestRunCycle_storageErrorIsLogged(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ms := &mockStorage[int]{
		popBefore: func(t time.Time) ([]int, error) {
			return nil, assertAnError("storage error")
		},
	}

	var logged atomic.Bool
	s := New(ctx, &mockWorker[int]{},
		WithStorage[int](ms),
		WithLogger[int](func(err error) {
			logged.Store(true)
		}),
	)
	s.ticker.Close()

	s.runCycle(time.Now())

	if !logged.Load() {
		t.Fatal("expected storage error to be logged")
	}
}

func TestRunCycle_workerErrorIsLogged(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ms := &mockStorage[int]{
		popBefore: func(t time.Time) ([]int, error) {
			return []int{1, 2}, nil
		},
	}
	mw := &mockWorker[int]{
		submitFn: func(t int) error {
			if t == 2 {
				return assertAnError("worker error")
			}
			return nil
		},
	}

	var (
		mu        sync.Mutex
		logged    bool
		loggedMsg string
	)
	s := New(ctx, mw,
		WithStorage[int](ms),
		WithLogger[int](func(err error) {
			mu.Lock()
			logged = true
			loggedMsg = err.Error()
			mu.Unlock()
		}),
	)
	s.ticker.Close()

	s.runCycle(time.Now())

	mu.Lock()
	if !logged {
		t.Fatal("expected worker error to be logged")
	}
	if loggedMsg != "failed to submit item 2: worker error" {
		t.Fatalf("unexpected error message: %s", loggedMsg)
	}
	mu.Unlock()
}

func TestRunCycle_panicRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ms := &mockStorage[int]{
		popBefore: func(t time.Time) ([]int, error) {
			panic(assertAnError("boom"))
		},
	}

	var (
		mu     sync.Mutex
		logged bool
	)
	s := New(ctx, &mockWorker[int]{},
		WithStorage[int](ms),
		WithLogger[int](func(err error) {
			mu.Lock()
			logged = true
			mu.Unlock()
		}),
	)
	s.ticker.Close()

	// Should not panic
	s.runCycle(time.Now())

	mu.Lock()
	if !logged {
		t.Fatal("expected panic to be logged")
	}
	mu.Unlock()
}

func TestRunCycle_panicWithNonError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ms := &mockStorage[int]{
		popBefore: func(t time.Time) ([]int, error) {
			panic("string panic")
		},
	}

	var (
		mu     sync.Mutex
		logged bool
	)
	s := New(ctx, &mockWorker[int]{},
		WithStorage[int](ms),
		WithLogger[int](func(err error) {
			mu.Lock()
			logged = true
			mu.Unlock()
		}),
	)
	s.ticker.Close()

	s.runCycle(time.Now())

	mu.Lock()
	if !logged {
		t.Fatal("expected non-error panic to be logged")
	}
	mu.Unlock()
}

// ---------------------------------------------------------------------------
// Close tests
// ---------------------------------------------------------------------------

func TestScheduler_Close(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ms := &mockStorage[int]{}
	mw := &mockWorker[int]{}

	s := New(ctx, mw, WithStorage[int](ms))
	s.Close()

	ms.mu.Lock()
	if !ms.closed {
		t.Fatal("expected storage to be closed")
	}
	ms.mu.Unlock()

	mw.mu.Lock()
	if !mw.closed {
		t.Fatal("expected worker to be closed")
	}
	mw.mu.Unlock()

	// ticker should have stopped (best-effort check)
	select {
	case _, ok := <-s.ticker.C():
		if ok {
			t.Fatal("expected ticker channel to be closed after Stop")
		}
	default:
	}
}

// ---------------------------------------------------------------------------
// Integration tests (short-lived scheduler)
// ---------------------------------------------------------------------------

func TestScheduler_integration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var results []int

	mem := storage.NewBTreeStorage[int]()
	sw := worker.NewSync(ctx, func(_ context.Context, v int) {
		mu.Lock()
		results = append(results, v)
		mu.Unlock()
	})

	s := New(ctx, sw,
		WithStorage[int](mem),
		WithTickerCycle[int](10*time.Millisecond),
	)
	defer s.Close()

	now := time.Now()
	s.Add(now.Add(-time.Hour), 1)   // past => immediate
	s.Add(now.Add(-time.Minute), 2) // past => immediate

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	count := len(results)
	mu.Unlock()

	if count < 2 {
		t.Fatalf("expected at least 2 tasks executed, got %d", count)
	}
}

func TestScheduler_integrationWithCallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var results []string

	s := NewCallback(ctx,
		WithTickerCycle[Callback](10*time.Millisecond),
	)
	defer s.Close()

	now := time.Now()
	s.Add(now.Add(-time.Hour), func(_ context.Context) {
		mu.Lock()
		results = append(results, "done")
		mu.Unlock()
	})

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if len(results) != 1 {
		t.Fatalf("expected 1 callback executed, got %d", len(results))
	}
	mu.Unlock()
}

// ---------------------------------------------------------------------------
// Options tests
// ---------------------------------------------------------------------------

func TestWithTickerCycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := &mockWorker[int]{}
	s := New(ctx, w, WithTickerCycle[int](50*time.Millisecond))

	if s.ticker == nil {
		t.Fatal("expected non-nil ticker")
	}
}

func TestWithStorage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mem := storage.NewBTreeStorage[int]()
	w := &mockWorker[int]{}
	s := New(ctx, w, WithStorage[int](mem))

	if s.storage != mem {
		t.Fatal("expected custom storage")
	}
}

func TestWithWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mw := &mockWorker[int]{}
	// Pass a different worker to New, override with WithWorker
	s := New(ctx, &mockWorker[int]{}, WithWorker[int](mw))

	if s.worker != mw {
		t.Fatal("expected custom worker from option")
	}
}

func TestWithWorkerPool(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	executed := make(chan int, 1)
	s := New(ctx, &mockWorker[int]{},
		WithWorkerPool[int](ctx, func(_ context.Context, v int) {
			executed <- v
		}, 2, 10),
	)
	defer s.Close()

	// Add a past-due task and wait for it to be picked up
	_, _ = s.Add(time.Now().Add(-time.Hour), 42)
	time.Sleep(100 * time.Millisecond)

	select {
	case v := <-executed:
		if v != 42 {
			t.Fatalf("expected 42, got %d", v)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for task execution via WorkerPool")
	}
}

func TestWithLogger(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var logged atomic.Bool
	logFn := func(err error) {
		logged.Store(true)
	}

	w := &mockWorker[int]{}
	s := New(ctx, w, WithLogger[int](logFn))
	defer s.Close()

	if s.logFn == nil {
		t.Fatal("expected logFn to be set")
	}

	// Log something manually
	s.logFn(assertAnError("test error"))
	if !logged.Load() {
		t.Fatal("expected logFn to be called")
	}
}

// ---------------------------------------------------------------------------
// Context cancellation stops scheduler
// ---------------------------------------------------------------------------

func TestScheduler_contextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	w := &mockWorker[int]{}
	s := New(ctx, w)

	// Give the goroutine a moment to see the cancellation
	time.Sleep(50 * time.Millisecond)
	s.Close()
	// If we got here without deadlock, the goroutine exited cleanly
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// assertAnError returns a sentinel error for testing error paths.
type assertAnError string

func (e assertAnError) Error() string { return string(e) }

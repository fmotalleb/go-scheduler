package worker

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestNewSync(t *testing.T) {
	ctx := context.Background()
	w := NewSync(ctx, func(_ context.Context, _ int) {})
	if w == nil {
		t.Fatal("expected non-nil Sync worker")
	}
	if w.ctx == nil {
		t.Fatal("expected non-nil context")
	}
}

func TestSyncSubmit_callsHandler(t *testing.T) {
	ctx := context.Background()
	var (
		mu     sync.Mutex
		result int
	)
	w := NewSync(ctx, func(_ context.Context, v int) {
		mu.Lock()
		result = v
		mu.Unlock()
	})

	err := w.Submit(42)
	if err != nil {
		t.Fatalf("Submit failed: %v", err)
	}

	mu.Lock()
	if result != 42 {
		t.Fatalf("expected 42, got %d", result)
	}
	mu.Unlock()
}

func TestSyncSubmit_passesNonNilContext(t *testing.T) {
	ctx := context.Background()
	w := NewSync(ctx, func(c context.Context, _ int) {
		if c == nil {
			t.Fatal("expected non-nil context")
		}
	})

	_ = w.Submit(0)
}

func TestSyncSubmit_doesNotReturnError(t *testing.T) {
	ctx := context.Background()
	w := NewSync(ctx, func(_ context.Context, _ int) {})

	err := w.Submit(99)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
}

func TestSyncClose_cancelsContext(t *testing.T) {
	ctx := context.Background()
	done := make(chan struct{})

	w := NewSync(ctx, func(c context.Context, _ int) {
		select {
		case <-c.Done():
			close(done)
		default:
			t.Error("expected context to be cancelled")
		}
	})

	w.Close()
	w.Submit(0) // handler gets cancelled context

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for cancelled context")
	}
}

func TestSyncMultipleSubmits_areSequential(t *testing.T) {
	ctx := context.Background()
	var (
		mu     sync.Mutex
		events []int
	)
	w := NewSync(ctx, func(_ context.Context, v int) {
		mu.Lock()
		events = append(events, v)
		mu.Unlock()
	})

	for i := 0; i < 10; i++ {
		_ = w.Submit(i)
	}

	mu.Lock()
	if len(events) != 10 {
		t.Fatalf("expected 10 events, got %d", len(events))
	}
	for i := 0; i < 10; i++ {
		if events[i] != i {
			t.Fatalf("expected events[%d] = %d, got %d", i, i, events[i])
		}
	}
	mu.Unlock()
}

func TestSync_StringType(t *testing.T) {
	ctx := context.Background()
	var result string
	w := NewSync(ctx, func(_ context.Context, v string) {
		result = v
	})

	_ = w.Submit("hello")
	if result != "hello" {
		t.Fatalf("expected 'hello', got %q", result)
	}
}

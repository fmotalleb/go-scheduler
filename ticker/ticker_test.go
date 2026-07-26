package ticker

import (
	"testing"
	"time"
)

// TestNewTimeTicker verifies that NewTimeTicker returns a non-nil
// TimeTicker with a working channel.
func TestNewTimeTicker(t *testing.T) {
	tt := NewTimeTicker(100 * time.Millisecond)
	if tt == nil {
		t.Fatal("expected non-nil TimeTicker")
	}
	if tt.t == nil {
		t.Fatal("expected non-nil inner time.Ticker")
	}
	tt.Close()
}

// TestTimeTicker_C_returnsNonNilChannel verifies that C() returns a
// non-nil channel.
func TestTimeTicker_C_returnsNonNilChannel(t *testing.T) {
	tt := NewTimeTicker(time.Hour) // long enough to not fire during test
	defer tt.Close()

	ch := tt.C()
	if ch == nil {
		t.Fatal("expected non-nil channel from C()")
	}
}

// TestTimeTicker_C_returnsSameChannel verifies that multiple calls to
// C() return the same channel (as required by the scheduler.Ticker
// contract).
func TestTimeTicker_C_returnsSameChannel(t *testing.T) {
	tt := NewTimeTicker(time.Hour)
	defer tt.Close()

	c1 := tt.C()
	c2 := tt.C()

	if c1 != c2 {
		t.Fatal("expected C() to return the same channel on each call")
	}
}

// TestTimeTicker_fires verifies that the ticker sends a value on its
// channel after the configured duration.
func TestTimeTicker_fires(t *testing.T) {
	tt := NewTimeTicker(10 * time.Millisecond)
	defer tt.Close()

	select {
	case <-tt.C():
		// Expected — ticker fired.
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ticker to fire")
	}
}

// TestTimeTicker_firesMultipleTimes verifies that the ticker continues
// to fire at the configured interval.
func TestTimeTicker_firesMultipleTimes(t *testing.T) {
	tt := NewTimeTicker(5 * time.Millisecond)
	defer tt.Close()

	for i := 0; i < 5; i++ {
		select {
		case <-tt.C():
			// Expected — ticker fired.
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for tick %d", i+1)
		}
	}
}

// TestTimeTicker_Close_stopsFiring verifies that after Close, no more
// ticks are delivered on the channel.
func TestTimeTicker_Close_stopsFiring(t *testing.T) {
	tt := NewTimeTicker(1 * time.Millisecond)

	// Let a few ticks through, then close.
	<-tt.C()
	<-tt.C()
	<-tt.C()

	tt.Close()

	// After Close, the channel should not deliver any more values.
	// There might be one stale tick buffered, so we use a small
	// window and check the bool.
	select {
	case _, ok := <-tt.C():
		if ok {
			// A single buffered tick is acceptable; what matters
			// is that the ticker stopped producing new ones.
			// Check that no further tick arrives.
			select {
			case <-tt.C():
				t.Fatal("received tick after Close (ticker did not stop)")
			case <-time.After(20 * time.Millisecond):
				// No second tick — ticker is stopped.
			}
		}
	case <-time.After(50 * time.Millisecond):
		// Channel may never fire again, which is also fine.
	}
}

// TestTimeTicker_Close_idempotent verifies that calling Close multiple
// times does not panic.
func TestTimeTicker_Close_idempotent(t *testing.T) {
	tt := NewTimeTicker(time.Hour)

	// Should not panic.
	tt.Close()
	tt.Close()
	tt.Close()
}

// TestNewTimeTicker_zeroDurationPanics verifies that creating a ticker
// with a non-positive duration panics (matching time.NewTicker behaviour).
func TestNewTimeTicker_zeroDurationPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for zero duration")
		}
	}()

	NewTimeTicker(0)
}

// TestNewTimeTicker_negativeDurationPanics verifies that creating a
// ticker with a negative duration panics.
func TestNewTimeTicker_negativeDurationPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for negative duration")
		}
	}()

	NewTimeTicker(-time.Second)
}

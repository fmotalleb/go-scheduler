// Package ticker provides a concrete implementation of the scheduler.Ticker
// interface backed by time.Ticker.
package ticker

import "time"

// TimeTicker wraps a standard library time.Ticker to satisfy the
// scheduler.Ticker interface. It fires ticks at a configurable interval.
//
// Use NewTimeTicker to create one; call Close to stop it.
type TimeTicker struct {
	t *time.Ticker
}

// NewTimeTicker creates a new TimeTicker that sends the current time on
// its channel every d. The underlying time.Ticker is started immediately.
//
// d must be greater than zero; otherwise, NewTimeTicker will panic
// (matching the behaviour of time.NewTicker).
func NewTimeTicker(d time.Duration) *TimeTicker {
	return &TimeTicker{
		t: time.NewTicker(d),
	}
}

// C returns a receive-only channel that delivers the current time each
// time the ticker fires. The channel is shared; each call returns the
// same channel.
func (t *TimeTicker) C() <-chan time.Time {
	return t.t.C
}

// Close stops the TimeTicker. After Close returns, no more ticks are
// sent on the channel returned by C. Calling Close more than once is
// safe and is a no-op on subsequent calls.
func (t *TimeTicker) Close() {
	t.t.Stop()
}

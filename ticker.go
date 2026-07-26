package scheduler

import "time"

// Ticker provides a time-based signal source for the scheduler.
//
// Implementations wrap a time source (e.g. a time.Ticker) and expose two
// operations:
//   - C returns a channel that receives successive clock ticks.
//   - Close stops the ticker and releases any associated resources.
//
// The scheduler calls C once per tick to receive the next tick time and
// calls Close when the scheduler is shut down.
type Ticker interface {
	// C returns a receive-only channel that delivers the current time
	// on each tick. After Close is called the channel should be closed
	// or become unresponsive.
	C() <-chan time.Time

	// Close stops the ticker and releases its resources. After Close
	// returns, no more values should be sent on the channel returned by
	// C. Calling Close more than once must not panic.
	Close()
}

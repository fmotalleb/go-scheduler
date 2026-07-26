package worker

import "context"

// Sync is a synchronous worker that processes each task immediately on
// the caller's goroutine (the scheduler's runCycle goroutine). Tasks are
// handled one at a time in insertion order.
//
// Create one via NewSync.
type Sync[T any] struct {
	ctx     context.Context
	handler Handler[T]
	cancel  func()
}

// NewSync creates a synchronous worker.
//
// The handler is called for each task submitted via Submit. The worker
// respects context cancellation during Close.
func NewSync[T any](ctx context.Context, handler Handler[T]) *Sync[T] {
	ctx, cancel := context.WithCancel(ctx) //nolint:gosec // cancel is stored and called during shutdown
	return &Sync[T]{
		ctx:     ctx,
		handler: handler,
		cancel:  cancel,
	}
}

// Submit calls the handler with the given task immediately on the
// calling goroutine. It always returns nil.
func (s *Sync[T]) Submit(t T) error {
	s.handler(s.ctx, t)
	return nil
}

// Close cancels the worker's context. Any subsequent call to Submit will
// call the handler with a cancelled context.
func (s *Sync[T]) Close() {
	s.cancel()
}

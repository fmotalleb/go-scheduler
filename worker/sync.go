package worker

import "context"

type Sync[T any] struct {
	ctx     context.Context
	handler Handler[T]
	cancel  func()
}

func NewSync[T any](ctx context.Context, handler Handler[T]) *Sync[T] {
	ctx, cancel := context.WithCancel(ctx)
	return &Sync[T]{
		ctx:     ctx,
		handler: handler,
		cancel:  cancel,
	}
}

func (s *Sync[T]) Submit(t T) error {
	s.handler(s.ctx, t)
	return nil
}

func (s *Sync[T]) Close() {
	s.cancel()
}

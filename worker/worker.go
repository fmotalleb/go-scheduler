package worker

import "context"

// Handler is a function type that processes a single task of type T.
// It receives a context (which may be cancelled during shutdown) and
// the task value.
//
// Use Handler when creating a Sync or WorkerPool worker.
type Handler[T any] = func(context.Context, T)

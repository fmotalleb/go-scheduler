package worker

import "context"

type Handler[T any] = func(context.Context, T)

package scheduler

import "time"

type Storage[T any] interface {
	Add(time.Time, T) (int, error)
	Remove(int) (T, error)
	PopBefore(time.Time) ([]T, error)
	Close()
}

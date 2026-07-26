package scheduler

import "time"

// Storage defines the interface for persisting scheduled tasks.
//
// Implementations store tasks keyed by their execution time. The scheduler
// periodically calls PopBefore to retrieve all tasks that are due.
type Storage[T any] interface {
	// Add stores a task to be executed at (or after) the given time.
	// It returns a unique ID that can be used to remove the task later.
	Add(time.Time, T) (int, error)

	// Remove deletes a previously stored task by ID, returning the
	// original value. An error is returned if the ID does not exist.
	Remove(int) (T, error)

	// PopBefore retrieves and removes all tasks scheduled strictly
	// before the given time. The returned slice is in insertion order
	// for tasks with the same timestamp.
	PopBefore(time.Time) ([]T, error)

	// Close releases any resources held by the storage.
	Close()
}

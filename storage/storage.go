package storage

import (
	"errors"
	"time"

	"github.com/google/btree"
)

type storageKey struct {
	When time.Time
	ID   int
}

func (k storageKey) Less(other storageKey) bool {
	if k.When.Before(other.When) {
		return true
	}
	if k.When.After(other.When) {
		return false
	}
	return k.ID < other.ID
}

type item[T any] struct {
	key   storageKey
	value T
}

func (i *item[T]) Less(other btree.Item) bool {
	i, ok := other.(*item[T])
	if !ok {
		return false
	}
	return i.key.Less(i.key)
}

// MemoryStorage is an in-memory implementation of Storage[T] backed by a
// B-tree (google/btree). Tasks are ordered by their scheduled time and,
// for identical timestamps, by insertion order.
//
// Data is lost when the process exits. For production use with persistence
// requirements, implement the Storage[T] interface against a database.
type MemoryStorage[T any] struct {
	tree   *btree.BTree
	lookup map[int]*item[T]
	nextID int
}

// NewMemoryStorage creates an in-memory task storage with the given B-tree
// degree. If degree is less than 2, it defaults to 8, which is a sensible
// value for most workloads.
func NewMemoryStorage[T any](degree int) *MemoryStorage[T] {
	if degree < 2 {
		degree = 8
	}

	return &MemoryStorage[T]{
		tree:   btree.New(degree),
		lookup: make(map[int]*item[T]),
	}
}

// Add stores a task to be executed at (or after) the given time. It
// assigns an auto-incrementing ID and returns it. The ID can be used with
// Remove to cancel the task.
func (s *MemoryStorage[T]) Add(t time.Time, v T) (int, error) {
	s.nextID++

	it := &item[T]{
		key: storageKey{
			When: t,
			ID:   s.nextID,
		},
		value: v,
	}

	s.tree.ReplaceOrInsert(it)
	s.lookup[it.key.ID] = it

	return it.key.ID, nil
}

// Remove deletes a previously stored task by ID, returning the original
// value. An error is returned if the ID does not exist.
func (s *MemoryStorage[T]) Remove(id int) (T, error) {
	var zero T

	it, ok := s.lookup[id]
	if !ok {
		return zero, errors.New("item not found")
	}

	s.tree.Delete(it)
	delete(s.lookup, id)

	return it.value, nil
}

// PopBefore retrieves and removes all tasks scheduled strictly before
// the given time. Tasks are returned in insertion order for identical
// timestamps.
func (s *MemoryStorage[T]) PopBefore(t time.Time) ([]T, error) {
	limit := &item[T]{
		key: storageKey{
			When: t,
			ID:   0,
		},
	}

	var (
		items []*item[T]
		out   []T
	)

	s.tree.AscendLessThan(limit, func(i btree.Item) bool {
		it, ok := i.(*item[T])
		if !ok {
			return false
		}
		items = append(items, it)
		out = append(out, it.value)
		return true
	})

	for _, it := range items {
		s.tree.Delete(it)
		delete(s.lookup, it.key.ID)
	}

	return out, nil
}

// Close releases all resources held by the storage, clearing the lookup
// map and the B-tree.
func (s *MemoryStorage[T]) Close() {
	for k := range s.lookup {
		delete(s.lookup, k)
	}
	s.tree.Clear(false)
}

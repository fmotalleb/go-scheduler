package scheduler

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

type MemoryStorage[T any] struct {
	tree   *btree.BTree
	lookup map[int]*item[T]
	nextID int
}

func NewMemoryStorage[T any](degree int) *MemoryStorage[T] {
	if degree < 2 {
		degree = 8
	}

	return &MemoryStorage[T]{
		tree:   btree.New(degree),
		lookup: make(map[int]*item[T]),
	}
}

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

func (s *MemoryStorage[T]) Close() {
	for k := range s.lookup {
		delete(s.lookup, k)
	}
	s.tree.Clear(false)
}

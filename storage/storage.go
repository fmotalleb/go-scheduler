package storage

import (
	"errors"
	"sort"
	"sync"
	"time"
)

// ErrNotFound is returned by Remove when no entry with the given id exists.
var ErrNotFound = errors.New("scheduler: id not found")

// ErrClosed is returned by Add, Remove, and PopBefore once Close has been
// called on the storage.
var ErrClosed = errors.New("scheduler: storage closed")

// defaultDegree is the minimum degree (t) used when NewBTreeStorage is
// called without an explicit degree. Each node holds between t-1 and 2t-1
// keys (except the root, which may hold fewer).
const defaultDegree = 8

// entry is a single scheduled value paired with the id it was assigned and
// the time it fires at.
type entry[T any] struct {
	id    int
	when  time.Time
	value T
}

// bucket groups every entry scheduled for exactly the same time.Time. This
// is what the B-tree actually stores one-per-key, since multiple callers
// may legitimately schedule work for an identical instant.
type bucket[T any] struct {
	when    time.Time
	entries []*entry[T]
}

type node[T any] struct {
	leaf     bool
	buckets  []*bucket[T]
	children []*node[T] // len(children) == len(buckets)+1 when non-leaf
}

// BTreeStorage is an in-memory, B-tree backed implementation of Storage[T].
// Keys are time.Time values ordered ascending; Add, Remove and GetBefore are
// all O(log n) in the number of distinct scheduled times. It is safe for
// concurrent use.
//
// Call Close when the storage is no longer needed; after that, Add,
// Remove, and PopBefore all return ErrClosed.
type BTreeStorage[T any] struct {
	mu     sync.Mutex
	root   *node[T]
	degree int
	nextID int
	index  map[int]*bucket[T] // id -> bucket currently holding that id
	size   int
	closed bool
}

// NewBTreeStorage creates a ready-to-use Storage[T] backed by a B-tree.
func NewBTreeStorage[T any]() *BTreeStorage[T] {
	return NewBTreeStorageWithDegree[T](defaultDegree)
}

// NewBTreeStorageWithDegree lets the caller tune the B-tree's minimum
// degree. Larger degrees mean shallower trees and fewer allocations at the
// cost of more work per node; the default is a reasonable general-purpose
// choice. Degrees below 2 fall back to the default.
func NewBTreeStorageWithDegree[T any](degree int) *BTreeStorage[T] {
	if degree < 2 {
		degree = defaultDegree
	}
	return &BTreeStorage[T]{
		root:   &node[T]{leaf: true},
		degree: degree,
		nextID: 1,
		index:  make(map[int]*bucket[T]),
	}
}

// Len reports the number of entries currently stored.
func (s *BTreeStorage[T]) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size
}

// Close releases the storage's internal tree and index, allowing the
// memory to be garbage collected, and marks the storage closed. It is safe
// to call Close more than once. There are no background goroutines or
// external resources to release, but Close still fulfills the Storage
// contract and gives callers a clean, explicit lifecycle boundary — and a
// backing implementation could later add real resources (e.g. a file or
// connection) without changing this method's signature.
func (s *BTreeStorage[T]) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true
	s.root = &node[T]{leaf: true}
	s.index = nil
	s.size = 0
}

// Add schedules value to fire at when and returns a unique id that can
// later be passed to Remove.
func (s *BTreeStorage[T]) Add(when time.Time, value T) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, ErrClosed
	}

	id := s.nextID
	s.nextID++
	e := &entry[T]{id: id, when: when, value: value}

	t := s.degree
	if len(s.root.buckets) == 2*t-1 {
		newRoot := &node[T]{leaf: false, children: []*node[T]{s.root}}
		s.splitChild(newRoot, 0)
		s.root = newRoot
	}

	b := s.insertNonFull(s.root, e)
	s.index[id] = b
	s.size++
	return id, nil
}

// Remove deletes the entry with the given id and returns its value.
// It returns ErrNotFound if no such entry exists.
func (s *BTreeStorage[T]) Remove(id int) (T, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var zero T
	if s.closed {
		return zero, ErrClosed
	}
	b, ok := s.index[id]
	if !ok {
		return zero, ErrNotFound
	}

	value := zero
	for i, e := range b.entries {
		if e.id == id {
			value = e.value
			b.entries = append(b.entries[:i], b.entries[i+1:]...)
			break
		}
	}
	delete(s.index, id)
	s.size--

	if len(b.entries) == 0 {
		s.remove(s.root, b.when)
		s.shrinkRoot()
	}

	return value, nil
}

// PopBefore retrieves and removes all tasks scheduled strictly before t.
// The returned slice is ordered by time ascending, and, for tasks sharing
// the same timestamp, in the order they were added. If nothing qualifies
// it returns a nil slice and a nil error.
func (s *BTreeStorage[T]) PopBefore(t time.Time) ([]T, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, ErrClosed
	}

	var due []*bucket[T]
	collectBefore(s.root, t, &due)
	if len(due) == 0 {
		return nil, nil
	}

	out := make([]T, 0, len(due))
	for _, b := range due {
		for _, e := range b.entries {
			out = append(out, e.value)
			delete(s.index, e.id)
			s.size--
		}
	}

	for _, b := range due {
		// The root can collapse to a shorter tree after any deletion, so
		// it must be reassessed before each subsequent one.
		s.shrinkRoot()
		s.remove(s.root, b.when)
	}
	s.shrinkRoot()

	return out, nil
}

// shrinkRoot collapses an empty internal root down to its sole child,
// keeping the tree's height minimal after deletions.
func (s *BTreeStorage[T]) shrinkRoot() {
	if !s.root.leaf && len(s.root.buckets) == 0 {
		s.root = s.root.children[0]
	}
}

// collectBefore performs an in-order walk, appending every bucket keyed
// before t and stopping as soon as it finds one that isn't (everything to
// its right in a B-tree is >= it, so no further searching is needed).
func collectBefore[T any](n *node[T], t time.Time, out *[]*bucket[T]) {
	for i, b := range n.buckets {
		if !n.leaf {
			collectBefore(n.children[i], t, out)
		}
		if !b.when.Before(t) {
			return
		}
		*out = append(*out, b)
	}
	if !n.leaf {
		collectBefore(n.children[len(n.buckets)], t, out)
	}
}

// insertNonFull inserts e into the subtree rooted at n, which must not be
// full, splitting full children as it descends. It returns the bucket that
// now holds e (either newly created, or an existing one for the same time).
func (s *BTreeStorage[T]) insertNonFull(n *node[T], e *entry[T]) *bucket[T] {
	i := sort.Search(len(n.buckets), func(i int) bool {
		return !n.buckets[i].when.Before(e.when)
	})

	if i < len(n.buckets) && n.buckets[i].when.Equal(e.when) {
		n.buckets[i].entries = append(n.buckets[i].entries, e)
		return n.buckets[i]
	}

	if n.leaf {
		b := &bucket[T]{when: e.when, entries: []*entry[T]{e}}
		n.buckets = append(n.buckets, nil)
		copy(n.buckets[i+1:], n.buckets[i:])
		n.buckets[i] = b
		return b
	}

	t := s.degree
	if len(n.children[i].buckets) == 2*t-1 {
		s.splitChild(n, i)
		switch {
		case e.when.Equal(n.buckets[i].when):
			n.buckets[i].entries = append(n.buckets[i].entries, e)
			return n.buckets[i]
		case e.when.After(n.buckets[i].when):
			i++
		}
	}
	return s.insertNonFull(n.children[i], e)
}

// splitChild splits the full child at parent.children[i] into two nodes,
// promoting its median bucket up into parent at index i.
func (s *BTreeStorage[T]) splitChild(parent *node[T], i int) {
	t := s.degree
	child := parent.children[i]
	mid := child.buckets[t-1]

	right := &node[T]{leaf: child.leaf}
	right.buckets = append(right.buckets, child.buckets[t:]...)
	if !child.leaf {
		right.children = append(right.children, child.children[t:]...)
		child.children = child.children[:t]
	}
	child.buckets = child.buckets[:t-1]

	parent.children = append(parent.children, nil)
	copy(parent.children[i+2:], parent.children[i+1:])
	parent.children[i+1] = right

	parent.buckets = append(parent.buckets, nil)
	copy(parent.buckets[i+1:], parent.buckets[i:])
	parent.buckets[i] = mid
}

// remove deletes the key `when` from the subtree rooted at n. n must
// either be the root or already hold at least s.degree keys.
func (s *BTreeStorage[T]) remove(n *node[T], when time.Time) {
	t := s.degree
	idx := sort.Search(len(n.buckets), func(i int) bool {
		return !n.buckets[i].when.Before(when)
	})

	if idx < len(n.buckets) && n.buckets[idx].when.Equal(when) {
		if n.leaf {
			n.buckets = append(n.buckets[:idx], n.buckets[idx+1:]...)
			return
		}
		s.removeFromInternal(n, idx)
		return
	}

	if n.leaf {
		return // key not present in the tree
	}

	if len(n.children[idx].buckets) < t {
		idx = s.fill(n, idx)
	}
	s.remove(n.children[idx], when)
}

// removeFromInternal deletes the key found at n.buckets[idx], where n is
// not a leaf, replacing it with its predecessor or successor (borrowing
// from whichever adjacent child can spare a key) or merging the two
// children when neither can.
func (s *BTreeStorage[T]) removeFromInternal(n *node[T], idx int) {
	t := s.degree
	when := n.buckets[idx].when

	switch {
	case len(n.children[idx].buckets) >= t:
		pred := maxBucket(n.children[idx])
		n.buckets[idx] = pred
		s.remove(n.children[idx], pred.when)
	case len(n.children[idx+1].buckets) >= t:
		succ := minBucket(n.children[idx+1])
		n.buckets[idx] = succ
		s.remove(n.children[idx+1], succ.when)
	default:
		s.merge(n, idx)
		s.remove(n.children[idx], when)
	}
}

func maxBucket[T any](n *node[T]) *bucket[T] {
	for !n.leaf {
		n = n.children[len(n.children)-1]
	}
	return n.buckets[len(n.buckets)-1]
}

func minBucket[T any](n *node[T]) *bucket[T] {
	for !n.leaf {
		n = n.children[0]
	}
	return n.buckets[0]
}

// fill ensures n.children[idx] holds at least s.degree keys, borrowing from
// a sibling if one has spare keys or merging with one otherwise. It returns
// the index (possibly idx-1, if a merge folded idx into its left sibling)
// of the child the caller should now descend into.
func (s *BTreeStorage[T]) fill(n *node[T], idx int) int {
	t := s.degree
	switch {
	case idx != 0 && len(n.children[idx-1].buckets) >= t:
		s.borrowFromPrev(n, idx)
		return idx
	case idx != len(n.children)-1 && len(n.children[idx+1].buckets) >= t:
		s.borrowFromNext(n, idx)
		return idx
	case idx != len(n.children)-1:
		s.merge(n, idx)
		return idx
	default:
		s.merge(n, idx-1)
		return idx - 1
	}
}

// borrowFromPrev rotates one key from n.children[idx-1] through n into
// n.children[idx].
func (s *BTreeStorage[T]) borrowFromPrev(n *node[T], idx int) {
	child := n.children[idx]
	sibling := n.children[idx-1]

	child.buckets = append([]*bucket[T]{n.buckets[idx-1]}, child.buckets...)
	if !child.leaf {
		last := sibling.children[len(sibling.children)-1]
		child.children = append([]*node[T]{last}, child.children...)
		sibling.children = sibling.children[:len(sibling.children)-1]
	}
	n.buckets[idx-1] = sibling.buckets[len(sibling.buckets)-1]
	sibling.buckets = sibling.buckets[:len(sibling.buckets)-1]
}

// borrowFromNext rotates one key from n.children[idx+1] through n into
// n.children[idx].
func (s *BTreeStorage[T]) borrowFromNext(n *node[T], idx int) {
	child := n.children[idx]
	sibling := n.children[idx+1]

	child.buckets = append(child.buckets, n.buckets[idx])
	if !child.leaf {
		child.children = append(child.children, sibling.children[0])
		sibling.children = sibling.children[1:]
	}
	n.buckets[idx] = sibling.buckets[0]
	sibling.buckets = sibling.buckets[1:]
}

// merge folds n.buckets[idx], n.children[idx] and n.children[idx+1] into a
// single node stored back at n.children[idx].
func (s *BTreeStorage[T]) merge(n *node[T], idx int) {
	child := n.children[idx]
	sibling := n.children[idx+1]

	child.buckets = append(child.buckets, n.buckets[idx])
	child.buckets = append(child.buckets, sibling.buckets...)
	if !child.leaf {
		child.children = append(child.children, sibling.children...)
	}

	n.buckets = append(n.buckets[:idx], n.buckets[idx+1:]...)
	n.children = append(n.children[:idx+1], n.children[idx+2:]...)
}

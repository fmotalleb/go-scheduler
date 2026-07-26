package storage

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"
)

func mustStorage(t *testing.T, degree int) *BTreeStorage[string] {
	t.Helper()
	return NewBTreeStorageWithDegree[string](degree)
}

func at(seconds int64) time.Time {
	return time.Unix(seconds, 0).UTC()
}

func TestAddAndPopBeforeOrdering(t *testing.T) {
	s := mustStorage(t, 2) // small degree forces splits quickly
	want := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	order := []int64{5, 1, 8, 3, 7, 2, 6, 4} // shuffled times, values a..h

	for i, sec := range order {
		if _, err := s.Add(at(sec), want[i]); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	if got := s.Len(); got != len(want) {
		t.Fatalf("Len() = %d, want %d", got, len(want))
	}

	got, err := s.PopBefore(at(100))
	if err != nil {
		t.Fatalf("PopBefore: %v", err)
	}
	sortedWant := []string{"b", "f", "d", "h", "a", "g", "e", "c"} // by time 1..8
	if !equalSlices(got, sortedWant) {
		t.Fatalf("PopBefore order = %v, want %v", got, sortedWant)
	}
	if s.Len() != 0 {
		t.Fatalf("expected storage empty after PopBefore, Len() = %d", s.Len())
	}
}

func TestPopBeforeIsExclusiveAndPartial(t *testing.T) {
	s := mustStorage(t, 3)
	idEarly, _ := s.Add(at(10), "early")
	_, _ = s.Add(at(20), "boundary")
	idLate, _ := s.Add(at(30), "late")

	got, err := s.PopBefore(at(20))
	if err != nil {
		t.Fatalf("PopBefore: %v", err)
	}
	if !equalSlices(got, []string{"early"}) {
		t.Fatalf("got %v, want [early]", got)
	}
	if s.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", s.Len())
	}

	// idEarly was consumed by PopBefore, so removing it again must fail.
	if _, err := s.Remove(idEarly); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Remove(consumed id) error = %v, want ErrNotFound", err)
	}

	v, err := s.Remove(idLate)
	if err != nil || v != "late" {
		t.Fatalf("Remove(idLate) = %q, %v, want late, nil", v, err)
	}
}

func TestRemoveUnknownID(t *testing.T) {
	s := mustStorage(t, 4)
	_, _ = s.Add(at(1), "x")
	if _, err := s.Remove(9999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Remove(unknown) error = %v, want ErrNotFound", err)
	}
}

func TestSameTimestampMultipleEntries(t *testing.T) {
	s := mustStorage(t, 2)
	ts := at(42)
	idA, _ := s.Add(ts, "a")
	idB, _ := s.Add(ts, "b")
	idC, _ := s.Add(ts, "c")

	v, err := s.Remove(idB)
	if err != nil || v != "b" {
		t.Fatalf("Remove(idB) = %q, %v", v, err)
	}
	if s.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", s.Len())
	}

	got, err := s.PopBefore(at(43))
	if err != nil {
		t.Fatalf("PopBefore: %v", err)
	}
	if !equalSlices(got, []string{"a", "c"}) {
		t.Fatalf("got %v, want [a c]", got)
	}
	_ = idA
	_ = idC
}

func TestClose(t *testing.T) {
	s := mustStorage(t, 3)
	id, _ := s.Add(at(1), "x")

	s.Close()

	if _, err := s.Add(at(2), "y"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Add after Close error = %v, want ErrClosed", err)
	}
	if _, err := s.Remove(id); !errors.Is(err, ErrClosed) {
		t.Fatalf("Remove after Close error = %v, want ErrClosed", err)
	}
	if _, err := s.PopBefore(at(100)); !errors.Is(err, ErrClosed) {
		t.Fatalf("PopBefore after Close error = %v, want ErrClosed", err)
	}

	// Calling Close again must not panic.
	s.Close()
}

// TestRandomizedAgainstModel drives the storage with a long randomized
// sequence of Add/Remove/PopBefore calls and cross-checks every result
// against a naive slice-based model, across a range of degrees so both
// split and merge/borrow code paths get exercised.
func TestRandomizedAgainstModel(t *testing.T) {
	for _, degree := range []int{2, 3, 4, 8} {
		degree := degree
		t.Run(fmt.Sprintf("degree=%d", degree), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(degree) + 1))
			s := mustStorage(t, degree)

			var model []*modelEntry

			const ops = 4000
			for i := 0; i < ops; i++ {
				switch rng.Intn(3) {
				case 0: // Add
					sec := rng.Int63n(500)
					val := fmt.Sprintf("v%d", i)
					id, err := s.Add(at(sec), val)
					if err != nil {
						t.Fatalf("Add: %v", err)
					}
					model = append(model, &modelEntry{id: id, when: at(sec), value: val, live: true})

				case 1: // Remove a random live entry (if any)
					live := liveEntries(model)
					if len(live) == 0 {
						continue
					}
					target := live[rng.Intn(len(live))]
					v, err := s.Remove(target.id)
					if err != nil {
						t.Fatalf("Remove(%d): unexpected error %v", target.id, err)
					}
					if v != target.value {
						t.Fatalf("Remove(%d) = %q, want %q", target.id, v, target.value)
					}
					target.live = false

				case 2: // PopBefore
					sec := rng.Int63n(500)
					threshold := at(sec)

					var wantEntries []*modelEntry
					for _, e := range model {
						if e.live && e.when.Before(threshold) {
							wantEntries = append(wantEntries, e)
						}
					}
					sort.SliceStable(wantEntries, func(a, b int) bool {
						return wantEntries[a].when.Before(wantEntries[b].when)
					})
					want := make([]string, len(wantEntries))
					for i, e := range wantEntries {
						want[i] = e.value
						e.live = false
					}

					got, err := s.PopBefore(threshold)
					if err != nil {
						t.Fatalf("PopBefore: %v", err)
					}
					if !equalSlices(got, want) {
						t.Fatalf("PopBefore(%v) = %v, want %v", threshold, got, want)
					}
				}
			}

			wantLen := len(liveEntries(model))
			if got := s.Len(); got != wantLen {
				t.Fatalf("final Len() = %d, want %d", got, wantLen)
			}

			// Drain everything and confirm it matches the remaining live set,
			// ordered by time.
			live := liveEntries(model)
			sort.SliceStable(live, func(a, b int) bool { return live[a].when.Before(live[b].when) })
			want := make([]string, len(live))
			for i, e := range live {
				want[i] = e.value
			}
			got, err := s.PopBefore(at(100000))
			if err != nil {
				t.Fatalf("final PopBefore: %v", err)
			}
			if !equalSlices(got, want) {
				t.Fatalf("final drain = %v, want %v", got, want)
			}
			if s.Len() != 0 {
				t.Fatalf("Len() after full drain = %d, want 0", s.Len())
			}
		})
	}
}

type modelEntry struct {
	id    int
	when  time.Time
	value string
	live  bool
}

func liveEntries(model []*modelEntry) []*modelEntry {
	var out []*modelEntry
	for _, e := range model {
		if e.live {
			out = append(out, e)
		}
	}
	return out
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

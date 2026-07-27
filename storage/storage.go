package storage

import "errors"

// ErrNotFound is returned by Remove when no entry with the given id exists.
var ErrNotFound = errors.New("scheduler: id not found")

// ErrClosed is returned by Add, Remove, and PopBefore once Close has been
// called on the storage.
var ErrClosed = errors.New("scheduler: storage closed")

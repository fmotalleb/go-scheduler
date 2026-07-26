# go-scheduler

[![Go Reference](https://pkg.go.dev/badge/github.com/fmotalleb/go-scheduler.svg)](https://pkg.go.dev/github.com/fmotalleb/go-scheduler)
[![Go Version](https://img.shields.io/github/go-mod/go-version/fmotalleb/go-scheduler)](https://go.dev/)
[![License](https://img.shields.io/github/license/fmotalleb/go-scheduler)](LICENSE)

A **generic, extensible task scheduling engine** for Go. Schedule arbitrary
values of any type `T` to be processed at a future time, with pluggable storage
back-ends and execution workers.

---

## Features

- **Generic** — works with any task type (`Scheduler[T]`).
- **Pluggable storage** — swap in-memory storage for Redis, Postgres, etc. by
  implementing the `Storage[T]` interface.
- **Pluggable workers** — synchronous execution, goroutine pool, or your own
  `Worker[T]` implementation.
- **Functional options** — clean, composable configuration via `Option[T]`.
- **Panic-safe** — the scheduler recovers from panics during tick processing
  and reports them via the configured logger.
- **Context-aware** — lifecycle is tied to a `context.Context`; cancellation
  shuts everything down cleanly.
- **Zero external dependencies in user code** — only `google/btree` is used
  internally by the included memory storage.

---

## Installation

```bash
go get github.com/fmotalleb/go-scheduler
```

Requires Go 1.25+.

---

## Quick start

### Minimal example with Callback

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/fmotalleb/go-scheduler"
)

func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    s := scheduler.NewCallback(ctx,
        scheduler.WithTickerCycle(100*time.Millisecond),
    )

    s.Add(time.Now().Add(1*time.Second), func(ctx context.Context) {
        fmt.Println("Hello, world!")
    })

    s.Add(time.Now().Add(2*time.Second), func(ctx context.Context) {
        fmt.Println("Goodbye, world!")
    })

    // Let the scheduler run for 3 seconds, then exit.
    <-time.After(3 * time.Second)
}
```

### Custom task type

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/fmotalleb/go-scheduler"
    "github.com/fmotalleb/go-scheduler/worker"
)

type Email struct {
    To   string
    Body string
}

func main() {
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    w := worker.NewSync(ctx, func(ctx context.Context, e Email) {
        fmt.Printf("Sending email to %s: %s\n", e.To, e.Body)
    })

    s := scheduler.New(ctx, w,
        scheduler.WithTickerCycle(200*time.Millisecond),
    )

    s.Add(time.Now().Add(5*time.Second), Email{"alice@example.com", "Hi!"})
    s.Add(time.Now().Add(10*time.Second), Email{"bob@example.com", "Hello!"})

    <-time.After(15 * time.Second)
}
```

---

## Architecture

```
┌─────────────────────────────────────────────┐
│               Scheduler[T]                   │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐ │
│  │ Ticker   │──▶│ runCycle │──▶│ Worker[T]│ │
│  └──────────┘   │          │   └──────────┘ │
│                 │  ┌──────┐│                 │
│                 │  │Storage││                 │
│                 │  │ [T]   ││                 │
│                 │  └──────┘│                 │
│                 └──────────┘                 │
└─────────────────────────────────────────────┘
```

1. A `time.Ticker` fires at a configurable interval (default: 1 second).
2. On each tick, `runCycle` calls `Storage.PopBefore(time.Now())` to retrieve
   all items whose scheduled time has passed.
3. Each ready item is passed to `Worker.Submit()` for processing.
4. The scheduler logs any errors via a pluggable `LogFn`.

---

## API reference

### Creating a scheduler

```go
func New[T any](ctx context.Context, worker Worker[T], opts ...Option[T]) *Scheduler[T]
func NewCallback(ctx context.Context, opts ...Option[Callback]) *Scheduler[Callback]
```

- `New` — generic constructor; requires a `Worker[T]` implementation.
- `NewCallback` — convenience constructor for `func(context.Context)` tasks.
  Creates a default synchronous worker internally if none is provided via
  options.

Both functions start the scheduler loop in a background goroutine immediately.

### Adding and removing tasks

```go
func (s *Scheduler[T]) Add(when time.Time, task T) (id int, err error)
func (s *Scheduler[T]) Remove(id int) (T, error)
```

- `Add` stores a task to be run at (or after) the given time. Returns a unique
  ID that can be used to cancel the task later.
- `Remove` cancels a pending task by ID, returning the original value. Returns
  an error if the ID does not exist.

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `WithTickerCycle(d time.Duration)` | How often to check for due tasks | 1 second |
| `WithStorage(s Storage[T])` | Custom storage backend | `MemoryStorage(degree=8)` |
| `WithWorker(w Worker[T])` | Custom worker | — |
| `WithWorkerPool(ctx, handler, workers, queueSize)` | Create a goroutine pool worker inline | — |
| `WithLogger(fn LogFn)` | Error/hook callback | No-op |

### Worker implementations

**`worker.Sync[T]`** — processes tasks synchronously, one at a time, on the
scheduler's goroutine. Useful for simple, ordered execution.

```go
import "github.com/fmotalleb/go-scheduler/worker"

w := worker.NewSync(ctx, func(ctx context.Context, task MyType) {
    // handle task
})
```

**`worker.WorkerPool[T]`** — fans tasks out to a fixed pool of goroutines.
Tasks are submitted to a buffered channel and picked up by available workers.

```go
w := worker.NewWorkerPool(ctx, func(ctx context.Context, task MyType) {
    // handle task
}, workers=10, queueSize=100)

// Or via WithWorkerPool option:
s := scheduler.New(ctx, nil,
    scheduler.WithWorkerPool(ctx, handler, 10, 100),
)
```

### Storage

**`storage.MemoryStorage[T]`** — an in-memory store backed by a B-tree from
[google/btree](https://github.com/google/btree). Tasks are ordered by their
scheduled time (and insertion order for identical timestamps). Data is lost
on process exit.

```go
import "github.com/fmotalleb/go-scheduler/storage"

store := storage.NewMemoryStorage[MyType](8)   // degree ≥ 2; defaults to 8

s := scheduler.New(ctx, myWorker,
    scheduler.WithStorage(store),
)
```

To persist tasks across restarts, implement the `Storage[T]` interface:

```go
type Storage[T any] interface {
    Add(time.Time, T) (int, error)
    Remove(int) (T, error)
    PopBefore(time.Time) ([]T, error)
    Close()
}
```

### Logging errors

```go
s := scheduler.New(ctx, worker,
    scheduler.WithLogger(func(err error) {
        log.Printf("scheduler error: %v", err)
    }),
)
```

---

## Advanced examples

### Fan-out with WorkerPool

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

handler := func(ctx context.Context, url string) {
    resp, err := http.Get(url)
    if err != nil {
        log.Printf("GET %s: %v", url, err)
        return
    }
    resp.Body.Close()
    log.Printf("GET %s: %s", url, resp.Status)
}

s := scheduler.New(ctx, nil,
    scheduler.WithTickerCycle(500*time.Millisecond),
    scheduler.WithWorkerPool(ctx, handler, 20, 200),
)

// Schedule 100 URL checks spread over the next hour.
for i := 0; i < 100; i++ {
    s.Add(time.Now().Add(time.Duration(i)*36*time.Second),
        fmt.Sprintf("https://example.com/check/%d", i))
}
```

### Custom persistent storage

Implement `Storage[T]` with your database of choice:

```go
import "github.com/fmotalleb/go-scheduler/storage"

type PostgresStorage[T any] struct {
    db *sql.DB
    // JSON-encode T into a jsonb column, etc.
}

func (p *PostgresStorage[T]) Add(when time.Time, v T) (int, error) {
    // INSERT INTO tasks (run_at, payload) VALUES ($1, $2) RETURNING id
}

func (p *PostgresStorage[T]) Remove(id int) (T, error) {
    // DELETE FROM tasks WHERE id = $1 RETURNING payload
}

func (p *PostgresStorage[T]) PopBefore(t time.Time) ([]T, error) {
    // SELECT id, payload FROM tasks WHERE run_at <= $1 ORDER BY run_at, id
    // DELETE FROM tasks WHERE id IN (...)
}

func (p *PostgresStorage[T]) Close() {
    p.db.Close()
}
```

---

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

- Ensure code passes `golangci-lint run`.
- Add tests for new functionality.
- Update documentation if the public API changes.

---

## License

[MIT](LICENSE)

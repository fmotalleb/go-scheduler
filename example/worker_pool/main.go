// Example worker_pool demonstrates using WorkerPool for concurrent task
// processing. Tasks are fanned out across multiple goroutines, which is
// useful for I/O-bound or CPU-bound workloads that benefit from parallelism.
package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fmotalleb/go-scheduler"
	"github.com/fmotalleb/go-scheduler/worker"
)

// BatchJob represents a unit of work to be processed by the pool.
type BatchJob struct {
	ID      int
	Payload string
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		completed atomic.Int64
		mu        sync.Mutex
		results   []string
	)

	// Create a worker pool with 4 concurrent goroutines and a queue of 100.
	// Tasks submitted to the pool are dispatched to idle workers.
	pool := worker.NewWorkerPool(ctx,
		func(ctx context.Context, job BatchJob) {
			// Simulate some work (e.g., API call, file processing).
			time.Sleep(50 * time.Millisecond)

			completed.Add(1)
			mu.Lock()
			results = append(results, fmt.Sprintf("job_%d:%s", job.ID, job.Payload))
			mu.Unlock()

			fmt.Printf("⚙️  Worker processed job %d: %s\n", job.ID, job.Payload)
		},
		4,   // workers
		100, // queue size
	)

	// The WithWorkerPool option is a shorthand that creates the pool and sets
	// it as the scheduler's worker in one call.
	s := scheduler.New(ctx, pool,
		scheduler.WithTickerCycle[BatchJob](100*time.Millisecond),
	)
	defer s.Close()

	now := time.Now()

	// Schedule 20 jobs to fire at staggered times.
	for i := 1; i <= 20; i++ {
		job := BatchJob{
			ID:      i,
			Payload: fmt.Sprintf("data_%d", i),
		}
		delay := time.Duration(i%5) * 300 * time.Millisecond
		id, err := s.Add(now.Add(delay), job)
		if err != nil {
			log.Fatalf("failed to schedule job %d: %v", i, err)
		}
		fmt.Printf("📋 Scheduled job %d with id=%d (delay=%v)\n", i, id, delay)
	}

	// Wait for all jobs to be processed, then shut down.
	// Close triggers worker shutdown and waits for in-flight tasks.
	time.Sleep(3 * time.Second)
	s.Close()

	fmt.Printf("\n📊 Results: %d jobs completed out of 20\n", completed.Load())
	mu.Lock()
	fmt.Printf("   Outputs: %v\n", results)
	mu.Unlock()
	fmt.Println("✅ Worker pool example finished")
}

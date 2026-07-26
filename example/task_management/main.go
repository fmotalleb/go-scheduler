// Example task_management demonstrates the full scheduler lifecycle:
// adding tasks, removing/cancelling them, handling errors via the logger,
// and clean shutdown.
package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/fmotalleb/go-scheduler"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		mu       sync.Mutex
		executed []string
	)

	// Collect any internal scheduler errors.
	var errMu sync.Mutex
	var schedulerErrors []error

	logger := func(err error) {
		errMu.Lock()
		schedulerErrors = append(schedulerErrors, err)
		errMu.Unlock()
		fmt.Printf("⚠️  Scheduler error: %v\n", err)
	}

	// Create a scheduler with a fast tick cycle and an error logger.
	s := scheduler.NewCallback(ctx,
		scheduler.WithTickerCycle[func(context.Context)](150*time.Millisecond),
		scheduler.WithLogger[func(context.Context)](logger),
	)
	defer s.Close()

	now := time.Now()

	// --- Add tasks ---
	id1, _ := s.Add(now.Add(300*time.Millisecond), func(ctx context.Context) {
		mu.Lock()
		executed = append(executed, "task_1")
		mu.Unlock()
		fmt.Println("  ✅ Task 1 executed")
	})
	fmt.Printf("📋 Added task 1: id=%d\n", id1)

	id2, _ := s.Add(now.Add(600*time.Millisecond), func(ctx context.Context) {
		mu.Lock()
		executed = append(executed, "task_2")
		mu.Unlock()
		fmt.Println("  ✅ Task 2 executed")
	})
	fmt.Printf("📋 Added task 2: id=%d\n", id2)

	id3, _ := s.Add(now.Add(900*time.Millisecond), func(ctx context.Context) {
		mu.Lock()
		executed = append(executed, "task_3")
		mu.Unlock()
		fmt.Println("  ✅ Task 3 executed")
	})
	fmt.Printf("📋 Added task 3: id=%d\n", id3)

	// --- Cancel a task before it fires ---
	removed, err := s.Remove(id2)
	if err != nil {
		log.Fatalf("failed to remove task 2: %v", err)
	}
	fmt.Printf("✂️  Cancelled task 2 (id=%d) before it could fire, got callback=%v\n", id2, removed != nil)

	// --- Add a replacement task ---
	id4, _ := s.Add(now.Add(700*time.Millisecond), func(ctx context.Context) {
		mu.Lock()
		executed = append(executed, "task_4_replacement")
		mu.Unlock()
		fmt.Println("  ✅ Task 4 (replacement) executed")
	})
	fmt.Printf("📋 Added replacement task 4: id=%d\n", id4)

	// --- Wait for all tasks ---
	time.Sleep(1500 * time.Millisecond)

	// --- Report results ---
	mu.Lock()
	fmt.Printf("\n📊 Executed tasks: %v\n", executed)
	mu.Unlock()

	errMu.Lock()
	if len(schedulerErrors) > 0 {
		fmt.Printf("⚠️  Scheduler encountered %d error(s)\n", len(schedulerErrors))
	} else {
		fmt.Println("✅ No scheduler errors")
	}
	errMu.Unlock()

	fmt.Println("✅ Task management example finished")
}

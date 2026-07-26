// Example basic demonstrates the simplest way to use the scheduler with
// the NewCallback constructor, which accepts context-aware functions directly.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/fmotalleb/go-scheduler"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a scheduler that checks for due tasks every 200ms.
	// NewCallback automatically creates a synchronous worker internally.
	s := scheduler.NewCallback(ctx,
		scheduler.WithTickerCycle[func(context.Context)](200*time.Millisecond),
	)
	defer s.Close()

	now := time.Now()

	// Schedule a task for 500ms from now.
	id1, err := s.Add(now.Add(500*time.Millisecond), func(ctx context.Context) {
		fmt.Println("🔥 Task 1 fired (500ms delay)")
	})
	if err != nil {
		log.Fatalf("failed to add task 1: %v", err)
	}
	fmt.Printf("📋 Added task 1 with id=%d\n", id1)

	// Schedule another task for 1.2s from now.
	id2, err := s.Add(now.Add(1200*time.Millisecond), func(ctx context.Context) {
		fmt.Println("🚀 Task 2 fired (1.2s delay)")
	})
	if err != nil {
		log.Fatalf("failed to add task 2: %v", err)
	}
	fmt.Printf("📋 Added task 2 with id=%d\n", id2)

	// Let the scheduler run for a bit, then cancel.
	time.Sleep(2 * time.Second)
	fmt.Println("✅ Done — all expected tasks have fired")
}

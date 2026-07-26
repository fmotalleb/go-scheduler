// Example custom_type demonstrates using the generic New constructor with a
// custom task type and a synchronous worker. This pattern is useful when your
// tasks carry structured data beyond a simple callback.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/fmotalleb/go-scheduler"
	"github.com/fmotalleb/go-scheduler/worker"
)

// EmailTask carries all the data needed to send one email.
type EmailTask struct {
	To      string
	Subject string
	Body    string
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a synchronous worker that "sends" each email.
	w := worker.NewSync(ctx, func(ctx context.Context, task EmailTask) {
		fmt.Printf("📧 Sending email to %s:\n", task.To)
		fmt.Printf("   Subject: %s\n", task.Subject)
		fmt.Printf("   Body:    %s\n", task.Body)
		fmt.Println()
	})

	// Create the scheduler with a fast tick cycle for demonstration.
	s := scheduler.New(ctx, w,
		scheduler.WithTickerCycle[EmailTask](300*time.Millisecond),
	)
	defer s.Close()

	now := time.Now()

	emails := []EmailTask{
		{To: "alice@example.com", Subject: "Welcome!", Body: "Hi Alice, welcome aboard!"},
		{To: "bob@example.com", Subject: "Reminder", Body: "Bob, your meeting is in 1 hour."},
		{To: "carol@example.com", Subject: "Invoice", Body: "Carol, your invoice is attached."},
	}

	for i, email := range emails {
		id, err := s.Add(now.Add(time.Duration(i)*500*time.Millisecond), email)
		if err != nil {
			log.Fatalf("failed to schedule email %d: %v", i, err)
		}
		fmt.Printf("📋 Scheduled email %d with id=%d\n", i+1, id)
	}

	// Let the scheduler process all emails.
	time.Sleep(2 * time.Second)
	fmt.Println("✅ All emails processed")
}

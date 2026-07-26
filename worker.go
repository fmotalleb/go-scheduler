package scheduler

// Worker defines the interface for processing scheduled tasks.
//
// Implementations receive items from the scheduler and are responsible for
// executing or dispatching them. The scheduler calls Submit for each due
// item and Close during shutdown.
type Worker[T any] interface {
	// Submit hands a task to the worker for processing.
	// The worker may process it synchronously or enqueue it for later.
	Submit(T) error

	// Close signals the worker to shut down and release any resources.
	Close()
}

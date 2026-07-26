package scheduler

type Worker[T any] interface {
	Submit(T) error
	Close()
}

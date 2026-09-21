package worker

import (
	"context"
	"log"
	"time"
)

const consumerErrorDelay = time.Second

type Consumer interface {
	ConsumeOnce(ctx context.Context) (int, error)
}

type ConsumerWorker struct {
	consumer Consumer
}

func NewConsumerWorker(consumer Consumer) *ConsumerWorker {
	return &ConsumerWorker{
		consumer: consumer,
	}
}

func (w *ConsumerWorker) Run(ctx context.Context) {
	log.Println("SQS consumer worker started")

	for {
		if ctx.Err() != nil {
			log.Println("SQS consumer worker stopped")
			return
		}

		processed, err := w.consumer.ConsumeOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("SQS consumer worker stopped")
				return
			}

			log.Printf(
				"SQS consumer worker error: %v",
				err,
			)

			if !wait(ctx, consumerErrorDelay) {
				log.Println("SQS consumer worker stopped")
				return
			}

			continue
		}

		if processed > 0 {
			log.Printf(
				"SQS consumer processed %d message(s)",
				processed,
			)
		}
	}
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

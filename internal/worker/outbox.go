package worker

import (
	"context"
	"log"
	"time"
)

const (
	outboxIdleDelay  = 500 * time.Millisecond
	outboxErrorDelay = time.Second
)

type OutboxPublisher interface {
	PublishBatch(ctx context.Context) (int, error)
}

type OutboxWorker struct {
	publisher OutboxPublisher
}

func NewOutboxWorker(
	publisher OutboxPublisher,
) *OutboxWorker {
	return &OutboxWorker{
		publisher: publisher,
	}
}

func (w *OutboxWorker) Run(ctx context.Context) {
	log.Println("outbox publisher worker started")

	for {
		if ctx.Err() != nil {
			log.Println("outbox publisher worker stopped")
			return
		}

		published, err := w.publisher.PublishBatch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("outbox publisher worker stopped")
				return
			}

			log.Printf(
				"outbox publisher worker error: %v",
				err,
			)

			if !wait(ctx, outboxErrorDelay) {
				log.Println("outbox publisher worker stopped")
				return
			}

			continue
		}

		// If there was work, immediately ask for another batch.
		// This drains a backlog without an unnecessary delay.
		if published > 0 {
			log.Printf(
				"outbox publisher published %d event(s)",
				published,
			)
			continue
		}

		if !wait(ctx, outboxIdleDelay) {
			log.Println("outbox publisher worker stopped")
			return
		}
	}
}

package worker

import (
	"context"
	"log/slog"
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
	logger    *slog.Logger
}

func NewOutboxWorker(
	publisher OutboxPublisher,
	logger *slog.Logger,
) *OutboxWorker {
	return &OutboxWorker{
		publisher: publisher,
		logger: logger.With(
			slog.String("component", "outbox_publisher"),
		),
	}
}

func (w *OutboxWorker) Run(ctx context.Context) {
	w.logger.Info("worker started")

	for {
		if ctx.Err() != nil {
			w.logger.Info("worker stopped")
			return
		}

		published, err := w.publisher.PublishBatch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				w.logger.Info("worker stopped")
				return
			}

			w.logger.Error(
				"outbox publish failed",
				slog.Any("error", err),
			)

			if !wait(ctx, outboxErrorDelay) {
				w.logger.Info("worker stopped")
				return
			}

			continue
		}

		// If there was work, immediately ask for another batch.
		// This drains a backlog without an unnecessary delay.
		if published > 0 {
			w.logger.Info(
				"outbox events published",
				slog.Int("count", published),
			)
			continue
		}

		if !wait(ctx, outboxIdleDelay) {
			w.logger.Info("worker stopped")
			return
		}
	}
}

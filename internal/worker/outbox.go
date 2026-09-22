package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/Tharik/wagering-platform/internal/observability"
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
	metrics   *observability.Metrics
}

func NewOutboxWorker(
	publisher OutboxPublisher,
	logger *slog.Logger,
	metrics *observability.Metrics,
) *OutboxWorker {
	return &OutboxWorker{
		publisher: publisher,
		logger: logger.With(
			slog.String("component", "outbox_publisher"),
		),
		metrics: metrics,
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

			w.metrics.IncOutboxErrors()

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

		if published > 0 {
			w.metrics.IncOutboxEventsPublished(uint64(published))

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

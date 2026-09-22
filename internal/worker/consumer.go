package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/Tharik/wagering-platform/internal/observability"
)

const consumerErrorDelay = time.Second

type Consumer interface {
	ConsumeOnce(ctx context.Context) (int, error)
}

type ConsumerWorker struct {
	consumer Consumer
	logger   *slog.Logger
	metrics  *observability.Metrics
}

func NewConsumerWorker(
	consumer Consumer,
	logger *slog.Logger,
	metrics *observability.Metrics,
) *ConsumerWorker {
	return &ConsumerWorker{
		consumer: consumer,
		logger: logger.With(
			slog.String("component", "sqs_consumer"),
		),
		metrics: metrics,
	}
}

func (w *ConsumerWorker) Run(ctx context.Context) {
	w.logger.Info("worker started")

	for {
		if ctx.Err() != nil {
			w.logger.Info("worker stopped")
			return
		}

		processed, err := w.consumer.ConsumeOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				w.logger.Info("worker stopped")
				return
			}

			w.metrics.IncSQSErrors()

			w.logger.Error(
				"message consumption failed",
				slog.Any("error", err),
			)

			if !wait(ctx, consumerErrorDelay) {
				w.logger.Info("worker stopped")
				return
			}

			continue
		}

		if processed > 0 {
			w.metrics.IncSQSMessagesProcessed(uint64(processed))

			w.logger.Info(
				"messages processed",
				slog.Int("count", processed),
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

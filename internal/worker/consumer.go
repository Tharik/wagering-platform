package worker

import (
	"context"
	"log/slog"
	"time"
)

const consumerErrorDelay = time.Second

type Consumer interface {
	ConsumeOnce(ctx context.Context) (int, error)
}

type ConsumerWorker struct {
	consumer Consumer
	logger   *slog.Logger
}

func NewConsumerWorker(
	consumer Consumer,
	logger *slog.Logger,
) *ConsumerWorker {
	return &ConsumerWorker{
		consumer: consumer,
		logger: logger.With(
			slog.String("component", "sqs_consumer"),
		),
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

package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/Tharik/wagering-platform/internal/observability"
)

const (
	consumerErrorDelay   = time.Second
	consumerDrainTimeout = 10 * time.Second
)

type Consumer interface {
	ConsumeOnceWithContexts(
		receiveCtx context.Context,
		processingCtx context.Context,
	) (int, error)
}

type ConsumerWorker struct {
	consumer         Consumer
	logger           *slog.Logger
	metrics          *observability.Metrics
	drainTimeout     time.Duration
	shutdownContexts chan context.Context
}

func NewConsumerWorker(
	consumer Consumer,
	logger *slog.Logger,
	metrics *observability.Metrics,
) *ConsumerWorker {
	return &ConsumerWorker{
		consumer:         consumer,
		metrics:          metrics,
		drainTimeout:     consumerDrainTimeout,
		shutdownContexts: make(chan context.Context, 1),
		logger: logger.With(
			slog.String("component", "sqs_consumer"),
		),
	}
}

func newConsumerWorkerWithDrainTimeout(
	consumer Consumer,
	logger *slog.Logger,
	metrics *observability.Metrics,
	drainTimeout time.Duration,
) *ConsumerWorker {
	worker := NewConsumerWorker(consumer, logger, metrics)
	worker.drainTimeout = drainTimeout
	return worker
}

func (w *ConsumerWorker) PrepareShutdown(ctx context.Context) {
	select {
	case w.shutdownContexts <- ctx:
	default:
	}
}

func (w *ConsumerWorker) Run(ctx context.Context) {
	w.logger.Info("worker started")

	for {
		if ctx.Err() != nil {
			w.logger.Info("worker stopped")
			return
		}

		processingCtx, cancelProcessing := context.WithCancel(
			context.WithoutCancel(ctx),
		)
		operationDone := make(chan struct{})
		watcherDone := make(chan struct{})

		go func() {
			defer close(watcherDone)
			w.watchForShutdown(
				ctx,
				operationDone,
				cancelProcessing,
			)
		}()

		processed, err := w.consumer.ConsumeOnceWithContexts(
			ctx,
			processingCtx,
		)
		close(operationDone)
		<-watcherDone
		cancelProcessing()

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

func (w *ConsumerWorker) watchForShutdown(
	receiveCtx context.Context,
	operationDone <-chan struct{},
	cancelProcessing context.CancelFunc,
) {
	select {
	case <-operationDone:
		return
	case <-receiveCtx.Done():
	}

	w.logger.Info("waiting for in-flight SQS operation")

	shutdownCtx := w.shutdownContext()
	drainDuration := w.drainTimeout
	if deadline, ok := shutdownCtx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining < drainDuration {
			drainDuration = remaining
		}
	}
	if drainDuration < 0 {
		drainDuration = 0
	}

	timer := time.NewTimer(drainDuration)
	defer timer.Stop()

	select {
	case <-operationDone:
		w.logger.Info("SQS operation drained")
		return
	case <-timer.C:
		w.logger.Warn("SQS drain timeout reached")
	case <-shutdownCtx.Done():
		w.logger.Warn("SQS shutdown deadline reached")
	}

	cancelProcessing()
	// Production database and AWS calls observe processingCtx and should
	// return promptly. Go cannot forcibly stop a synchronous function that
	// ignores context cancellation after the drain deadline.

	select {
	case <-operationDone:
		w.logger.Info("SQS operation stopped after cancellation")
	case <-shutdownCtx.Done():
	}
}

func (w *ConsumerWorker) shutdownContext() context.Context {
	select {
	case ctx := <-w.shutdownContexts:
		return ctx
	default:
		return context.Background()
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

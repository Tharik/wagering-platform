package worker

import (
	"context"
	"log/slog"
	"time"
)

const (
	pendingReferenceIdleDelay  = 500 * time.Millisecond
	pendingReferenceErrorDelay = time.Second
)

type PendingReferenceResolver interface {
	ResolveOne(ctx context.Context) (bool, error)
}

type PendingReferenceWorker struct {
	resolver PendingReferenceResolver
	logger   *slog.Logger
}

func NewPendingReferenceWorker(
	resolver PendingReferenceResolver,
	logger *slog.Logger,
) *PendingReferenceWorker {
	return &PendingReferenceWorker{
		resolver: resolver,
		logger: logger.With(
			slog.String("component", "pending_reference_resolver"),
		),
	}
}

func (w *PendingReferenceWorker) Run(ctx context.Context) {
	w.logger.Info("worker started")

	for {
		if ctx.Err() != nil {
			w.logger.Info("worker stopped")
			return
		}

		handled, err := w.resolver.ResolveOne(ctx)
		if err != nil {
			if ctx.Err() != nil {
				w.logger.Info("worker stopped")
				return
			}

			w.logger.Error(
				"pending reference resolution failed",
				slog.Any("error", err),
			)

			if !wait(ctx, pendingReferenceErrorDelay) {
				w.logger.Info("worker stopped")
				return
			}

			continue
		}

		if handled {
			// There may be more due work. Process the next one immediately.
			continue
		}

		if !wait(ctx, pendingReferenceIdleDelay) {
			w.logger.Info("worker stopped")
			return
		}
	}
}

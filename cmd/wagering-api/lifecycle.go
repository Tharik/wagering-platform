package main

import (
	"context"
	"sync"

	"github.com/Tharik/wagering-platform/internal/infrastructure/httpapi"
	messagingsqs "github.com/Tharik/wagering-platform/internal/infrastructure/messaging/sqs"
	"github.com/Tharik/wagering-platform/internal/worker"
	"go.uber.org/fx"
)

func registerLifecycle(
	lifecycle fx.Lifecycle,
	consumerWorker *worker.ConsumerWorker,
	outboxWorker *worker.OutboxWorker,
	pendingReferenceWorker *worker.PendingReferenceWorker,
	dlqMonitor *messagingsqs.DLQMonitor,
	httpServer *httpapi.Server,
) {
	var cancel context.CancelFunc
	var workers sync.WaitGroup

	lifecycle.Append(
		fx.Hook{
			OnStart: func(ctx context.Context) error {
				workerContext, workerCancel :=
					context.WithCancel(context.Background())

				cancel = workerCancel

				if err := httpServer.Start(); err != nil {
					workerCancel()
					return err
				}

				workers.Add(4)

				go func() {
					defer workers.Done()
					consumerWorker.Run(workerContext)
				}()

				go func() {
					defer workers.Done()
					outboxWorker.Run(workerContext)
				}()

				go func() {
					defer workers.Done()
					pendingReferenceWorker.Run(workerContext)
				}()

				go func() {
					defer workers.Done()
					dlqMonitor.Run(workerContext)
				}()

				return nil
			},

			OnStop: func(ctx context.Context) error {
				consumerWorker.PrepareShutdown(ctx)

				if cancel != nil {
					cancel()
				}

				workersStopped := make(chan struct{})

				go func() {
					workers.Wait()
					close(workersStopped)
				}()

				select {
				case <-workersStopped:
				case <-ctx.Done():
					return ctx.Err()
				}

				if err := httpServer.Shutdown(ctx); err != nil {
					return err
				}

				return nil
			},
		},
	)
}

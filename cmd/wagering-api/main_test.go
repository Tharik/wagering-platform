package main

import (
	"context"
	"testing"
	"time"

	"github.com/Tharik/wagering-platform/internal/application/wagering"
	"github.com/Tharik/wagering-platform/internal/application/wallet"
	"github.com/Tharik/wagering-platform/internal/infrastructure/httpapi"
	"github.com/Tharik/wagering-platform/internal/observability"
	"go.uber.org/fx"
)

func TestFxCompositionRootStartsAndStops(t *testing.T) {
	t.Setenv("HTTP_ADDRESS", ":0")

	app := fx.New(
		fx.Provide(
			loadConfig,
			newDatabase,
			newPool,
			newSQSClient,
			newAuthMiddleware,
			newLogger,
			observability.NewMetrics,

			wagering.NewServiceWithMetrics,
			wagering.NewMessageProcessor,
			wallet.NewService,
			wagering.NewPendingReferenceResolverWithMetrics,
			newPendingReferenceWorker,

			httpapi.NewWalletHandler,
			httpapi.NewWagerHandler,
			newHealthHandler,
			newHTTPServer,
			httpapi.NewMetricsHandler,

			newSQSConsumer,
			newSQSPublisher,
			newOutboxPublisher,
			newDLQMonitor,

			newConsumerWorker,
			newOutboxWorker,
		),
		fx.Invoke(registerLifecycle),
	)

	startCtx, cancelStart := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancelStart()

	if err := app.Start(startCtx); err != nil {
		t.Fatalf(
			"start Fx application; ensure PostgreSQL, LocalStack and Keycloak are running: %v",
			err,
		)
	}

	// Give the lifecycle-managed goroutines a brief opportunity to enter
	// their run loops. The important assertion here is that the complete
	// production dependency graph can be built and started successfully.
	time.Sleep(100 * time.Millisecond)

	stopCtx, cancelStop := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancelStop()

	if err := app.Stop(stopCtx); err != nil {
		t.Fatalf("stop Fx application cleanly: %v", err)
	}
}

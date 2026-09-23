package main

import (
	"context"
	"testing"
	"time"

	"go.uber.org/fx"
)

func TestFxCompositionRootStartsAndStops(t *testing.T) {
	testConfig := loadConfig()
	testConfig.HTTPAddress = ":0"
	var resolvedConfig config

	app := fx.New(
		applicationModule,
		fx.Replace(testConfig),
		fx.Invoke(func(cfg config) {
			resolvedConfig = cfg
		}),
	)
	if resolvedConfig.HTTPAddress != ":0" {
		t.Fatalf("test config replacement was not injected: %+v", resolvedConfig)
	}

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

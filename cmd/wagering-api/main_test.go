package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.uber.org/fx"
)

func TestSQSClientUsesDefaultCredentialChain(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "environment-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "environment-secret-key")
	t.Setenv("AWS_SESSION_TOKEN", "environment-session-token")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	var authorization string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorization = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		_, _ = w.Write([]byte(`{"Attributes":{"QueueArn":"arn:aws:sqs:us-east-1:123456789012:test.fifo"}}`))
	}))
	defer server.Close()

	client, err := newSQSClient(config{
		AWSRegion:   "us-east-1",
		SQSEndpoint: server.URL,
	})
	if err != nil {
		t.Fatalf("create SQS client: %v", err)
	}

	_, err = client.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
		QueueUrl: &server.URL,
	})
	if err != nil {
		t.Fatalf("call local SQS endpoint: %v", err)
	}
	if !strings.Contains(authorization, "Credential=environment-access-key/") {
		t.Fatalf("expected request signed with environment credentials, got %q", authorization)
	}
}

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

package main

import (
	"go.uber.org/fx"
	"log/slog"
	"os"
)

const (
	defaultDatabaseURL = "postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable"

	defaultAWSRegion = "us-east-1"

	defaultCommandsQueueURL = "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/wager-transactions.fifo"
	defaultCommandsDLQURL   = "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/wager-transactions-dlq.fifo"
	defaultEventsQueueURL   = "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/wager-events.fifo"

	defaultOIDCIssuer = "http://localhost:8081/realms/wagering"

	defaultHTTPAddress = ":8080"
)

type config struct {
	DatabaseURL      string
	AWSRegion        string
	SQSEndpoint      string
	CommandsQueueURL string
	CommandsDLQURL   string
	EventsQueueURL   string
	OIDCIssuer       string
	OIDCJWKSURL      string
	HTTPAddress      string
}

func main() {
	fx.New(applicationModule).Run()
}

func loadConfig() config {
	return config{
		DatabaseURL: envOrDefault(
			"DATABASE_URL",
			defaultDatabaseURL,
		),
		AWSRegion: envOrDefault(
			"AWS_REGION",
			defaultAWSRegion,
		),
		SQSEndpoint: os.Getenv("SQS_ENDPOINT"),
		CommandsQueueURL: envOrDefault(
			"SQS_COMMANDS_QUEUE_URL",
			defaultCommandsQueueURL,
		),
		CommandsDLQURL: envOrDefault(
			"SQS_COMMANDS_DLQ_URL",
			defaultCommandsDLQURL,
		),
		EventsQueueURL: envOrDefault(
			"SQS_EVENTS_QUEUE_URL",
			defaultEventsQueueURL,
		),
		OIDCIssuer: envOrDefault(
			"OIDC_ISSUER",
			defaultOIDCIssuer,
		),
		OIDCJWKSURL: os.Getenv("OIDC_JWKS_URL"),
		HTTPAddress: envOrDefault(
			"HTTP_ADDRESS",
			defaultHTTPAddress,
		),
	}
}

func newLogger() *slog.Logger {
	return slog.New(
		slog.NewJSONHandler(
			os.Stdout,
			&slog.HandlerOptions{
				Level: slog.LevelInfo,
			},
		),
	)
}

func envOrDefault(
	name string,
	defaultValue string,
) string {
	value := os.Getenv(name)

	if value == "" {
		return defaultValue
	}

	return value
}

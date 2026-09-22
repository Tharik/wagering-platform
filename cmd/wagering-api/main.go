package main

import (
	"context"
	"log/slog"
	"os"
	"sync"

	"github.com/Tharik/wagering-platform/internal/application/wagering"
	"github.com/Tharik/wagering-platform/internal/application/wallet"
	"github.com/Tharik/wagering-platform/internal/infrastructure/httpapi"
	"github.com/Tharik/wagering-platform/internal/infrastructure/messaging/outbox"
	messagingsqs "github.com/Tharik/wagering-platform/internal/infrastructure/messaging/sqs"
	"github.com/Tharik/wagering-platform/internal/infrastructure/postgres"
	"github.com/Tharik/wagering-platform/internal/observability"
	"github.com/Tharik/wagering-platform/internal/worker"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

const (
	defaultDatabaseURL = "postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable"

	defaultAWSRegion   = "us-east-1"
	defaultSQSEndpoint = "http://localhost:4566"

	defaultCommandsQueueURL = "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/wager-commands.fifo"
	defaultEventsQueueURL   = "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/wager-events.fifo"

	defaultOIDCIssuer = "http://localhost:8081/realms/wagering"
)

type config struct {
	DatabaseURL      string
	AWSRegion        string
	SQSEndpoint      string
	CommandsQueueURL string
	EventsQueueURL   string
	OIDCIssuer       string
}

func main() {
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
			httpapi.NewHealthHandler,
			httpapi.NewServer,
			httpapi.NewMetricsHandler,

			newSQSConsumer,
			newSQSPublisher,
			newOutboxPublisher,

			newConsumerWorker,
			newOutboxWorker,
		),

		fx.Invoke(registerLifecycle),
	)

	app.Run()
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
		SQSEndpoint: envOrDefault(
			"SQS_ENDPOINT",
			defaultSQSEndpoint,
		),
		CommandsQueueURL: envOrDefault(
			"SQS_COMMANDS_QUEUE_URL",
			defaultCommandsQueueURL,
		),
		EventsQueueURL: envOrDefault(
			"SQS_EVENTS_QUEUE_URL",
			defaultEventsQueueURL,
		),
		OIDCIssuer: envOrDefault(
			"OIDC_ISSUER",
			defaultOIDCIssuer,
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

func newDatabase(
	lifecycle fx.Lifecycle,
	cfg config,
) (*postgres.Database, error) {
	db, err := postgres.NewDatabase(
		context.Background(),
		cfg.DatabaseURL,
	)
	if err != nil {
		return nil, err
	}

	lifecycle.Append(
		fx.Hook{
			OnStop: func(ctx context.Context) error {
				db.Close()
				return nil
			},
		},
	)

	return db, nil
}

func newPool(
	db *postgres.Database,
) *pgxpool.Pool {
	return db.Pool
}

func newAuthMiddleware(
	cfg config,
) (*httpapi.AuthMiddleware, error) {
	return httpapi.NewAuthMiddleware(
		context.Background(),
		cfg.OIDCIssuer,
	)
}

func newSQSClient(
	cfg config,
) *awssqs.Client {
	options := awssqs.Options{
		Region: cfg.AWSRegion,
		Credentials: aws.NewCredentialsCache(
			credentials.NewStaticCredentialsProvider(
				"test",
				"test",
				"",
			),
		),
	}

	if cfg.SQSEndpoint != "" {
		options.BaseEndpoint = aws.String(
			cfg.SQSEndpoint,
		)
	}

	return awssqs.New(options)
}

func newSQSConsumer(
	client *awssqs.Client,
	processor *wagering.MessageProcessor,
	cfg config,
	metrics *observability.Metrics,
) *messagingsqs.Consumer {
	return messagingsqs.NewConsumerWithMetrics(
		client,
		processor,
		cfg.CommandsQueueURL,
		metrics,
	)
}

func newSQSPublisher(
	client *awssqs.Client,
	cfg config,
) *messagingsqs.Publisher {
	return messagingsqs.NewPublisher(
		client,
		cfg.EventsQueueURL,
	)
}

func newOutboxPublisher(
	pool *pgxpool.Pool,
	publisher *messagingsqs.Publisher,
	metrics *observability.Metrics,
) *outbox.Publisher {
	return outbox.NewPublisherWithMetrics(
		pool,
		publisher,
		metrics,
	)
}

func newConsumerWorker(
	consumer *messagingsqs.Consumer,
	logger *slog.Logger,
	metrics *observability.Metrics,
) *worker.ConsumerWorker {
	return worker.NewConsumerWorker(
		consumer,
		logger,
		metrics,
	)
}

func newOutboxWorker(
	publisher *outbox.Publisher,
	logger *slog.Logger,
	metrics *observability.Metrics,
) *worker.OutboxWorker {
	return worker.NewOutboxWorker(
		publisher,
		logger,
		metrics,
	)
}

func newPendingReferenceWorker(
	resolver *wagering.PendingReferenceResolver,
	logger *slog.Logger,
) *worker.PendingReferenceWorker {
	return worker.NewPendingReferenceWorker(
		resolver,
		logger,
	)
}

func registerLifecycle(
	lifecycle fx.Lifecycle,
	consumerWorker *worker.ConsumerWorker,
	outboxWorker *worker.OutboxWorker,
	pendingReferenceWorker *worker.PendingReferenceWorker,
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

				httpServer.Start()

				workers.Add(3)

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

				return nil
			},

			OnStop: func(ctx context.Context) error {
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

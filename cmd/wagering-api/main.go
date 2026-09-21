package main

import (
	"context"
	"fmt"
	"os"

	"github.com/Tharik/wagering-platform/internal/application/wagering"
	"github.com/Tharik/wagering-platform/internal/application/wallet"
	"github.com/Tharik/wagering-platform/internal/infrastructure/httpapi"
	"github.com/Tharik/wagering-platform/internal/infrastructure/messaging/outbox"
	messagingsqs "github.com/Tharik/wagering-platform/internal/infrastructure/messaging/sqs"
	"github.com/Tharik/wagering-platform/internal/infrastructure/postgres"
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
)

type config struct {
	DatabaseURL      string
	AWSRegion        string
	SQSEndpoint      string
	CommandsQueueURL string
	EventsQueueURL   string
}

func main() {
	app := fx.New(
		fx.Provide(
			loadConfig,
			newDatabase,
			newPool,
			newSQSClient,

			wagering.NewService,
			wagering.NewMessageProcessor,
			wallet.NewService,

			httpapi.NewWalletHandler,
			httpapi.NewWagerHandler,
			httpapi.NewServer,

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
	}
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
) *messagingsqs.Consumer {
	return messagingsqs.NewConsumer(
		client,
		processor,
		cfg.CommandsQueueURL,
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
) *outbox.Publisher {
	return outbox.NewPublisher(
		pool,
		publisher,
	)
}

func newConsumerWorker(
	consumer *messagingsqs.Consumer,
) *worker.ConsumerWorker {
	return worker.NewConsumerWorker(
		consumer,
	)
}

func newOutboxWorker(
	publisher *outbox.Publisher,
) *worker.OutboxWorker {
	return worker.NewOutboxWorker(
		publisher,
	)
}

func registerLifecycle(
	lifecycle fx.Lifecycle,
	consumerWorker *worker.ConsumerWorker,
	outboxWorker *worker.OutboxWorker,
	httpServer *httpapi.Server,
) {
	var cancel context.CancelFunc

	lifecycle.Append(
		fx.Hook{
			OnStart: func(ctx context.Context) error {
				workerContext, workerCancel :=
					context.WithCancel(context.Background())

				cancel = workerCancel

				httpServer.Start()

				go consumerWorker.Run(workerContext)
				go outboxWorker.Run(workerContext)

				return nil
			},

			OnStop: func(ctx context.Context) error {
				if cancel != nil {
					cancel()

					if err := httpServer.Shutdown(ctx); err != nil {
						return err
					}
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

func init() {
	_ = fmt.Sprintf
}

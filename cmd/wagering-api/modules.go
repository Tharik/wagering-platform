package main

import (
	"context"
	"log/slog"

	"github.com/Tharik/wagering-platform/internal/application/wagering"
	"github.com/Tharik/wagering-platform/internal/application/wallet"
	"github.com/Tharik/wagering-platform/internal/infrastructure/httpapi"
	"github.com/Tharik/wagering-platform/internal/infrastructure/messaging/outbox"
	messagingsqs "github.com/Tharik/wagering-platform/internal/infrastructure/messaging/sqs"
	"github.com/Tharik/wagering-platform/internal/infrastructure/postgres"
	"github.com/Tharik/wagering-platform/internal/observability"
	"github.com/Tharik/wagering-platform/internal/worker"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

var infrastructureModule = fx.Options(
	fx.Provide(
		loadConfig,
		newDatabase,
		newPool,
		newSQSClient,
		newAuthMiddleware,
		newLogger,
		observability.NewMetrics,
	),
)

var applicationServicesModule = fx.Options(
	fx.Provide(
		wagering.NewServiceWithMetrics,
		wagering.NewMessageProcessor,
		newWalletService,
		wagering.NewPendingReferenceResolverWithMetrics,
	),
)

var httpModule = fx.Options(
	fx.Provide(
		httpapi.NewWalletHandler,
		newWagerHandler,
		newHealthHandler,
		newHTTPServer,
		httpapi.NewMetricsHandler,
	),
)

var messagingWorkerModule = fx.Options(
	fx.Provide(
		newSQSConsumer,
		newSQSPublisher,
		newOutboxPublisher,
		newDLQMonitor,
		newConsumerWorker,
		newOutboxWorker,
		newPendingReferenceWorker,
	),
)

var applicationModule = fx.Options(
	infrastructureModule,
	applicationServicesModule,
	httpModule,
	messagingWorkerModule,
	fx.Invoke(registerLifecycle),
)

func newHTTPServer(
	walletHandler *httpapi.WalletHandler,
	wagerHandler *httpapi.WagerHandler,
	healthHandler *httpapi.HealthHandler,
	metricsHandler *httpapi.MetricsHandler,
	auth *httpapi.AuthMiddleware,
	logger *slog.Logger,
	cfg config,
) *httpapi.Server {
	return httpapi.NewServerWithAddress(
		walletHandler,
		wagerHandler,
		healthHandler,
		metricsHandler,
		auth,
		logger,
		cfg.HTTPAddress,
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

func newPool(db *postgres.Database) *pgxpool.Pool {
	return db.Pool
}

func newAuthMiddleware(cfg config) (*httpapi.AuthMiddleware, error) {
	if cfg.OIDCJWKSURL != "" {
		return httpapi.NewAuthMiddlewareWithJWKSURL(
			context.Background(),
			cfg.OIDCIssuer,
			cfg.OIDCJWKSURL,
		)
	}

	return httpapi.NewAuthMiddleware(
		context.Background(),
		cfg.OIDCIssuer,
	)
}

func newSQSClient(cfg config) (*awssqs.Client, error) {
	awsConfig, err := awsconfig.LoadDefaultConfig(
		context.Background(),
		awsconfig.WithRegion(cfg.AWSRegion),
	)
	if err != nil {
		return nil, err
	}

	return awssqs.NewFromConfig(
		awsConfig,
		func(options *awssqs.Options) {
			if cfg.SQSEndpoint != "" {
				options.BaseEndpoint = aws.String(cfg.SQSEndpoint)
			}
		},
	), nil
}

func newWalletService(
	pool *pgxpool.Pool,
	metrics *observability.Metrics,
	logger *slog.Logger,
) *wallet.Service {
	return wallet.NewServiceWithMetricsAndLogger(pool, metrics, logger)
}

func newHealthHandler(
	pool *pgxpool.Pool,
	client *awssqs.Client,
	cfg config,
) *httpapi.HealthHandler {
	return httpapi.NewHealthHandler(pool, client, cfg.CommandsQueueURL)
}

func newSQSConsumer(
	client *awssqs.Client,
	processor *wagering.MessageProcessor,
	cfg config,
	metrics *observability.Metrics,
	logger *slog.Logger,
) *messagingsqs.Consumer {
	return messagingsqs.NewConsumerWithMetricsAndLogger(
		client,
		processor,
		cfg.CommandsQueueURL,
		metrics,
		logger,
	)
}

func newSQSPublisher(client *awssqs.Client, cfg config) *messagingsqs.Publisher {
	return messagingsqs.NewPublisher(client, cfg.EventsQueueURL)
}

func newOutboxPublisher(
	pool *pgxpool.Pool,
	publisher *messagingsqs.Publisher,
	metrics *observability.Metrics,
) *outbox.Publisher {
	return outbox.NewPublisherWithMetrics(pool, publisher, metrics)
}

func newDLQMonitor(
	client *awssqs.Client,
	cfg config,
	metrics *observability.Metrics,
	logger *slog.Logger,
) *messagingsqs.DLQMonitor {
	return messagingsqs.NewDLQMonitor(
		client,
		cfg.CommandsDLQURL,
		metrics,
		logger,
	)
}

func newConsumerWorker(
	consumer *messagingsqs.Consumer,
	logger *slog.Logger,
	metrics *observability.Metrics,
) *worker.ConsumerWorker {
	return worker.NewConsumerWorker(consumer, logger, metrics)
}

func newOutboxWorker(
	publisher *outbox.Publisher,
	logger *slog.Logger,
	metrics *observability.Metrics,
) *worker.OutboxWorker {
	return worker.NewOutboxWorker(publisher, logger, metrics)
}

func newPendingReferenceWorker(
	resolver *wagering.PendingReferenceResolver,
	logger *slog.Logger,
) *worker.PendingReferenceWorker {
	return worker.NewPendingReferenceWorker(resolver, logger)
}

func newWagerHandler(
	service *wagering.Service,
	logger *slog.Logger,
) *httpapi.WagerHandler {
	return httpapi.NewWagerHandlerWithLogger(service, logger)
}

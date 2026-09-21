package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tharik/wagering-platform/internal/infrastructure/messaging/outbox"
	sqspublisher "github.com/Tharik/wagering-platform/internal/infrastructure/messaging/sqs"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const queueURL = "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/wager-events.fifo"

func TestOutboxPublishesPersistedEventToSQS(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	pool, err := pgxpool.New(
		ctx,
		"postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable",
	)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	sqsClient := awssqs.New(awssqs.Options{
		Region: "us-east-1",
		Credentials: aws.NewCredentialsCache(
			credentials.NewStaticCredentialsProvider(
				"test",
				"test",
				"",
			),
		),
		BaseEndpoint: aws.String(
			"http://localhost:4566",
		),
	})

	cleanOutbox(t, ctx, pool)
	purgeQueue(t, ctx, sqsClient)

	eventID := uuid.New()
	aggregateID := uuid.New()
	now := time.Now().UTC()

	payload, err := json.Marshal(map[string]any{
		"eventId":     eventID.String(),
		"type":        "WagerTransactionProcessed",
		"aggregateId": aggregateID.String(),
		"occurredAt":  now.Format(time.RFC3339Nano),
		"version":     1,
		"data": map[string]any{
			"transactionId": aggregateID.String(),
		},
	})
	if err != nil {
		t.Fatalf("marshal event payload: %v", err)
	}

	_, err = pool.Exec(
		ctx,
		`
		INSERT INTO outbox_events (
			id,
			aggregate_id,
			event_type,
			payload,
			occurred_at,
			attempts,
			next_attempt_at
		)
		VALUES (
			$1,
			$2,
			$3,
			$4::jsonb,
			$5,
			0,
			$5
		)
		`,
		eventID,
		aggregateID,
		"WagerTransactionProcessed",
		payload,
		now,
	)
	if err != nil {
		t.Fatalf("insert outbox event: %v", err)
	}

	sqsPublisher := sqspublisher.NewPublisher(
		sqsClient,
		queueURL,
	)

	outboxPublisher := outbox.NewPublisher(
		pool,
		sqsPublisher,
	)

	published, err := outboxPublisher.PublishBatch(ctx)
	if err != nil {
		t.Fatalf("publish outbox batch: %v", err)
	}

	if published != 1 {
		t.Fatalf(
			"expected 1 published outbox event, got %d",
			published,
		)
	}

	var publishedAt *time.Time

	err = pool.QueryRow(
		ctx,
		`
		SELECT published_at
		FROM outbox_events
		WHERE id = $1
		`,
		eventID,
	).Scan(&publishedAt)
	if err != nil {
		t.Fatalf("query outbox event: %v", err)
	}

	if publishedAt == nil {
		t.Fatal("expected outbox event to be marked published")
	}

	result, err := sqsClient.ReceiveMessage(
		ctx,
		&awssqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueURL),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     1,
		},
	)
	if err != nil {
		t.Fatalf("receive SQS message: %v", err)
	}

	if len(result.Messages) != 1 {
		t.Fatalf(
			"expected 1 message from SQS, got %d",
			len(result.Messages),
		)
	}

	if result.Messages[0].Body == nil {
		t.Fatal("expected SQS message body")
	}

	var received struct {
		EventID     string `json:"eventId"`
		Type        string `json:"type"`
		AggregateID string `json:"aggregateId"`
	}

	if err := json.Unmarshal(
		[]byte(*result.Messages[0].Body),
		&received,
	); err != nil {
		t.Fatalf("unmarshal SQS message: %v", err)
	}

	if received.EventID != eventID.String() {
		t.Fatalf(
			"expected eventId %s, got %s",
			eventID,
			received.EventID,
		)
	}

	if received.Type != "WagerTransactionProcessed" {
		t.Fatalf(
			"expected WagerTransactionProcessed, got %s",
			received.Type,
		)
	}

	if received.AggregateID != aggregateID.String() {
		t.Fatalf(
			"expected aggregateId %s, got %s",
			aggregateID,
			received.AggregateID,
		)
	}
}

func cleanOutbox(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
) {
	t.Helper()

	if _, err := pool.Exec(
		ctx,
		`DELETE FROM outbox_events`,
	); err != nil {
		t.Fatalf("clean outbox: %v", err)
	}
}

func purgeQueue(
	t *testing.T,
	ctx context.Context,
	client *awssqs.Client,
) {
	t.Helper()

	_, err := client.PurgeQueue(
		ctx,
		&awssqs.PurgeQueueInput{
			QueueUrl: aws.String(queueURL),
		},
	)
	if err != nil {
		t.Fatalf("purge SQS queue: %v", err)
	}
}

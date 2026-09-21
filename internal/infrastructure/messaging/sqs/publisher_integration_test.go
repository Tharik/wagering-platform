package sqs

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tharik/wagering-platform/internal/infrastructure/messaging/outbox"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
)

const testQueueURL = "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/wager-events.fifo"

func TestPublisherPublishesEventToSQS(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	client := awssqs.New(awssqs.Options{
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

	// Keep the integration test independent from previous executions.
	purgeQueue(t, ctx, client)

	eventID := uuid.New()
	aggregateID := uuid.New()

	payload, err := json.Marshal(map[string]any{
		"eventId":     eventID.String(),
		"type":        "WagerTransactionProcessed",
		"aggregateId": aggregateID.String(),
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	publisher := NewPublisher(
		client,
		testQueueURL,
	)

	err = publisher.Publish(
		ctx,
		outbox.Event{
			ID:          eventID,
			AggregateID: aggregateID,
			EventType:   "WagerTransactionProcessed",
			Payload:     payload,
			OccurredAt:  time.Now().UTC(),
		},
	)
	if err != nil {
		t.Fatalf("publish event: %v", err)
	}

	result, err := client.ReceiveMessage(
		ctx,
		&awssqs.ReceiveMessageInput{
			QueueUrl:            aws.String(testQueueURL),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     1,
		},
	)
	if err != nil {
		t.Fatalf("receive message: %v", err)
	}

	if len(result.Messages) != 1 {
		t.Fatalf(
			"expected 1 SQS message, got %d",
			len(result.Messages),
		)
	}

	if result.Messages[0].Body == nil {
		t.Fatal("expected SQS message body")
	}

	var received map[string]any

	if err := json.Unmarshal(
		[]byte(*result.Messages[0].Body),
		&received,
	); err != nil {
		t.Fatalf("unmarshal received message: %v", err)
	}

	if received["eventId"] != eventID.String() {
		t.Fatalf(
			"expected eventId %s, got %v",
			eventID,
			received["eventId"],
		)
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
			QueueUrl: aws.String(testQueueURL),
		},
	)
	if err != nil {
		t.Fatalf("purge SQS queue: %v", err)
	}
}

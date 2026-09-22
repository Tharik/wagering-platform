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

func TestPublisherPublishesEventToSQS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client := newPublisherTestSQSClient()
	queueURL := createPublisherTestQueue(t, ctx, client)

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

	publisher := NewPublisher(client, queueURL)

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

	var result *awssqs.ReceiveMessageOutput

	for {
		result, err = client.ReceiveMessage(
			ctx,
			&awssqs.ReceiveMessageInput{
				QueueUrl:            aws.String(queueURL),
				MaxNumberOfMessages: 1,
				WaitTimeSeconds:     1,
			},
		)
		if err != nil {
			t.Fatalf("receive message: %v", err)
		}

		if len(result.Messages) == 1 {
			break
		}

		if ctx.Err() != nil {
			t.Fatalf("timed out waiting for published SQS event: %v", ctx.Err())
		}
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

func newPublisherTestSQSClient() *awssqs.Client {
	return awssqs.New(
		awssqs.Options{
			Region: "us-east-1",
			Credentials: aws.NewCredentialsCache(
				credentials.NewStaticCredentialsProvider(
					"test",
					"test",
					"",
				),
			),
			BaseEndpoint: aws.String("http://localhost:4566"),
		},
	)
}

func createPublisherTestQueue(
	t *testing.T,
	ctx context.Context,
	client *awssqs.Client,
) string {
	t.Helper()

	result, err := client.CreateQueue(
		ctx,
		&awssqs.CreateQueueInput{
			QueueName: aws.String("publisher-test-" + uuid.NewString() + ".fifo"),
			Attributes: map[string]string{
				"FifoQueue":                 "true",
				"ContentBasedDeduplication": "false",
			},
		},
	)
	if err != nil {
		t.Fatalf("create isolated publisher queue: %v", err)
	}

	if result.QueueUrl == nil || *result.QueueUrl == "" {
		t.Fatal("create isolated publisher queue returned empty URL")
	}

	queueURL := *result.QueueUrl

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err := client.DeleteQueue(
			cleanupCtx,
			&awssqs.DeleteQueueInput{
				QueueUrl: aws.String(queueURL),
			},
		)
		if err != nil {
			t.Errorf("delete isolated publisher queue %s: %v", queueURL, err)
		}
	})

	return queueURL
}

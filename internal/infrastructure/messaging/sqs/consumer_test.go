package sqs

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"testing"

	"github.com/Tharik/wagering-platform/internal/application/wagering"
	"github.com/Tharik/wagering-platform/internal/domain"
	"github.com/Tharik/wagering-platform/internal/observability"
	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	awstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func TestRetryVisibility(t *testing.T) {
	tests := []struct {
		receiveCount int
		expected     int32
	}{
		{receiveCount: 1, expected: 5},
		{receiveCount: 2, expected: 10},
		{receiveCount: 3, expected: 20},
		{receiveCount: 4, expected: 40},
		{receiveCount: 5, expected: 60},
		{receiveCount: 100, expected: 60},
	}

	for _, tt := range tests {
		if actual := retryVisibility(tt.receiveCount); actual != tt.expected {
			t.Fatalf("receive count %d: expected %d, got %d", tt.receiveCount, tt.expected, actual)
		}
	}
}

func TestConsumerReceiveSettings(t *testing.T) {
	client := &consumerClientFake{}
	consumer := newUnitConsumer(client, &messageProcessorFake{})

	if _, err := consumer.ConsumeOnce(context.Background()); err != nil {
		t.Fatalf("consume once: %v", err)
	}

	input := client.receiveInput
	if input == nil {
		t.Fatal("expected ReceiveMessage call")
	}
	if input.MaxNumberOfMessages != 1 {
		t.Fatalf("expected one message per receive, got %d", input.MaxNumberOfMessages)
	}
	if input.WaitTimeSeconds != 10 {
		t.Fatalf("expected 10-second long polling, got %d", input.WaitTimeSeconds)
	}
	if len(input.MessageSystemAttributeNames) != 1 ||
		input.MessageSystemAttributeNames[0] != awstypes.MessageSystemAttributeNameApproximateReceiveCount {
		t.Fatalf("expected ApproximateReceiveCount, got %v", input.MessageSystemAttributeNames)
	}
}

func TestConsumerSuccessfulProcessingDeletesWithoutChangingVisibility(t *testing.T) {
	client := &consumerClientFake{messages: []awstypes.Message{validUnitMessage(1)}}
	processor := &messageProcessorFake{result: wagering.MessageProcessResult{
		Result: wagering.ProcessResult{State: domain.WagerStateRejected},
	}}
	consumer := newUnitConsumer(client, processor)

	processed, err := consumer.ConsumeOnce(context.Background())
	if err != nil {
		t.Fatalf("consume once: %v", err)
	}
	if processed != 1 {
		t.Fatalf("expected one processed message, got %d", processed)
	}
	if client.deleteCalls != 1 {
		t.Fatalf("expected one delete, got %d", client.deleteCalls)
	}
	if client.visibilityCalls != 0 {
		t.Fatalf("expected no visibility change, got %d", client.visibilityCalls)
	}
}

func TestConsumerProcessingFailureSchedulesRetryWithoutDelete(t *testing.T) {
	client := &consumerClientFake{messages: []awstypes.Message{validUnitMessage(3)}}
	processorErr := errors.New("temporary database failure")
	consumer := newUnitConsumer(client, &messageProcessorFake{err: processorErr})

	processed, err := consumer.ConsumeOnce(context.Background())
	if !errors.Is(err, processorErr) {
		t.Fatalf("expected processing error, got %v", err)
	}
	if processed != 0 || client.deleteCalls != 0 {
		t.Fatalf("expected no processing/delete, got processed=%d deletes=%d", processed, client.deleteCalls)
	}
	if client.visibilityCalls != 1 || client.visibilityInput.VisibilityTimeout != 20 {
		t.Fatalf("expected one 20-second visibility change, got calls=%d input=%+v", client.visibilityCalls, client.visibilityInput)
	}
}

func TestConsumerVisibilityFailureStillDoesNotDelete(t *testing.T) {
	client := &consumerClientFake{
		messages:      []awstypes.Message{validUnitMessage(2)},
		visibilityErr: errors.New("visibility unavailable"),
	}
	processorErr := errors.New("temporary processing failure")
	consumer := newUnitConsumer(client, &messageProcessorFake{err: processorErr})

	_, err := consumer.ConsumeOnce(context.Background())
	if !errors.Is(err, processorErr) {
		t.Fatalf("expected original processing error, got %v", err)
	}
	if client.deleteCalls != 0 || client.visibilityCalls != 1 {
		t.Fatalf("expected visibility attempt without delete, got visibility=%d deletes=%d", client.visibilityCalls, client.deleteCalls)
	}
}

func TestConsumerCanceledContextDoesNotChangeVisibilityOrDelete(t *testing.T) {
	client := &consumerClientFake{messages: []awstypes.Message{validUnitMessage(1)}}
	consumer := newUnitConsumer(client, &messageProcessorFake{err: context.Canceled})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := consumer.ConsumeOnce(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if client.deleteCalls != 0 || client.visibilityCalls != 0 {
		t.Fatalf("expected no transport mutation, got visibility=%d deletes=%d", client.visibilityCalls, client.deleteCalls)
	}
}

func TestConsumerProcessorCancellationWithActiveContextSchedulesRetry(t *testing.T) {
	client := &consumerClientFake{messages: []awstypes.Message{validUnitMessage(1)}}
	consumer := newUnitConsumer(client, &messageProcessorFake{err: context.Canceled})

	_, err := consumer.ConsumeOnce(context.Background())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected processor cancellation, got %v", err)
	}
	if client.visibilityCalls != 1 || client.visibilityInput.VisibilityTimeout != 5 {
		t.Fatalf("expected five-second retry, got calls=%d input=%+v", client.visibilityCalls, client.visibilityInput)
	}
	if client.deleteCalls != 0 {
		t.Fatalf("expected no delete, got %d", client.deleteCalls)
	}
}

func TestConsumerMalformedMessageSchedulesRetryWithoutDelete(t *testing.T) {
	client := &consumerClientFake{messages: []awstypes.Message{{
		MessageId:     aws.String("aws-message-id"),
		Body:          aws.String("{"),
		ReceiptHandle: aws.String("receipt"),
		Attributes: map[string]string{
			string(awstypes.MessageSystemAttributeNameApproximateReceiveCount): "2",
		},
	}}}
	consumer := newUnitConsumer(client, &messageProcessorFake{})

	if _, err := consumer.ConsumeOnce(context.Background()); err == nil {
		t.Fatal("expected malformed-message error")
	}
	if client.visibilityCalls != 1 || client.visibilityInput.VisibilityTimeout != 10 {
		t.Fatalf("expected ten-second retry, got calls=%d input=%+v", client.visibilityCalls, client.visibilityInput)
	}
	if client.deleteCalls != 0 {
		t.Fatalf("expected no delete, got %d", client.deleteCalls)
	}
}

type consumerClientFake struct {
	messages        []awstypes.Message
	receiveInput    *awssqs.ReceiveMessageInput
	deleteCalls     int
	visibilityCalls int
	visibilityInput *awssqs.ChangeMessageVisibilityInput
	visibilityErr   error
}

func (c *consumerClientFake) ReceiveMessage(
	_ context.Context,
	input *awssqs.ReceiveMessageInput,
	_ ...func(*awssqs.Options),
) (*awssqs.ReceiveMessageOutput, error) {
	c.receiveInput = input
	return &awssqs.ReceiveMessageOutput{Messages: c.messages}, nil
}

func (c *consumerClientFake) DeleteMessage(
	_ context.Context,
	_ *awssqs.DeleteMessageInput,
	_ ...func(*awssqs.Options),
) (*awssqs.DeleteMessageOutput, error) {
	c.deleteCalls++
	return &awssqs.DeleteMessageOutput{}, nil
}

func (c *consumerClientFake) ChangeMessageVisibility(
	_ context.Context,
	input *awssqs.ChangeMessageVisibilityInput,
	_ ...func(*awssqs.Options),
) (*awssqs.ChangeMessageVisibilityOutput, error) {
	c.visibilityCalls++
	c.visibilityInput = input
	return &awssqs.ChangeMessageVisibilityOutput{}, c.visibilityErr
}

type messageProcessorFake struct {
	result wagering.MessageProcessResult
	err    error
}

func (p *messageProcessorFake) Process(
	_ context.Context,
	_ wagering.MessageProcessCommand,
) (wagering.MessageProcessResult, error) {
	return p.result, p.err
}

func newUnitConsumer(client ConsumerClient, processor MessageProcessor) *Consumer {
	return NewConsumerWithMetricsAndLogger(
		client,
		processor,
		"queue-url",
		observability.NewMetrics(),
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
}

func validUnitMessage(receiveCount int) awstypes.Message {
	return awstypes.Message{
		MessageId:     aws.String("aws-message-id"),
		ReceiptHandle: aws.String("receipt"),
		Body: aws.String(`{
			"messageId":"message-id",
			"type":"WagerTransactionRequested",
			"occurredAt":"2026-09-08T12:00:00Z",
			"data":{
				"providerId":"provider-a",
				"externalTransactionId":"transaction-id",
				"idempotencyKey":"provider-a:transaction-id",
				"playerId":"player-id",
				"walletId":"wallet-id",
				"roundId":"round-id",
				"gameId":"game-id",
				"kind":"BET",
				"money":{"amount":"10.00","currency":"BRL"}
			}
		}`),
		Attributes: map[string]string{
			string(awstypes.MessageSystemAttributeNameApproximateReceiveCount): strconv.Itoa(receiveCount),
		},
	}
}

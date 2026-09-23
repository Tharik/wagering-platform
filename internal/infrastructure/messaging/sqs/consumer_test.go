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

func TestConsumerShutdownAfterReceiveReleasesWithoutProcessingOrDelete(t *testing.T) {
	client := &consumerClientFake{messages: []awstypes.Message{validUnitMessage(1)}}
	processor := &messageProcessorFake{err: context.Canceled}
	consumer := newUnitConsumer(client, processor)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := consumer.ConsumeOnce(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation, got %v", err)
	}
	if processor.calls != 0 {
		t.Fatalf("expected processor not to run, got %d calls", processor.calls)
	}
	if client.deleteCalls != 0 || client.visibilityCalls != 1 {
		t.Fatalf("expected one visibility release without delete, got visibility=%d deletes=%d", client.visibilityCalls, client.deleteCalls)
	}
	if client.visibilityInput.VisibilityTimeout != 0 {
		t.Fatalf("expected immediate visibility release, got %d", client.visibilityInput.VisibilityTimeout)
	}
	if client.visibilityContextErr != nil {
		t.Fatalf("expected fresh release context, got %v", client.visibilityContextErr)
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

func TestConsumerProcessingFailureDuringDrainReleasesVisibility(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	processorErr := errors.New("processing failed during drain")
	client := &consumerClientFake{messages: []awstypes.Message{validUnitMessage(1)}}
	processor := &messageProcessorFake{process: func(context.Context) (wagering.MessageProcessResult, error) {
		close(started)
		<-release
		return wagering.MessageProcessResult{}, processorErr
	}}
	consumer := newUnitConsumer(client, processor)
	receiveCtx, cancelReceive := context.WithCancel(context.Background())
	result := make(chan error, 1)

	go func() {
		_, err := consumer.ConsumeOnceWithContexts(receiveCtx, context.Background())
		result <- err
	}()

	<-started
	cancelReceive()
	close(release)

	if err := <-result; !errors.Is(err, processorErr) {
		t.Fatalf("expected processing error, got %v", err)
	}
	if client.deleteCalls != 0 || client.visibilityCalls != 1 {
		t.Fatalf("expected immediate release without delete, got visibility=%d deletes=%d", client.visibilityCalls, client.deleteCalls)
	}
	if client.visibilityInput.VisibilityTimeout != 0 {
		t.Fatalf("expected zero-second visibility release, got %d", client.visibilityInput.VisibilityTimeout)
	}
}

func TestConsumerSuccessfulProcessingDuringDrainDeletesMessage(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	client := &consumerClientFake{messages: []awstypes.Message{validUnitMessage(1)}}
	processor := &messageProcessorFake{process: func(context.Context) (wagering.MessageProcessResult, error) {
		close(started)
		<-release
		return wagering.MessageProcessResult{}, nil
	}}
	consumer := newUnitConsumer(client, processor)
	receiveCtx, cancelReceive := context.WithCancel(context.Background())
	result := make(chan error, 1)

	go func() {
		_, err := consumer.ConsumeOnceWithContexts(receiveCtx, context.Background())
		result <- err
	}()

	<-started
	cancelReceive()
	close(release)

	if err := <-result; err != nil {
		t.Fatalf("consume during drain: %v", err)
	}
	if client.deleteCalls != 1 || client.visibilityCalls != 0 {
		t.Fatalf("expected delete without visibility change, got visibility=%d deletes=%d", client.visibilityCalls, client.deleteCalls)
	}
}

func TestConsumerDrainCancellationReleasesOnlyAfterProcessorReturns(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	allowReturn := make(chan struct{})
	client := &consumerClientFake{messages: []awstypes.Message{validUnitMessage(1)}}
	processor := &messageProcessorFake{process: func(ctx context.Context) (wagering.MessageProcessResult, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-allowReturn
		return wagering.MessageProcessResult{}, ctx.Err()
	}}
	consumer := newUnitConsumer(client, processor)
	receiveCtx, cancelReceive := context.WithCancel(context.Background())
	processingCtx, cancelProcessing := context.WithCancel(context.Background())
	result := make(chan error, 1)

	go func() {
		_, err := consumer.ConsumeOnceWithContexts(receiveCtx, processingCtx)
		result <- err
	}()

	<-started
	cancelReceive()
	cancelProcessing()
	<-canceled

	if client.visibilityCalls != 0 {
		t.Fatalf("visibility released while processor was still running: %d calls", client.visibilityCalls)
	}

	close(allowReturn)

	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected processing cancellation, got %v", err)
	}
	if client.deleteCalls != 0 || client.visibilityCalls != 1 {
		t.Fatalf("expected release after processor return without delete, got visibility=%d deletes=%d", client.visibilityCalls, client.deleteCalls)
	}
	if client.visibilityInput.VisibilityTimeout != 0 {
		t.Fatalf("expected zero-second visibility release, got %d", client.visibilityInput.VisibilityTimeout)
	}
}

func TestConsumerDoesNotProcessMessageReturnedDuringShutdown(t *testing.T) {
	receiveCtx := &cancelBeforeProcessingContext{Context: context.Background()}
	client := &consumerClientFake{messages: []awstypes.Message{validUnitMessage(1)}}
	processor := &messageProcessorFake{}
	consumer := newUnitConsumer(client, processor)

	_, err := consumer.ConsumeOnceWithContexts(receiveCtx, context.Background())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected receive cancellation, got %v", err)
	}
	if processor.calls != 0 {
		t.Fatalf("expected processor not to run, got %d calls", processor.calls)
	}
	if client.deleteCalls != 0 || client.visibilityCalls != 1 {
		t.Fatalf("expected visibility release without delete, got visibility=%d deletes=%d", client.visibilityCalls, client.deleteCalls)
	}
	if client.visibilityInput.VisibilityTimeout != 0 {
		t.Fatalf("expected zero-second visibility release, got %d", client.visibilityInput.VisibilityTimeout)
	}
}

func TestConsumerShutdownVisibilityReleaseFailurePreservesProcessingError(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	processorErr := errors.New("processing failed during shutdown")
	client := &consumerClientFake{
		messages:      []awstypes.Message{validUnitMessage(1)},
		visibilityErr: errors.New("visibility unavailable"),
	}
	processor := &messageProcessorFake{process: func(context.Context) (wagering.MessageProcessResult, error) {
		close(started)
		<-release
		return wagering.MessageProcessResult{}, processorErr
	}}
	consumer := newUnitConsumer(client, processor)
	receiveCtx, cancelReceive := context.WithCancel(context.Background())
	result := make(chan error, 1)

	go func() {
		_, err := consumer.ConsumeOnceWithContexts(receiveCtx, context.Background())
		result <- err
	}()

	<-started
	cancelReceive()
	close(release)

	if err := <-result; !errors.Is(err, processorErr) {
		t.Fatalf("expected original processing error, got %v", err)
	}
	if client.deleteCalls != 0 || client.visibilityCalls != 1 {
		t.Fatalf("expected failed release without delete, got visibility=%d deletes=%d", client.visibilityCalls, client.deleteCalls)
	}
	if client.visibilityInput.VisibilityTimeout != 0 {
		t.Fatalf("expected zero-second visibility release, got %d", client.visibilityInput.VisibilityTimeout)
	}
}

func TestConsumerDeleteFailureAfterSuccessfulProcessDuringShutdownDoesNotRelease(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	deleteErr := errors.New("delete failed after commit")
	client := &consumerClientFake{
		messages:  []awstypes.Message{validUnitMessage(1)},
		deleteErr: deleteErr,
	}
	processor := &messageProcessorFake{process: func(context.Context) (wagering.MessageProcessResult, error) {
		close(started)
		<-release
		return wagering.MessageProcessResult{}, nil
	}}
	consumer := newUnitConsumer(client, processor)
	receiveCtx, cancelReceive := context.WithCancel(context.Background())
	result := make(chan error, 1)

	go func() {
		_, err := consumer.ConsumeOnceWithContexts(receiveCtx, context.Background())
		result <- err
	}()

	<-started
	cancelReceive()
	close(release)

	if err := <-result; !errors.Is(err, deleteErr) {
		t.Fatalf("expected delete error, got %v", err)
	}
	if client.deleteCalls != 1 || client.visibilityCalls != 0 {
		t.Fatalf("expected delete attempt without visibility release, got visibility=%d deletes=%d", client.visibilityCalls, client.deleteCalls)
	}
}

type cancelBeforeProcessingContext struct {
	context.Context
	errChecks int
}

func (c *cancelBeforeProcessingContext) Err() error {
	c.errChecks++
	if c.errChecks >= 2 {
		return context.Canceled
	}
	return nil
}

type consumerClientFake struct {
	messages             []awstypes.Message
	receiveInput         *awssqs.ReceiveMessageInput
	deleteCalls          int
	visibilityCalls      int
	visibilityInput      *awssqs.ChangeMessageVisibilityInput
	visibilityErr        error
	visibilityContextErr error
	deleteErr            error
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
	return &awssqs.DeleteMessageOutput{}, c.deleteErr
}

func (c *consumerClientFake) ChangeMessageVisibility(
	ctx context.Context,
	input *awssqs.ChangeMessageVisibilityInput,
	_ ...func(*awssqs.Options),
) (*awssqs.ChangeMessageVisibilityOutput, error) {
	c.visibilityCalls++
	c.visibilityInput = input
	c.visibilityContextErr = ctx.Err()
	return &awssqs.ChangeMessageVisibilityOutput{}, c.visibilityErr
}

type messageProcessorFake struct {
	result  wagering.MessageProcessResult
	err     error
	process func(context.Context) (wagering.MessageProcessResult, error)
	calls   int
}

func (p *messageProcessorFake) Process(
	ctx context.Context,
	_ wagering.MessageProcessCommand,
) (wagering.MessageProcessResult, error) {
	p.calls++
	if p.process != nil {
		return p.process(ctx)
	}
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

package worker

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Tharik/wagering-platform/internal/observability"
)

func TestConsumerWorkerShutdownInterruptsReceivePromptly(t *testing.T) {
	receiveStarted := make(chan struct{})
	consumer := &drainConsumerFake{consume: func(receiveCtx, _ context.Context) (int, error) {
		close(receiveStarted)
		<-receiveCtx.Done()
		return 0, receiveCtx.Err()
	}}
	worker := newTestConsumerWorker(consumer, time.Second)
	receiveCtx, cancelReceive := context.WithCancel(context.Background())
	done := runConsumerWorker(worker, receiveCtx)

	<-receiveStarted
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), time.Second)
	defer cancelShutdown()
	worker.PrepareShutdown(shutdownCtx)
	cancelReceive()

	awaitWorkerExit(t, done)
}

func TestConsumerWorkerDrainsSuccessfulInFlightProcessing(t *testing.T) {
	processingStarted := make(chan struct{})
	releaseProcessing := make(chan struct{})
	consumeCalls := 0
	consumer := &drainConsumerFake{consume: func(_, _ context.Context) (int, error) {
		consumeCalls++
		close(processingStarted)
		<-releaseProcessing
		return 1, nil
	}}
	worker := newTestConsumerWorker(consumer, time.Second)
	receiveCtx, cancelReceive := context.WithCancel(context.Background())
	done := runConsumerWorker(worker, receiveCtx)

	<-processingStarted
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), time.Second)
	defer cancelShutdown()
	worker.PrepareShutdown(shutdownCtx)
	cancelReceive()

	select {
	case <-done:
		t.Fatal("worker exited before in-flight processing completed")
	default:
	}

	close(releaseProcessing)
	awaitWorkerExit(t, done)

	if consumeCalls != 1 {
		t.Fatalf("expected no new receive after shutdown, got %d consume calls", consumeCalls)
	}
}

func TestConsumerWorkerCancelsProcessingAtDrainTimeout(t *testing.T) {
	processingStarted := make(chan struct{})
	processingCanceled := make(chan struct{})
	consumer := &drainConsumerFake{consume: func(_, processingCtx context.Context) (int, error) {
		close(processingStarted)
		<-processingCtx.Done()
		close(processingCanceled)
		return 0, processingCtx.Err()
	}}
	worker := newTestConsumerWorker(consumer, 10*time.Millisecond)
	receiveCtx, cancelReceive := context.WithCancel(context.Background())
	done := runConsumerWorker(worker, receiveCtx)

	<-processingStarted
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), time.Second)
	defer cancelShutdown()
	worker.PrepareShutdown(shutdownCtx)
	cancelReceive()

	select {
	case <-processingCanceled:
	case <-shutdownCtx.Done():
		t.Fatal("processing was not canceled within shutdown budget")
	}
	awaitWorkerExit(t, done)
}

func TestConsumerWorkerRespectsEarlierShutdownDeadline(t *testing.T) {
	processingStarted := make(chan struct{})
	processingCanceled := make(chan struct{})
	consumer := &drainConsumerFake{consume: func(_, processingCtx context.Context) (int, error) {
		close(processingStarted)
		<-processingCtx.Done()
		close(processingCanceled)
		return 0, processingCtx.Err()
	}}
	worker := newTestConsumerWorker(consumer, time.Second)
	receiveCtx, cancelReceive := context.WithCancel(context.Background())
	done := runConsumerWorker(worker, receiveCtx)

	<-processingStarted
	shutdownCtx, cancelShutdown := context.WithCancel(context.Background())
	worker.PrepareShutdown(shutdownCtx)
	cancelShutdown()
	cancelReceive()

	select {
	case <-processingCanceled:
	case <-time.After(time.Second):
		t.Fatal("processing did not respect earlier shutdown deadline")
	}
	awaitWorkerExit(t, done)
}

type drainConsumerFake struct {
	consume func(receiveCtx, processingCtx context.Context) (int, error)
}

func (c *drainConsumerFake) ConsumeOnceWithContexts(
	receiveCtx context.Context,
	processingCtx context.Context,
) (int, error) {
	return c.consume(receiveCtx, processingCtx)
}

func newTestConsumerWorker(
	consumer Consumer,
	drainTimeout time.Duration,
) *ConsumerWorker {
	return newConsumerWorkerWithDrainTimeout(
		consumer,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		observability.NewMetrics(),
		drainTimeout,
	)
}

func runConsumerWorker(
	worker *ConsumerWorker,
	ctx context.Context,
) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		worker.Run(ctx)
	}()
	return done
}

func awaitWorkerExit(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("consumer worker did not exit")
	}
}

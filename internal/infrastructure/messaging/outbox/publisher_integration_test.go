package outbox

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type fakeMessagePublisher struct {
	mu         sync.Mutex
	events     []Event
	publishErr error
}

func (f *fakeMessagePublisher) Publish(
	ctx context.Context,
	event Event,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.events = append(f.events, event)

	return f.publishErr
}

func TestPublishBatchPublishesPendingEventAndMarksItPublished(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool := newTestPool(t, ctx)
	defer pool.Close()

	cleanOutbox(t, ctx, pool)

	eventID := insertTestEvent(t, ctx, pool)

	messagePublisher := &fakeMessagePublisher{}

	publisher := NewPublisher(
		pool,
		messagePublisher,
	)

	count, err := publisher.PublishBatch(ctx)
	if err != nil {
		t.Fatalf("publish batch: %v", err)
	}

	if count != 1 {
		t.Fatalf("expected 1 published event, got %d", count)
	}

	if len(messagePublisher.events) != 1 {
		t.Fatalf(
			"expected publisher to receive 1 event, got %d",
			len(messagePublisher.events),
		)
	}

	if messagePublisher.events[0].ID != eventID {
		t.Fatalf(
			"expected event ID %s, got %s",
			eventID,
			messagePublisher.events[0].ID,
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
		t.Fatalf("query published event: %v", err)
	}

	if publishedAt == nil {
		t.Fatal("expected published_at to be set")
	}
}

func TestPublishBatchSchedulesRetryWhenPublishingFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool := newTestPool(t, ctx)
	defer pool.Close()

	cleanOutbox(t, ctx, pool)

	eventID := insertTestEvent(t, ctx, pool)

	messagePublisher := &fakeMessagePublisher{
		publishErr: errors.New("broker unavailable"),
	}

	publisher := NewPublisher(
		pool,
		messagePublisher,
	)

	count, err := publisher.PublishBatch(ctx)
	if err != nil {
		t.Fatalf("publish batch: %v", err)
	}

	if count != 0 {
		t.Fatalf(
			"expected 0 successfully published events, got %d",
			count,
		)
	}

	var (
		attempts      int
		nextAttemptAt time.Time
		publishedAt   *time.Time
	)

	err = pool.QueryRow(
		ctx,
		`
		SELECT
			attempts,
			next_attempt_at,
			published_at
		FROM outbox_events
		WHERE id = $1
		`,
		eventID,
	).Scan(
		&attempts,
		&nextAttemptAt,
		&publishedAt,
	)
	if err != nil {
		t.Fatalf("query failed outbox event: %v", err)
	}

	if attempts != 1 {
		t.Fatalf(
			"expected attempts 1, got %d",
			attempts,
		)
	}

	if publishedAt != nil {
		t.Fatal("expected published_at to remain NULL")
	}

	if !nextAttemptAt.After(time.Now().UTC()) {
		t.Fatalf(
			"expected next_attempt_at in the future, got %s",
			nextAttemptAt,
		)
	}
}

func newTestPool(
	t *testing.T,
	ctx context.Context,
) *pgxpool.Pool {
	t.Helper()

	pool, err := pgxpool.New(
		ctx,
		"postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable",
	)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}

	return pool
}

func cleanOutbox(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
) {
	t.Helper()

	_, err := pool.Exec(
		ctx,
		`DELETE FROM outbox_events`,
	)
	if err != nil {
		t.Fatalf("clean outbox: %v", err)
	}
}

func insertTestEvent(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
) uuid.UUID {
	t.Helper()

	eventID := uuid.New()
	aggregateID := uuid.New()
	now := time.Now().UTC()

	_, err := pool.Exec(
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
			'TestEvent',
			'{"message":"hello"}'::jsonb,
			$3,
			0,
			$3
		)
		`,
		eventID,
		aggregateID,
		now,
	)
	if err != nil {
		t.Fatalf("insert test outbox event: %v", err)
	}

	return eventID
}

func TestConcurrentPublishersDoNotPublishSameEventSimultaneously(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool := newTestPool(t, ctx)
	defer pool.Close()

	cleanOutbox(t, ctx, pool)

	eventID := insertTestEvent(t, ctx, pool)

	messagePublisher := &blockingMessagePublisher{
		started: make(chan uuid.UUID, 2),
		release: make(chan struct{}),
	}

	publisher1 := NewPublisher(pool, messagePublisher)
	publisher2 := NewPublisher(pool, messagePublisher)

	type result struct {
		count int
		err   error
	}

	results := make(chan result, 2)

	go func() {
		count, err := publisher1.PublishBatch(ctx)
		results <- result{count: count, err: err}
	}()

	firstPublishedID := <-messagePublisher.started

	if firstPublishedID != eventID {
		t.Fatalf(
			"expected first publisher to receive %s, got %s",
			eventID,
			firstPublishedID,
		)
	}

	go func() {
		count, err := publisher2.PublishBatch(ctx)
		results <- result{count: count, err: err}
	}()

	select {
	case secondPublishedID := <-messagePublisher.started:
		close(messagePublisher.release)

		t.Fatalf(
			"same event was concurrently published twice: %s",
			secondPublishedID,
		)

	case <-time.After(500 * time.Millisecond):
		// Expected: the second publisher must not claim the same event.
	}

	close(messagePublisher.release)

	for i := 0; i < 2; i++ {
		result := <-results

		if result.err != nil {
			t.Fatalf(
				"publisher returned error: %v",
				result.err,
			)
		}
	}
}

type blockingMessagePublisher struct {
	started chan uuid.UUID
	release chan struct{}
}

func (p *blockingMessagePublisher) Publish(
	ctx context.Context,
	event Event,
) error {
	p.started <- event.ID

	select {
	case <-p.release:
		return nil

	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestPendingEventIsRepublishedWithSameEventIDAfterFailedAttempt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool := newTestPool(t, ctx)
	defer pool.Close()

	cleanOutbox(t, ctx, pool)

	eventID := insertTestEvent(t, ctx, pool)

	failingPublisher := &fakeMessagePublisher{
		publishErr: errors.New("simulated publisher failure"),
	}

	firstPublisher := NewPublisher(
		pool,
		failingPublisher,
	)

	count, err := firstPublisher.PublishBatch(ctx)
	if err != nil {
		t.Fatalf("first publish batch: %v", err)
	}

	if count != 0 {
		t.Fatalf(
			"expected 0 successfully published events, got %d",
			count,
		)
	}

	if len(failingPublisher.events) != 1 {
		t.Fatalf(
			"expected first publisher to receive 1 event, got %d",
			len(failingPublisher.events),
		)
	}

	if failingPublisher.events[0].ID != eventID {
		t.Fatalf(
			"expected first attempt event ID %s, got %s",
			eventID,
			failingPublisher.events[0].ID,
		)
	}

	// Make the failed event immediately eligible for another attempt.
	_, err = pool.Exec(
		ctx,
		`
		UPDATE outbox_events
		SET next_attempt_at = NOW()
		WHERE id = $1
		`,
		eventID,
	)
	if err != nil {
		t.Fatalf(
			"make event eligible for retry: %v",
			err,
		)
	}

	recoveryPublisher := &fakeMessagePublisher{}

	secondPublisher := NewPublisher(
		pool,
		recoveryPublisher,
	)

	count, err = secondPublisher.PublishBatch(ctx)
	if err != nil {
		t.Fatalf("recovery publish batch: %v", err)
	}

	if count != 1 {
		t.Fatalf(
			"expected recovery publisher to publish 1 event, got %d",
			count,
		)
	}

	if len(recoveryPublisher.events) != 1 {
		t.Fatalf(
			"expected recovery publisher to receive 1 event, got %d",
			len(recoveryPublisher.events),
		)
	}

	recoveredEvent := recoveryPublisher.events[0]

	if recoveredEvent.ID != eventID {
		t.Fatalf(
			"expected stable event ID %s after recovery, got %s",
			eventID,
			recoveredEvent.ID,
		)
	}

	var (
		attempts    int
		publishedAt *time.Time
	)

	err = pool.QueryRow(
		ctx,
		`
		SELECT
			attempts,
			published_at
		FROM outbox_events
		WHERE id = $1
		`,
		eventID,
	).Scan(
		&attempts,
		&publishedAt,
	)
	if err != nil {
		t.Fatalf(
			"query recovered event: %v",
			err,
		)
	}

	if attempts != 1 {
		t.Fatalf(
			"expected attempts to remain 1 after successful recovery, got %d",
			attempts,
		)
	}

	if publishedAt == nil {
		t.Fatal(
			"expected recovered event to be marked published",
		)
	}
}

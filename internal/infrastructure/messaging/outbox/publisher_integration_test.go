package outbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
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
	publisher := NewPublisher(pool, messagePublisher)

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
	publisher := NewPublisher(pool, messagePublisher)

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

	var firstPublishedID uuid.UUID
	select {
	case firstPublishedID = <-messagePublisher.started:
	case <-ctx.Done():
		t.Fatalf(
			"timed out waiting for first publisher to claim event: %v",
			ctx.Err(),
		)
	}

	if firstPublishedID != eventID {
		close(messagePublisher.release)
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
		// Expected: FOR UPDATE SKIP LOCKED prevents the second publisher
		// from claiming the row while the first publisher owns its lock.
	case <-ctx.Done():
		close(messagePublisher.release)
		t.Fatalf(
			"context ended while checking concurrent publisher: %v",
			ctx.Err(),
		)
	}

	close(messagePublisher.release)

	totalPublished := 0
	for i := 0; i < 2; i++ {
		select {
		case result := <-results:
			if result.err != nil {
				t.Fatalf(
					"publisher returned error: %v",
					result.err,
				)
			}
			totalPublished += result.count

		case <-ctx.Done():
			t.Fatalf(
				"timed out waiting for publisher result: %v",
				ctx.Err(),
			)
		}
	}

	if totalPublished != 1 {
		t.Fatalf(
			"expected exactly 1 published event across both publishers, got %d",
			totalPublished,
		)
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
	firstPublisher := NewPublisher(pool, failingPublisher)

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

	// Make the failed event unambiguously eligible for another attempt.
	_, err = pool.Exec(
		ctx,
		`
		UPDATE outbox_events
		SET next_attempt_at = NOW() - INTERVAL '1 second'
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
	secondPublisher := NewPublisher(pool, recoveryPublisher)

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
		t.Fatal("expected recovered event to be marked published")
	}
}

func TestPublishedEventIsRepublishedWithSameEventIDWhenConfirmationCommitFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool := newTestPool(t, ctx)
	t.Cleanup(pool.Close)

	cleanOutbox(t, ctx, pool)

	eventID, initialNextAttemptAt, persistedPayload := insertTestEnvelopeEvent(t, ctx, pool)
	dropCommitFailureTrigger := installDeferredCommitFailureTrigger(t, ctx, pool)
	t.Cleanup(dropCommitFailureTrigger)

	firstExternalPublisher := &fakeMessagePublisher{}
	firstPublisher := NewPublisher(pool, firstExternalPublisher)

	count, err := firstPublisher.PublishBatch(ctx)
	if err == nil {
		t.Fatal("expected publication confirmation commit to fail")
	}
	if count != 1 {
		t.Fatalf("expected one externally published event before commit failure, got %d", count)
	}
	if len(firstExternalPublisher.events) != 1 {
		t.Fatalf("expected one successful external Publish call, got %d", len(firstExternalPublisher.events))
	}

	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) ||
		pgErr.Code != "P0001" ||
		!strings.Contains(pgErr.Message, "test deferred outbox confirmation failure") {
		t.Fatalf("expected deliberate deferred-trigger commit failure, got %v", err)
	}

	var (
		persistedID       uuid.UUID
		publishedAt       *time.Time
		attempts          int
		nextAttemptAt     time.Time
		payloadAfterError []byte
	)
	if err := pool.QueryRow(ctx, `
		SELECT id, published_at, attempts, next_attempt_at, payload
		FROM outbox_events
		WHERE id = $1
	`, eventID).Scan(
		&persistedID,
		&publishedAt,
		&attempts,
		&nextAttemptAt,
		&payloadAfterError,
	); err != nil {
		t.Fatalf("query event after failed confirmation commit: %v", err)
	}
	if persistedID != eventID {
		t.Fatalf("persisted event ID changed: got %s, want %s", persistedID, eventID)
	}
	if publishedAt != nil {
		t.Fatalf("expected published_at rollback to NULL, got %s", publishedAt)
	}
	if attempts != 0 {
		t.Fatalf("expected retry attempts to remain 0, got %d", attempts)
	}
	if !nextAttemptAt.Equal(initialNextAttemptAt) {
		t.Fatalf("next_attempt_at changed: got %s, want %s", nextAttemptAt, initialNextAttemptAt)
	}
	if !bytes.Equal(payloadAfterError, persistedPayload) {
		t.Fatalf("persisted payload changed after rollback: got %s, want %s", payloadAfterError, persistedPayload)
	}

	dropCommitFailureTrigger()

	secondExternalPublisher := &fakeMessagePublisher{}
	secondPublisher := NewPublisher(pool, secondExternalPublisher)

	count, err = secondPublisher.PublishBatch(ctx)
	if err != nil {
		t.Fatalf("recover pending event: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one recovered publication, got %d", count)
	}
	if len(secondExternalPublisher.events) != 1 {
		t.Fatalf("expected one recovery Publish call, got %d", len(secondExternalPublisher.events))
	}

	firstEvent := firstExternalPublisher.events[0]
	secondEvent := secondExternalPublisher.events[0]
	firstEnvelopeID := payloadEventID(t, firstEvent.Payload)
	secondEnvelopeID := payloadEventID(t, secondEvent.Payload)

	if firstEvent.ID != eventID || secondEvent.ID != eventID {
		t.Fatalf(
			"expected persisted event ID %s on both publications, got first=%s second=%s",
			eventID,
			firstEvent.ID,
			secondEvent.ID,
		)
	}
	if firstEnvelopeID != eventID.String() || secondEnvelopeID != eventID.String() {
		t.Fatalf(
			"expected payload eventId %s on both publications, got first=%s second=%s",
			eventID,
			firstEnvelopeID,
			secondEnvelopeID,
		)
	}
	if !bytes.Equal(firstEvent.Payload, secondEvent.Payload) ||
		!bytes.Equal(firstEvent.Payload, persistedPayload) {
		t.Fatalf("expected identical persisted payload on both publications: first=%s second=%s", firstEvent.Payload, secondEvent.Payload)
	}

	if err := pool.QueryRow(
		ctx,
		`SELECT published_at FROM outbox_events WHERE id = $1`,
		eventID,
	).Scan(&publishedAt); err != nil {
		t.Fatalf("query recovered publication confirmation: %v", err)
	}
	if publishedAt == nil {
		t.Fatal("expected recovery transaction to commit published_at")
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

	// The production query selects rows with next_attempt_at <= NOW().
	// Put fixtures clearly in the past instead of relying on the Go and
	// PostgreSQL clocks being identical at a timestamp boundary.
	eligibleAt := time.Now().UTC().Add(-time.Second)

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
		eligibleAt,
	)
	if err != nil {
		t.Fatalf("insert test outbox event: %v", err)
	}

	return eventID
}

func insertTestEnvelopeEvent(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
) (uuid.UUID, time.Time, []byte) {
	t.Helper()

	eventID := uuid.New()
	aggregateID := uuid.New()
	eligibleAt := time.Now().UTC().Add(-time.Second).Truncate(time.Microsecond)
	payload, err := json.Marshal(map[string]any{
		"eventId":     eventID.String(),
		"eventType":   "TestEvent",
		"aggregateId": aggregateID.String(),
		"occurredAt":  eligibleAt.Format(time.RFC3339Nano),
		"version":     1,
		"data": map[string]string{
			"message": "hello",
		},
	})
	if err != nil {
		t.Fatalf("marshal test event envelope: %v", err)
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO outbox_events (
			id, aggregate_id, event_type, payload, occurred_at, attempts, next_attempt_at
		) VALUES ($1, $2, 'TestEvent', $3::jsonb, $4, 0, $4)
	`, eventID, aggregateID, payload, eligibleAt)
	if err != nil {
		t.Fatalf("insert test envelope event: %v", err)
	}

	var persistedPayload []byte
	if err := pool.QueryRow(
		ctx,
		`SELECT payload FROM outbox_events WHERE id = $1`,
		eventID,
	).Scan(&persistedPayload); err != nil {
		t.Fatalf("query persisted test envelope: %v", err)
	}

	return eventID, eligibleAt, persistedPayload
}

func installDeferredCommitFailureTrigger(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
) func() {
	t.Helper()

	_, err := pool.Exec(ctx, `
		CREATE FUNCTION test_fail_outbox_confirmation_commit()
		RETURNS TRIGGER AS $$
		BEGIN
			IF OLD.published_at IS NULL AND NEW.published_at IS NOT NULL THEN
				RAISE EXCEPTION 'test deferred outbox confirmation failure';
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;

		CREATE CONSTRAINT TRIGGER test_fail_outbox_confirmation_commit
		AFTER UPDATE ON outbox_events
		DEFERRABLE INITIALLY DEFERRED
		FOR EACH ROW
		EXECUTE FUNCTION test_fail_outbox_confirmation_commit();
	`)
	if err != nil {
		t.Fatalf("install deferred confirmation failure trigger: %v", err)
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, cleanupErr := pool.Exec(cleanupCtx, `
				DROP TRIGGER IF EXISTS test_fail_outbox_confirmation_commit ON outbox_events;
				DROP FUNCTION IF EXISTS test_fail_outbox_confirmation_commit();
			`)
			if cleanupErr != nil {
				t.Errorf("remove deferred confirmation failure trigger: %v", cleanupErr)
			}
		})
	}
}

func payloadEventID(t *testing.T, payload []byte) string {
	t.Helper()
	var envelope struct {
		EventID string `json:"eventId"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		t.Fatalf("decode published event envelope: %v", err)
	}
	return envelope.EventID
}

type blockingMessagePublisher struct {
	started chan uuid.UUID
	release chan struct{}
}

func (p *blockingMessagePublisher) Publish(
	ctx context.Context,
	event Event,
) error {
	select {
	case p.started <- event.ID:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case <-p.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

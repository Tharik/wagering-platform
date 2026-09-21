package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultBatchSize = 10
	retryBaseDelay   = 5 * time.Second
	maxRetryDelay    = time.Minute
)

type Publisher struct {
	pool      *pgxpool.Pool
	publisher MessagePublisher
}

func NewPublisher(
	pool *pgxpool.Pool,
	publisher MessagePublisher,
) *Publisher {
	return &Publisher{
		pool:      pool,
		publisher: publisher,
	}
}

func (p *Publisher) PublishBatch(ctx context.Context) (int, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf(
			"begin outbox transaction: %w",
			err,
		)
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	events, err := findPending(
		ctx,
		tx,
		defaultBatchSize,
	)
	if err != nil {
		return 0, err
	}

	published := 0

	for _, event := range events {
		if err := p.publisher.Publish(ctx, event); err != nil {
			if err := scheduleRetry(
				ctx,
				tx,
				event,
			); err != nil {
				return published, err
			}

			continue
		}

		if err := markPublished(
			ctx,
			tx,
			event.ID,
		); err != nil {
			return published, err
		}

		published++
	}

	if err := tx.Commit(ctx); err != nil {
		return published, fmt.Errorf(
			"commit outbox transaction: %w",
			err,
		)
	}

	return published, nil
}

func findPending(
	ctx context.Context,
	tx pgx.Tx,
	limit int,
) ([]Event, error) {
	rows, err := tx.Query(
		ctx,
		`
		SELECT
			id,
			aggregate_id,
			event_type,
			payload,
			occurred_at,
			attempts
		FROM outbox_events
		WHERE published_at IS NULL
		  AND next_attempt_at <= NOW()
		ORDER BY occurred_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED
		`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"query pending outbox events: %w",
			err,
		)
	}
	defer rows.Close()

	events := make([]Event, 0, limit)

	for rows.Next() {
		var event Event

		if err := rows.Scan(
			&event.ID,
			&event.AggregateID,
			&event.EventType,
			&event.Payload,
			&event.OccurredAt,
			&event.Attempts,
		); err != nil {
			return nil, fmt.Errorf(
				"scan outbox event: %w",
				err,
			)
		}

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate outbox events: %w",
			err,
		)
	}

	return events, nil
}

func markPublished(
	ctx context.Context,
	tx pgx.Tx,
	eventID uuid.UUID,
) error {
	result, err := tx.Exec(
		ctx,
		`
		UPDATE outbox_events
		SET published_at = NOW()
		WHERE id = $1
		  AND published_at IS NULL
		`,
		eventID,
	)
	if err != nil {
		return fmt.Errorf(
			"mark outbox event published: %w",
			err,
		)
	}

	if result.RowsAffected() != 1 {
		return fmt.Errorf(
			"expected to mark one outbox event published, marked %d",
			result.RowsAffected(),
		)
	}

	return nil
}

func scheduleRetry(
	ctx context.Context,
	tx pgx.Tx,
	event Event,
) error {
	delay := retryDelay(event.Attempts + 1)

	result, err := tx.Exec(
		ctx,
		`
		UPDATE outbox_events
		SET
			attempts = attempts + 1,
			next_attempt_at = NOW() + ($2 * INTERVAL '1 second')
		WHERE id = $1
		  AND published_at IS NULL
		`,
		event.ID,
		int(delay.Seconds()),
	)
	if err != nil {
		return fmt.Errorf(
			"schedule outbox retry: %w",
			err,
		)
	}

	if result.RowsAffected() != 1 {
		return fmt.Errorf(
			"expected to schedule one outbox event retry, updated %d",
			result.RowsAffected(),
		)
	}

	return nil
}

func retryDelay(attempt int) time.Duration {
	if attempt <= 1 {
		return retryBaseDelay
	}

	delay := retryBaseDelay

	for i := 1; i < attempt; i++ {
		delay *= 2

		if delay >= maxRetryDelay {
			return maxRetryDelay
		}
	}

	return delay
}

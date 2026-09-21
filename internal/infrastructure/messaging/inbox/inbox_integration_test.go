package inbox

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRegisterAndCompleteMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool := newTestPool(t, ctx)
	defer pool.Close()

	cleanInbox(t, ctx, pool)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}

	completed, err := Register(
		ctx,
		tx,
		"wager-consumer",
		"message-1",
		[]byte(`{"transactionId":"tx-1"}`),
	)
	if err != nil {
		t.Fatalf("register message: %v", err)
	}

	if completed {
		t.Fatal("new message must not already be completed")
	}

	if err := Complete(
		ctx,
		tx,
		"wager-consumer",
		"message-1",
	); err != nil {
		t.Fatalf("complete message: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit transaction: %v", err)
	}

	var completedAt *time.Time

	err = pool.QueryRow(
		ctx,
		`
		SELECT completed_at
		FROM inbox_messages
		WHERE consumer_name = $1
		  AND message_id = $2
		`,
		"wager-consumer",
		"message-1",
	).Scan(&completedAt)
	if err != nil {
		t.Fatalf("query inbox message: %v", err)
	}

	if completedAt == nil {
		t.Fatal("expected completed_at to be set")
	}
}

func TestRegisterCompletedMessageIsReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool := newTestPool(t, ctx)
	defer pool.Close()

	cleanInbox(t, ctx, pool)

	payload := []byte(`{"transactionId":"tx-1"}`)

	tx1, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin first transaction: %v", err)
	}

	_, err = Register(
		ctx,
		tx1,
		"wager-consumer",
		"message-1",
		payload,
	)
	if err != nil {
		t.Fatalf("register first message: %v", err)
	}

	if err := Complete(
		ctx,
		tx1,
		"wager-consumer",
		"message-1",
	); err != nil {
		t.Fatalf("complete first message: %v", err)
	}

	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("commit first transaction: %v", err)
	}

	tx2, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin replay transaction: %v", err)
	}
	defer func() {
		_ = tx2.Rollback(ctx)
	}()

	completed, err := Register(
		ctx,
		tx2,
		"wager-consumer",
		"message-1",
		payload,
	)
	if err != nil {
		t.Fatalf("register replay: %v", err)
	}

	if !completed {
		t.Fatal("expected completed message to be recognized as replay")
	}
}

func TestRegisterSameMessageIDWithDifferentPayloadIsConflict(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool := newTestPool(t, ctx)
	defer pool.Close()

	cleanInbox(t, ctx, pool)

	tx1, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin first transaction: %v", err)
	}

	_, err = Register(
		ctx,
		tx1,
		"wager-consumer",
		"message-1",
		[]byte(`{"transactionId":"tx-1"}`),
	)
	if err != nil {
		t.Fatalf("register first message: %v", err)
	}

	if err := Complete(
		ctx,
		tx1,
		"wager-consumer",
		"message-1",
	); err != nil {
		t.Fatalf("complete first message: %v", err)
	}

	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("commit first transaction: %v", err)
	}

	tx2, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin conflict transaction: %v", err)
	}
	defer func() {
		_ = tx2.Rollback(ctx)
	}()

	_, err = Register(
		ctx,
		tx2,
		"wager-consumer",
		"message-1",
		[]byte(`{"transactionId":"SOMETHING-ELSE"}`),
	)

	if !errors.Is(err, ErrPayloadConflict) {
		t.Fatalf(
			"expected ErrPayloadConflict, got %v",
			err,
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

func cleanInbox(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
) {
	t.Helper()

	if _, err := pool.Exec(
		ctx,
		`DELETE FROM inbox_messages`,
	); err != nil {
		t.Fatalf("clean inbox: %v", err)
	}
}

func TestConcurrentRegisterSameMessageIsHandledAsReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool := newTestPool(t, ctx)
	defer pool.Close()

	cleanInbox(t, ctx, pool)

	const (
		consumerName = "wager-consumer"
		messageID    = "concurrent-message"
	)

	payload := []byte(`{"transactionId":"tx-1"}`)

	type result struct {
		completed bool
		err       error
	}

	results := make(chan result, 2)
	start := make(chan struct{})

	register := func() {
		tx, err := pool.Begin(ctx)
		if err != nil {
			results <- result{err: err}
			return
		}

		defer func() {
			_ = tx.Rollback(ctx)
		}()

		<-start

		completed, err := Register(
			ctx,
			tx,
			consumerName,
			messageID,
			payload,
		)
		if err != nil {
			results <- result{err: err}
			return
		}

		if !completed {
			if err := Complete(
				ctx,
				tx,
				consumerName,
				messageID,
			); err != nil {
				results <- result{err: err}
				return
			}
		}

		if err := tx.Commit(ctx); err != nil {
			results <- result{err: err}
			return
		}

		results <- result{
			completed: completed,
		}
	}

	go register()
	go register()

	close(start)

	first := <-results
	second := <-results

	if first.err != nil {
		t.Fatalf("first concurrent registration failed: %v", first.err)
	}

	if second.err != nil {
		t.Fatalf("second concurrent registration failed: %v", second.err)
	}

	var count int

	err := pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM inbox_messages
		WHERE consumer_name = $1
		  AND message_id = $2
		`,
		consumerName,
		messageID,
	).Scan(&count)
	if err != nil {
		t.Fatalf("count inbox messages: %v", err)
	}

	if count != 1 {
		t.Fatalf(
			"expected exactly 1 inbox message, got %d",
			count,
		)
	}
}

func TestConcurrentRegisterSameMessageDoesNotFail(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.New(
		ctx,
		"postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable",
	)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	_, err = pool.Exec(
		ctx,
		`
		TRUNCATE TABLE
			inbox_messages
		CASCADE
		`,
	)
	if err != nil {
		t.Fatalf("clean inbox: %v", err)
	}

	const (
		consumerName = "concurrent-consumer"
		messageID    = "concurrent-message-001"
	)

	payload := []byte(`{"type":"BET","amount":"10.00"}`)

	type outcome struct {
		alreadyCompleted bool
		err              error
	}

	start := make(chan struct{})
	outcomes := make(chan outcome, 2)

	var wg sync.WaitGroup

	for i := 0; i < 2; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			tx, err := pool.Begin(ctx)
			if err != nil {
				outcomes <- outcome{err: err}
				return
			}
			defer tx.Rollback(ctx)

			<-start

			alreadyCompleted, err := Register(
				ctx,
				tx,
				consumerName,
				messageID,
				payload,
			)
			if err != nil {
				outcomes <- outcome{err: err}
				return
			}

			if err := tx.Commit(ctx); err != nil {
				outcomes <- outcome{err: err}
				return
			}

			outcomes <- outcome{
				alreadyCompleted: alreadyCompleted,
			}
		}()
	}

	close(start)

	wg.Wait()
	close(outcomes)

	successes := 0

	for result := range outcomes {
		if result.err != nil {
			t.Fatalf("concurrent Register failed: %v", result.err)
		}

		successes++
	}

	if successes != 2 {
		t.Fatalf("expected 2 successful registrations, got %d", successes)
	}

	var count int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM inbox_messages
		WHERE consumer_name = $1
		  AND message_id = $2
		`,
		consumerName,
		messageID,
	).Scan(&count)
	if err != nil {
		t.Fatalf("count inbox messages: %v", err)
	}

	if count != 1 {
		t.Fatalf(
			"expected exactly 1 inbox row, got %d",
			count,
		)
	}
}

func TestConcurrentRegisterSameMessageDifferentPayloadReturnsConflict(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.New(
		ctx,
		"postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable",
	)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	_, err = pool.Exec(
		ctx,
		`TRUNCATE TABLE inbox_messages CASCADE`,
	)
	if err != nil {
		t.Fatalf("clean inbox: %v", err)
	}

	const (
		consumerName = "concurrent-conflict-consumer"
		messageID    = "concurrent-conflict-message"
	)

	payloads := [][]byte{
		[]byte(`{"type":"BET","amount":"10.00"}`),
		[]byte(`{"type":"BET","amount":"50.00"}`),
	}

	type outcome struct {
		err error
	}

	start := make(chan struct{})
	outcomes := make(chan outcome, 2)

	var wg sync.WaitGroup

	for _, payload := range payloads {
		payload := payload

		wg.Add(1)

		go func() {
			defer wg.Done()

			tx, err := pool.Begin(ctx)
			if err != nil {
				outcomes <- outcome{err: err}
				return
			}
			defer tx.Rollback(ctx)

			<-start

			_, err = Register(
				ctx,
				tx,
				consumerName,
				messageID,
				payload,
			)
			if err != nil {
				outcomes <- outcome{err: err}
				return
			}

			if err := tx.Commit(ctx); err != nil {
				outcomes <- outcome{err: err}
				return
			}

			outcomes <- outcome{}
		}()
	}

	close(start)

	wg.Wait()
	close(outcomes)

	successes := 0
	conflicts := 0

	for result := range outcomes {
		switch {
		case result.err == nil:
			successes++

		case errors.Is(result.err, ErrPayloadConflict):
			conflicts++

		default:
			t.Fatalf("unexpected concurrent Register error: %v", result.err)
		}
	}

	if successes != 1 {
		t.Fatalf(
			"expected exactly 1 successful registration, got %d",
			successes,
		)
	}

	if conflicts != 1 {
		t.Fatalf(
			"expected exactly 1 payload conflict, got %d",
			conflicts,
		)
	}

	var count int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM inbox_messages
		WHERE consumer_name = $1
		  AND message_id = $2
		`,
		consumerName,
		messageID,
	).Scan(&count)
	if err != nil {
		t.Fatalf("count inbox messages: %v", err)
	}

	if count != 1 {
		t.Fatalf(
			"expected exactly 1 inbox row, got %d",
			count,
		)
	}
}

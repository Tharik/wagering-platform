package wallet

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Tharik/wagering-platform/internal/application/eventpayload"
	"github.com/Tharik/wagering-platform/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestCreateWalletWithOpeningBalance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(
		ctx,
		"postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable",
	)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	cleanDatabase(t, ctx, pool)

	service := NewService(pool)

	result, err := service.Create(
		ctx,
		CreateWalletCommand{
			PlayerID:       "player-opening-test",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	if result.Balance.Amount() != 10000 {
		t.Fatalf(
			"expected balance 10000, got %d",
			result.Balance.Amount(),
		)
	}

	assertCount(t, ctx, pool, "wallets", 1)
	assertCount(t, ctx, pool, "wager_transactions", 1)
	assertCount(t, ctx, pool, "ledger_entries", 1)
	assertCount(t, ctx, pool, "outbox_events", 2)

	var (
		kind          string
		state         string
		amount        int64
		resultBalance int64
	)

	err = pool.QueryRow(
		ctx,
		`
		SELECT
			kind::text,
			state::text,
			amount,
			result_balance
		FROM wager_transactions
		LIMIT 1
		`,
	).Scan(
		&kind,
		&state,
		&amount,
		&resultBalance,
	)
	if err != nil {
		t.Fatalf("query opening transaction: %v", err)
	}

	if kind != "OPENING" {
		t.Fatalf("expected OPENING, got %s", kind)
	}

	if state != "PROCESSED" {
		t.Fatalf("expected PROCESSED, got %s", state)
	}

	if amount != 10000 {
		t.Fatalf("expected amount 10000, got %d", amount)
	}

	if resultBalance != 10000 {
		t.Fatalf(
			"expected result balance 10000, got %d",
			resultBalance,
		)
	}

	var (
		direction     string
		ledgerAmount  int64
		balanceBefore int64
		balanceAfter  int64
	)

	err = pool.QueryRow(
		ctx,
		`
		SELECT
			direction::text,
			amount,
			balance_before,
			balance_after
		FROM ledger_entries
		LIMIT 1
		`,
	).Scan(
		&direction,
		&ledgerAmount,
		&balanceBefore,
		&balanceAfter,
	)
	if err != nil {
		t.Fatalf("query ledger: %v", err)
	}

	if direction != "CREDIT" {
		t.Fatalf("expected CREDIT, got %s", direction)
	}

	if ledgerAmount != 10000 {
		t.Fatalf("expected ledger amount 10000, got %d", ledgerAmount)
	}

	if balanceBefore != 0 || balanceAfter != 10000 {
		t.Fatalf(
			"unexpected ledger balances: before=%d after=%d",
			balanceBefore,
			balanceAfter,
		)
	}

	assertOpeningOutboxContract(t, ctx, pool, result.WalletID)
}

func assertOpeningOutboxContract(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	walletID string,
) {
	t.Helper()

	rows, err := pool.Query(
		ctx,
		`
		SELECT event_type, payload
		FROM outbox_events
		ORDER BY event_type
		`,
	)
	if err != nil {
		t.Fatalf("query opening outbox events: %v", err)
	}
	defer rows.Close()

	events := make(map[string]map[string]any)

	for rows.Next() {
		var (
			eventType string
			payload   []byte
		)

		if err := rows.Scan(&eventType, &payload); err != nil {
			t.Fatalf("scan opening outbox event: %v", err)
		}

		var envelope map[string]any
		if err := json.Unmarshal(payload, &envelope); err != nil {
			t.Fatalf("unmarshal %s event: %v", eventType, err)
		}

		events[eventType] = envelope
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("iterate opening outbox events: %v", err)
	}

	if len(events) != 2 {
		t.Fatalf("expected 2 distinct opening event types, got %d", len(events))
	}

	processed, ok := events["WagerTransactionProcessed"]
	if !ok {
		t.Fatal("missing WagerTransactionProcessed event")
	}

	balanceChanged, ok := events["WalletBalanceChanged"]
	if !ok {
		t.Fatal("missing WalletBalanceChanged event")
	}

	assertEventEnvelope(t, processed, "WagerTransactionProcessed", walletID)
	assertEventEnvelope(t, balanceChanged, "WalletBalanceChanged", walletID)

	processedData := eventData(t, processed)
	var processedPayload eventpayload.WagerTransactionProcessedData
	decodeEventData(t, processedData, &processedPayload)

	if processedPayload.WalletID != walletID {
		t.Fatalf(
			"processed event walletId: expected %s, got %v",
			walletID,
			processedPayload.WalletID,
		)
	}

	if processedPayload.Kind != "OPENING" {
		t.Fatalf(
			"processed event kind: expected OPENING, got %v",
			processedPayload.Kind,
		)
	}

	transactionID := processedPayload.TransactionID
	if transactionID == "" {
		t.Fatalf(
			"processed event transactionId must be a non-empty string, got %v",
			transactionID,
		)
	}

	balanceData := eventData(t, balanceChanged)
	if _, exists := balanceData["amount"]; exists {
		t.Fatal("WalletBalanceChanged data must not contain legacy amount")
	}
	if _, exists := balanceData["currency"]; exists {
		t.Fatal("WalletBalanceChanged data must not contain legacy currency")
	}
	if len(balanceData) != 7 {
		t.Fatalf("expected exactly 7 WalletBalanceChanged fields, got %d", len(balanceData))
	}

	var balancePayload eventpayload.WalletBalanceChangedData
	decodeEventData(t, balanceData, &balancePayload)
	expectedMoney := eventpayload.Money{Amount: "100.00", Currency: "BRL"}
	if balancePayload.WalletID != walletID ||
		balancePayload.TransactionID != transactionID ||
		balancePayload.Direction != "CREDIT" ||
		balancePayload.Money != expectedMoney ||
		balancePayload.BalanceBefore != (eventpayload.Money{Amount: "0.00", Currency: "BRL"}) ||
		balancePayload.BalanceAfter != expectedMoney ||
		balancePayload.WalletVersion != 1 {
		t.Fatalf("unexpected opening WalletBalanceChanged data: %+v", balancePayload)
	}

	if processed["correlationId"] != balanceChanged["correlationId"] {
		t.Fatalf(
			"opening events must share correlationId: processed=%v balance=%v",
			processed["correlationId"],
			balanceChanged["correlationId"],
		)
	}

	if _, exists := processed["causationId"]; exists {
		t.Fatal("OPENING processed event must not contain causationId")
	}

	if _, exists := balanceChanged["causationId"]; exists {
		t.Fatal("OPENING balance event must not contain causationId")
	}
}

func assertEventEnvelope(
	t *testing.T,
	envelope map[string]any,
	expectedEventType string,
	expectedAggregateID string,
) {
	t.Helper()

	requiredStrings := []string{
		"eventId",
		"eventType",
		"aggregateId",
		"correlationId",
		"occurredAt",
	}

	for _, field := range requiredStrings {
		value, ok := envelope[field].(string)
		if !ok || value == "" {
			t.Fatalf(
				"%s event: %s must be a non-empty string, got %v",
				expectedEventType,
				field,
				envelope[field],
			)
		}
	}

	if envelope["eventType"] != expectedEventType {
		t.Fatalf(
			"eventType: expected %s, got %v",
			expectedEventType,
			envelope["eventType"],
		)
	}

	if envelope["aggregateId"] != expectedAggregateID {
		t.Fatalf(
			"%s aggregateId: expected %s, got %v",
			expectedEventType,
			expectedAggregateID,
			envelope["aggregateId"],
		)
	}

	if envelope["version"] != float64(1) {
		t.Fatalf(
			"%s version: expected 1, got %v",
			expectedEventType,
			envelope["version"],
		)
	}

	if _, err := time.Parse(
		time.RFC3339Nano,
		envelope["occurredAt"].(string),
	); err != nil {
		t.Fatalf(
			"%s occurredAt must be RFC3339Nano: %v",
			expectedEventType,
			err,
		)
	}

	if _, ok := envelope["data"].(map[string]any); !ok {
		t.Fatalf(
			"%s data must be an object, got %T",
			expectedEventType,
			envelope["data"],
		)
	}
}

func eventData(
	t *testing.T,
	envelope map[string]any,
) map[string]any {
	t.Helper()

	data, ok := envelope["data"].(map[string]any)
	if !ok {
		t.Fatalf("event data must be an object, got %T", envelope["data"])
	}

	return data
}

func decodeEventData(t *testing.T, data map[string]any, target any) {
	t.Helper()
	payload, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("marshal event data: %v", err)
	}
	if err := json.Unmarshal(payload, target); err != nil {
		t.Fatalf("decode event data: %v", err)
	}
}

func TestCreateZeroBalanceWalletDoesNotCreateOpeningMovement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(
		ctx,
		"postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable",
	)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	cleanDatabase(t, ctx, pool)

	service := NewService(pool)

	_, err = service.Create(
		ctx,
		CreateWalletCommand{
			PlayerID:       "player-zero-test",
			InitialBalance: domain.Zero(domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	assertCount(t, ctx, pool, "wallets", 1)
	assertCount(t, ctx, pool, "wager_transactions", 0)
	assertCount(t, ctx, pool, "ledger_entries", 0)
	assertCount(t, ctx, pool, "outbox_events", 0)
}

func TestConcurrentDuplicateWalletCreationUsesUniqueConstraint(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, "postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable")
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()
	cleanDatabase(t, ctx, pool)

	const attempts = 8
	service := NewService(pool)
	start := make(chan struct{})
	results := make(chan error, attempts)
	var ready sync.WaitGroup
	ready.Add(attempts)

	for i := 0; i < attempts; i++ {
		go func() {
			ready.Done()
			<-start
			_, createErr := service.Create(ctx, CreateWalletCommand{
				PlayerID:       "player-concurrent-duplicate",
				InitialBalance: domain.Zero(domain.BRL),
			})
			results <- createErr
		}()
	}

	ready.Wait()
	close(start)

	successes := 0
	for i := 0; i < attempts; i++ {
		createErr := <-results
		if createErr == nil {
			successes++
			continue
		}
		if !errors.Is(createErr, ErrWalletAlreadyExists) {
			t.Fatalf("expected ErrWalletAlreadyExists, got %v", createErr)
		}
	}
	if successes != 1 {
		t.Fatalf("expected exactly one successful create, got %d", successes)
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM wallets WHERE player_id = $1 AND currency = $2`, "player-concurrent-duplicate", string(domain.BRL)).Scan(&count); err != nil {
		t.Fatalf("count wallets: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly one wallet row, got %d", count)
	}
}

func TestSamePlayerDifferentCurrencyIsNotDuplicate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, "postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable")
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()
	cleanDatabase(t, ctx, pool)

	service := NewService(pool)
	for _, currency := range []domain.Currency{domain.BRL, domain.Currency("USD")} {
		if _, err := service.Create(ctx, CreateWalletCommand{
			PlayerID:       "player-multiple-currencies",
			InitialBalance: domain.Zero(currency),
		}); err != nil {
			t.Fatalf("create %s wallet: %v", currency, err)
		}
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM wallets WHERE player_id = $1`, "player-multiple-currencies").Scan(&count); err != nil {
		t.Fatalf("count player wallets: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected one wallet per currency, got %d", count)
	}
}

func cleanDatabase(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
) {
	t.Helper()

	_, err := pool.Exec(
		ctx,
		`
		TRUNCATE TABLE
			outbox_events,
			inbox_messages,
			ledger_entries,
			wager_transactions,
			wallets
		CASCADE
		`,
	)
	if err != nil {
		t.Fatalf("clean database: %v", err)
	}
}

func assertCount(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	table string,
	expected int,
) {
	t.Helper()

	var count int

	err := pool.QueryRow(
		ctx,
		"SELECT COUNT(*) FROM "+table,
	).Scan(&count)

	if err != nil {
		t.Fatalf("count %s: %v", table, err)
	}

	if count != expected {
		t.Fatalf(
			"expected %d rows in %s, got %d",
			expected,
			table,
			count,
		)
	}
}

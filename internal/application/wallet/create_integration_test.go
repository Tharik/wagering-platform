package wallet

import (
	"context"
	"testing"
	"time"

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

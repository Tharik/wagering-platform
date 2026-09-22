package wallet

import (
	"context"
	"testing"
	"time"

	"github.com/Tharik/wagering-platform/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestReconciliationDetectsConsistentAndDivergentWallet(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	const databaseURL = "postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable"

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	cleanReconciliationDatabase(t, ctx, pool)

	service := NewService(pool)

	created, err := service.Create(
		ctx,
		CreateWalletCommand{
			PlayerID:       "player-reconciliation",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	//
	// 1. Healthy wallet.
	//
	// Wallet creation with 100.00 creates the OPENING ledger entry.
	// Reconciliation must reconstruct exactly the same balance.
	//
	result, err := service.Reconcile(
		ctx,
		created.WalletID,
	)
	if err != nil {
		t.Fatalf("reconcile healthy wallet: %v", err)
	}

	if !result.Consistent {
		t.Fatal("expected newly created wallet to be consistent")
	}

	if result.WalletBalance != 10000 {
		t.Fatalf(
			"expected wallet balance 10000, got %d",
			result.WalletBalance,
		)
	}

	if result.LedgerBalance != 10000 {
		t.Fatalf(
			"expected ledger balance 10000, got %d",
			result.LedgerBalance,
		)
	}

	if result.EntryCount != 1 {
		t.Fatalf(
			"expected exactly 1 opening ledger entry, got %d",
			result.EntryCount,
		)
	}

	if result.Currency != string(domain.BRL) {
		t.Fatalf(
			"expected BRL currency, got %s",
			result.Currency,
		)
	}

	//
	// 2. Simulate database corruption / operational divergence.
	//
	// We intentionally modify only the materialized wallet balance.
	// The immutable ledger remains untouched.
	//
	_, err = pool.Exec(
		ctx,
		`
		UPDATE wallets
		SET balance = 12345
		WHERE id = $1
		`,
		created.WalletID,
	)
	if err != nil {
		t.Fatalf("corrupt wallet balance: %v", err)
	}

	divergent, err := service.Reconcile(
		ctx,
		created.WalletID,
	)
	if err != nil {
		t.Fatalf("reconcile divergent wallet: %v", err)
	}

	if divergent.Consistent {
		t.Fatal(
			"expected reconciliation to detect wallet/ledger divergence",
		)
	}

	if divergent.WalletBalance != 12345 {
		t.Fatalf(
			"expected corrupted wallet balance 12345, got %d",
			divergent.WalletBalance,
		)
	}

	if divergent.LedgerBalance != 10000 {
		t.Fatalf(
			"expected immutable ledger balance 10000, got %d",
			divergent.LedgerBalance,
		)
	}

	if divergent.EntryCount != 1 {
		t.Fatalf(
			"expected ledger entry count to remain 1, got %d",
			divergent.EntryCount,
		)
	}

	//
	// 3. Reconciliation is diagnostic only.
	//
	// It must report the divergence, never silently repair financial state.
	//
	var persistedBalance int64

	err = pool.QueryRow(
		ctx,
		`
		SELECT balance
		FROM wallets
		WHERE id = $1
		`,
		created.WalletID,
	).Scan(&persistedBalance)
	if err != nil {
		t.Fatalf("query wallet after reconciliation: %v", err)
	}

	if persistedBalance != 12345 {
		t.Fatalf(
			"expected reconciliation not to mutate wallet; got balance %d",
			persistedBalance,
		)
	}

	var ledgerBalanceAfter int64

	err = pool.QueryRow(
		ctx,
		`
		SELECT balance_after
		FROM ledger_entries
		WHERE wallet_id = $1
		ORDER BY created_at DESC, id DESC
		LIMIT 1
		`,
		created.WalletID,
	).Scan(&ledgerBalanceAfter)
	if err != nil {
		t.Fatalf("query ledger after reconciliation: %v", err)
	}

	if ledgerBalanceAfter != 10000 {
		t.Fatalf(
			"expected reconciliation not to mutate ledger; got %d",
			ledgerBalanceAfter,
		)
	}
}

func cleanReconciliationDatabase(
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

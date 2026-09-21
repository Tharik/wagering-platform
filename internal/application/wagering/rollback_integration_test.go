package wagering

import (
	"context"
	"testing"
	"time"

	"github.com/Tharik/wagering-platform/internal/application/wallet"
	"github.com/Tharik/wagering-platform/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRollbackBetCreditsWallet(t *testing.T) {
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

	cleanDatabase(t, ctx, pool)

	createdWallet, err := wallet.NewService(pool).Create(
		ctx,
		wallet.CreateWalletCommand{
			PlayerID:       "player-rollback-bet",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	service := NewService(pool)

	bet, err := service.Process(ctx, ProcessCommand{
		IdempotencyKey: "bet-before-rollback",
		Request: domain.WagerRequest{
			ProviderID:            "provider-a",
			ExternalTransactionID: "bet-rollback-1",
			PlayerID:              "player-rollback-bet",
			WalletID:              createdWallet.WalletID,
			RoundID:               "round-1",
			GameID:                "game-1",
			Kind:                  domain.WagerKindBet,
			Amount:                domain.NewMoney(3000, domain.BRL),
		},
	})
	if err != nil {
		t.Fatalf("process BET: %v", err)
	}

	if bet.Balance.Amount() != 7000 {
		t.Fatalf("expected 7000 after BET, got %d", bet.Balance.Amount())
	}

	result, err := service.Process(ctx, ProcessCommand{
		IdempotencyKey: "rollback-bet",
		Request: domain.WagerRequest{
			ProviderID:                     "provider-a",
			ExternalTransactionID:          "rollback-bet-1",
			PlayerID:                       "player-rollback-bet",
			WalletID:                       createdWallet.WalletID,
			RoundID:                        "round-1",
			GameID:                         "game-1",
			Kind:                           domain.WagerKindRollback,
			Amount:                         domain.NewMoney(3000, domain.BRL),
			ReferenceExternalTransactionID: "bet-rollback-1",
		},
	})
	if err != nil {
		t.Fatalf("process ROLLBACK: %v", err)
	}

	if result.State != domain.WagerStateProcessed {
		t.Fatalf("expected PROCESSED, got %s", result.State)
	}

	if result.Balance.Amount() != 10000 {
		t.Fatalf(
			"expected balance 10000 after rollback, got %d",
			result.Balance.Amount(),
		)
	}

	assertRollbackPersistence(
		t,
		ctx,
		pool,
		createdWallet.WalletID,
		bet.TransactionID,
		"CREDIT",
		3000,
		7000,
		10000,
		3,
	)
}

func TestRollbackWinDebitsWallet(t *testing.T) {
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

	cleanDatabase(t, ctx, pool)

	createdWallet, err := wallet.NewService(pool).Create(
		ctx,
		wallet.CreateWalletCommand{
			PlayerID:       "player-rollback-win",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	service := NewService(pool)

	win, err := service.Process(ctx, ProcessCommand{
		IdempotencyKey: "win-before-rollback",
		Request: domain.WagerRequest{
			ProviderID:            "provider-a",
			ExternalTransactionID: "win-rollback-1",
			PlayerID:              "player-rollback-win",
			WalletID:              createdWallet.WalletID,
			RoundID:               "round-1",
			GameID:                "game-1",
			Kind:                  domain.WagerKindWin,
			Amount:                domain.NewMoney(3000, domain.BRL),
		},
	})
	if err != nil {
		t.Fatalf("process WIN: %v", err)
	}

	if win.Balance.Amount() != 13000 {
		t.Fatalf("expected 13000 after WIN, got %d", win.Balance.Amount())
	}

	result, err := service.Process(ctx, ProcessCommand{
		IdempotencyKey: "rollback-win",
		Request: domain.WagerRequest{
			ProviderID:                     "provider-a",
			ExternalTransactionID:          "rollback-win-1",
			PlayerID:                       "player-rollback-win",
			WalletID:                       createdWallet.WalletID,
			RoundID:                        "round-1",
			GameID:                         "game-1",
			Kind:                           domain.WagerKindRollback,
			Amount:                         domain.NewMoney(3000, domain.BRL),
			ReferenceExternalTransactionID: "win-rollback-1",
		},
	})
	if err != nil {
		t.Fatalf("process ROLLBACK: %v", err)
	}

	if result.Balance.Amount() != 10000 {
		t.Fatalf(
			"expected balance 10000 after rollback, got %d",
			result.Balance.Amount(),
		)
	}

	assertRollbackPersistence(
		t,
		ctx,
		pool,
		createdWallet.WalletID,
		win.TransactionID,
		"DEBIT",
		3000,
		13000,
		10000,
		3,
	)
}

func TestRollbackRefundDebitsWallet(t *testing.T) {
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

	cleanDatabase(t, ctx, pool)

	createdWallet, err := wallet.NewService(pool).Create(
		ctx,
		wallet.CreateWalletCommand{
			PlayerID:       "player-rollback-refund",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	service := NewService(pool)

	_, err = service.Process(ctx, ProcessCommand{
		IdempotencyKey: "bet-before-refund",
		Request: domain.WagerRequest{
			ProviderID:            "provider-a",
			ExternalTransactionID: "bet-before-refund-1",
			PlayerID:              "player-rollback-refund",
			WalletID:              createdWallet.WalletID,
			RoundID:               "round-1",
			GameID:                "game-1",
			Kind:                  domain.WagerKindBet,
			Amount:                domain.NewMoney(3000, domain.BRL),
		},
	})
	if err != nil {
		t.Fatalf("process BET: %v", err)
	}

	refund, err := service.Process(ctx, ProcessCommand{
		IdempotencyKey: "refund-before-rollback",
		Request: domain.WagerRequest{
			ProviderID:                     "provider-a",
			ExternalTransactionID:          "refund-before-rollback-1",
			PlayerID:                       "player-rollback-refund",
			WalletID:                       createdWallet.WalletID,
			RoundID:                        "round-1",
			GameID:                         "game-1",
			Kind:                           domain.WagerKindRefund,
			Amount:                         domain.NewMoney(3000, domain.BRL),
			ReferenceExternalTransactionID: "bet-before-refund-1",
		},
	})
	if err != nil {
		t.Fatalf("process REFUND: %v", err)
	}

	if refund.Balance.Amount() != 10000 {
		t.Fatalf(
			"expected 10000 after REFUND, got %d",
			refund.Balance.Amount(),
		)
	}

	result, err := service.Process(ctx, ProcessCommand{
		IdempotencyKey: "rollback-refund",
		Request: domain.WagerRequest{
			ProviderID:                     "provider-a",
			ExternalTransactionID:          "rollback-refund-1",
			PlayerID:                       "player-rollback-refund",
			WalletID:                       createdWallet.WalletID,
			RoundID:                        "round-1",
			GameID:                         "game-1",
			Kind:                           domain.WagerKindRollback,
			Amount:                         domain.NewMoney(3000, domain.BRL),
			ReferenceExternalTransactionID: "refund-before-rollback-1",
		},
	})
	if err != nil {
		t.Fatalf("process ROLLBACK: %v", err)
	}

	if result.Balance.Amount() != 7000 {
		t.Fatalf(
			"expected balance 7000 after rollback, got %d",
			result.Balance.Amount(),
		)
	}

	assertRollbackPersistence(
		t,
		ctx,
		pool,
		createdWallet.WalletID,
		refund.TransactionID,
		"DEBIT",
		3000,
		10000,
		7000,
		4,
	)
}

func TestRollbackWinWithInsufficientFundsIsRejected(t *testing.T) {
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

	cleanDatabase(t, ctx, pool)

	createdWallet, err := wallet.NewService(pool).Create(
		ctx,
		wallet.CreateWalletCommand{
			PlayerID:       "player-rollback-insufficient",
			InitialBalance: domain.Zero(domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	service := NewService(pool)

	// 0 -> WIN 30 -> balance 30.
	_, err = service.Process(ctx, ProcessCommand{
		IdempotencyKey: "win-insufficient-rollback",
		Request: domain.WagerRequest{
			ProviderID:            "provider-a",
			ExternalTransactionID: "win-insufficient-rollback-1",
			PlayerID:              "player-rollback-insufficient",
			WalletID:              createdWallet.WalletID,
			RoundID:               "round-1",
			GameID:                "game-1",
			Kind:                  domain.WagerKindWin,
			Amount:                domain.NewMoney(3000, domain.BRL),
		},
	})
	if err != nil {
		t.Fatalf("process WIN: %v", err)
	}

	// Spend part of that WIN.
	// 30 -> BET 20 -> balance 10.
	_, err = service.Process(ctx, ProcessCommand{
		IdempotencyKey: "spend-win-money",
		Request: domain.WagerRequest{
			ProviderID:            "provider-a",
			ExternalTransactionID: "bet-after-win-1",
			PlayerID:              "player-rollback-insufficient",
			WalletID:              createdWallet.WalletID,
			RoundID:               "round-2",
			GameID:                "game-1",
			Kind:                  domain.WagerKindBet,
			Amount:                domain.NewMoney(2000, domain.BRL),
		},
	})
	if err != nil {
		t.Fatalf("process BET: %v", err)
	}

	// Rolling back the WIN would require a debit of 30,
	// but only 10 remains.
	result, err := service.Process(ctx, ProcessCommand{
		IdempotencyKey: "rollback-win-insufficient",
		Request: domain.WagerRequest{
			ProviderID:                     "provider-a",
			ExternalTransactionID:          "rollback-win-insufficient-1",
			PlayerID:                       "player-rollback-insufficient",
			WalletID:                       createdWallet.WalletID,
			RoundID:                        "round-1",
			GameID:                         "game-1",
			Kind:                           domain.WagerKindRollback,
			Amount:                         domain.NewMoney(3000, domain.BRL),
			ReferenceExternalTransactionID: "win-insufficient-rollback-1",
		},
	})
	if err != nil {
		t.Fatalf("expected financial rejection, got error: %v", err)
	}

	if result.State != domain.WagerStateRejected {
		t.Fatalf(
			"expected REJECTED, got %s",
			result.State,
		)
	}

	if result.FailureCode != "REVERSAL_INSUFFICIENT_FUNDS" {
		t.Fatalf(
			"expected REVERSAL_INSUFFICIENT_FUNDS, got %s",
			result.FailureCode,
		)
	}

	if result.Balance.Amount() != 1000 {
		t.Fatalf(
			"expected unchanged balance 1000, got %d",
			result.Balance.Amount(),
		)
	}

	var balance int64
	var version int64

	err = pool.QueryRow(
		ctx,
		`
		SELECT balance, version
		FROM wallets
		WHERE id = $1
		`,
		createdWallet.WalletID,
	).Scan(&balance, &version)
	if err != nil {
		t.Fatalf("query wallet: %v", err)
	}

	if balance != 1000 {
		t.Fatalf(
			"expected persisted balance 1000, got %d",
			balance,
		)
	}

	// Initial zero wallet = 1
	// WIN = 2
	// BET = 3
	// rejected ROLLBACK must not increment it.
	if version != 3 {
		t.Fatalf(
			"expected version to remain 3, got %d",
			version,
		)
	}

	var rollbackLedgerCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM ledger_entries le
		JOIN wager_transactions wt
		  ON wt.id = le.transaction_id
		WHERE wt.wallet_id = $1
		  AND wt.kind = 'ROLLBACK'
		`,
		createdWallet.WalletID,
	).Scan(&rollbackLedgerCount)
	if err != nil {
		t.Fatalf("count ROLLBACK ledger entries: %v", err)
	}

	if rollbackLedgerCount != 0 {
		t.Fatalf(
			"expected rejected ROLLBACK to create no ledger entry, got %d",
			rollbackLedgerCount,
		)
	}
}

func assertRollbackPersistence(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	walletID string,
	expectedReferenceID string,
	expectedDirection string,
	expectedAmount int64,
	expectedBefore int64,
	expectedAfter int64,
	expectedVersion int64,
) {
	t.Helper()

	var balance int64
	var version int64

	err := pool.QueryRow(
		ctx,
		`
		SELECT balance, version
		FROM wallets
		WHERE id = $1
		`,
		walletID,
	).Scan(&balance, &version)
	if err != nil {
		t.Fatalf("query wallet: %v", err)
	}

	if balance != expectedAfter {
		t.Fatalf(
			"expected persisted balance %d, got %d",
			expectedAfter,
			balance,
		)
	}

	if version != expectedVersion {
		t.Fatalf(
			"expected wallet version %d, got %d",
			expectedVersion,
			version,
		)
	}

	var referencedTransactionID string

	err = pool.QueryRow(
		ctx,
		`
		SELECT referenced_transaction_id::text
		FROM wager_transactions
		WHERE wallet_id = $1
		  AND kind = 'ROLLBACK'
		  AND state = 'PROCESSED'
		`,
		walletID,
	).Scan(&referencedTransactionID)
	if err != nil {
		t.Fatalf("query ROLLBACK: %v", err)
	}

	if referencedTransactionID != expectedReferenceID {
		t.Fatalf(
			"expected reference %s, got %s",
			expectedReferenceID,
			referencedTransactionID,
		)
	}

	var ledgerCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM ledger_entries le
		JOIN wager_transactions wt
		  ON wt.id = le.transaction_id
		WHERE wt.wallet_id = $1
		  AND wt.kind = 'ROLLBACK'
		  AND le.direction = $2
		  AND le.amount = $3
		  AND le.balance_before = $4
		  AND le.balance_after = $5
		`,
		walletID,
		expectedDirection,
		expectedAmount,
		expectedBefore,
		expectedAfter,
	).Scan(&ledgerCount)
	if err != nil {
		t.Fatalf("query ROLLBACK ledger: %v", err)
	}

	if ledgerCount != 1 {
		t.Fatalf(
			"expected exactly 1 matching ROLLBACK ledger entry, got %d",
			ledgerCount,
		)
	}
}

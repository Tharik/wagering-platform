package wagering

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Tharik/wagering-platform/internal/application/wallet"
	"github.com/Tharik/wagering-platform/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRefundWithPartialAmountIsRejected(t *testing.T) {
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

	walletService := wallet.NewService(pool)

	createdWallet, err := walletService.Create(
		ctx,
		wallet.CreateWalletCommand{
			PlayerID:       "player-partial-refund",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	service := NewService(pool)

	_, err = service.Process(
		ctx,
		ProcessCommand{
			IdempotencyKey: "bet-partial-refund",
			Request: domain.WagerRequest{
				ProviderID:            "provider-a",
				ExternalTransactionID: "external-bet-partial",
				PlayerID:              "player-partial-refund",
				WalletID:              createdWallet.WalletID,
				RoundID:               "round-1",
				GameID:                "game-1",
				Kind:                  domain.WagerKindBet,
				Amount:                domain.NewMoney(3000, domain.BRL),
			},
		},
	)
	if err != nil {
		t.Fatalf("process BET: %v", err)
	}

	// Attempt to refund only 20 of the original 30.
	_, err = service.Process(
		ctx,
		ProcessCommand{
			IdempotencyKey: "partial-refund",
			Request: domain.WagerRequest{
				ProviderID:                     "provider-a",
				ExternalTransactionID:          "external-partial-refund",
				PlayerID:                       "player-partial-refund",
				WalletID:                       createdWallet.WalletID,
				RoundID:                        "round-1",
				GameID:                         "game-1",
				Kind:                           domain.WagerKindRefund,
				Amount:                         domain.NewMoney(2000, domain.BRL),
				ReferenceExternalTransactionID: "external-bet-partial",
			},
		},
	)

	if !errors.Is(err, ErrReferenceAmountMismatch) {
		t.Fatalf(
			"expected ErrReferenceAmountMismatch, got %v",
			err,
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

	// OPENING = version 1
	// BET = version 2
	// rejected partial refund must do nothing.
	if balance != 7000 {
		t.Fatalf(
			"expected balance to remain 7000, got %d",
			balance,
		)
	}

	if version != 2 {
		t.Fatalf(
			"expected wallet version to remain 2, got %d",
			version,
		)
	}

	var refundCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM wager_transactions
		WHERE wallet_id = $1
		  AND kind = 'REFUND'
		`,
		createdWallet.WalletID,
	).Scan(&refundCount)
	if err != nil {
		t.Fatalf("count REFUND transactions: %v", err)
	}

	if refundCount != 0 {
		t.Fatalf(
			"expected no REFUND transaction, got %d",
			refundCount,
		)
	}
}

func TestRefundCannotReferenceWin(t *testing.T) {
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

	walletService := wallet.NewService(pool)

	createdWallet, err := walletService.Create(
		ctx,
		wallet.CreateWalletCommand{
			PlayerID:       "player-refund-win",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	service := NewService(pool)

	_, err = service.Process(
		ctx,
		ProcessCommand{
			IdempotencyKey: "win-for-invalid-refund",
			Request: domain.WagerRequest{
				ProviderID:            "provider-a",
				ExternalTransactionID: "external-win-refund",
				PlayerID:              "player-refund-win",
				WalletID:              createdWallet.WalletID,
				RoundID:               "round-1",
				GameID:                "game-1",
				Kind:                  domain.WagerKindWin,
				Amount:                domain.NewMoney(3000, domain.BRL),
			},
		},
	)
	if err != nil {
		t.Fatalf("process WIN: %v", err)
	}

	// REFUND is only allowed to reference BET.
	_, err = service.Process(
		ctx,
		ProcessCommand{
			IdempotencyKey: "refund-win-invalid",
			Request: domain.WagerRequest{
				ProviderID:                     "provider-a",
				ExternalTransactionID:          "external-refund-win-invalid",
				PlayerID:                       "player-refund-win",
				WalletID:                       createdWallet.WalletID,
				RoundID:                        "round-1",
				GameID:                         "game-1",
				Kind:                           domain.WagerKindRefund,
				Amount:                         domain.NewMoney(3000, domain.BRL),
				ReferenceExternalTransactionID: "external-win-refund",
			},
		},
	)

	if !errors.Is(err, ErrInvalidReferenceKind) {
		t.Fatalf(
			"expected ErrInvalidReferenceKind, got %v",
			err,
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

	// 100 opening + 30 WIN.
	// Invalid REFUND must not change anything.
	if balance != 13000 {
		t.Fatalf(
			"expected balance to remain 13000, got %d",
			balance,
		)
	}

	if version != 2 {
		t.Fatalf(
			"expected wallet version to remain 2, got %d",
			version,
		)
	}

	var refundCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM wager_transactions
		WHERE wallet_id = $1
		  AND kind = 'REFUND'
		`,
		createdWallet.WalletID,
	).Scan(&refundCount)
	if err != nil {
		t.Fatalf("count REFUND transactions: %v", err)
	}

	if refundCount != 0 {
		t.Fatalf(
			"expected no REFUND transaction, got %d",
			refundCount,
		)
	}
}

func TestSecondRefundOfSameBetIsRejected(t *testing.T) {
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

	walletService := wallet.NewService(pool)

	createdWallet, err := walletService.Create(
		ctx,
		wallet.CreateWalletCommand{
			PlayerID:       "player-double-refund",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	service := NewService(pool)

	_, err = service.Process(
		ctx,
		ProcessCommand{
			IdempotencyKey: "bet-double-refund",
			Request: domain.WagerRequest{
				ProviderID:            "provider-a",
				ExternalTransactionID: "external-bet-double-refund",
				PlayerID:              "player-double-refund",
				WalletID:              createdWallet.WalletID,
				RoundID:               "round-1",
				GameID:                "game-1",
				Kind:                  domain.WagerKindBet,
				Amount:                domain.NewMoney(3000, domain.BRL),
			},
		},
	)
	if err != nil {
		t.Fatalf("process BET: %v", err)
	}

	firstRefund := ProcessCommand{
		IdempotencyKey: "refund-one",
		Request: domain.WagerRequest{
			ProviderID:                     "provider-a",
			ExternalTransactionID:          "external-refund-one",
			PlayerID:                       "player-double-refund",
			WalletID:                       createdWallet.WalletID,
			RoundID:                        "round-1",
			GameID:                         "game-1",
			Kind:                           domain.WagerKindRefund,
			Amount:                         domain.NewMoney(3000, domain.BRL),
			ReferenceExternalTransactionID: "external-bet-double-refund",
		},
	}

	result, err := service.Process(ctx, firstRefund)
	if err != nil {
		t.Fatalf("process first REFUND: %v", err)
	}

	if result.State != domain.WagerStateProcessed {
		t.Fatalf(
			"expected first REFUND PROCESSED, got %s",
			result.State,
		)
	}

	// This is intentionally NOT an idempotent replay.
	// It has a different external transaction ID and idempotency key,
	// but tries to refund the same BET again.
	secondRefund := ProcessCommand{
		IdempotencyKey: "refund-two",
		Request: domain.WagerRequest{
			ProviderID:                     "provider-a",
			ExternalTransactionID:          "external-refund-two",
			PlayerID:                       "player-double-refund",
			WalletID:                       createdWallet.WalletID,
			RoundID:                        "round-1",
			GameID:                         "game-1",
			Kind:                           domain.WagerKindRefund,
			Amount:                         domain.NewMoney(3000, domain.BRL),
			ReferenceExternalTransactionID: "external-bet-double-refund",
		},
	}

	_, err = service.Process(ctx, secondRefund)

	if err == nil {
		t.Fatal("expected second REFUND of same BET to fail")
	}

	// The failed INSERT must roll back the entire PostgreSQL transaction.
	// Therefore the wallet cannot receive a second credit.
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

	if balance != 10000 {
		t.Fatalf(
			"expected balance to remain 10000, got %d",
			balance,
		)
	}

	if version != 3 {
		t.Fatalf(
			"expected wallet version to remain 3, got %d",
			version,
		)
	}

	var processedRefundCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM wager_transactions
		WHERE wallet_id = $1
		  AND kind = 'REFUND'
		  AND state = 'PROCESSED'
		`,
		createdWallet.WalletID,
	).Scan(&processedRefundCount)
	if err != nil {
		t.Fatalf("count processed REFUND transactions: %v", err)
	}

	if processedRefundCount != 1 {
		t.Fatalf(
			"expected exactly 1 processed REFUND, got %d",
			processedRefundCount,
		)
	}

	var refundLedgerCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM ledger_entries le
		JOIN wager_transactions wt
		  ON wt.id = le.transaction_id
		WHERE wt.wallet_id = $1
		  AND wt.kind = 'REFUND'
		`,
		createdWallet.WalletID,
	).Scan(&refundLedgerCount)
	if err != nil {
		t.Fatalf("count REFUND ledger entries: %v", err)
	}

	if refundLedgerCount != 1 {
		t.Fatalf(
			"expected exactly 1 REFUND ledger entry, got %d",
			refundLedgerCount,
		)
	}
}

package wagering

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Tharik/wagering-platform/internal/application/wallet"
	"github.com/Tharik/wagering-platform/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestConcurrentRefundAndRollbackOfSameBetAllowsOnlyOneReversal(t *testing.T) {
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
			PlayerID:       "player-concurrent-reversal",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	service := NewService(pool)

	betResult, err := service.Process(
		ctx,
		ProcessCommand{
			IdempotencyKey: "bet-concurrent-reversal",
			Request: domain.WagerRequest{
				ProviderID:            "provider-a",
				ExternalTransactionID: "external-bet-concurrent-reversal",
				PlayerID:              "player-concurrent-reversal",
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

	if betResult.State != domain.WagerStateProcessed {
		t.Fatalf(
			"expected BET PROCESSED, got %s",
			betResult.State,
		)
	}

	refund := ProcessCommand{
		IdempotencyKey: "concurrent-refund",
		Request: domain.WagerRequest{
			ProviderID:                     "provider-a",
			ExternalTransactionID:          "external-concurrent-refund",
			PlayerID:                       "player-concurrent-reversal",
			WalletID:                       createdWallet.WalletID,
			RoundID:                        "round-1",
			GameID:                         "game-1",
			Kind:                           domain.WagerKindRefund,
			Amount:                         domain.NewMoney(3000, domain.BRL),
			ReferenceExternalTransactionID: "external-bet-concurrent-reversal",
		},
	}

	rollback := ProcessCommand{
		IdempotencyKey: "concurrent-rollback",
		Request: domain.WagerRequest{
			ProviderID:                     "provider-a",
			ExternalTransactionID:          "external-concurrent-rollback",
			PlayerID:                       "player-concurrent-reversal",
			WalletID:                       createdWallet.WalletID,
			RoundID:                        "round-1",
			GameID:                         "game-1",
			Kind:                           domain.WagerKindRollback,
			Amount:                         domain.NewMoney(3000, domain.BRL),
			ReferenceExternalTransactionID: "external-bet-concurrent-reversal",
		},
	}

	start := make(chan struct{})

	type processOutcome struct {
		result ProcessResult
		err    error
	}

	outcomes := make(chan processOutcome, 2)

	var wg sync.WaitGroup
	wg.Add(2)

	run := func(cmd ProcessCommand) {
		defer wg.Done()

		<-start

		result, err := service.Process(ctx, cmd)

		outcomes <- processOutcome{
			result: result,
			err:    err,
		}
	}

	go run(refund)
	go run(rollback)

	close(start)

	wg.Wait()
	close(outcomes)

	processed := 0
	rejected := 0

	for outcome := range outcomes {
		if outcome.err != nil {
			t.Fatalf(
				"concurrent reversal returned unexpected error: %v",
				outcome.err,
			)
		}

		switch outcome.result.State {
		case domain.WagerStateProcessed:
			processed++

		case domain.WagerStateRejected:
			rejected++

			if outcome.result.FailureCode != "ALREADY_REVERSED" {
				t.Fatalf(
					"expected rejected reversal to have ALREADY_REVERSED, got %s",
					outcome.result.FailureCode,
				)
			}

		default:
			t.Fatalf(
				"unexpected reversal state: %s",
				outcome.result.State,
			)
		}
	}

	if processed != 1 {
		t.Fatalf(
			"expected exactly 1 processed reversal, got %d",
			processed,
		)
	}

	if rejected != 1 {
		t.Fatalf(
			"expected exactly 1 rejected reversal, got %d",
			rejected,
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
	// Exactly one reversal = version 3.
	if balance != 10000 {
		t.Fatalf(
			"expected final balance 10000, got %d",
			balance,
		)
	}

	if version != 3 {
		t.Fatalf(
			"expected wallet version 3, got %d",
			version,
		)
	}

	var processedReversalCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM wager_transactions
		WHERE referenced_transaction_id = $1
		  AND kind IN ('REFUND', 'ROLLBACK')
		  AND state = 'PROCESSED'
		`,
		betResult.TransactionID,
	).Scan(&processedReversalCount)
	if err != nil {
		t.Fatalf(
			"count processed reversals: %v",
			err,
		)
	}

	if processedReversalCount != 1 {
		t.Fatalf(
			"expected exactly 1 processed reversal, got %d",
			processedReversalCount,
		)
	}

	var reversalLedgerCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM ledger_entries le
		JOIN wager_transactions wt
		  ON wt.id = le.transaction_id
		WHERE wt.referenced_transaction_id = $1
		  AND wt.kind IN ('REFUND', 'ROLLBACK')
		`,
		betResult.TransactionID,
	).Scan(&reversalLedgerCount)
	if err != nil {
		t.Fatalf(
			"count reversal ledger entries: %v",
			err,
		)
	}

	if reversalLedgerCount != 1 {
		t.Fatalf(
			"expected exactly 1 reversal ledger entry, got %d",
			reversalLedgerCount,
		)
	}
}

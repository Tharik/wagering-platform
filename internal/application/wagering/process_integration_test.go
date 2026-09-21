package wagering

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Tharik/wagering-platform/internal/application/wallet"
	"github.com/Tharik/wagering-platform/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestConcurrentBetsOnSameWalletAllowOnlyOneDebit(t *testing.T) {
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
			PlayerID:       "player-concurrent",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	service := NewService(pool)

	commands := []ProcessCommand{
		{
			IdempotencyKey: "bet-80-a",
			Request: domain.WagerRequest{
				ProviderID:            "provider-a",
				ExternalTransactionID: "external-bet-a",
				PlayerID:              "player-concurrent",
				WalletID:              createdWallet.WalletID,
				RoundID:               "round-1",
				GameID:                "game-1",
				Kind:                  domain.WagerKindBet,
				Amount:                domain.NewMoney(8000, domain.BRL),
			},
		},
		{
			IdempotencyKey: "bet-80-b",
			Request: domain.WagerRequest{
				ProviderID:            "provider-a",
				ExternalTransactionID: "external-bet-b",
				PlayerID:              "player-concurrent",
				WalletID:              createdWallet.WalletID,
				RoundID:               "round-1",
				GameID:                "game-1",
				Kind:                  domain.WagerKindBet,
				Amount:                domain.NewMoney(8000, domain.BRL),
			},
		},
	}

	type processOutcome struct {
		result ProcessResult
		err    error
	}

	start := make(chan struct{})
	outcomes := make(chan processOutcome, len(commands))

	var wg sync.WaitGroup

	for _, command := range commands {
		wg.Add(1)

		go func(cmd ProcessCommand) {
			defer wg.Done()

			// Both goroutines wait here so that they start as close
			// together as possible.
			<-start

			result, err := service.Process(ctx, cmd)

			outcomes <- processOutcome{
				result: result,
				err:    err,
			}
		}(command)
	}

	close(start)

	wg.Wait()
	close(outcomes)

	processed := 0
	rejected := 0

	for outcome := range outcomes {
		if outcome.err != nil {
			t.Fatalf("unexpected processing error: %v", outcome.err)
		}

		switch outcome.result.State {
		case domain.WagerStateProcessed:
			processed++

		case domain.WagerStateRejected:
			rejected++

			if outcome.result.FailureCode != "INSUFFICIENT_FUNDS" {
				t.Fatalf(
					"expected INSUFFICIENT_FUNDS, got %s",
					outcome.result.FailureCode,
				)
			}

		default:
			t.Fatalf(
				"unexpected transaction state: %s",
				outcome.result.State,
			)
		}
	}

	if processed != 1 {
		t.Fatalf("expected 1 processed bet, got %d", processed)
	}

	if rejected != 1 {
		t.Fatalf("expected 1 rejected bet, got %d", rejected)
	}

	var finalBalance int64

	err = pool.QueryRow(
		ctx,
		`
		SELECT balance
		FROM wallets
		WHERE id = $1
		`,
		createdWallet.WalletID,
	).Scan(&finalBalance)
	if err != nil {
		t.Fatalf("query final wallet balance: %v", err)
	}

	if finalBalance != 2000 {
		t.Fatalf(
			"expected final balance 2000, got %d",
			finalBalance,
		)
	}

	var debitCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM ledger_entries
		WHERE wallet_id = $1
		  AND direction = 'DEBIT'
		`,
		createdWallet.WalletID,
	).Scan(&debitCount)
	if err != nil {
		t.Fatalf("count debit ledger entries: %v", err)
	}

	if debitCount != 1 {
		t.Fatalf(
			"expected exactly 1 debit ledger entry, got %d",
			debitCount,
		)
	}

	var processedTransactions int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM wager_transactions
		WHERE wallet_id = $1
		  AND kind = 'BET'
		  AND state = 'PROCESSED'
		`,
		createdWallet.WalletID,
	).Scan(&processedTransactions)
	if err != nil {
		t.Fatalf("count processed transactions: %v", err)
	}

	if processedTransactions != 1 {
		t.Fatalf(
			"expected 1 processed BET, got %d",
			processedTransactions,
		)
	}

	var rejectedTransactions int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM wager_transactions
		WHERE wallet_id = $1
		  AND kind = 'BET'
		  AND state = 'REJECTED'
		`,
		createdWallet.WalletID,
	).Scan(&rejectedTransactions)
	if err != nil {
		t.Fatalf("count rejected transactions: %v", err)
	}

	if rejectedTransactions != 1 {
		t.Fatalf(
			"expected 1 rejected BET, got %d",
			rejectedTransactions,
		)
	}
}

func TestProcessedBetReplayDoesNotMoveMoneyAgain(t *testing.T) {
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
			PlayerID:       "player-idempotency",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	service := NewService(pool)

	command := ProcessCommand{
		IdempotencyKey: "idem-bet-1",
		Request: domain.WagerRequest{
			ProviderID:            "provider-a",
			ExternalTransactionID: "external-bet-1",
			PlayerID:              "player-idempotency",
			WalletID:              createdWallet.WalletID,
			RoundID:               "round-1",
			GameID:                "game-1",
			Kind:                  domain.WagerKindBet,
			Amount:                domain.NewMoney(3000, domain.BRL),
		},
	}

	first, err := service.Process(ctx, command)
	if err != nil {
		t.Fatalf("first processing: %v", err)
	}

	if first.State != domain.WagerStateProcessed {
		t.Fatalf("expected PROCESSED, got %s", first.State)
	}

	if first.IdempotentReplay {
		t.Fatal("first processing must not be marked as replay")
	}

	if first.Balance.Amount() != 7000 {
		t.Fatalf(
			"expected first balance 7000, got %d",
			first.Balance.Amount(),
		)
	}

	replay, err := service.Process(ctx, command)
	if err != nil {
		t.Fatalf("replay processing: %v", err)
	}

	if !replay.IdempotentReplay {
		t.Fatal("expected second processing to be an idempotent replay")
	}

	if replay.TransactionID != first.TransactionID {
		t.Fatalf(
			"expected original transaction %s, got %s",
			first.TransactionID,
			replay.TransactionID,
		)
	}

	if replay.Balance.Amount() != 7000 {
		t.Fatalf(
			"expected persisted original balance 7000, got %d",
			replay.Balance.Amount(),
		)
	}

	var finalBalance int64

	err = pool.QueryRow(
		ctx,
		`
		SELECT balance
		FROM wallets
		WHERE id = $1
		`,
		createdWallet.WalletID,
	).Scan(&finalBalance)
	if err != nil {
		t.Fatalf("query wallet: %v", err)
	}

	if finalBalance != 7000 {
		t.Fatalf(
			"expected final balance 7000, got %d",
			finalBalance,
		)
	}

	var betCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM wager_transactions
		WHERE wallet_id = $1
		  AND kind = 'BET'
		`,
		createdWallet.WalletID,
	).Scan(&betCount)
	if err != nil {
		t.Fatalf("count bets: %v", err)
	}

	if betCount != 1 {
		t.Fatalf(
			"expected exactly 1 BET transaction, got %d",
			betCount,
		)
	}

	var debitCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM ledger_entries
		WHERE wallet_id = $1
		  AND direction = 'DEBIT'
		`,
		createdWallet.WalletID,
	).Scan(&debitCount)
	if err != nil {
		t.Fatalf("count debits: %v", err)
	}

	if debitCount != 1 {
		t.Fatalf(
			"expected exactly 1 debit, got %d",
			debitCount,
		)
	}
}

func TestSameIdempotencyKeyWithDifferentPayloadReturnsConflict(t *testing.T) {
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
			PlayerID:       "player-conflict",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	service := NewService(pool)

	first := ProcessCommand{
		IdempotencyKey: "same-key",
		Request: domain.WagerRequest{
			ProviderID:            "provider-a",
			ExternalTransactionID: "external-bet-1",
			PlayerID:              "player-conflict",
			WalletID:              createdWallet.WalletID,
			RoundID:               "round-1",
			GameID:                "game-1",
			Kind:                  domain.WagerKindBet,
			Amount:                domain.NewMoney(3000, domain.BRL),
		},
	}

	_, err = service.Process(ctx, first)
	if err != nil {
		t.Fatalf("first processing: %v", err)
	}

	second := first
	second.Request.Amount = domain.NewMoney(4000, domain.BRL)

	_, err = service.Process(ctx, second)

	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf(
			"expected ErrIdempotencyConflict, got %v",
			err,
		)
	}

	var balance int64

	err = pool.QueryRow(
		ctx,
		"SELECT balance FROM wallets WHERE id = $1",
		createdWallet.WalletID,
	).Scan(&balance)
	if err != nil {
		t.Fatalf("query wallet: %v", err)
	}

	if balance != 7000 {
		t.Fatalf(
			"expected balance to remain 7000, got %d",
			balance,
		)
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

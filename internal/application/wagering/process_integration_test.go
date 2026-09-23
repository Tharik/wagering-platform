package wagering

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Tharik/wagering-platform/internal/application/wallet"
	"github.com/Tharik/wagering-platform/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestConcurrentBetsAcrossIndependentInstancesAllowOnlyOneDebit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const databaseURL = "postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable"

	setupPool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect setup postgres: %v", err)
	}
	defer setupPool.Close()

	cleanDatabase(t, ctx, setupPool)

	walletService := wallet.NewService(setupPool)

	createdWallet, err := walletService.Create(
		ctx,
		wallet.CreateWalletCommand{
			PlayerID:       "player-multi-instance",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	// Three independent connection pools simulate three independent
	// application instances. No in-memory synchronization is shared
	// between these services; coordination must happen in PostgreSQL.
	instancePools := make([]*pgxpool.Pool, 3)
	services := make([]*Service, 3)

	for i := range instancePools {
		instancePools[i], err = pgxpool.New(ctx, databaseURL)
		if err != nil {
			t.Fatalf("connect postgres for instance %d: %v", i+1, err)
		}
		defer instancePools[i].Close()

		services[i] = NewService(instancePools[i])
	}

	commands := []ProcessCommand{
		{
			IdempotencyKey: "multi-instance-bet-80-a",
			Request: domain.WagerRequest{
				ProviderID:            "provider-a",
				ExternalTransactionID: "external-multi-instance-bet-a",
				PlayerID:              "player-multi-instance",
				WalletID:              createdWallet.WalletID,
				RoundID:               "round-multi-instance",
				GameID:                "game-1",
				Kind:                  domain.WagerKindBet,
				Amount:                domain.NewMoney(8000, domain.BRL),
			},
		},
		{
			IdempotencyKey: "multi-instance-bet-80-b",
			Request: domain.WagerRequest{
				ProviderID:            "provider-a",
				ExternalTransactionID: "external-multi-instance-bet-b",
				PlayerID:              "player-multi-instance",
				WalletID:              createdWallet.WalletID,
				RoundID:               "round-multi-instance",
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

	// Requests intentionally go through different service instances.
	for i, command := range commands {
		wg.Add(1)

		go func(service *Service, cmd ProcessCommand) {
			defer wg.Done()

			<-start

			result, err := service.Process(ctx, cmd)

			outcomes <- processOutcome{
				result: result,
				err:    err,
			}
		}(services[i], command)
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
	var finalVersion int64

	err = setupPool.QueryRow(
		ctx,
		`
		SELECT balance, version
		FROM wallets
		WHERE id = $1
		`,
		createdWallet.WalletID,
	).Scan(&finalBalance, &finalVersion)
	if err != nil {
		t.Fatalf("query final wallet: %v", err)
	}

	if finalBalance != 2000 {
		t.Fatalf(
			"expected final balance 2000, got %d",
			finalBalance,
		)
	}

	if finalVersion != 2 {
		t.Fatalf(
			"expected wallet version 2, got %d",
			finalVersion,
		)
	}

	var debitCount int

	err = setupPool.QueryRow(
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

	err = setupPool.QueryRow(
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

	err = setupPool.QueryRow(
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

	// The third instance did not participate in these two requests on purpose.
	// Its existence demonstrates that the service can be independently
	// instantiated against the same database without shared process state.
	if services[2] == nil {
		t.Fatal("expected third independent service instance")
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

func TestProcessedWagerReplaySurvivesApplicationRestart(t *testing.T) {
	const databaseURL = "postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable"

	identity := uuid.NewString()
	playerID := "player-restart-replay-" + identity
	externalTransactionID := "external-restart-replay-" + identity
	idempotencyKey := "idempotency-restart-replay-" + identity

	var walletID string
	var firstResult ProcessResult

	func() {
		firstCtx, firstCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer firstCancel()

		firstPool, err := pgxpool.New(firstCtx, databaseURL)
		if err != nil {
			t.Fatalf("connect first application pool: %v", err)
		}
		defer firstPool.Close()

		cleanDatabase(t, firstCtx, firstPool)

		createdWallet, err := wallet.NewService(firstPool).Create(
			firstCtx,
			wallet.CreateWalletCommand{
				PlayerID:       playerID,
				InitialBalance: domain.NewMoney(10000, domain.BRL),
			},
		)
		if err != nil {
			t.Fatalf("create wallet: %v", err)
		}
		walletID = createdWallet.WalletID

		command := ProcessCommand{
			IdempotencyKey: idempotencyKey,
			Request: domain.WagerRequest{
				ProviderID:            "provider-a",
				ExternalTransactionID: externalTransactionID,
				PlayerID:              playerID,
				WalletID:              walletID,
				RoundID:               "round-restart-replay",
				GameID:                "game-restart-replay",
				Kind:                  domain.WagerKindBet,
				Amount:                domain.NewMoney(1000, domain.BRL),
			},
		}

		firstResult, err = NewService(firstPool).Process(firstCtx, command)
		if err != nil {
			t.Fatalf("process wager before restart: %v", err)
		}
		if firstResult.State != domain.WagerStateProcessed {
			t.Fatalf("expected first wager PROCESSED, got %s", firstResult.State)
		}
		if firstResult.IdempotentReplay {
			t.Fatal("first processing must not be an idempotent replay")
		}
		if firstResult.Balance.Amount() != 9000 {
			t.Fatalf("expected first result balance 9000, got %d", firstResult.Balance.Amount())
		}

		var ledgerCount int
		if err := firstPool.QueryRow(
			firstCtx,
			`SELECT COUNT(*) FROM ledger_entries WHERE transaction_id = $1`,
			firstResult.TransactionID,
		).Scan(&ledgerCount); err != nil {
			t.Fatalf("count first wager ledger entries: %v", err)
		}
		if ledgerCount != 1 {
			t.Fatalf("expected one first wager ledger entry, got %d", ledgerCount)
		}
	}()

	secondCtx, secondCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer secondCancel()

	secondPool, err := pgxpool.New(secondCtx, databaseURL)
	if err != nil {
		t.Fatalf("connect restarted application pool: %v", err)
	}
	defer secondPool.Close()

	secondService := NewService(secondPool)
	command := ProcessCommand{
		IdempotencyKey: idempotencyKey,
		Request: domain.WagerRequest{
			ProviderID:            "provider-a",
			ExternalTransactionID: externalTransactionID,
			PlayerID:              playerID,
			WalletID:              walletID,
			RoundID:               "round-restart-replay",
			GameID:                "game-restart-replay",
			Kind:                  domain.WagerKindBet,
			Amount:                domain.NewMoney(1000, domain.BRL),
		},
	}

	var balanceBeforeReplay, versionBeforeReplay int64
	if err := secondPool.QueryRow(
		secondCtx,
		`SELECT balance, version FROM wallets WHERE id = $1`,
		walletID,
	).Scan(&balanceBeforeReplay, &versionBeforeReplay); err != nil {
		t.Fatalf("load wallet before replay: %v", err)
	}

	replay, err := secondService.Process(secondCtx, command)
	if err != nil {
		t.Fatalf("replay wager after restart: %v", err)
	}
	if !replay.IdempotentReplay {
		t.Fatal("expected replay after restart to be idempotent")
	}
	if replay.TransactionID != firstResult.TransactionID {
		t.Fatalf("expected original transaction %s, got %s", firstResult.TransactionID, replay.TransactionID)
	}
	if replay.State != domain.WagerStateProcessed {
		t.Fatalf("expected replay state PROCESSED, got %s", replay.State)
	}
	if replay.Balance.Amount() != firstResult.Balance.Amount() {
		t.Fatalf("expected persisted result balance %d, got %d", firstResult.Balance.Amount(), replay.Balance.Amount())
	}

	var balanceAfterReplay, versionAfterReplay int64
	if err := secondPool.QueryRow(
		secondCtx,
		`SELECT balance, version FROM wallets WHERE id = $1`,
		walletID,
	).Scan(&balanceAfterReplay, &versionAfterReplay); err != nil {
		t.Fatalf("load wallet after replay: %v", err)
	}
	if balanceBeforeReplay != 9000 || balanceAfterReplay != balanceBeforeReplay {
		t.Fatalf("expected wallet balance to remain 9000, before=%d after=%d", balanceBeforeReplay, balanceAfterReplay)
	}
	if versionBeforeReplay != 2 || versionAfterReplay != versionBeforeReplay {
		t.Fatalf("expected wallet version to remain 2, before=%d after=%d", versionBeforeReplay, versionAfterReplay)
	}

	var wagerCount, ledgerCount, processedEventCount, balanceEventCount int
	if err := secondPool.QueryRow(
		secondCtx,
		`SELECT COUNT(*) FROM wager_transactions WHERE provider_id = $1 AND external_transaction_id = $2 AND idempotency_key = $3`,
		"provider-a",
		externalTransactionID,
		idempotencyKey,
	).Scan(&wagerCount); err != nil {
		t.Fatalf("count persisted wagers: %v", err)
	}
	if err := secondPool.QueryRow(
		secondCtx,
		`SELECT COUNT(*) FROM ledger_entries WHERE transaction_id = $1`,
		firstResult.TransactionID,
	).Scan(&ledgerCount); err != nil {
		t.Fatalf("count persisted ledger entries: %v", err)
	}
	if err := secondPool.QueryRow(
		secondCtx,
		`SELECT COUNT(*) FILTER (WHERE event_type = 'WagerTransactionProcessed'), COUNT(*) FILTER (WHERE event_type = 'WalletBalanceChanged') FROM outbox_events WHERE aggregate_id = $1`,
		firstResult.TransactionID,
	).Scan(&processedEventCount, &balanceEventCount); err != nil {
		t.Fatalf("count persisted outbox events: %v", err)
	}

	if wagerCount != 1 {
		t.Fatalf("expected one persisted wager, got %d", wagerCount)
	}
	if ledgerCount != 1 {
		t.Fatalf("expected one persisted wager ledger entry, got %d", ledgerCount)
	}
	if processedEventCount != 1 || balanceEventCount != 1 {
		t.Fatalf("expected one processed and one balance event, got processed=%d balance=%d", processedEventCount, balanceEventCount)
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

func TestSameBetFiftyTimesInParallelMovesMoneyOnlyOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
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
			PlayerID:       "player-duplicate",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	service := NewService(pool)

	command := ProcessCommand{
		IdempotencyKey: "same-bet-50-times",
		Request: domain.WagerRequest{
			ProviderID:            "provider-a",
			ExternalTransactionID: "external-same-bet",
			PlayerID:              "player-duplicate",
			WalletID:              createdWallet.WalletID,
			RoundID:               "round-1",
			GameID:                "game-1",
			Kind:                  domain.WagerKindBet,
			Amount:                domain.NewMoney(3000, domain.BRL),
		},
	}

	const workers = 50

	type outcome struct {
		result ProcessResult
		err    error
	}

	start := make(chan struct{})
	outcomes := make(chan outcome, workers)

	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			<-start

			result, err := service.Process(ctx, command)

			outcomes <- outcome{
				result: result,
				err:    err,
			}
		}()
	}

	close(start)

	wg.Wait()
	close(outcomes)

	successes := 0
	replays := 0

	var originalTransactionID string

	for outcome := range outcomes {
		if outcome.err != nil {
			t.Fatalf(
				"expected all duplicate requests to succeed, got: %v",
				outcome.err,
			)
		}

		if outcome.result.State != domain.WagerStateProcessed {
			t.Fatalf(
				"expected PROCESSED, got %s",
				outcome.result.State,
			)
		}

		if outcome.result.Balance.Amount() != 7000 {
			t.Fatalf(
				"expected original result balance 7000, got %d",
				outcome.result.Balance.Amount(),
			)
		}

		if outcome.result.IdempotentReplay {
			replays++
		} else {
			successes++
			originalTransactionID = outcome.result.TransactionID
		}
	}

	if successes != 1 {
		t.Fatalf(
			"expected exactly 1 original processing, got %d",
			successes,
		)
	}

	if replays != workers-1 {
		t.Fatalf(
			"expected %d replays, got %d",
			workers-1,
			replays,
		)
	}

	if originalTransactionID == "" {
		t.Fatal("expected original transaction ID")
	}

	var finalBalance int64

	err = pool.QueryRow(
		ctx,
		"SELECT balance FROM wallets WHERE id = $1",
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

func TestWinCreditsWalletAndCreatesLedgerEntry(t *testing.T) {
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
			PlayerID:       "player-win",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	service := NewService(pool)

	result, err := service.Process(
		ctx,
		ProcessCommand{
			IdempotencyKey: "win-25",
			Request: domain.WagerRequest{
				ProviderID:            "provider-a",
				ExternalTransactionID: "external-win-1",
				PlayerID:              "player-win",
				WalletID:              createdWallet.WalletID,
				RoundID:               "round-1",
				GameID:                "game-1",
				Kind:                  domain.WagerKindWin,
				Amount:                domain.NewMoney(2500, domain.BRL),
			},
		},
	)
	if err != nil {
		t.Fatalf("process WIN: %v", err)
	}

	if result.State != domain.WagerStateProcessed {
		t.Fatalf("expected PROCESSED, got %s", result.State)
	}

	if result.Balance.Amount() != 12500 {
		t.Fatalf(
			"expected result balance 12500, got %d",
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

	if balance != 12500 {
		t.Fatalf("expected wallet balance 12500, got %d", balance)
	}

	if version != 2 {
		t.Fatalf("expected wallet version 2, got %d", version)
	}

	var creditCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM ledger_entries
		WHERE wallet_id = $1
		  AND direction = 'CREDIT'
		  AND amount = 2500
		  AND balance_before = 10000
		  AND balance_after = 12500
		`,
		createdWallet.WalletID,
	).Scan(&creditCount)
	if err != nil {
		t.Fatalf("count WIN ledger entries: %v", err)
	}

	if creditCount != 1 {
		t.Fatalf(
			"expected exactly 1 WIN credit ledger entry, got %d",
			creditCount,
		)
	}

	var winCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM wager_transactions
		WHERE wallet_id = $1
		  AND kind = 'WIN'
		  AND state = 'PROCESSED'
		  AND amount = 2500
		  AND result_balance = 12500
		`,
		createdWallet.WalletID,
	).Scan(&winCount)
	if err != nil {
		t.Fatalf("count WIN transactions: %v", err)
	}

	if winCount != 1 {
		t.Fatalf("expected exactly 1 processed WIN, got %d", winCount)
	}
}

func TestLossDoesNotMoveMoneyOrCreateLedgerEntry(t *testing.T) {
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
			PlayerID:       "player-loss",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	service := NewService(pool)

	result, err := service.Process(
		ctx,
		ProcessCommand{
			IdempotencyKey: "loss-1",
			Request: domain.WagerRequest{
				ProviderID:            "provider-a",
				ExternalTransactionID: "external-loss-1",
				PlayerID:              "player-loss",
				WalletID:              createdWallet.WalletID,
				RoundID:               "round-1",
				GameID:                "game-1",
				Kind:                  domain.WagerKindLoss,
				Amount:                domain.Zero(domain.BRL),
			},
		},
	)
	if err != nil {
		t.Fatalf("process LOSS: %v", err)
	}

	if result.State != domain.WagerStateProcessed {
		t.Fatalf("expected PROCESSED, got %s", result.State)
	}

	if result.Balance.Amount() != 10000 {
		t.Fatalf(
			"expected result balance 10000, got %d",
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

	if balance != 10000 {
		t.Fatalf("expected wallet balance 10000, got %d", balance)
	}

	// Creating the wallet starts at version 1.
	// LOSS must not move money, so it must not increment the version.
	if version != 1 {
		t.Fatalf("expected wallet version to remain 1, got %d", version)
	}

	var lossCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM wager_transactions
		WHERE wallet_id = $1
		  AND kind = 'LOSS'
		  AND state = 'PROCESSED'
		  AND amount = 0
		  AND result_balance = 10000
		`,
		createdWallet.WalletID,
	).Scan(&lossCount)
	if err != nil {
		t.Fatalf("count LOSS transactions: %v", err)
	}

	if lossCount != 1 {
		t.Fatalf("expected exactly 1 processed LOSS, got %d", lossCount)
	}

	var lossLedgerCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM ledger_entries le
		JOIN wager_transactions wt
		  ON wt.id = le.transaction_id
		WHERE wt.wallet_id = $1
		  AND wt.kind = 'LOSS'
		`,
		createdWallet.WalletID,
	).Scan(&lossLedgerCount)
	if err != nil {
		t.Fatalf("count LOSS ledger entries: %v", err)
	}

	if lossLedgerCount != 0 {
		t.Fatalf(
			"expected LOSS to create no ledger entry, got %d",
			lossLedgerCount,
		)
	}
}

func TestLossWithNonZeroAmountIsRejectedBeforePersistence(t *testing.T) {
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
			PlayerID:       "player-invalid-loss",
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
			IdempotencyKey: "invalid-loss",
			Request: domain.WagerRequest{
				ProviderID:            "provider-a",
				ExternalTransactionID: "external-invalid-loss",
				PlayerID:              "player-invalid-loss",
				WalletID:              createdWallet.WalletID,
				RoundID:               "round-1",
				GameID:                "game-1",
				Kind:                  domain.WagerKindLoss,
				Amount:                domain.NewMoney(100, domain.BRL),
			},
		},
	)

	if !errors.Is(err, ErrInvalidLossAmount) {
		t.Fatalf(
			"expected ErrInvalidLossAmount, got %v",
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

	if balance != 10000 {
		t.Fatalf("expected balance to remain 10000, got %d", balance)
	}

	if version != 1 {
		t.Fatalf("expected version to remain 1, got %d", version)
	}

	var transactionCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM wager_transactions
		WHERE wallet_id = $1
		  AND kind = 'LOSS'
		`,
		createdWallet.WalletID,
	).Scan(&transactionCount)
	if err != nil {
		t.Fatalf("count LOSS transactions: %v", err)
	}

	if transactionCount != 0 {
		t.Fatalf(
			"expected no LOSS transaction to be persisted, got %d",
			transactionCount,
		)
	}
}

func TestRefundRestoresBetAmountAndReferencesOriginalBet(t *testing.T) {
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
			PlayerID:       "player-refund",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	service := NewService(pool)

	// First place a BET of 30.
	bet, err := service.Process(
		ctx,
		ProcessCommand{
			IdempotencyKey: "bet-for-refund",
			Request: domain.WagerRequest{
				ProviderID:            "provider-a",
				ExternalTransactionID: "external-bet-refund-1",
				PlayerID:              "player-refund",
				WalletID:              createdWallet.WalletID,
				RoundID:               "round-refund-1",
				GameID:                "game-1",
				Kind:                  domain.WagerKindBet,
				Amount:                domain.NewMoney(3000, domain.BRL),
			},
		},
	)
	if err != nil {
		t.Fatalf("process BET: %v", err)
	}

	if bet.State != domain.WagerStateProcessed {
		t.Fatalf("expected BET PROCESSED, got %s", bet.State)
	}

	if bet.Balance.Amount() != 7000 {
		t.Fatalf(
			"expected balance 7000 after BET, got %d",
			bet.Balance.Amount(),
		)
	}

	// Refund the complete BET.
	refund, err := service.Process(
		ctx,
		ProcessCommand{
			IdempotencyKey: "refund-bet-1",
			Request: domain.WagerRequest{
				ProviderID:                     "provider-a",
				ExternalTransactionID:          "external-refund-1",
				PlayerID:                       "player-refund",
				WalletID:                       createdWallet.WalletID,
				RoundID:                        "round-refund-1",
				GameID:                         "game-1",
				Kind:                           domain.WagerKindRefund,
				Amount:                         domain.NewMoney(3000, domain.BRL),
				ReferenceExternalTransactionID: "external-bet-refund-1",
			},
		},
	)
	if err != nil {
		t.Fatalf("process REFUND: %v", err)
	}

	if refund.State != domain.WagerStateProcessed {
		t.Fatalf(
			"expected REFUND PROCESSED, got %s",
			refund.State,
		)
	}

	if refund.Balance.Amount() != 10000 {
		t.Fatalf(
			"expected balance 10000 after REFUND, got %d",
			refund.Balance.Amount(),
		)
	}

	// Wallet:
	// version 1 = creation
	// version 2 = BET
	// version 3 = REFUND
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
			"expected persisted balance 10000, got %d",
			balance,
		)
	}

	if version != 3 {
		t.Fatalf(
			"expected wallet version 3, got %d",
			version,
		)
	}

	// The REFUND must point to the original BET using both:
	// - external reference
	// - internal transaction UUID
	var referenceExternalID string
	var referencedTransactionID string
	var refundAmount int64
	var refundResultBalance int64

	err = pool.QueryRow(
		ctx,
		`
		SELECT
			reference_external_transaction_id,
			referenced_transaction_id::text,
			amount,
			result_balance
		FROM wager_transactions
		WHERE provider_id = $1
		  AND external_transaction_id = $2
		  AND kind = 'REFUND'
		  AND state = 'PROCESSED'
		`,
		"provider-a",
		"external-refund-1",
	).Scan(
		&referenceExternalID,
		&referencedTransactionID,
		&refundAmount,
		&refundResultBalance,
	)
	if err != nil {
		t.Fatalf("query REFUND transaction: %v", err)
	}

	if referenceExternalID != "external-bet-refund-1" {
		t.Fatalf(
			"expected external reference external-bet-refund-1, got %s",
			referenceExternalID,
		)
	}

	if referencedTransactionID != bet.TransactionID {
		t.Fatalf(
			"expected REFUND to reference BET %s, got %s",
			bet.TransactionID,
			referencedTransactionID,
		)
	}

	if refundAmount != 3000 {
		t.Fatalf(
			"expected REFUND amount 3000, got %d",
			refundAmount,
		)
	}

	if refundResultBalance != 10000 {
		t.Fatalf(
			"expected REFUND result balance 10000, got %d",
			refundResultBalance,
		)
	}

	// The REFUND must create exactly one CREDIT ledger entry.
	var refundCreditCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM ledger_entries le
		JOIN wager_transactions wt
		  ON wt.id = le.transaction_id
		WHERE wt.wallet_id = $1
		  AND wt.kind = 'REFUND'
		  AND le.direction = 'CREDIT'
		  AND le.amount = 3000
		  AND le.balance_before = 7000
		  AND le.balance_after = 10000
		`,
		createdWallet.WalletID,
	).Scan(&refundCreditCount)
	if err != nil {
		t.Fatalf("count REFUND ledger entries: %v", err)
	}

	if refundCreditCount != 1 {
		t.Fatalf(
			"expected exactly 1 REFUND credit ledger entry, got %d",
			refundCreditCount,
		)
	}

	// Financial history for this wallet must now contain:
	// OPENING CREDIT + BET DEBIT + REFUND CREDIT.
	var ledgerCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM ledger_entries
		WHERE wallet_id = $1
		`,
		createdWallet.WalletID,
	).Scan(&ledgerCount)
	if err != nil {
		t.Fatalf("count wallet ledger entries: %v", err)
	}

	if ledgerCount != 3 {
		t.Fatalf(
			"expected 3 total ledger entries, got %d",
			ledgerCount,
		)
	}
}

func TestConcurrentSameIdempotencyKeyAcrossDifferentWallets(t *testing.T) {
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

	walletA, err := walletService.Create(
		ctx,
		wallet.CreateWalletCommand{
			PlayerID:       "player-race-a",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet A: %v", err)
	}

	walletB, err := walletService.Create(
		ctx,
		wallet.CreateWalletCommand{
			PlayerID:       "player-race-b",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet B: %v", err)
	}

	service := NewService(pool)

	commands := []ProcessCommand{
		{
			IdempotencyKey: "same-idempotency-key",
			Request: domain.WagerRequest{
				ProviderID:            "provider-race",
				ExternalTransactionID: "external-race-a",
				PlayerID:              "player-race-a",
				WalletID:              walletA.WalletID,
				RoundID:               "round-race",
				GameID:                "game-race",
				Kind:                  domain.WagerKindBet,
				Amount:                domain.NewMoney(1000, domain.BRL),
			},
		},
		{
			IdempotencyKey: "same-idempotency-key",
			Request: domain.WagerRequest{
				ProviderID:            "provider-race",
				ExternalTransactionID: "external-race-b",
				PlayerID:              "player-race-b",
				WalletID:              walletB.WalletID,
				RoundID:               "round-race",
				GameID:                "game-race",
				Kind:                  domain.WagerKindBet,
				Amount:                domain.NewMoney(1000, domain.BRL),
			},
		},
	}

	type outcome struct {
		result ProcessResult
		err    error
	}

	start := make(chan struct{})
	outcomes := make(chan outcome, len(commands))

	var wg sync.WaitGroup

	for _, command := range commands {
		wg.Add(1)

		go func(cmd ProcessCommand) {
			defer wg.Done()

			<-start

			result, err := service.Process(ctx, cmd)

			outcomes <- outcome{
				result: result,
				err:    err,
			}
		}(command)
	}

	close(start)

	wg.Wait()
	close(outcomes)

	processed := 0
	conflicts := 0

	for outcome := range outcomes {
		switch {
		case outcome.err == nil:
			if outcome.result.State != domain.WagerStateProcessed {
				t.Fatalf(
					"expected successful request to be PROCESSED, got %s",
					outcome.result.State,
				)
			}

			processed++

		case errors.Is(outcome.err, ErrIdempotencyConflict):
			conflicts++

		default:
			t.Fatalf("unexpected processing error: %v", outcome.err)
		}
	}

	if processed != 1 {
		t.Fatalf("expected exactly 1 processed request, got %d", processed)
	}

	if conflicts != 1 {
		t.Fatalf("expected exactly 1 idempotency conflict, got %d", conflicts)
	}

	var transactionCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM wager_transactions
		WHERE provider_id = 'provider-race'
		  AND idempotency_key = 'same-idempotency-key'
		`,
	).Scan(&transactionCount)
	if err != nil {
		t.Fatalf("count wager transactions: %v", err)
	}

	if transactionCount != 1 {
		t.Fatalf("expected exactly 1 persisted transaction, got %d", transactionCount)
	}

	var balanceA int64
	var balanceB int64

	err = pool.QueryRow(
		ctx,
		`SELECT balance FROM wallets WHERE id = $1`,
		walletA.WalletID,
	).Scan(&balanceA)
	if err != nil {
		t.Fatalf("read wallet A: %v", err)
	}

	err = pool.QueryRow(
		ctx,
		`SELECT balance FROM wallets WHERE id = $1`,
		walletB.WalletID,
	).Scan(&balanceB)
	if err != nil {
		t.Fatalf("read wallet B: %v", err)
	}

	if !((balanceA == 9000 && balanceB == 10000) ||
		(balanceA == 10000 && balanceB == 9000)) {
		t.Fatalf(
			"expected exactly one wallet to be debited; got walletA=%d walletB=%d",
			balanceA,
			balanceB,
		)
	}
}

func TestConcurrentSameExternalTransactionAcrossDifferentWallets(t *testing.T) {
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

	walletA, err := walletService.Create(
		ctx,
		wallet.CreateWalletCommand{
			PlayerID:       "player-external-race-a",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet A: %v", err)
	}

	walletB, err := walletService.Create(
		ctx,
		wallet.CreateWalletCommand{
			PlayerID:       "player-external-race-b",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet B: %v", err)
	}

	service := NewService(pool)

	commands := []ProcessCommand{
		{
			IdempotencyKey: "external-race-idem-a",
			Request: domain.WagerRequest{
				ProviderID:            "provider-external-race",
				ExternalTransactionID: "same-external-transaction",
				PlayerID:              "player-external-race-a",
				WalletID:              walletA.WalletID,
				RoundID:               "round-external-race",
				GameID:                "game-external-race",
				Kind:                  domain.WagerKindBet,
				Amount:                domain.NewMoney(1000, domain.BRL),
			},
		},
		{
			IdempotencyKey: "external-race-idem-b",
			Request: domain.WagerRequest{
				ProviderID:            "provider-external-race",
				ExternalTransactionID: "same-external-transaction",
				PlayerID:              "player-external-race-b",
				WalletID:              walletB.WalletID,
				RoundID:               "round-external-race",
				GameID:                "game-external-race",
				Kind:                  domain.WagerKindBet,
				Amount:                domain.NewMoney(1000, domain.BRL),
			},
		},
	}

	type outcome struct {
		result ProcessResult
		err    error
	}

	start := make(chan struct{})
	outcomes := make(chan outcome, len(commands))

	var wg sync.WaitGroup

	for _, command := range commands {
		wg.Add(1)

		go func(cmd ProcessCommand) {
			defer wg.Done()

			<-start

			result, err := service.Process(ctx, cmd)

			outcomes <- outcome{
				result: result,
				err:    err,
			}
		}(command)
	}

	close(start)

	wg.Wait()
	close(outcomes)

	processed := 0
	duplicates := 0

	for outcome := range outcomes {
		switch {
		case outcome.err == nil:
			if outcome.result.State != domain.WagerStateProcessed {
				t.Fatalf(
					"expected successful request to be PROCESSED, got %s",
					outcome.result.State,
				)
			}
			processed++

		case errors.Is(outcome.err, ErrExternalTransactionExists):
			duplicates++

		default:
			t.Fatalf("unexpected processing error: %v", outcome.err)
		}
	}

	if processed != 1 {
		t.Fatalf("expected exactly 1 processed request, got %d", processed)
	}

	if duplicates != 1 {
		t.Fatalf("expected exactly 1 external transaction conflict, got %d", duplicates)
	}

	var transactionCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM wager_transactions
		WHERE provider_id = 'provider-external-race'
		  AND external_transaction_id = 'same-external-transaction'
		`,
	).Scan(&transactionCount)
	if err != nil {
		t.Fatalf("count wager transactions: %v", err)
	}

	if transactionCount != 1 {
		t.Fatalf("expected exactly 1 persisted transaction, got %d", transactionCount)
	}

	var balanceA int64
	var balanceB int64

	err = pool.QueryRow(
		ctx,
		`SELECT balance FROM wallets WHERE id = $1`,
		walletA.WalletID,
	).Scan(&balanceA)
	if err != nil {
		t.Fatalf("read wallet A: %v", err)
	}

	err = pool.QueryRow(
		ctx,
		`SELECT balance FROM wallets WHERE id = $1`,
		walletB.WalletID,
	).Scan(&balanceB)
	if err != nil {
		t.Fatalf("read wallet B: %v", err)
	}

	if !((balanceA == 9000 && balanceB == 10000) ||
		(balanceA == 10000 && balanceB == 9000)) {
		t.Fatalf(
			"expected exactly one wallet to be debited; got walletA=%d walletB=%d",
			balanceA,
			balanceB,
		)
	}
}

func TestConcurrentBetsOnDifferentWalletsAreProcessedIndependently(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const databaseURL = "postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable"

	setupPool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect setup postgres: %v", err)
	}
	defer setupPool.Close()

	cleanDatabase(t, ctx, setupPool)

	walletService := wallet.NewService(setupPool)

	walletA, err := walletService.Create(
		ctx,
		wallet.CreateWalletCommand{
			PlayerID:       "player-parallel-a",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet A: %v", err)
	}

	walletB, err := walletService.Create(
		ctx,
		wallet.CreateWalletCommand{
			PlayerID:       "player-parallel-b",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet B: %v", err)
	}

	// Independent pools/services simulate requests handled by
	// different application instances.
	poolA, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect instance A: %v", err)
	}
	defer poolA.Close()

	poolB, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect instance B: %v", err)
	}
	defer poolB.Close()

	serviceA := NewService(poolA)
	serviceB := NewService(poolB)

	commandA := ProcessCommand{
		IdempotencyKey: "parallel-wallet-a",
		Request: domain.WagerRequest{
			ProviderID:            "provider-a",
			ExternalTransactionID: "external-parallel-a",
			PlayerID:              "player-parallel-a",
			WalletID:              walletA.WalletID,
			RoundID:               "round-parallel-a",
			GameID:                "game-1",
			Kind:                  domain.WagerKindBet,
			Amount:                domain.NewMoney(3000, domain.BRL),
		},
	}

	commandB := ProcessCommand{
		IdempotencyKey: "parallel-wallet-b",
		Request: domain.WagerRequest{
			ProviderID:            "provider-a",
			ExternalTransactionID: "external-parallel-b",
			PlayerID:              "player-parallel-b",
			WalletID:              walletB.WalletID,
			RoundID:               "round-parallel-b",
			GameID:                "game-1",
			Kind:                  domain.WagerKindBet,
			Amount:                domain.NewMoney(4000, domain.BRL),
		},
	}

	type outcome struct {
		name   string
		result ProcessResult
		err    error
	}

	start := make(chan struct{})
	outcomes := make(chan outcome, 2)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		<-start

		result, err := serviceA.Process(ctx, commandA)
		outcomes <- outcome{
			name:   "wallet A",
			result: result,
			err:    err,
		}
	}()

	go func() {
		defer wg.Done()
		<-start

		result, err := serviceB.Process(ctx, commandB)
		outcomes <- outcome{
			name:   "wallet B",
			result: result,
			err:    err,
		}
	}()

	close(start)

	wg.Wait()
	close(outcomes)

	for outcome := range outcomes {
		if outcome.err != nil {
			t.Fatalf("%s processing failed: %v", outcome.name, outcome.err)
		}

		if outcome.result.State != domain.WagerStateProcessed {
			t.Fatalf(
				"%s expected PROCESSED, got %s",
				outcome.name,
				outcome.result.State,
			)
		}
	}

	var balanceA int64
	var versionA int64

	err = setupPool.QueryRow(
		ctx,
		`
		SELECT balance, version
		FROM wallets
		WHERE id = $1
		`,
		walletA.WalletID,
	).Scan(&balanceA, &versionA)
	if err != nil {
		t.Fatalf("query wallet A: %v", err)
	}

	var balanceB int64
	var versionB int64

	err = setupPool.QueryRow(
		ctx,
		`
		SELECT balance, version
		FROM wallets
		WHERE id = $1
		`,
		walletB.WalletID,
	).Scan(&balanceB, &versionB)
	if err != nil {
		t.Fatalf("query wallet B: %v", err)
	}

	if balanceA != 7000 {
		t.Fatalf("expected wallet A balance 7000, got %d", balanceA)
	}

	if balanceB != 6000 {
		t.Fatalf("expected wallet B balance 6000, got %d", balanceB)
	}

	if versionA != 2 {
		t.Fatalf("expected wallet A version 2, got %d", versionA)
	}

	if versionB != 2 {
		t.Fatalf("expected wallet B version 2, got %d", versionB)
	}

	var debitCountA int

	err = setupPool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM ledger_entries
		WHERE wallet_id = $1
		  AND direction = 'DEBIT'
		`,
		walletA.WalletID,
	).Scan(&debitCountA)
	if err != nil {
		t.Fatalf("count wallet A debits: %v", err)
	}

	var debitCountB int

	err = setupPool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM ledger_entries
		WHERE wallet_id = $1
		  AND direction = 'DEBIT'
		`,
		walletB.WalletID,
	).Scan(&debitCountB)
	if err != nil {
		t.Fatalf("count wallet B debits: %v", err)
	}

	if debitCountA != 1 {
		t.Fatalf(
			"expected exactly 1 debit for wallet A, got %d",
			debitCountA,
		)
	}

	if debitCountB != 1 {
		t.Fatalf(
			"expected exactly 1 debit for wallet B, got %d",
			debitCountB,
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

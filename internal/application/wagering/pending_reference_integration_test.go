package wagering

import (
	"context"
	"testing"
	"time"

	"github.com/Tharik/wagering-platform/internal/application/wallet"
	"github.com/Tharik/wagering-platform/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMissingReferenceCreatesPendingReferenceWithoutMovingMoney(t *testing.T) {
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
			PlayerID:       "player-pending-reference",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	service := NewService(pool)

	cmd := ProcessCommand{
		IdempotencyKey: "rollback-before-bet",
		CorrelationID:  "correlation-pending-persisted",
		CausationID:    "message-pending-persisted",
		Request: domain.WagerRequest{
			ProviderID:                     "provider-a",
			ExternalTransactionID:          "rollback-before-bet-1",
			PlayerID:                       "player-pending-reference",
			WalletID:                       createdWallet.WalletID,
			RoundID:                        "round-1",
			GameID:                         "game-1",
			Kind:                           domain.WagerKindRollback,
			Amount:                         domain.NewMoney(3000, domain.BRL),
			ReferenceExternalTransactionID: "bet-that-does-not-exist-yet",
		},
	}

	result, err := service.Process(ctx, cmd)
	if err != nil {
		t.Fatalf("process missing-reference ROLLBACK: %v", err)
	}

	if result.State != domain.WagerStatePendingReference {
		t.Fatalf(
			"expected PENDING_REFERENCE, got %s",
			result.State,
		)
	}

	if result.Balance.Amount() != 10000 {
		t.Fatalf(
			"expected unchanged balance 10000, got %d",
			result.Balance.Amount(),
		)
	}

	if result.IdempotentReplay {
		t.Fatal("first request must not be an idempotent replay")
	}

	var (
		state                   string
		referenceExternalID     string
		referencedTransactionID *string
		resultBalance           int64
		referenceAttempts       int
		referenceNextAttemptAt  *time.Time
		referenceExpiresAt      *time.Time
		correlationID           string
		causationID             string
	)

	err = pool.QueryRow(
		ctx,
		`
		SELECT
			state,
			reference_external_transaction_id,
			referenced_transaction_id::text,
			result_balance,
			reference_attempts,
			reference_next_attempt_at,
			reference_expires_at,
			correlation_id,
			causation_id
		FROM wager_transactions
		WHERE id = $1
		`,
		result.TransactionID,
	).Scan(
		&state,
		&referenceExternalID,
		&referencedTransactionID,
		&resultBalance,
		&referenceAttempts,
		&referenceNextAttemptAt,
		&referenceExpiresAt,
		&correlationID,
		&causationID,
	)
	if err != nil {
		t.Fatalf("query pending transaction: %v", err)
	}

	if state != string(domain.WagerStatePendingReference) {
		t.Fatalf(
			"expected persisted PENDING_REFERENCE, got %s",
			state,
		)
	}

	if referenceExternalID != "bet-that-does-not-exist-yet" {
		t.Fatalf(
			"unexpected reference external ID: %s",
			referenceExternalID,
		)
	}

	if referencedTransactionID != nil {
		t.Fatalf(
			"expected referenced_transaction_id to be NULL, got %v",
			*referencedTransactionID,
		)
	}

	if resultBalance != 10000 {
		t.Fatalf(
			"expected persisted result balance 10000, got %d",
			resultBalance,
		)
	}

	if referenceAttempts != 0 {
		t.Fatalf(
			"expected 0 reference attempts, got %d",
			referenceAttempts,
		)
	}

	if referenceNextAttemptAt == nil {
		t.Fatal("expected reference_next_attempt_at to be set")
	}

	if referenceExpiresAt == nil {
		t.Fatal("expected reference_expires_at to be set")
	}

	if !referenceExpiresAt.After(*referenceNextAttemptAt) {
		t.Fatal("expected reference expiration after next retry")
	}

	if correlationID != cmd.CorrelationID {
		t.Fatalf("expected persisted correlation ID %q, got %q", cmd.CorrelationID, correlationID)
	}
	if causationID != cmd.CausationID {
		t.Fatalf("expected persisted causation ID %q, got %q", cmd.CausationID, causationID)
	}

	// No financial movement may occur while the transaction is pending.
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
			"expected wallet balance 10000, got %d",
			balance,
		)
	}

	if version != 1 {
		t.Fatalf(
			"expected wallet version to remain 1, got %d",
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
		WHERE wt.id = $1
		`,
		result.TransactionID,
	).Scan(&rollbackLedgerCount)
	if err != nil {
		t.Fatalf("count pending ledger entries: %v", err)
	}

	if rollbackLedgerCount != 0 {
		t.Fatalf(
			"expected pending transaction to create no ledger, got %d",
			rollbackLedgerCount,
		)
	}

	var pendingEventCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM outbox_events
		WHERE event_type = 'WagerTransactionPendingReference'
		`,
	).Scan(&pendingEventCount)
	if err != nil {
		t.Fatalf("count pending-reference events: %v", err)
	}

	if pendingEventCount != 1 {
		t.Fatalf(
			"expected exactly 1 pending-reference event, got %d",
			pendingEventCount,
		)
	}

	// Same delivery again must return the original durable result.
	replay, err := service.Process(ctx, cmd)
	if err != nil {
		t.Fatalf("replay pending transaction: %v", err)
	}

	if !replay.IdempotentReplay {
		t.Fatal("expected second request to be idempotent replay")
	}

	if replay.TransactionID != result.TransactionID {
		t.Fatalf(
			"expected original transaction ID %s, got %s",
			result.TransactionID,
			replay.TransactionID,
		)
	}

	if replay.State != domain.WagerStatePendingReference {
		t.Fatalf(
			"expected replay state PENDING_REFERENCE, got %s",
			replay.State,
		)
	}

	if replay.Balance.Amount() != 10000 {
		t.Fatalf(
			"expected replay balance 10000, got %d",
			replay.Balance.Amount(),
		)
	}

	// Replay must not create anything else.
	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM outbox_events
		WHERE event_type = 'WagerTransactionPendingReference'
		`,
	).Scan(&pendingEventCount)
	if err != nil {
		t.Fatalf("count events after replay: %v", err)
	}

	if pendingEventCount != 1 {
		t.Fatalf(
			"expected replay not to duplicate pending event, got %d events",
			pendingEventCount,
		)
	}

	var transactionCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM wager_transactions
		WHERE provider_id = $1
		  AND external_transaction_id = $2
		`,
		cmd.Request.ProviderID,
		cmd.Request.ExternalTransactionID,
	).Scan(&transactionCount)
	if err != nil {
		t.Fatalf("count transactions: %v", err)
	}

	if transactionCount != 1 {
		t.Fatalf(
			"expected exactly 1 transaction, got %d",
			transactionCount,
		)
	}
}

func TestPendingRollbackIsProcessedWhenReferenceArrivesLater(t *testing.T) {
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
			PlayerID:       "player-late-reference",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	service := NewService(pool)

	// 1. ROLLBACK arrives BEFORE the BET it references.
	pendingResult, err := service.Process(
		ctx,
		ProcessCommand{
			IdempotencyKey: "rollback-before-reference",
			Request: domain.WagerRequest{
				ProviderID:                     "provider-a",
				ExternalTransactionID:          "rollback-late-ref-1",
				PlayerID:                       "player-late-reference",
				WalletID:                       createdWallet.WalletID,
				RoundID:                        "round-late-ref",
				GameID:                         "game-1",
				Kind:                           domain.WagerKindRollback,
				Amount:                         domain.NewMoney(3000, domain.BRL),
				ReferenceExternalTransactionID: "bet-arrives-later-1",
			},
		},
	)
	if err != nil {
		t.Fatalf("process early ROLLBACK: %v", err)
	}

	if pendingResult.State != domain.WagerStatePendingReference {
		t.Fatalf(
			"expected PENDING_REFERENCE, got %s",
			pendingResult.State,
		)
	}

	if pendingResult.Balance.Amount() != 10000 {
		t.Fatalf(
			"expected unchanged balance 10000, got %d",
			pendingResult.Balance.Amount(),
		)
	}

	originalPendingTransactionID := pendingResult.TransactionID

	// 2. The referenced BET arrives later.
	betResult, err := service.Process(
		ctx,
		ProcessCommand{
			IdempotencyKey: "late-bet",
			Request: domain.WagerRequest{
				ProviderID:            "provider-a",
				ExternalTransactionID: "bet-arrives-later-1",
				PlayerID:              "player-late-reference",
				WalletID:              createdWallet.WalletID,
				RoundID:               "round-late-ref",
				GameID:                "game-1",
				Kind:                  domain.WagerKindBet,
				Amount:                domain.NewMoney(3000, domain.BRL),
			},
		},
	)
	if err != nil {
		t.Fatalf("process late BET: %v", err)
	}

	if betResult.State != domain.WagerStateProcessed {
		t.Fatalf(
			"expected BET PROCESSED, got %s",
			betResult.State,
		)
	}

	if betResult.Balance.Amount() != 7000 {
		t.Fatalf(
			"expected balance 7000 after BET, got %d",
			betResult.Balance.Amount(),
		)
	}

	// The production retry delay is 5 seconds.
	// Do not sleep in the integration test; make the pending item due now.
	_, err = pool.Exec(
		ctx,
		`
		UPDATE wager_transactions
		SET reference_next_attempt_at = NOW()
		WHERE id = $1
		`,
		originalPendingTransactionID,
	)
	if err != nil {
		t.Fatalf("make pending reference due: %v", err)
	}

	// 3. Resolver sees that the BET now exists and completes
	// the SAME pending transaction.
	resolver := NewPendingReferenceResolver(pool)

	handled, err := resolver.ResolveOne(ctx)
	if err != nil {
		t.Fatalf("resolve pending reference: %v", err)
	}

	if !handled {
		t.Fatal("expected resolver to handle pending transaction")
	}

	// 4. The original ROLLBACK row must now be PROCESSED.
	var (
		state                   string
		referencedTransactionID string
		resultBalance           int64
		failureCode             *string
	)

	err = pool.QueryRow(
		ctx,
		`
		SELECT
			state,
			referenced_transaction_id::text,
			result_balance,
			failure_code
		FROM wager_transactions
		WHERE id = $1
		`,
		originalPendingTransactionID,
	).Scan(
		&state,
		&referencedTransactionID,
		&resultBalance,
		&failureCode,
	)
	if err != nil {
		t.Fatalf("query resolved pending transaction: %v", err)
	}

	if state != string(domain.WagerStateProcessed) {
		t.Fatalf(
			"expected resolved transaction PROCESSED, got %s",
			state,
		)
	}

	if referencedTransactionID != betResult.TransactionID {
		t.Fatalf(
			"expected reference %s, got %s",
			betResult.TransactionID,
			referencedTransactionID,
		)
	}

	if resultBalance != 10000 {
		t.Fatalf(
			"expected result balance 10000, got %d",
			resultBalance,
		)
	}

	if failureCode != nil {
		t.Fatalf(
			"expected no failure code, got %s",
			*failureCode,
		)
	}

	// 5. BET debited 30 and ROLLBACK credited exactly 30.
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
			"expected final balance 10000, got %d",
			balance,
		)
	}

	// Opening wallet = version 1.
	// BET = version 2.
	// Resolved ROLLBACK = version 3.
	if version != 3 {
		t.Fatalf(
			"expected wallet version 3, got %d",
			version,
		)
	}

	// 6. The resolved ROLLBACK must have exactly one CREDIT ledger entry.
	var (
		ledgerCount   int
		direction     string
		ledgerAmount  int64
		balanceBefore int64
		balanceAfter  int64
	)

	err = pool.QueryRow(
		ctx,
		`
		SELECT
			COUNT(*),
			MIN(direction::text),
			MIN(amount),
			MIN(balance_before),
			MIN(balance_after)
		FROM ledger_entries
		WHERE transaction_id = $1
		`,
		originalPendingTransactionID,
	).Scan(
		&ledgerCount,
		&direction,
		&ledgerAmount,
		&balanceBefore,
		&balanceAfter,
	)
	if err != nil {
		t.Fatalf("query rollback ledger: %v", err)
	}

	if ledgerCount != 1 {
		t.Fatalf(
			"expected exactly 1 ROLLBACK ledger entry, got %d",
			ledgerCount,
		)
	}

	if direction != "CREDIT" {
		t.Fatalf(
			"expected ROLLBACK direction CREDIT, got %s",
			direction,
		)
	}

	if ledgerAmount != 3000 {
		t.Fatalf(
			"expected ledger amount 3000, got %d",
			ledgerAmount,
		)
	}

	if balanceBefore != 7000 || balanceAfter != 10000 {
		t.Fatalf(
			"expected ledger balance 7000 -> 10000, got %d -> %d",
			balanceBefore,
			balanceAfter,
		)
	}

	// 7. Both lifecycle events must exist:
	// first PendingReference, then Processed.
	var pendingEventCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM outbox_events
		WHERE aggregate_id = $1
		  AND event_type = 'WagerTransactionPendingReference'
		`,
		originalPendingTransactionID,
	).Scan(&pendingEventCount)
	if err != nil {
		t.Fatalf("count pending-reference event: %v", err)
	}

	if pendingEventCount != 1 {
		t.Fatalf(
			"expected exactly 1 pending-reference event, got %d",
			pendingEventCount,
		)
	}

	var processedEventCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM outbox_events
		WHERE aggregate_id = $1
		  AND event_type = 'WagerTransactionProcessed'
		`,
		originalPendingTransactionID,
	).Scan(&processedEventCount)
	if err != nil {
		t.Fatalf("count processed event: %v", err)
	}

	if processedEventCount != 1 {
		t.Fatalf(
			"expected exactly 1 processed event, got %d",
			processedEventCount,
		)
	}

	// 8. Calling the resolver again must NOT process it twice.
	handled, err = resolver.ResolveOne(ctx)
	if err != nil {
		t.Fatalf("second resolver call: %v", err)
	}

	if handled {
		t.Fatal("expected already-resolved transaction not to be handled again")
	}

	var finalLedgerCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM ledger_entries
		WHERE transaction_id = $1
		`,
		originalPendingTransactionID,
	).Scan(&finalLedgerCount)
	if err != nil {
		t.Fatalf("count final ledger entries: %v", err)
	}

	if finalLedgerCount != 1 {
		t.Fatalf(
			"expected ROLLBACK to move money exactly once, got %d ledger entries",
			finalLedgerCount,
		)
	}
}

func TestPendingReferenceSurvivesApplicationRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const databaseURL = "postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable"

	// First application instance.
	firstPool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect first application instance: %v", err)
	}

	cleanDatabase(t, ctx, firstPool)

	createdWallet, err := wallet.NewService(firstPool).Create(
		ctx,
		wallet.CreateWalletCommand{
			PlayerID:       "player-restart",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		firstPool.Close()
		t.Fatalf("create wallet: %v", err)
	}

	firstService := NewService(firstPool)

	// ROLLBACK arrives before its BET.
	pendingResult, err := firstService.Process(
		ctx,
		ProcessCommand{
			IdempotencyKey: "rollback-before-restart",
			CorrelationID:  "correlation-before-restart",
			CausationID:    "message-before-restart",
			Request: domain.WagerRequest{
				ProviderID:                     "provider-a",
				ExternalTransactionID:          "rollback-before-restart-1",
				PlayerID:                       "player-restart",
				WalletID:                       createdWallet.WalletID,
				RoundID:                        "round-restart",
				GameID:                         "game-1",
				Kind:                           domain.WagerKindRollback,
				Amount:                         domain.NewMoney(3000, domain.BRL),
				ReferenceExternalTransactionID: "bet-after-restart-1",
			},
		},
	)
	if err != nil {
		firstPool.Close()
		t.Fatalf("create pending ROLLBACK: %v", err)
	}

	if pendingResult.State != domain.WagerStatePendingReference {
		firstPool.Close()
		t.Fatalf(
			"expected PENDING_REFERENCE, got %s",
			pendingResult.State,
		)
	}

	pendingTransactionID := pendingResult.TransactionID

	// Simulate the application stopping completely.
	// From this point on, neither the original Service nor its pool is reused.
	firstPool.Close()

	// Second application instance starts with a completely new connection pool.
	secondPool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect restarted application instance: %v", err)
	}
	defer secondPool.Close()

	secondService := NewService(secondPool)

	// The referenced BET arrives after the restart.
	betResult, err := secondService.Process(
		ctx,
		ProcessCommand{
			IdempotencyKey: "bet-after-restart",
			Request: domain.WagerRequest{
				ProviderID:            "provider-a",
				ExternalTransactionID: "bet-after-restart-1",
				PlayerID:              "player-restart",
				WalletID:              createdWallet.WalletID,
				RoundID:               "round-restart",
				GameID:                "game-1",
				Kind:                  domain.WagerKindBet,
				Amount:                domain.NewMoney(3000, domain.BRL),
			},
		},
	)
	if err != nil {
		t.Fatalf("process BET after restart: %v", err)
	}

	if betResult.State != domain.WagerStateProcessed {
		t.Fatalf(
			"expected BET PROCESSED, got %s",
			betResult.State,
		)
	}

	if betResult.Balance.Amount() != 7000 {
		t.Fatalf(
			"expected balance 7000 after BET, got %d",
			betResult.Balance.Amount(),
		)
	}

	// Make the persisted pending transaction immediately eligible for retry.
	_, err = secondPool.Exec(
		ctx,
		`
		UPDATE wager_transactions
		SET reference_next_attempt_at = NOW()
		WHERE id = $1
		`,
		pendingTransactionID,
	)
	if err != nil {
		t.Fatalf("make pending transaction due: %v", err)
	}

	// A brand-new resolver, created after the simulated restart,
	// must discover the pending work exclusively from PostgreSQL.
	resolver := NewPendingReferenceResolver(secondPool)

	handled, err := resolver.ResolveOne(ctx)
	if err != nil {
		t.Fatalf("resolve pending reference after restart: %v", err)
	}

	if !handled {
		t.Fatal("expected restarted resolver to discover persisted pending transaction")
	}

	var (
		state                   string
		referencedTransactionID string
		resultBalance           int64
		failureCode             *string
	)

	err = secondPool.QueryRow(
		ctx,
		`
		SELECT
			state,
			referenced_transaction_id::text,
			result_balance,
			failure_code
		FROM wager_transactions
		WHERE id = $1
		`,
		pendingTransactionID,
	).Scan(
		&state,
		&referencedTransactionID,
		&resultBalance,
		&failureCode,
	)
	if err != nil {
		t.Fatalf("query recovered pending transaction: %v", err)
	}

	if state != string(domain.WagerStateProcessed) {
		t.Fatalf(
			"expected recovered transaction PROCESSED, got %s",
			state,
		)
	}

	if referencedTransactionID != betResult.TransactionID {
		t.Fatalf(
			"expected reference %s, got %s",
			betResult.TransactionID,
			referencedTransactionID,
		)
	}

	if resultBalance != 10000 {
		t.Fatalf(
			"expected recovered result balance 10000, got %d",
			resultBalance,
		)
	}

	if failureCode != nil {
		t.Fatalf(
			"expected no failure code, got %s",
			*failureCode,
		)
	}

	var balance int64
	var version int64

	err = secondPool.QueryRow(
		ctx,
		`
		SELECT balance, version
		FROM wallets
		WHERE id = $1
		`,
		createdWallet.WalletID,
	).Scan(&balance, &version)
	if err != nil {
		t.Fatalf("query wallet after recovery: %v", err)
	}

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

	var ledgerCount int

	err = secondPool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM ledger_entries
		WHERE transaction_id = $1
		`,
		pendingTransactionID,
	).Scan(&ledgerCount)
	if err != nil {
		t.Fatalf("count recovered ROLLBACK ledger entries: %v", err)
	}

	if ledgerCount != 1 {
		t.Fatalf(
			"expected exactly 1 recovered ROLLBACK ledger entry, got %d",
			ledgerCount,
		)
	}

	var processedCorrelationID string
	var processedCausationID string
	err = secondPool.QueryRow(
		ctx,
		`
		SELECT payload->>'correlationId', payload->>'causationId'
		FROM outbox_events
		WHERE aggregate_id = $1
		  AND event_type = 'WagerTransactionProcessed'
		`,
		pendingTransactionID,
	).Scan(&processedCorrelationID, &processedCausationID)
	if err != nil {
		t.Fatalf("query recovered processed event tracing metadata: %v", err)
	}
	if processedCorrelationID != "correlation-before-restart" {
		t.Fatalf("expected processed event correlation ID after restart to be preserved, got %q", processedCorrelationID)
	}
	if processedCausationID != "message-before-restart" {
		t.Fatalf("expected processed event causation ID after restart to be preserved, got %q", processedCausationID)
	}

	// Running the resolver again must not repeat the financial movement.
	handled, err = resolver.ResolveOne(ctx)
	if err != nil {
		t.Fatalf("second resolver call after restart: %v", err)
	}

	if handled {
		t.Fatal("expected recovered transaction not to be processed twice")
	}

	var finalLedgerCount int

	err = secondPool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM ledger_entries
		WHERE transaction_id = $1
		`,
		pendingTransactionID,
	).Scan(&finalLedgerCount)
	if err != nil {
		t.Fatalf("count final recovered ledger entries: %v", err)
	}

	if finalLedgerCount != 1 {
		t.Fatalf(
			"expected recovery to move money exactly once, got %d ledger entries",
			finalLedgerCount,
		)
	}
}

func TestPendingReferenceSchedulesRetryWhenReferenceStillDoesNotExist(t *testing.T) {
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
			PlayerID:       "player-retry",
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
			IdempotencyKey: "pending-retry-1",
			Request: domain.WagerRequest{
				ProviderID:                     "provider-a",
				ExternalTransactionID:          "rollback-retry-1",
				PlayerID:                       "player-retry",
				WalletID:                       createdWallet.WalletID,
				RoundID:                        "round-retry",
				GameID:                         "game-1",
				Kind:                           domain.WagerKindRollback,
				Amount:                         domain.NewMoney(3000, domain.BRL),
				ReferenceExternalTransactionID: "missing-bet-retry",
			},
		},
	)
	if err != nil {
		t.Fatalf("create pending reference: %v", err)
	}

	if result.State != domain.WagerStatePendingReference {
		t.Fatalf(
			"expected PENDING_REFERENCE, got %s",
			result.State,
		)
	}

	// Make it immediately due instead of sleeping for the production delay.
	_, err = pool.Exec(
		ctx,
		`
		UPDATE wager_transactions
		SET reference_next_attempt_at = NOW()
		WHERE id = $1
		`,
		result.TransactionID,
	)
	if err != nil {
		t.Fatalf("make pending reference due: %v", err)
	}

	beforeResolve := time.Now().UTC()

	resolver := NewPendingReferenceResolver(pool)

	handled, err := resolver.ResolveOne(ctx)
	if err != nil {
		t.Fatalf("resolve missing reference: %v", err)
	}

	if !handled {
		t.Fatal("expected resolver to handle due pending reference")
	}

	var (
		state         string
		attempts      int
		nextAttemptAt time.Time
		resultBalance int64
	)

	err = pool.QueryRow(
		ctx,
		`
		SELECT
			state,
			reference_attempts,
			reference_next_attempt_at,
			result_balance
		FROM wager_transactions
		WHERE id = $1
		`,
		result.TransactionID,
	).Scan(
		&state,
		&attempts,
		&nextAttemptAt,
		&resultBalance,
	)
	if err != nil {
		t.Fatalf("query pending retry: %v", err)
	}

	if state != string(domain.WagerStatePendingReference) {
		t.Fatalf(
			"expected transaction to remain PENDING_REFERENCE, got %s",
			state,
		)
	}

	if attempts != 1 {
		t.Fatalf(
			"expected reference_attempts 1, got %d",
			attempts,
		)
	}

	// First retry uses referenceRetryDelay (5 seconds).
	if nextAttemptAt.Before(beforeResolve.Add(referenceRetryDelay - time.Second)) {
		t.Fatalf(
			"expected next retry roughly %s later, got %s",
			referenceRetryDelay,
			nextAttemptAt.Sub(beforeResolve),
		)
	}

	if resultBalance != 10000 {
		t.Fatalf(
			"expected result balance to remain 10000, got %d",
			resultBalance,
		)
	}

	// No financial movement may happen while waiting for the reference.
	var (
		balance int64
		version int64
	)

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

	if balance != 10000 || version != 1 {
		t.Fatalf(
			"expected unchanged wallet 10000/version 1, got %d/version %d",
			balance,
			version,
		)
	}

	var ledgerCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM ledger_entries
		WHERE transaction_id = $1
		`,
		result.TransactionID,
	).Scan(&ledgerCount)
	if err != nil {
		t.Fatalf("count ledger entries: %v", err)
	}

	if ledgerCount != 0 {
		t.Fatalf(
			"expected no ledger movement, got %d entries",
			ledgerCount,
		)
	}
}

func TestPendingReferenceExpiresWithoutMovingMoney(t *testing.T) {
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
			PlayerID:       "player-expired-reference",
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
			IdempotencyKey: "expired-reference-1",
			CorrelationID:  "correlation-expired-reference",
			CausationID:    "message-expired-reference",
			Request: domain.WagerRequest{
				ProviderID:                     "provider-a",
				ExternalTransactionID:          "rollback-expired-1",
				PlayerID:                       "player-expired-reference",
				WalletID:                       createdWallet.WalletID,
				RoundID:                        "round-expired",
				GameID:                         "game-1",
				Kind:                           domain.WagerKindRollback,
				Amount:                         domain.NewMoney(3000, domain.BRL),
				ReferenceExternalTransactionID: "bet-never-arrives",
			},
		},
	)
	if err != nil {
		t.Fatalf("create pending reference: %v", err)
	}

	// Simulate the durable transaction surviving until after its TTL.
	_, err = pool.Exec(
		ctx,
		`
		UPDATE wager_transactions
		SET
			reference_next_attempt_at = NOW(),
			reference_expires_at = NOW() - INTERVAL '1 second'
		WHERE id = $1
		`,
		result.TransactionID,
	)
	if err != nil {
		t.Fatalf("expire pending reference: %v", err)
	}

	resolver := NewPendingReferenceResolver(pool)

	handled, err := resolver.ResolveOne(ctx)
	if err != nil {
		t.Fatalf("resolve expired reference: %v", err)
	}

	if !handled {
		t.Fatal("expected expired pending reference to be handled")
	}

	var (
		state         string
		failureCode   *string
		resultBalance int64
		nextAttemptAt *time.Time
	)

	err = pool.QueryRow(
		ctx,
		`
		SELECT
			state,
			failure_code,
			result_balance,
			reference_next_attempt_at
		FROM wager_transactions
		WHERE id = $1
		`,
		result.TransactionID,
	).Scan(
		&state,
		&failureCode,
		&resultBalance,
		&nextAttemptAt,
	)
	if err != nil {
		t.Fatalf("query expired transaction: %v", err)
	}

	if state != string(domain.WagerStateRejected) {
		t.Fatalf(
			"expected REJECTED, got %s",
			state,
		)
	}

	if failureCode == nil || *failureCode != "REFERENCE_EXPIRED" {
		t.Fatalf(
			"expected REFERENCE_EXPIRED, got %v",
			failureCode,
		)
	}

	if resultBalance != 10000 {
		t.Fatalf(
			"expected result balance 10000, got %d",
			resultBalance,
		)
	}

	if nextAttemptAt != nil {
		t.Fatalf(
			"expected no next retry after expiration, got %v",
			*nextAttemptAt,
		)
	}

	// Expiration is not a financial operation.
	var (
		balance int64
		version int64
	)

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

	if balance != 10000 || version != 1 {
		t.Fatalf(
			"expected unchanged wallet 10000/version 1, got %d/version %d",
			balance,
			version,
		)
	}

	var ledgerCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM ledger_entries
		WHERE transaction_id = $1
		`,
		result.TransactionID,
	).Scan(&ledgerCount)
	if err != nil {
		t.Fatalf("count ledger entries: %v", err)
	}

	if ledgerCount != 0 {
		t.Fatalf(
			"expected no ledger entry for expired reversal, got %d",
			ledgerCount,
		)
	}

	// Rejection must also be emitted through the transactional outbox.
	var rejectedEventCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM outbox_events
		WHERE aggregate_id = $1
		  AND event_type = 'WagerTransactionRejected'
		`,
		result.TransactionID,
	).Scan(&rejectedEventCount)
	if err != nil {
		t.Fatalf("count rejected event: %v", err)
	}

	if rejectedEventCount != 1 {
		t.Fatalf(
			"expected exactly 1 rejected event, got %d",
			rejectedEventCount,
		)
	}

	var rejectedCorrelationID string
	var rejectedCausationID string
	err = pool.QueryRow(
		ctx,
		`
		SELECT payload->>'correlationId', payload->>'causationId'
		FROM outbox_events
		WHERE aggregate_id = $1
		  AND event_type = 'WagerTransactionRejected'
		`,
		result.TransactionID,
	).Scan(&rejectedCorrelationID, &rejectedCausationID)
	if err != nil {
		t.Fatalf("query rejected event tracing metadata: %v", err)
	}
	if rejectedCorrelationID != "correlation-expired-reference" {
		t.Fatalf("expected rejected event correlation ID to be preserved, got %q", rejectedCorrelationID)
	}
	if rejectedCausationID != "message-expired-reference" {
		t.Fatalf("expected rejected event causation ID to be preserved, got %q", rejectedCausationID)
	}
}

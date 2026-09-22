package wagering

import (
	"context"
	"testing"
	"time"

	"github.com/Tharik/wagering-platform/internal/application/wallet"
	"github.com/Tharik/wagering-platform/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestReferenceToTerminalUnsuccessfulTransactionIsDurablyRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, "postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable")
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()
	cleanDatabase(t, ctx, pool)

	createdWallet, err := wallet.NewService(pool).Create(ctx, wallet.CreateWalletCommand{
		PlayerID:       "player-terminal-reference",
		InitialBalance: domain.NewMoney(1000, domain.BRL),
	})
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	service := NewService(pool)
	reference, err := service.Process(ctx, ProcessCommand{
		IdempotencyKey: "rejected-reference",
		Request: domain.WagerRequest{
			ProviderID:            "provider-a",
			ExternalTransactionID: "rejected-bet",
			PlayerID:              "player-terminal-reference",
			WalletID:              createdWallet.WalletID,
			RoundID:               "round-1",
			GameID:                "game-1",
			Kind:                  domain.WagerKindBet,
			Amount:                domain.NewMoney(2000, domain.BRL),
		},
	})
	if err != nil || reference.State != domain.WagerStateRejected {
		t.Fatalf("create rejected reference: result=%+v err=%v", reference, err)
	}

	command := ProcessCommand{
		IdempotencyKey: "refund-terminal-reference",
		Request: domain.WagerRequest{
			ProviderID:                     "provider-a",
			ExternalTransactionID:          "refund-rejected-bet",
			PlayerID:                       "player-terminal-reference",
			WalletID:                       createdWallet.WalletID,
			RoundID:                        "round-1",
			GameID:                         "game-1",
			Kind:                           domain.WagerKindRefund,
			Amount:                         domain.NewMoney(2000, domain.BRL),
			ReferenceExternalTransactionID: "rejected-bet",
		},
	}

	result, err := service.Process(ctx, command)
	if err != nil {
		t.Fatalf("process terminal reference: %v", err)
	}
	if result.State != domain.WagerStateRejected || result.FailureCode != failureCodeReferenceTerminalUnsuccessful {
		t.Fatalf("unexpected result: %+v", result)
	}

	replay, err := service.Process(ctx, command)
	if err != nil {
		t.Fatalf("replay terminal reference: %v", err)
	}
	if !replay.IdempotentReplay || replay.TransactionID != result.TransactionID || replay.Balance.Amount() != 1000 {
		t.Fatalf("unexpected replay: %+v", replay)
	}

	var (
		referencedID string
		balance      int64
		version      int64
		ledgerCount  int
		outboxCount  int
	)
	if err := pool.QueryRow(ctx, `SELECT referenced_transaction_id::text FROM wager_transactions WHERE id = $1`, result.TransactionID).Scan(&referencedID); err != nil {
		t.Fatalf("query rejected reference linkage: %v", err)
	}
	if referencedID != reference.TransactionID {
		t.Fatalf("expected reference linkage %s, got %s", reference.TransactionID, referencedID)
	}
	if err := pool.QueryRow(ctx, `SELECT balance, version FROM wallets WHERE id = $1`, createdWallet.WalletID).Scan(&balance, &version); err != nil {
		t.Fatalf("query wallet: %v", err)
	}
	if balance != 1000 || version != 1 {
		t.Fatalf("expected unchanged wallet 1000/version 1, got %d/version %d", balance, version)
	}
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM ledger_entries WHERE transaction_id = $1`, result.TransactionID).Scan(&ledgerCount); err != nil {
		t.Fatalf("count ledger entries: %v", err)
	}
	if ledgerCount != 0 {
		t.Fatalf("expected no rejected ledger entry, got %d", ledgerCount)
	}
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'WagerTransactionRejected'`, result.TransactionID).Scan(&outboxCount); err != nil {
		t.Fatalf("count rejected events: %v", err)
	}
	if outboxCount != 1 {
		t.Fatalf("expected one rejected event, got %d", outboxCount)
	}
}

func TestExistingPendingReferenceKeepsDependentPending(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, "postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable")
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()
	cleanDatabase(t, ctx, pool)

	createdWallet, err := wallet.NewService(pool).Create(ctx, wallet.CreateWalletCommand{
		PlayerID:       "player-pending-chain",
		InitialBalance: domain.NewMoney(10000, domain.BRL),
	})
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}
	service := NewService(pool)

	pendingRefund, err := service.Process(ctx, ProcessCommand{
		IdempotencyKey: "pending-refund",
		Request: domain.WagerRequest{
			ProviderID: "provider-a", ExternalTransactionID: "pending-refund", PlayerID: "player-pending-chain",
			WalletID: createdWallet.WalletID, RoundID: "round-1", GameID: "game-1", Kind: domain.WagerKindRefund,
			Amount: domain.NewMoney(3000, domain.BRL), ReferenceExternalTransactionID: "missing-bet",
		},
	})
	if err != nil || pendingRefund.State != domain.WagerStatePendingReference {
		t.Fatalf("create pending refund: result=%+v err=%v", pendingRefund, err)
	}

	dependent, err := service.Process(ctx, ProcessCommand{
		IdempotencyKey: "dependent-rollback",
		Request: domain.WagerRequest{
			ProviderID: "provider-a", ExternalTransactionID: "dependent-rollback", PlayerID: "player-pending-chain",
			WalletID: createdWallet.WalletID, RoundID: "round-1", GameID: "game-1", Kind: domain.WagerKindRollback,
			Amount: domain.NewMoney(3000, domain.BRL), ReferenceExternalTransactionID: "pending-refund",
		},
	})
	if err != nil {
		t.Fatalf("process dependent rollback: %v", err)
	}
	if dependent.State != domain.WagerStatePendingReference || dependent.Balance.Amount() != 10000 {
		t.Fatalf("expected unchanged pending dependent, got %+v", dependent)
	}

	if _, err := pool.Exec(ctx, `UPDATE wager_transactions SET reference_next_attempt_at = NOW() WHERE id = $1`, dependent.TransactionID); err != nil {
		t.Fatalf("make dependent due: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE wager_transactions SET reference_next_attempt_at = NOW() + INTERVAL '1 hour' WHERE id = $1`, pendingRefund.TransactionID); err != nil {
		t.Fatalf("defer original pending row: %v", err)
	}

	handled, err := NewPendingReferenceResolver(pool).ResolveOne(ctx)
	if err != nil || !handled {
		t.Fatalf("retry dependent: handled=%v err=%v", handled, err)
	}

	var state string
	var attempts int
	if err := pool.QueryRow(ctx, `SELECT state, reference_attempts FROM wager_transactions WHERE id = $1`, dependent.TransactionID).Scan(&state, &attempts); err != nil {
		t.Fatalf("query dependent: %v", err)
	}
	if state != string(domain.WagerStatePendingReference) || attempts != 1 {
		t.Fatalf("expected pending retry with one attempt, got state=%s attempts=%d", state, attempts)
	}

	_, err = service.Process(ctx, ProcessCommand{
		IdempotencyKey: "late-chain-bet",
		Request: domain.WagerRequest{
			ProviderID: "provider-a", ExternalTransactionID: "missing-bet", PlayerID: "player-pending-chain",
			WalletID: createdWallet.WalletID, RoundID: "round-1", GameID: "game-1", Kind: domain.WagerKindBet,
			Amount: domain.NewMoney(3000, domain.BRL),
		},
	})
	if err != nil {
		t.Fatalf("process late BET: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE wager_transactions SET reference_next_attempt_at = NOW() WHERE id IN ($1, $2)`, pendingRefund.TransactionID, dependent.TransactionID); err != nil {
		t.Fatalf("make pending chain due: %v", err)
	}
	resolver := NewPendingReferenceResolver(pool)
	for i := 0; i < 2; i++ {
		handled, err = resolver.ResolveOne(ctx)
		if err != nil || !handled {
			t.Fatalf("resolve pending chain step %d: handled=%v err=%v", i+1, handled, err)
		}
	}
	if err := pool.QueryRow(ctx, `SELECT state FROM wager_transactions WHERE id = $1`, dependent.TransactionID).Scan(&state); err != nil {
		t.Fatalf("query resolved dependent: %v", err)
	}
	if state != string(domain.WagerStateProcessed) {
		t.Fatalf("expected dependent eventually PROCESSED, got %s", state)
	}
}

func TestPendingDependentIsRejectedWhenReferenceBecomesRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, "postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable")
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()
	cleanDatabase(t, ctx, pool)

	createdWallet, err := wallet.NewService(pool).Create(ctx, wallet.CreateWalletCommand{PlayerID: "player-rejected-chain", InitialBalance: domain.NewMoney(1000, domain.BRL)})
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}
	service := NewService(pool)
	pendingRefund, err := service.Process(ctx, ProcessCommand{IdempotencyKey: "pending-rejected-refund", Request: domain.WagerRequest{
		ProviderID: "provider-a", ExternalTransactionID: "pending-rejected-refund", PlayerID: "player-rejected-chain", WalletID: createdWallet.WalletID,
		RoundID: "round-1", GameID: "game-1", Kind: domain.WagerKindRefund, Amount: domain.NewMoney(2000, domain.BRL), ReferenceExternalTransactionID: "future-rejected-bet",
	}})
	if err != nil {
		t.Fatalf("create pending refund: %v", err)
	}
	dependent, err := service.Process(ctx, ProcessCommand{IdempotencyKey: "dependent-on-rejected", Request: domain.WagerRequest{
		ProviderID: "provider-a", ExternalTransactionID: "dependent-on-rejected", PlayerID: "player-rejected-chain", WalletID: createdWallet.WalletID,
		RoundID: "round-1", GameID: "game-1", Kind: domain.WagerKindRollback, Amount: domain.NewMoney(2000, domain.BRL), ReferenceExternalTransactionID: "pending-rejected-refund",
	}})
	if err != nil {
		t.Fatalf("create dependent: %v", err)
	}
	_, err = service.Process(ctx, ProcessCommand{IdempotencyKey: "future-rejected-bet", Request: domain.WagerRequest{
		ProviderID: "provider-a", ExternalTransactionID: "future-rejected-bet", PlayerID: "player-rejected-chain", WalletID: createdWallet.WalletID,
		RoundID: "round-1", GameID: "game-1", Kind: domain.WagerKindBet, Amount: domain.NewMoney(2000, domain.BRL),
	}})
	if err != nil {
		t.Fatalf("create rejected BET: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE wager_transactions SET reference_next_attempt_at = NOW() WHERE id IN ($1, $2)`, pendingRefund.TransactionID, dependent.TransactionID); err != nil {
		t.Fatalf("make rejected chain due: %v", err)
	}
	resolver := NewPendingReferenceResolver(pool)
	for i := 0; i < 2; i++ {
		handled, resolveErr := resolver.ResolveOne(ctx)
		if resolveErr != nil || !handled {
			t.Fatalf("resolve rejected chain step %d: handled=%v err=%v", i+1, handled, resolveErr)
		}
	}
	var state, failureCode, referencedID string
	if err := pool.QueryRow(ctx, `SELECT state, failure_code, referenced_transaction_id::text FROM wager_transactions WHERE id = $1`, dependent.TransactionID).Scan(&state, &failureCode, &referencedID); err != nil {
		t.Fatalf("query rejected dependent: %v", err)
	}
	if state != string(domain.WagerStateRejected) || failureCode != failureCodeReferenceTerminalUnsuccessful || referencedID != pendingRefund.TransactionID {
		t.Fatalf("unexpected rejected dependent: state=%s code=%s reference=%s", state, failureCode, referencedID)
	}
}

func TestReferenceLookupRemainsProviderScoped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, "postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable")
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()
	cleanDatabase(t, ctx, pool)

	createdWallet, err := wallet.NewService(pool).Create(ctx, wallet.CreateWalletCommand{PlayerID: "player-provider-scope", InitialBalance: domain.NewMoney(10000, domain.BRL)})
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}
	service := NewService(pool)
	_, err = service.Process(ctx, ProcessCommand{IdempotencyKey: "provider-b-bet", Request: domain.WagerRequest{
		ProviderID: "provider-b", ExternalTransactionID: "shared-external-id", PlayerID: "player-provider-scope", WalletID: createdWallet.WalletID,
		RoundID: "round-1", GameID: "game-1", Kind: domain.WagerKindBet, Amount: domain.NewMoney(1000, domain.BRL),
	}})
	if err != nil {
		t.Fatalf("process provider B bet: %v", err)
	}

	result, err := service.Process(ctx, ProcessCommand{IdempotencyKey: "provider-a-refund", Request: domain.WagerRequest{
		ProviderID: "provider-a", ExternalTransactionID: "provider-a-refund", PlayerID: "player-provider-scope", WalletID: createdWallet.WalletID,
		RoundID: "round-1", GameID: "game-1", Kind: domain.WagerKindRefund, Amount: domain.NewMoney(1000, domain.BRL),
		ReferenceExternalTransactionID: "shared-external-id",
	}})
	if err != nil {
		t.Fatalf("process provider-scoped missing reference: %v", err)
	}
	if result.State != domain.WagerStatePendingReference {
		t.Fatalf("expected provider A reference to remain pending, got %+v", result)
	}
}

func TestProcessedReferenceContextMismatchesAreDurablyRejected(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(context.Context, *testing.T, *pgxpool.Pool, string, string, *domain.WagerRequest)
	}{
		{
			name: "player",
			mutate: func(ctx context.Context, t *testing.T, pool *pgxpool.Pool, referenceID, _ string, _ *domain.WagerRequest) {
				t.Helper()
				if _, err := pool.Exec(ctx, `UPDATE wager_transactions SET player_id = 'other-player' WHERE id = $1`, referenceID); err != nil {
					t.Fatalf("alter referenced player: %v", err)
				}
			},
		},
		{
			name: "wallet",
			mutate: func(_ context.Context, _ *testing.T, _ *pgxpool.Pool, _, otherWalletID string, request *domain.WagerRequest) {
				request.WalletID = otherWalletID
				request.PlayerID = "other-wallet-player"
			},
		},
		{
			name: "round",
			mutate: func(_ context.Context, _ *testing.T, _ *pgxpool.Pool, _, _ string, request *domain.WagerRequest) {
				request.RoundID = "other-round"
			},
		},
		{
			name: "currency",
			mutate: func(ctx context.Context, t *testing.T, pool *pgxpool.Pool, referenceID, _ string, _ *domain.WagerRequest) {
				t.Helper()
				if _, err := pool.Exec(ctx, `UPDATE wager_transactions SET currency = 'USD' WHERE id = $1`, referenceID); err != nil {
					t.Fatalf("alter referenced currency: %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			pool, err := pgxpool.New(ctx, "postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable")
			if err != nil {
				t.Fatalf("connect postgres: %v", err)
			}
			defer pool.Close()
			cleanDatabase(t, ctx, pool)

			walletService := wallet.NewService(pool)
			primary, err := walletService.Create(ctx, wallet.CreateWalletCommand{PlayerID: "reference-player", InitialBalance: domain.NewMoney(10000, domain.BRL)})
			if err != nil {
				t.Fatalf("create primary wallet: %v", err)
			}
			other, err := walletService.Create(ctx, wallet.CreateWalletCommand{PlayerID: "other-wallet-player", InitialBalance: domain.NewMoney(10000, domain.BRL)})
			if err != nil {
				t.Fatalf("create other wallet: %v", err)
			}
			service := NewService(pool)
			reference, err := service.Process(ctx, ProcessCommand{IdempotencyKey: "context-reference", Request: domain.WagerRequest{
				ProviderID: "provider-a", ExternalTransactionID: "context-reference", PlayerID: "reference-player", WalletID: primary.WalletID,
				RoundID: "round-1", GameID: "game-1", Kind: domain.WagerKindBet, Amount: domain.NewMoney(1000, domain.BRL),
			}})
			if err != nil {
				t.Fatalf("process reference BET: %v", err)
			}

			request := domain.WagerRequest{
				ProviderID: "provider-a", ExternalTransactionID: "context-refund", PlayerID: "reference-player", WalletID: primary.WalletID,
				RoundID: "round-1", GameID: "game-1", Kind: domain.WagerKindRefund, Amount: domain.NewMoney(1000, domain.BRL),
				ReferenceExternalTransactionID: "context-reference",
			}
			tt.mutate(ctx, t, pool, reference.TransactionID, other.WalletID, &request)
			result, err := service.Process(ctx, ProcessCommand{IdempotencyKey: "context-refund", Request: request})
			if err != nil {
				t.Fatalf("process mismatched reference: %v", err)
			}
			if result.State != domain.WagerStateRejected || result.FailureCode != failureCodeReferenceMismatch {
				t.Fatalf("expected durable mismatch rejection, got %+v", result)
			}
		})
	}
}

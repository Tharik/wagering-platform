package wagering

import (
	"context"
	"testing"
	"time"

	"github.com/Tharik/wagering-platform/internal/application/wallet"
	"github.com/Tharik/wagering-platform/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMultipleReferencedWinsProcessIndependently(t *testing.T) {
	ctx, pool, walletID := setupWinReferenceTest(t, "player-multiple-wins", 10000)
	service := NewService(pool)

	bet := processWinReferenceTestWager(t, ctx, service, ProcessCommand{
		IdempotencyKey: "shared-bet",
		Request: domain.WagerRequest{
			ProviderID: "provider-a", ExternalTransactionID: "shared-bet", PlayerID: "player-multiple-wins",
			WalletID: walletID, RoundID: "round-1", GameID: "game-1", Kind: domain.WagerKindBet,
			Amount: domain.NewMoney(1000, domain.BRL),
		},
	})

	winACommand := ProcessCommand{
		IdempotencyKey: "referenced-win-a",
		Request: domain.WagerRequest{
			ProviderID: "provider-a", ExternalTransactionID: "referenced-win-a", PlayerID: "player-multiple-wins",
			WalletID: walletID, RoundID: "round-1", GameID: "game-1", Kind: domain.WagerKindWin,
			Amount: domain.NewMoney(2500, domain.BRL), ReferenceExternalTransactionID: "shared-bet",
		},
	}
	winA := processWinReferenceTestWager(t, ctx, service, winACommand)
	winB := processWinReferenceTestWager(t, ctx, service, ProcessCommand{
		IdempotencyKey: "referenced-win-b",
		Request: domain.WagerRequest{
			ProviderID: "provider-a", ExternalTransactionID: "referenced-win-b", PlayerID: "player-multiple-wins",
			WalletID: walletID, RoundID: "round-1", GameID: "game-1", Kind: domain.WagerKindWin,
			Amount: domain.NewMoney(1500, domain.BRL), ReferenceExternalTransactionID: "shared-bet",
		},
	})
	if winA.State != domain.WagerStateProcessed || winB.State != domain.WagerStateProcessed {
		t.Fatalf("expected both WINs processed, got %s and %s", winA.State, winB.State)
	}
	if winA.FailureCode == failureCodeAlreadyReversed || winB.FailureCode == failureCodeAlreadyReversed {
		t.Fatal("referenced WIN must not use reversal rejection")
	}

	replay := processWinReferenceTestWager(t, ctx, service, winACommand)
	if !replay.IdempotentReplay || replay.TransactionID != winA.TransactionID || replay.Balance.Amount() != winA.Balance.Amount() {
		t.Fatalf("unexpected WIN replay: %+v", replay)
	}

	var balance, version int64
	if err := pool.QueryRow(ctx, `SELECT balance, version FROM wallets WHERE id = $1`, walletID).Scan(&balance, &version); err != nil {
		t.Fatalf("query wallet: %v", err)
	}
	if balance != 13000 || version != 4 {
		t.Fatalf("expected balance 13000/version 4, got %d/version %d", balance, version)
	}

	var linkedWins, winLedgerEntries int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM wager_transactions WHERE id IN ($1, $2) AND state = 'PROCESSED' AND referenced_transaction_id = $3`, winA.TransactionID, winB.TransactionID, bet.TransactionID).Scan(&linkedWins); err != nil {
		t.Fatalf("count linked WINs: %v", err)
	}
	if linkedWins != 2 {
		t.Fatalf("expected two WINs linked to the same BET, got %d", linkedWins)
	}
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM ledger_entries WHERE transaction_id IN ($1, $2) AND direction = 'CREDIT'`, winA.TransactionID, winB.TransactionID).Scan(&winLedgerEntries); err != nil {
		t.Fatalf("count WIN ledger entries: %v", err)
	}
	if winLedgerEntries != 2 {
		t.Fatalf("expected two independent WIN credits, got %d", winLedgerEntries)
	}
}

func TestReferencedWinWaitsForMissingBetThenCreditsWinAmount(t *testing.T) {
	ctx, pool, walletID := setupWinReferenceTest(t, "player-late-win-bet", 10000)
	service := NewService(pool)
	command := ProcessCommand{
		IdempotencyKey: "win-before-bet",
		Request: domain.WagerRequest{
			ProviderID: "provider-a", ExternalTransactionID: "win-before-bet", PlayerID: "player-late-win-bet",
			WalletID: walletID, RoundID: "round-1", GameID: "game-1", Kind: domain.WagerKindWin,
			Amount: domain.NewMoney(2500, domain.BRL), ReferenceExternalTransactionID: "late-bet",
		},
	}
	pending := processWinReferenceTestWager(t, ctx, service, command)
	if pending.State != domain.WagerStatePendingReference || pending.Balance.Amount() != 10000 {
		t.Fatalf("expected unchanged pending WIN, got %+v", pending)
	}
	replay := processWinReferenceTestWager(t, ctx, service, command)
	if !replay.IdempotentReplay || replay.TransactionID != pending.TransactionID {
		t.Fatalf("unexpected pending replay: %+v", replay)
	}

	bet := processWinReferenceTestWager(t, ctx, service, ProcessCommand{
		IdempotencyKey: "late-bet",
		Request: domain.WagerRequest{
			ProviderID: "provider-a", ExternalTransactionID: "late-bet", PlayerID: "player-late-win-bet",
			WalletID: walletID, RoundID: "round-1", GameID: "game-1", Kind: domain.WagerKindBet,
			Amount: domain.NewMoney(1000, domain.BRL),
		},
	})
	if _, err := pool.Exec(ctx, `UPDATE wager_transactions SET reference_next_attempt_at = NOW() WHERE id = $1`, pending.TransactionID); err != nil {
		t.Fatalf("make WIN due: %v", err)
	}
	handled, err := NewPendingReferenceResolver(pool).ResolveOne(ctx)
	if err != nil || !handled {
		t.Fatalf("resolve WIN: handled=%v err=%v", handled, err)
	}

	var state, referencedID, direction string
	var resultBalance, ledgerAmount, balance, version int64
	if err := pool.QueryRow(ctx, `SELECT state, referenced_transaction_id::text, result_balance FROM wager_transactions WHERE id = $1`, pending.TransactionID).Scan(&state, &referencedID, &resultBalance); err != nil {
		t.Fatalf("query resolved WIN: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT direction::text, amount FROM ledger_entries WHERE transaction_id = $1`, pending.TransactionID).Scan(&direction, &ledgerAmount); err != nil {
		t.Fatalf("query WIN ledger: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT balance, version FROM wallets WHERE id = $1`, walletID).Scan(&balance, &version); err != nil {
		t.Fatalf("query wallet: %v", err)
	}
	if state != string(domain.WagerStateProcessed) || referencedID != bet.TransactionID || resultBalance != 11500 {
		t.Fatalf("unexpected resolved WIN: state=%s reference=%s balance=%d", state, referencedID, resultBalance)
	}
	if direction != "CREDIT" || ledgerAmount != 2500 || balance != 11500 || version != 3 {
		t.Fatalf("unexpected WIN movement: %s %d, wallet=%d/version %d", direction, ledgerAmount, balance, version)
	}
}

func TestReferencedWinFollowsPendingReferenceToTerminalRejection(t *testing.T) {
	ctx, pool, walletID := setupWinReferenceTest(t, "player-pending-win-ref", 10000)
	service := NewService(pool)
	referenceID := uuid.New()
	now := time.Now().UTC()
	_, err := pool.Exec(ctx, `
		INSERT INTO wager_transactions (
			id, provider_id, external_transaction_id, idempotency_key, payload_hash,
			wallet_id, player_id, round_id, game_id, kind, state, amount, currency,
			result_balance, created_at, updated_at
		) VALUES ($1, 'provider-a', 'pending-bet', 'pending-bet', repeat('a', 64),
			$2, 'player-pending-win-ref', 'round-1', 'game-1', 'BET', 'PENDING', 1000, 'BRL',
			10000, $3::timestamptz, $3::timestamptz)
	`, referenceID, walletID, now)
	if err != nil {
		t.Fatalf("insert pending BET: %v", err)
	}

	pendingWin := processWinReferenceTestWager(t, ctx, service, ProcessCommand{
		IdempotencyKey: "win-on-pending-bet",
		Request: domain.WagerRequest{
			ProviderID: "provider-a", ExternalTransactionID: "win-on-pending-bet", PlayerID: "player-pending-win-ref",
			WalletID: walletID, RoundID: "round-1", GameID: "game-1", Kind: domain.WagerKindWin,
			Amount: domain.NewMoney(2500, domain.BRL), ReferenceExternalTransactionID: "pending-bet",
		},
	})
	if pendingWin.State != domain.WagerStatePendingReference {
		t.Fatalf("expected WIN pending on pending BET, got %+v", pendingWin)
	}
	if _, err := pool.Exec(ctx, `UPDATE wager_transactions SET reference_next_attempt_at = NOW() WHERE id = $1`, pendingWin.TransactionID); err != nil {
		t.Fatalf("make WIN due: %v", err)
	}
	resolver := NewPendingReferenceResolver(pool)
	handled, err := resolver.ResolveOne(ctx)
	if err != nil || !handled {
		t.Fatalf("retry pending WIN: handled=%v err=%v", handled, err)
	}
	var state string
	if err := pool.QueryRow(ctx, `SELECT state FROM wager_transactions WHERE id = $1`, pendingWin.TransactionID).Scan(&state); err != nil {
		t.Fatalf("query pending WIN: %v", err)
	}
	if state != string(domain.WagerStatePendingReference) {
		t.Fatalf("expected WIN to remain pending, got %s", state)
	}

	if _, err := pool.Exec(ctx, `UPDATE wager_transactions SET state = 'REJECTED', failure_code = 'TEST_REJECTED' WHERE id = $1`, referenceID); err != nil {
		t.Fatalf("reject referenced BET: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE wager_transactions SET reference_next_attempt_at = NOW() WHERE id = $1`, pendingWin.TransactionID); err != nil {
		t.Fatalf("make WIN due again: %v", err)
	}
	handled, err = resolver.ResolveOne(ctx)
	if err != nil || !handled {
		t.Fatalf("reject pending WIN: handled=%v err=%v", handled, err)
	}
	var failureCode, referencedID string
	if err := pool.QueryRow(ctx, `SELECT state, failure_code, referenced_transaction_id::text FROM wager_transactions WHERE id = $1`, pendingWin.TransactionID).Scan(&state, &failureCode, &referencedID); err != nil {
		t.Fatalf("query rejected WIN: %v", err)
	}
	if state != string(domain.WagerStateRejected) || failureCode != failureCodeReferenceTerminalUnsuccessful || referencedID != referenceID.String() {
		t.Fatalf("unexpected rejected WIN: state=%s code=%s reference=%s", state, failureCode, referencedID)
	}
}

func TestReferencedWinInvalidReferenceIsDurablyRejected(t *testing.T) {
	tests := []struct {
		name         string
		kind         domain.WagerKind
		requestRound string
		failureCode  string
		mutate       func(context.Context, *testing.T, *pgxpool.Pool, string, *domain.WagerRequest)
	}{
		{name: "wrong kind", kind: domain.WagerKindWin, requestRound: "round-1", failureCode: failureCodeInvalidReferenceKind},
		{name: "wrong round", kind: domain.WagerKindBet, requestRound: "round-2", failureCode: failureCodeReferenceMismatch},
		{
			name: "wrong player", kind: domain.WagerKindBet, requestRound: "round-1", failureCode: failureCodeReferenceMismatch,
			mutate: func(ctx context.Context, t *testing.T, pool *pgxpool.Pool, referenceID string, _ *domain.WagerRequest) {
				t.Helper()
				if _, err := pool.Exec(ctx, `UPDATE wager_transactions SET player_id = 'other-player' WHERE id = $1`, referenceID); err != nil {
					t.Fatalf("alter referenced player: %v", err)
				}
			},
		},
		{
			name: "wrong wallet", kind: domain.WagerKindBet, requestRound: "round-1", failureCode: failureCodeReferenceMismatch,
			mutate: func(ctx context.Context, t *testing.T, pool *pgxpool.Pool, _ string, request *domain.WagerRequest) {
				t.Helper()
				other, err := wallet.NewService(pool).Create(ctx, wallet.CreateWalletCommand{PlayerID: "other-win-player", InitialBalance: domain.NewMoney(10000, domain.BRL)})
				if err != nil {
					t.Fatalf("create other wallet: %v", err)
				}
				request.WalletID = other.WalletID
				request.PlayerID = "other-win-player"
			},
		},
		{
			name: "wrong currency", kind: domain.WagerKindBet, requestRound: "round-1", failureCode: failureCodeReferenceMismatch,
			mutate: func(ctx context.Context, t *testing.T, pool *pgxpool.Pool, referenceID string, _ *domain.WagerRequest) {
				t.Helper()
				if _, err := pool.Exec(ctx, `UPDATE wager_transactions SET currency = 'USD' WHERE id = $1`, referenceID); err != nil {
					t.Fatalf("alter referenced currency: %v", err)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, pool, walletID := setupWinReferenceTest(t, "player-invalid-win-ref", 10000)
			service := NewService(pool)
			reference := processWinReferenceTestWager(t, ctx, service, ProcessCommand{
				IdempotencyKey: "invalid-win-reference",
				Request: domain.WagerRequest{
					ProviderID: "provider-a", ExternalTransactionID: "invalid-win-reference", PlayerID: "player-invalid-win-ref",
					WalletID: walletID, RoundID: "round-1", GameID: "game-1", Kind: tt.kind, Amount: domain.NewMoney(1000, domain.BRL),
				},
			})
			request := domain.WagerRequest{
				ProviderID: "provider-a", ExternalTransactionID: "invalid-referenced-win", PlayerID: "player-invalid-win-ref",
				WalletID: walletID, RoundID: tt.requestRound, GameID: "game-1", Kind: domain.WagerKindWin,
				Amount: domain.NewMoney(2500, domain.BRL), ReferenceExternalTransactionID: "invalid-win-reference",
			}
			if tt.mutate != nil {
				tt.mutate(ctx, t, pool, reference.TransactionID, &request)
			}
			result := processWinReferenceTestWager(t, ctx, service, ProcessCommand{
				IdempotencyKey: "invalid-referenced-win",
				Request:        request,
			})
			if result.State != domain.WagerStateRejected || result.FailureCode != tt.failureCode {
				t.Fatalf("expected rejected %s, got %+v", tt.failureCode, result)
			}
			var referencedID string
			if err := pool.QueryRow(ctx, `SELECT referenced_transaction_id::text FROM wager_transactions WHERE id = $1`, result.TransactionID).Scan(&referencedID); err != nil {
				t.Fatalf("query rejected WIN linkage: %v", err)
			}
			if referencedID != reference.TransactionID {
				t.Fatalf("expected linkage %s, got %s", reference.TransactionID, referencedID)
			}
		})
	}
}

func TestReferencedWinLookupRemainsProviderScoped(t *testing.T) {
	ctx, pool, walletID := setupWinReferenceTest(t, "player-win-provider-scope", 10000)
	service := NewService(pool)
	processWinReferenceTestWager(t, ctx, service, ProcessCommand{IdempotencyKey: "provider-b-bet", Request: domain.WagerRequest{
		ProviderID: "provider-b", ExternalTransactionID: "provider-scoped-bet", PlayerID: "player-win-provider-scope", WalletID: walletID,
		RoundID: "round-1", GameID: "game-1", Kind: domain.WagerKindBet, Amount: domain.NewMoney(1000, domain.BRL),
	}})
	result := processWinReferenceTestWager(t, ctx, service, ProcessCommand{IdempotencyKey: "provider-a-win", Request: domain.WagerRequest{
		ProviderID: "provider-a", ExternalTransactionID: "provider-a-win", PlayerID: "player-win-provider-scope", WalletID: walletID,
		RoundID: "round-1", GameID: "game-1", Kind: domain.WagerKindWin, Amount: domain.NewMoney(2500, domain.BRL),
		ReferenceExternalTransactionID: "provider-scoped-bet",
	}})
	if result.State != domain.WagerStatePendingReference {
		t.Fatalf("expected provider-scoped reference to remain pending, got %+v", result)
	}
}

func setupWinReferenceTest(t *testing.T, playerID string, initialBalance int64) (context.Context, *pgxpool.Pool, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	pool, err := pgxpool.New(ctx, "postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable")
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	t.Cleanup(pool.Close)
	cleanDatabase(t, ctx, pool)
	createdWallet, err := wallet.NewService(pool).Create(ctx, wallet.CreateWalletCommand{PlayerID: playerID, InitialBalance: domain.NewMoney(initialBalance, domain.BRL)})
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}
	return ctx, pool, createdWallet.WalletID
}

func processWinReferenceTestWager(t *testing.T, ctx context.Context, service *Service, command ProcessCommand) ProcessResult {
	t.Helper()
	result, err := service.Process(ctx, command)
	if err != nil {
		t.Fatalf("process %s %s: %v", command.Request.Kind, command.Request.ExternalTransactionID, err)
	}
	return result
}

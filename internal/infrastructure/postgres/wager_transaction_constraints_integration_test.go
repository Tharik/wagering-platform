package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	originMetadataConstraint         = "wager_transactions_origin_metadata_check"
	amountByKindConstraint           = "wager_transactions_amount_by_kind_check"
	referenceApplicabilityConstraint = "wager_transactions_reference_applicability_check"
	resultBalanceConstraint          = "wager_transactions_result_balance_non_negative"
)

type wagerConstraintRow struct {
	id                             uuid.UUID
	providerID                     any
	externalTransactionID          any
	idempotencyKey                 any
	payloadHash                    any
	walletID                       uuid.UUID
	playerID                       string
	roundID                        any
	gameID                         any
	kind                           string
	state                          string
	amount                         int64
	currency                       string
	referenceExternalTransactionID any
	referencedTransactionID        any
	failureCode                    any
	resultBalance                  any
}

func TestWagerTransactionConstraintsRejectInvalidRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	pool := openConstraintTestPool(t, ctx)
	walletID := insertConstraintTestWallet(t, ctx, pool)
	reference := validExternalConstraintRow(walletID, "BET", "PROCESSED", 1000)
	reference.externalTransactionID = "reference-bet"
	reference.idempotencyKey = "reference-bet"
	insertConstraintRow(t, ctx, pool, reference)

	tests := []struct {
		name       string
		constraint string
		mutate     func(*wagerConstraintRow)
	}{
		{name: "external missing provider", constraint: originMetadataConstraint, mutate: func(row *wagerConstraintRow) { row.providerID = nil }},
		{name: "external missing transaction ID", constraint: originMetadataConstraint, mutate: func(row *wagerConstraintRow) { row.externalTransactionID = nil }},
		{name: "external missing idempotency key", constraint: originMetadataConstraint, mutate: func(row *wagerConstraintRow) { row.idempotencyKey = nil }},
		{name: "external missing payload hash", constraint: originMetadataConstraint, mutate: func(row *wagerConstraintRow) { row.payloadHash = nil }},
		{name: "external missing round", constraint: originMetadataConstraint, mutate: func(row *wagerConstraintRow) { row.roundID = nil }},
		{name: "external missing game", constraint: originMetadataConstraint, mutate: func(row *wagerConstraintRow) { row.gameID = nil }},
		{name: "OPENING carrying external metadata", constraint: originMetadataConstraint, mutate: func(row *wagerConstraintRow) {
			*row = validOpeningConstraintRow(walletID)
			row.providerID = "provider-a"
			row.externalTransactionID = "opening-external"
			row.idempotencyKey = "opening-idempotency"
		}},
		{name: "OPENING not processed", constraint: originMetadataConstraint, mutate: func(row *wagerConstraintRow) {
			*row = validOpeningConstraintRow(walletID)
			row.state = "PENDING"
		}},
		{name: "OPENING zero", constraint: amountByKindConstraint, mutate: func(row *wagerConstraintRow) {
			*row = validOpeningConstraintRow(walletID)
			row.amount = 0
			row.resultBalance = int64(0)
		}},
		{name: "OPENING negative", constraint: amountByKindConstraint, mutate: func(row *wagerConstraintRow) {
			*row = validOpeningConstraintRow(walletID)
			row.amount = -1
			row.resultBalance = int64(-1)
		}},
		{name: "OPENING missing result balance", constraint: originMetadataConstraint, mutate: func(row *wagerConstraintRow) {
			*row = validOpeningConstraintRow(walletID)
			row.resultBalance = nil
		}},
		{name: "OPENING result balance mismatch", constraint: originMetadataConstraint, mutate: func(row *wagerConstraintRow) {
			*row = validOpeningConstraintRow(walletID)
			row.resultBalance = int64(999)
		}},
		{name: "LOSS non-zero", constraint: amountByKindConstraint, mutate: func(row *wagerConstraintRow) { row.kind = "LOSS" }},
		{name: "BET zero", constraint: amountByKindConstraint, mutate: func(row *wagerConstraintRow) { row.amount = 0 }},
		{name: "WIN zero", constraint: amountByKindConstraint, mutate: func(row *wagerConstraintRow) { row.kind = "WIN"; row.amount = 0 }},
		{name: "REFUND zero", constraint: amountByKindConstraint, mutate: func(row *wagerConstraintRow) {
			row.kind = "REFUND"
			row.amount = 0
			row.referenceExternalTransactionID = "reference-bet"
		}},
		{name: "ROLLBACK zero", constraint: amountByKindConstraint, mutate: func(row *wagerConstraintRow) {
			row.kind = "ROLLBACK"
			row.amount = 0
			row.referenceExternalTransactionID = "reference-bet"
		}},
		{name: "BET with external reference", constraint: referenceApplicabilityConstraint, mutate: func(row *wagerConstraintRow) { row.referenceExternalTransactionID = "reference-bet" }},
		{name: "LOSS with external reference", constraint: referenceApplicabilityConstraint, mutate: func(row *wagerConstraintRow) {
			row.kind = "LOSS"
			row.amount = 0
			row.referenceExternalTransactionID = "reference-bet"
		}},
		{name: "BET with internal reference", constraint: referenceApplicabilityConstraint, mutate: func(row *wagerConstraintRow) { row.referencedTransactionID = reference.id }},
		{name: "LOSS with internal reference", constraint: referenceApplicabilityConstraint, mutate: func(row *wagerConstraintRow) {
			row.kind = "LOSS"
			row.amount = 0
			row.referencedTransactionID = reference.id
		}},
		{name: "REFUND without external reference", constraint: referenceApplicabilityConstraint, mutate: func(row *wagerConstraintRow) { row.kind = "REFUND" }},
		{name: "ROLLBACK without external reference", constraint: referenceApplicabilityConstraint, mutate: func(row *wagerConstraintRow) { row.kind = "ROLLBACK" }},
		{name: "WIN internal reference without external reference", constraint: referenceApplicabilityConstraint, mutate: func(row *wagerConstraintRow) { row.kind = "WIN"; row.referencedTransactionID = reference.id }},
		{name: "negative result balance", constraint: resultBalanceConstraint, mutate: func(row *wagerConstraintRow) { row.resultBalance = int64(-1) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := validExternalConstraintRow(walletID, "BET", "PROCESSED", 1000)
			tt.mutate(&row)
			assertConstraintViolation(t, insertConstraintRowError(ctx, pool, row), tt.constraint)
		})
	}
}

func TestWagerTransactionConstraintsAcceptValidRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	pool := openConstraintTestPool(t, ctx)
	walletID := insertConstraintTestWallet(t, ctx, pool)

	insertConstraintRow(t, ctx, pool, validOpeningConstraintRow(walletID))

	referenceForWin := validExternalConstraintRow(walletID, "BET", "PROCESSED", 1000)
	referenceForWin.externalTransactionID = "bet-for-win"
	referenceForWin.idempotencyKey = "bet-for-win"
	insertConstraintRow(t, ctx, pool, referenceForWin)

	referenceForRefund := validExternalConstraintRow(walletID, "BET", "PROCESSED", 1000)
	referenceForRefund.externalTransactionID = "bet-for-refund"
	referenceForRefund.idempotencyKey = "bet-for-refund"
	insertConstraintRow(t, ctx, pool, referenceForRefund)

	referenceForRollback := validExternalConstraintRow(walletID, "BET", "PROCESSED", 1000)
	referenceForRollback.externalTransactionID = "bet-for-rollback"
	referenceForRollback.idempotencyKey = "bet-for-rollback"
	insertConstraintRow(t, ctx, pool, referenceForRollback)

	validRows := []struct {
		name string
		row  wagerConstraintRow
	}{
		{name: "BET", row: validExternalConstraintRow(walletID, "BET", "PROCESSED", 1000)},
		{name: "WIN without reference", row: validExternalConstraintRow(walletID, "WIN", "PROCESSED", 1000)},
		{name: "WIN unresolved reference", row: withExternalReference(validExternalConstraintRow(walletID, "WIN", "PENDING_REFERENCE", 1000), "missing-bet")},
		{name: "WIN resolved reference", row: withResolvedReference(validExternalConstraintRow(walletID, "WIN", "PROCESSED", 1000), "bet-for-win", referenceForWin.id)},
		{name: "LOSS zero", row: validExternalConstraintRow(walletID, "LOSS", "PROCESSED", 0)},
		{name: "REFUND pending reference", row: withExternalReference(validExternalConstraintRow(walletID, "REFUND", "PENDING_REFERENCE", 1000), "missing-refund-bet")},
		{name: "REFUND resolved reference", row: withResolvedReference(validExternalConstraintRow(walletID, "REFUND", "PROCESSED", 1000), "bet-for-refund", referenceForRefund.id)},
		{name: "ROLLBACK pending reference", row: withExternalReference(validExternalConstraintRow(walletID, "ROLLBACK", "PENDING_REFERENCE", 1000), "missing-rollback-wager")},
		{name: "ROLLBACK resolved reference", row: withResolvedReference(validExternalConstraintRow(walletID, "ROLLBACK", "PROCESSED", 1000), "bet-for-rollback", referenceForRollback.id)},
		{name: "reserved PENDING", row: validExternalConstraintRow(walletID, "BET", "PENDING", 1000)},
		{name: "reserved FAILED", row: validExternalConstraintRow(walletID, "BET", "FAILED", 1000)},
	}

	for _, tt := range validRows {
		t.Run(tt.name, func(t *testing.T) {
			insertConstraintRow(t, ctx, pool, tt.row)
		})
	}
}

func validOpeningConstraintRow(walletID uuid.UUID) wagerConstraintRow {
	return wagerConstraintRow{
		id:            uuid.New(),
		walletID:      walletID,
		playerID:      "constraint-player",
		kind:          "OPENING",
		state:         "PROCESSED",
		amount:        10000,
		currency:      "BRL",
		resultBalance: int64(10000),
	}
}

func validExternalConstraintRow(walletID uuid.UUID, kind, state string, amount int64) wagerConstraintRow {
	id := uuid.NewString()
	return wagerConstraintRow{
		id:                             uuid.New(),
		providerID:                     "provider-a",
		externalTransactionID:          "external-" + id,
		idempotencyKey:                 "idempotency-" + id,
		payloadHash:                    strings.Repeat("a", 64),
		walletID:                       walletID,
		playerID:                       "constraint-player",
		roundID:                        "round-1",
		gameID:                         "game-1",
		kind:                           kind,
		state:                          state,
		amount:                         amount,
		currency:                       "BRL",
		referenceExternalTransactionID: "",
		resultBalance:                  int64(10000),
	}
}

func withExternalReference(row wagerConstraintRow, externalID string) wagerConstraintRow {
	row.referenceExternalTransactionID = externalID
	return row
}

func withResolvedReference(row wagerConstraintRow, externalID string, referencedID uuid.UUID) wagerConstraintRow {
	row.referenceExternalTransactionID = externalID
	row.referencedTransactionID = referencedID
	return row
}

func openConstraintTestPool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(ctx, "postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable")
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func insertConstraintTestWallet(t *testing.T, ctx context.Context, pool *pgxpool.Pool) uuid.UUID {
	t.Helper()
	walletID := uuid.New()
	_, err := pool.Exec(ctx, `
		INSERT INTO wallets (
			id, player_id, currency, balance, version, created_at, updated_at
		) VALUES ($1, $2, 'BRL', 10000, 1, NOW(), NOW())
	`, walletID, "constraint-player-"+uuid.NewString())
	if err != nil {
		t.Fatalf("insert wallet: %v", err)
	}
	return walletID
}

func insertConstraintRow(t *testing.T, ctx context.Context, pool *pgxpool.Pool, row wagerConstraintRow) {
	t.Helper()
	if err := insertConstraintRowError(ctx, pool, row); err != nil {
		t.Fatalf("insert valid %s/%s wager: %v", row.kind, row.state, err)
	}
}

func insertConstraintRowError(ctx context.Context, pool *pgxpool.Pool, row wagerConstraintRow) error {
	_, err := pool.Exec(ctx, `
		INSERT INTO wager_transactions (
			id, provider_id, external_transaction_id, idempotency_key, payload_hash,
			wallet_id, player_id, round_id, game_id, kind, state, amount, currency,
			reference_external_transaction_id, referenced_transaction_id,
			failure_code, result_balance, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9, $10, $11, $12, $13,
			$14, $15, $16, $17, NOW(), NOW()
		)
	`,
		row.id,
		row.providerID,
		row.externalTransactionID,
		row.idempotencyKey,
		row.payloadHash,
		row.walletID,
		row.playerID,
		row.roundID,
		row.gameID,
		row.kind,
		row.state,
		row.amount,
		row.currency,
		row.referenceExternalTransactionID,
		row.referencedTransactionID,
		row.failureCode,
		row.resultBalance,
	)
	return err
}

func assertConstraintViolation(t *testing.T, err error, constraint string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected constraint %s to reject row", constraint)
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("expected PostgreSQL error for %s, got %v", constraint, err)
	}
	if pgErr.Code != "23514" {
		t.Fatalf("expected check violation for %s, got SQLSTATE %s: %v", constraint, pgErr.Code, err)
	}
	if pgErr.ConstraintName != constraint {
		t.Fatalf("expected constraint %s, got %s: %v", constraint, pgErr.ConstraintName, err)
	}
}

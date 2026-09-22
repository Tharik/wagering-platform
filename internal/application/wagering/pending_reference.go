package wagering

import (
	"context"
	"fmt"
	"time"

	"github.com/Tharik/wagering-platform/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

const (
	referenceRetryDelay = 5 * time.Second
	referenceTTL        = 5 * time.Minute
)

func persistPendingReference(
	ctx context.Context,
	tx pgx.Tx,
	cmd ProcessCommand,
	payloadHash string,
	currentBalance domain.Money,
	now time.Time,
) (ProcessResult, error) {
	transactionID := uuid.New()
	nextAttemptAt := now.Add(referenceRetryDelay)
	expiresAt := now.Add(referenceTTL)

	_, err := tx.Exec(
		ctx,
		`
		INSERT INTO wager_transactions (
			id,
			provider_id,
			external_transaction_id,
			idempotency_key,
			payload_hash,
			wallet_id,
			player_id,
			round_id,
			game_id,
			kind,
			state,
			amount,
			currency,
			reference_external_transaction_id,
			result_balance,
			reference_attempts,
			reference_next_attempt_at,
			reference_expires_at,
			correlation_id,
			causation_id,
			created_at,
			updated_at
		)
		VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9,
			$10, 'PENDING_REFERENCE',
			$11, $12, $13, $14,
			0, $15, $16,
			$17, $18,
			$19, $19
		)
		`,
		transactionID,
		cmd.Request.ProviderID,
		cmd.Request.ExternalTransactionID,
		cmd.IdempotencyKey,
		payloadHash,
		cmd.Request.WalletID,
		cmd.Request.PlayerID,
		cmd.Request.RoundID,
		cmd.Request.GameID,
		string(cmd.Request.Kind),
		cmd.Request.Amount.Amount(),
		string(cmd.Request.Amount.Currency()),
		cmd.Request.ReferenceExternalTransactionID,
		currentBalance.Amount(),
		nextAttemptAt,
		expiresAt,
		cmd.CorrelationID,
		nullableString(cmd.CausationID),
		now,
	)
	if err != nil {
		return ProcessResult{}, fmt.Errorf(
			"insert pending reference transaction: %w",
			err,
		)
	}

	if err := insertPendingReferenceEvent(
		ctx,
		tx,
		transactionID,
		cmd,
		now,
	); err != nil {
		return ProcessResult{}, err
	}

	return ProcessResult{
		TransactionID: transactionID.String(),
		State:         domain.WagerStatePendingReference,
		Balance:       currentBalance,
	}, nil
}

func insertPendingReferenceEvent(
	ctx context.Context,
	tx pgx.Tx,
	transactionID uuid.UUID,
	cmd ProcessCommand,
	now time.Time,
) error {
	payload := map[string]any{
		"transactionId":                  transactionID.String(),
		"providerId":                     cmd.Request.ProviderID,
		"externalTransactionId":          cmd.Request.ExternalTransactionID,
		"referenceExternalTransactionId": cmd.Request.ReferenceExternalTransactionID,
	}

	if err := insertOutboxEvent(
		ctx,
		tx,
		transactionID,
		"WagerTransactionPendingReference",
		cmd.CorrelationID,
		cmd.CausationID,
		payload,
		now,
	); err != nil {
		return fmt.Errorf(
			"insert pending reference outbox event: %w",
			err,
		)
	}

	return nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

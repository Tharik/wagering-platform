package wagering

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Tharik/wagering-platform/internal/application/eventpayload"
	"github.com/Tharik/wagering-platform/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func persistRejectedTransaction(
	ctx context.Context,
	tx pgx.Tx,
	cmd ProcessCommand,
	payloadHash string,
	wallet domain.Wallet,
	failureCode string,
	referencedTransactionID *uuid.UUID,
) (ProcessResult, error) {
	transactionID := uuid.New()
	now := time.Now().UTC()

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
			referenced_transaction_id,
			failure_code,
			result_balance,
			created_at,
			updated_at
		)
		VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9,
			$10, 'REJECTED',
			$11, $12, $13, $14,
			$15, $16, $17, $17
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
		referencedTransactionID,
		failureCode,
		wallet.Balance.Amount(),
		now,
	)
	if err != nil {
		return ProcessResult{}, fmt.Errorf(
			"insert rejected wager transaction: %w",
			mapWagerUniqueViolation(err),
		)
	}

	if err := insertRejectedEvent(
		ctx,
		tx,
		transactionID,
		wallet.ID,
		cmd.Request.ProviderID,
		cmd.Request.Kind,
		failureCode,
		cmd.CorrelationID,
		cmd.CausationID,
		now,
	); err != nil {
		return ProcessResult{}, err
	}

	return ProcessResult{
		TransactionID: transactionID.String(),
		State:         domain.WagerStateRejected,
		Balance:       wallet.Balance,
		FailureCode:   failureCode,
	}, nil
}

func insertProcessedEvents(
	ctx context.Context,
	tx pgx.Tx,
	transactionID uuid.UUID,
	kind domain.WagerKind,
	wallet domain.Wallet,
	amount domain.Money,
	balanceBefore domain.Money,
	direction string,
	correlationID string,
	causationID string,
	now time.Time,
) error {
	if err := insertOutboxEvent(
		ctx,
		tx,
		transactionID,
		"WagerTransactionProcessed",
		correlationID,
		causationID,
		eventpayload.WagerTransactionProcessedData{
			TransactionID: transactionID.String(),
			WalletID:      wallet.ID,
			Kind:          string(kind),
		},
		now,
	); err != nil {
		return err
	}

	// LOSS is processed successfully, but there is no balance movement.
	if kind == domain.WagerKindLoss {
		return nil
	}

	if err := insertOutboxEvent(
		ctx,
		tx,
		transactionID,
		"WalletBalanceChanged",
		correlationID,
		causationID,
		eventpayload.WalletBalanceChangedData{
			WalletID:      wallet.ID,
			TransactionID: transactionID.String(),
			Direction:     direction,
			Money:         eventpayload.NewMoney(amount),
			BalanceBefore: eventpayload.NewMoney(balanceBefore),
			BalanceAfter:  eventpayload.NewMoney(wallet.Balance),
			WalletVersion: wallet.Version,
		},
		now,
	); err != nil {
		return err
	}

	return nil
}

func insertRejectedEvent(
	ctx context.Context,
	tx pgx.Tx,
	transactionID uuid.UUID,
	walletID string,
	providerID string,
	kind domain.WagerKind,
	failureCode string,
	correlationID string,
	causationID string,
	now time.Time,
) error {
	return insertOutboxEvent(
		ctx,
		tx,
		transactionID,
		"WagerTransactionRejected",
		correlationID,
		causationID,
		eventpayload.WagerTransactionRejectedData{
			TransactionID: transactionID.String(),
			WalletID:      walletID,
			ProviderID:    providerID,
			Kind:          string(kind),
			FailureCode:   failureCode,
		},
		now,
	)
}

func insertOutboxEvent(
	ctx context.Context,
	tx pgx.Tx,
	aggregateID uuid.UUID,
	eventType string,
	correlationID string,
	causationID string,
	data any,
	now time.Time,
) error {
	eventID := uuid.New()

	envelope := map[string]any{
		"eventId":       eventID.String(),
		"eventType":     eventType,
		"aggregateId":   aggregateID.String(),
		"correlationId": correlationID,
		"occurredAt":    now.Format(time.RFC3339Nano),
		"version":       1,
		"data":          data,
	}

	if causationID != "" {
		envelope["causationId"] = causationID
	}

	payload, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal outbox event: %w", err)
	}

	_, err = tx.Exec(
		ctx,
		`
		INSERT INTO outbox_events (
			id,
			aggregate_id,
			event_type,
			payload,
			occurred_at,
			attempts,
			next_attempt_at
		)
		VALUES ($1, $2, $3, $4::jsonb, $5, 0, $5)
		`,
		eventID,
		aggregateID,
		eventType,
		payload,
		now,
	)
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}

	return nil
}

package wagering

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Tharik/wagering-platform/internal/domain"
	"github.com/Tharik/wagering-platform/internal/observability"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const maxReferenceRetryDelay = time.Minute

type PendingReferenceResolver struct {
	pool    *pgxpool.Pool
	metrics *observability.Metrics
}

func NewPendingReferenceResolver(
	pool *pgxpool.Pool,
) *PendingReferenceResolver {
	return &PendingReferenceResolver{
		pool:    pool,
		metrics: observability.NewMetrics(),
	}
}

func NewPendingReferenceResolverWithMetrics(
	pool *pgxpool.Pool,
	metrics *observability.Metrics,
) *PendingReferenceResolver {
	return &PendingReferenceResolver{
		pool:    pool,
		metrics: metrics,
	}
}

// ResolveOne attempts to handle one due PENDING_REFERENCE transaction.
//
// It returns:
//   - true when a pending transaction was found and handled;
//   - false when there was nothing due to process.
func (r *PendingReferenceResolver) ResolveOne(
	ctx context.Context,
) (bool, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf(
			"begin pending reference transaction: %w",
			err,
		)
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	pending, found, err := findDuePendingReference(ctx, tx)
	if err != nil {
		return false, err
	}

	if !found {
		return false, nil
	}

	// Lock the wallet before making any financial decision.
	wallet, err := lockWallet(
		ctx,
		tx,
		pending.Request.WalletID,
	)
	if err != nil {
		return false, err
	}

	now := time.Now().UTC()
	// TTL is terminal. Once expired, the transaction must never move money.
	if pending.ReferenceExpiresAt != nil &&
		!now.Before(*pending.ReferenceExpiresAt) {

		if err := rejectPendingReference(
			ctx,
			tx,
			pending,
			wallet,
			"REFERENCE_EXPIRED",
			now,
			nil,
		); err != nil {
			return false, err
		}

		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf(
				"commit expired pending reference: %w",
				err,
			)
		}

		r.metrics.IncWagersRejected()

		return true, nil
	}

	reference, found, err := findReferencedTransaction(
		ctx,
		tx,
		pending.Request,
	)
	if err != nil {
		return false, err
	}

	if !found {
		if err := schedulePendingReferenceRetry(
			ctx,
			tx,
			pending,
			now,
		); err != nil {
			return false, err
		}

		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf(
				"commit pending reference retry: %w",
				err,
			)
		}

		// This is only a retry. The transaction is still
		// PENDING_REFERENCE, so do not count it again.
		return true, nil
	}

	switch reference.State {
	case domain.WagerStatePending,
		domain.WagerStatePendingReference:
		if err := schedulePendingReferenceRetry(ctx, tx, pending, now); err != nil {
			return false, err
		}

		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf(
				"commit pending reference dependency retry: %w",
				err,
			)
		}

		return true, nil

	case domain.WagerStateRejected,
		domain.WagerStateFailed:
		if err := rejectPendingReference(
			ctx,
			tx,
			pending,
			wallet,
			failureCodeReferenceTerminalUnsuccessful,
			now,
			&reference.ID,
		); err != nil {
			return false, err
		}

		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf(
				"commit terminal unsuccessful reference: %w",
				err,
			)
		}

		r.metrics.IncWagersRejected()

		return true, nil

	case domain.WagerStateProcessed:
		// Validate the immutable reference below.

	default:
		return false, fmt.Errorf(
			"unsupported referenced transaction state %q",
			reference.State,
		)
	}

	if err := validateReference(
		pending.Request,
		reference,
	); err != nil {
		failureCode := referenceFailureCode(err)

		if err := rejectPendingReference(
			ctx,
			tx,
			pending,
			wallet,
			failureCode,
			now,
			&reference.ID,
		); err != nil {
			return false, err
		}

		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf(
				"commit invalid pending reference: %w",
				err,
			)
		}

		r.metrics.IncWagersRejected()

		return true, nil
	}

	alreadyReversed, err := referenceAlreadyReversed(
		ctx,
		tx,
		reference.ID,
	)
	if err != nil {
		return false, err
	}

	if alreadyReversed {
		if err := rejectPendingReference(
			ctx,
			tx,
			pending,
			wallet,
			failureCodeAlreadyReversed,
			now,
			&reference.ID,
		); err != nil {
			return false, err
		}

		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf(
				"commit already reversed pending reference: %w",
				err,
			)
		}

		r.metrics.IncWagersRejected()

		return true, nil
	}

	direction, err := reversalDirection(
		pending.Request.Kind,
		reference,
	)
	if err != nil {
		return false, err
	}

	balanceBefore := wallet.Balance

	switch direction {
	case movementCredit:
		if err := wallet.Credit(
			pending.Request.Amount,
			now,
		); err != nil {
			return false, err
		}

	case movementDebit:
		if err := wallet.Debit(
			pending.Request.Amount,
			now,
		); err != nil {
			if errors.Is(err, domain.ErrInsufficientFunds) {
				if err := rejectPendingReference(
					ctx,
					tx,
					pending,
					wallet,
					"REVERSAL_INSUFFICIENT_FUNDS",
					now,
					&reference.ID,
				); err != nil {
					return false, err
				}

				if err := tx.Commit(ctx); err != nil {
					return false, fmt.Errorf(
						"commit rejected pending reference: %w",
						err,
					)
				}

				r.metrics.IncWagersRejected()

				return true, nil
			}

			return false, err
		}

	default:
		return false, ErrInvalidReferenceKind
	}

	_, err = tx.Exec(
		ctx,
		`
		UPDATE wallets
		SET
			balance = $2,
			version = $3,
			updated_at = $4
		WHERE id = $1
		`,
		wallet.ID,
		wallet.Balance.Amount(),
		wallet.Version,
		now,
	)
	if err != nil {
		return false, fmt.Errorf(
			"update wallet resolving pending reference: %w",
			err,
		)
	}

	commandTag, err := tx.Exec(
		ctx,
		`
		UPDATE wager_transactions
		SET
			state = 'PROCESSED',
			referenced_transaction_id = $2,
			result_balance = $3,
			failure_code = NULL,
			reference_next_attempt_at = NULL,
			updated_at = $4
		WHERE id = $1
		  AND state = 'PENDING_REFERENCE'
		`,
		pending.ID,
		reference.ID,
		wallet.Balance.Amount(),
		now,
	)
	if err != nil {
		return false, fmt.Errorf(
			"mark pending reference processed: %w",
			err,
		)
	}

	if commandTag.RowsAffected() != 1 {
		return false, fmt.Errorf(
			"pending reference %s was not marked processed",
			pending.ID,
		)
	}

	ledgerDirection := "CREDIT"

	if direction == movementDebit {
		ledgerDirection = "DEBIT"
	}

	_, err = tx.Exec(
		ctx,
		`
		INSERT INTO ledger_entries (
			id,
			wallet_id,
			transaction_id,
			direction,
			amount,
			balance_before,
			balance_after,
			created_at
		)
		VALUES (
			$1, $2, $3, $4,
			$5, $6, $7, $8
		)
		`,
		uuid.New(),
		wallet.ID,
		pending.ID,
		ledgerDirection,
		pending.Request.Amount.Amount(),
		balanceBefore.Amount(),
		wallet.Balance.Amount(),
		now,
	)
	if err != nil {
		return false, fmt.Errorf(
			"insert ledger resolving pending reference: %w",
			err,
		)
	}

	if err := insertProcessedEvents(
		ctx,
		tx,
		pending.ID,
		pending.Request.Kind,
		wallet,
		pending.Request.Amount,
		balanceBefore,
		ledgerDirection,
		pending.CorrelationID,
		pending.CausationID,
		now,
	); err != nil {
		return false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf(
			"commit resolved pending reference: %w",
			err,
		)
	}

	r.metrics.IncWagersProcessed()

	return true, nil
}

type pendingReferenceTransaction struct {
	ID                 uuid.UUID
	Request            domain.WagerRequest
	ReferenceAttempts  int
	ReferenceExpiresAt *time.Time
	CorrelationID      string
	CausationID        string
}

func findDuePendingReference(
	ctx context.Context,
	tx pgx.Tx,
) (pendingReferenceTransaction, bool, error) {
	var pending pendingReferenceTransaction

	var (
		walletID uuid.UUID
		kind     string
		amount   int64
		currency string
	)

	err := tx.QueryRow(
		ctx,
		`
		SELECT
			id,
			provider_id,
			external_transaction_id,
			player_id,
			wallet_id,
			COALESCE(round_id, ''),
			COALESCE(game_id, ''),
			kind,
			amount,
			currency,
			reference_external_transaction_id,
			reference_attempts,
			reference_expires_at,
			COALESCE(correlation_id, ''),
			COALESCE(causation_id, '')
		FROM wager_transactions
		WHERE state = 'PENDING_REFERENCE'
		  AND (
			reference_next_attempt_at IS NULL
			OR reference_next_attempt_at <= NOW()
		  )
		ORDER BY created_at
		FOR UPDATE SKIP LOCKED
		LIMIT 1
		`,
	).Scan(
		&pending.ID,
		&pending.Request.ProviderID,
		&pending.Request.ExternalTransactionID,
		&pending.Request.PlayerID,
		&walletID,
		&pending.Request.RoundID,
		&pending.Request.GameID,
		&kind,
		&amount,
		&currency,
		&pending.Request.ReferenceExternalTransactionID,
		&pending.ReferenceAttempts,
		&pending.ReferenceExpiresAt,
		&pending.CorrelationID,
		&pending.CausationID,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return pendingReferenceTransaction{}, false, nil
	}

	if err != nil {
		return pendingReferenceTransaction{}, false, fmt.Errorf(
			"find due pending reference: %w",
			err,
		)
	}

	pending.Request.WalletID = walletID.String()
	pending.Request.Kind = domain.WagerKind(kind)
	pending.Request.Amount = domain.NewMoney(
		amount,
		domain.Currency(currency),
	)

	return pending, true, nil
}

func schedulePendingReferenceRetry(
	ctx context.Context,
	tx pgx.Tx,
	pending pendingReferenceTransaction,
	now time.Time,
) error {
	nextAttempts := pending.ReferenceAttempts + 1
	delay := referenceRetryBackoff(nextAttempts)
	nextAttemptAt := now.Add(delay)

	// Never schedule a retry after the transaction TTL.
	// Scheduling exactly at expiry guarantees that the next resolver pass
	// takes the REFERENCE_EXPIRED path.
	if pending.ReferenceExpiresAt != nil &&
		nextAttemptAt.After(*pending.ReferenceExpiresAt) {
		nextAttemptAt = *pending.ReferenceExpiresAt
	}

	commandTag, err := tx.Exec(
		ctx,
		`
		UPDATE wager_transactions
		SET
			reference_attempts = $2,
			reference_next_attempt_at = $3,
			updated_at = $4
		WHERE id = $1
		  AND state = 'PENDING_REFERENCE'
		`,
		pending.ID,
		nextAttempts,
		nextAttemptAt,
		now,
	)
	if err != nil {
		return fmt.Errorf(
			"schedule pending reference retry: %w",
			err,
		)
	}

	if commandTag.RowsAffected() != 1 {
		return fmt.Errorf(
			"pending reference %s was not scheduled for retry",
			pending.ID,
		)
	}

	return nil
}

func referenceRetryBackoff(attempt int) time.Duration {
	if attempt <= 1 {
		return referenceRetryDelay
	}

	delay := referenceRetryDelay

	for i := 1; i < attempt; i++ {
		if delay >= maxReferenceRetryDelay/2 {
			return maxReferenceRetryDelay
		}

		delay *= 2
	}

	if delay > maxReferenceRetryDelay {
		return maxReferenceRetryDelay
	}

	return delay
}

func referenceFailureCode(err error) string {
	switch {
	case errors.Is(err, ErrReferenceMismatch):
		return failureCodeReferenceMismatch

	case errors.Is(err, ErrReferenceAmountMismatch):
		return failureCodeReferenceAmountMismatch

	case errors.Is(err, ErrInvalidReferenceKind):
		return failureCodeInvalidReferenceKind

	default:
		return failureCodeInvalidReference
	}
}

func rejectPendingReference(
	ctx context.Context,
	tx pgx.Tx,
	pending pendingReferenceTransaction,
	wallet domain.Wallet,
	failureCode string,
	now time.Time,
	referencedTransactionID *uuid.UUID,
) error {
	commandTag, err := tx.Exec(
		ctx,
		`
		UPDATE wager_transactions
		SET
			state = 'REJECTED',
			failure_code = $2,
			result_balance = $3,
			referenced_transaction_id = $4,
			reference_next_attempt_at = NULL,
			updated_at = $5
		WHERE id = $1
		  AND state = 'PENDING_REFERENCE'
		`,
		pending.ID,
		failureCode,
		wallet.Balance.Amount(),
		referencedTransactionID,
		now,
	)
	if err != nil {
		return fmt.Errorf(
			"reject pending reference: %w",
			err,
		)
	}

	if commandTag.RowsAffected() != 1 {
		return fmt.Errorf(
			"pending reference %s was not rejected",
			pending.ID,
		)
	}

	if err := insertOutboxEvent(
		ctx,
		tx,
		pending.ID,
		"WagerTransactionRejected",
		pending.CorrelationID,
		pending.CausationID,
		map[string]any{
			"transactionId": pending.ID.String(),
			"walletId":      wallet.ID,
			"providerId":    pending.Request.ProviderID,
			"kind":          string(pending.Request.Kind),
			"failureCode":   failureCode,
		},
		now,
	); err != nil {
		return err
	}

	return nil
}

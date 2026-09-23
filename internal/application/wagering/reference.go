package wagering

import (
	"context"
	"errors"
	"fmt"

	"github.com/Tharik/wagering-platform/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var (
	ErrReferenceMismatch       = errors.New("referenced transaction does not match reversal")
	ErrInvalidReferenceKind    = errors.New("invalid referenced transaction kind")
	ErrReferenceAmountMismatch = errors.New("reversal amount must match referenced transaction")
	ErrAlreadyReversed         = errors.New("referenced transaction has already been reversed")
)

const (
	failureCodeReferenceMismatch             = "REFERENCE_MISMATCH"
	failureCodeReferenceAmountMismatch       = "REFERENCE_AMOUNT_MISMATCH"
	failureCodeInvalidReferenceKind          = "INVALID_REFERENCE_KIND"
	failureCodeInvalidReference              = "INVALID_REFERENCE"
	failureCodeReferenceTerminalUnsuccessful = "REFERENCE_TERMINAL_UNSUCCESSFUL"
	failureCodeAlreadyReversed               = "ALREADY_REVERSED"
)

type referencedTransaction struct {
	ID                    uuid.UUID
	ProviderID            string
	ExternalTransactionID string
	WalletID              uuid.UUID
	PlayerID              string
	RoundID               string
	Kind                  domain.WagerKind
	State                 domain.WagerState
	Amount                domain.Money
}

func findReferencedTransaction(
	ctx context.Context,
	tx pgx.Tx,
	request domain.WagerRequest,
) (referencedTransaction, bool, error) {
	if request.ReferenceExternalTransactionID == "" {
		return referencedTransaction{}, false, ErrReferenceMismatch
	}

	var reference referencedTransaction
	var currency string
	var amount int64

	err := tx.QueryRow(
		ctx,
		`
		SELECT
			id,
			provider_id,
			external_transaction_id,
			wallet_id,
			player_id,
			COALESCE(round_id, ''),
			kind,
			state,
			amount,
			currency
		FROM wager_transactions
		WHERE provider_id = $1
		  AND external_transaction_id = $2
		`,
		request.ProviderID,
		request.ReferenceExternalTransactionID,
	).Scan(
		&reference.ID,
		&reference.ProviderID,
		&reference.ExternalTransactionID,
		&reference.WalletID,
		&reference.PlayerID,
		&reference.RoundID,
		&reference.Kind,
		&reference.State,
		&amount,
		&currency,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return referencedTransaction{}, false, nil
	}

	if err != nil {
		return referencedTransaction{}, false, fmt.Errorf(
			"find referenced transaction: %w",
			err,
		)
	}

	reference.Amount = domain.NewMoney(
		amount,
		domain.Currency(currency),
	)

	return reference, true, nil
}

func validateReference(
	request domain.WagerRequest,
	reference referencedTransaction,
) error {
	// The reference must belong to exactly the same financial context.
	if reference.ProviderID != request.ProviderID ||
		reference.PlayerID != request.PlayerID ||
		reference.WalletID.String() != request.WalletID ||
		reference.RoundID != request.RoundID ||
		reference.Amount.Currency() != request.Amount.Currency() {
		return ErrReferenceMismatch
	}

	switch request.Kind {
	case domain.WagerKindWin:
		if reference.Kind != domain.WagerKindBet {
			return ErrInvalidReferenceKind
		}

	case domain.WagerKindRefund:
		// REFUND is only valid for a previously processed BET.
		if reference.Kind != domain.WagerKindBet {
			return ErrInvalidReferenceKind
		}

		// No partial refunds.
		if reference.Amount.Amount() != request.Amount.Amount() {
			return ErrReferenceAmountMismatch
		}

	case domain.WagerKindRollback:
		// ROLLBACK reverses the financial effect of a previously
		// processed BET, WIN or REFUND.
		switch reference.Kind {
		case domain.WagerKindBet,
			domain.WagerKindWin,
			domain.WagerKindRefund:
			// Valid.

		default:
			return ErrInvalidReferenceKind
		}

		// No partial rollback.
		if reference.Amount.Amount() != request.Amount.Amount() {
			return ErrReferenceAmountMismatch
		}

	default:
		return ErrInvalidReferenceKind
	}

	return nil
}

func referenceAlreadyReversed(
	ctx context.Context,
	tx pgx.Tx,
	referenceID uuid.UUID,
) (bool, error) {
	var exists bool

	err := tx.QueryRow(
		ctx,
		`
		SELECT EXISTS (
			SELECT 1
			FROM wager_transactions
			WHERE referenced_transaction_id = $1
			  AND state = 'PROCESSED'
			  AND kind IN ('REFUND', 'ROLLBACK')
		)
		`,
		referenceID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf(
			"check existing reversal: %w",
			err,
		)
	}

	return exists, nil
}

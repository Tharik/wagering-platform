package wagering

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Tharik/wagering-platform/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	wagerIdempotencyUniqueConstraint      = "wager_idempotency_unique"
	wagerExternalIdentityUniqueConstraint = "wager_external_identity_unique"
)

func mapWagerUniqueViolation(err error) error {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		return err
	}

	switch pgErr.ConstraintName {
	case wagerIdempotencyUniqueConstraint:
		return ErrIdempotencyConflict

	case wagerExternalIdentityUniqueConstraint:
		return ErrExternalTransactionExists

	default:
		return err
	}
}

func findIdempotentReplay(
	ctx context.Context,
	tx pgx.Tx,
	providerID string,
	idempotencyKey string,
	payloadHash string,
) (ProcessResult, bool, error) {
	var (
		transactionID string
		storedHash    string
		state         string
		currency      string
		resultBalance *int64
		failureCode   *string
	)

	err := tx.QueryRow(
		ctx,
		`
		SELECT
			id::text,
			payload_hash,
			state::text,
			currency,
			result_balance,
			failure_code
		FROM wager_transactions
		WHERE provider_id = $1
		  AND idempotency_key = $2
		`,
		providerID,
		idempotencyKey,
	).Scan(
		&transactionID,
		&storedHash,
		&state,
		&currency,
		&resultBalance,
		&failureCode,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return ProcessResult{}, false, nil
	}

	if err != nil {
		return ProcessResult{}, false,
			fmt.Errorf("find idempotent transaction: %w", err)
	}

	if storedHash != payloadHash {
		return ProcessResult{}, false, ErrIdempotencyConflict
	}

	var balance int64
	if resultBalance != nil {
		balance = *resultBalance
	}

	var failure string
	if failureCode != nil {
		failure = *failureCode
	}

	return ProcessResult{
		TransactionID:    transactionID,
		State:            domain.WagerState(state),
		Balance:          domain.NewMoney(balance, domain.Currency(currency)),
		IdempotentReplay: true,
		FailureCode:      failure,
	}, true, nil
}

func externalTransactionExists(
	ctx context.Context,
	tx pgx.Tx,
	providerID string,
	externalTransactionID string,
) (bool, error) {
	var exists bool

	err := tx.QueryRow(
		ctx,
		`
		SELECT EXISTS (
			SELECT 1
			FROM wager_transactions
			WHERE provider_id = $1
			  AND external_transaction_id = $2
		)
		`,
		providerID,
		externalTransactionID,
	).Scan(&exists)

	if err != nil {
		return false, fmt.Errorf(
			"check external transaction identity: %w",
			err,
		)
	}

	return exists, nil
}

func lockWallet(
	ctx context.Context,
	tx pgx.Tx,
	walletID string,
) (domain.Wallet, error) {
	var (
		id        string
		playerID  string
		currency  string
		balance   int64
		version   int64
		createdAt time.Time
		updatedAt time.Time
	)

	err := tx.QueryRow(
		ctx,
		`
		SELECT
			id::text,
			player_id,
			currency,
			balance,
			version,
			created_at,
			updated_at
		FROM wallets
		WHERE id = $1
		FOR UPDATE
		`,
		walletID,
	).Scan(
		&id,
		&playerID,
		&currency,
		&balance,
		&version,
		&createdAt,
		&updatedAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Wallet{}, ErrWalletNotFound
	}

	if err != nil {
		return domain.Wallet{}, fmt.Errorf("lock wallet: %w", err)
	}

	wallet, err := domain.ReconstituteWallet(
		id,
		playerID,
		domain.NewMoney(balance, domain.Currency(currency)),
		version,
		createdAt,
		updatedAt,
	)
	if err != nil {
		return domain.Wallet{}, fmt.Errorf("reconstitute wallet: %w", err)
	}
	return wallet, nil
}

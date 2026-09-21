package wallet

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var ErrWalletNotFound = errors.New("wallet not found")

type WalletResult struct {
	WalletID string
	PlayerID string
	Currency string
	Balance  int64
	Version  int64
}

type LedgerEntryResult struct {
	ID            string
	TransactionID string
	Direction     string
	Amount        int64
	BalanceBefore int64
	BalanceAfter  int64
	CreatedAt     time.Time
}

func (s *Service) Get(
	ctx context.Context,
	walletID string,
) (WalletResult, error) {
	id, err := uuid.Parse(walletID)
	if err != nil {
		return WalletResult{}, ErrWalletNotFound
	}

	var result WalletResult

	err = s.pool.QueryRow(
		ctx,
		`
		SELECT
			id,
			player_id,
			currency,
			balance,
			version
		FROM wallets
		WHERE id = $1
		`,
		id,
	).Scan(
		&result.WalletID,
		&result.PlayerID,
		&result.Currency,
		&result.Balance,
		&result.Version,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return WalletResult{}, ErrWalletNotFound
	}

	if err != nil {
		return WalletResult{}, fmt.Errorf(
			"query wallet: %w",
			err,
		)
	}

	return result, nil
}

func (s *Service) Ledger(
	ctx context.Context,
	walletID string,
) ([]LedgerEntryResult, error) {
	id, err := uuid.Parse(walletID)
	if err != nil {
		return nil, ErrWalletNotFound
	}

	var exists bool

	err = s.pool.QueryRow(
		ctx,
		`
		SELECT EXISTS (
			SELECT 1
			FROM wallets
			WHERE id = $1
		)
		`,
		id,
	).Scan(&exists)

	if err != nil {
		return nil, fmt.Errorf(
			"check wallet existence: %w",
			err,
		)
	}

	if !exists {
		return nil, ErrWalletNotFound
	}

	rows, err := s.pool.Query(
		ctx,
		`
		SELECT
			id,
			transaction_id,
			direction,
			amount,
			balance_before,
			balance_after,
			created_at
		FROM ledger_entries
		WHERE wallet_id = $1
		ORDER BY created_at, id
		`,
		id,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"query wallet ledger: %w",
			err,
		)
	}
	defer rows.Close()

	entries := make([]LedgerEntryResult, 0)

	for rows.Next() {
		var entry LedgerEntryResult

		if err := rows.Scan(
			&entry.ID,
			&entry.TransactionID,
			&entry.Direction,
			&entry.Amount,
			&entry.BalanceBefore,
			&entry.BalanceAfter,
			&entry.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf(
				"scan ledger entry: %w",
				err,
			)
		}

		entries = append(entries, entry)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate ledger entries: %w",
			err,
		)
	}

	return entries, nil
}

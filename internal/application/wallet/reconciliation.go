package wallet

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/Tharik/wagering-platform/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type ReconciliationResult struct {
	WalletID          string
	Currency          string
	StoredBalance     int64
	CalculatedBalance int64
	Difference        int64
	CheckedEntries    int
	Consistent        bool
}

func (s *Service) Reconcile(
	ctx context.Context,
	walletID string,
) (ReconciliationResult, error) {
	id, err := uuid.Parse(walletID)
	if err != nil {
		return ReconciliationResult{}, ErrWalletNotFound
	}

	tx, err := s.pool.BeginTx(
		ctx,
		pgx.TxOptions{
			IsoLevel:   pgx.RepeatableRead,
			AccessMode: pgx.ReadOnly,
		},
	)
	if err != nil {
		return ReconciliationResult{}, fmt.Errorf(
			"begin reconciliation transaction: %w",
			err,
		)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	wallet, err := loadReconciliationWallet(ctx, tx, id)
	if err != nil {
		return ReconciliationResult{}, err
	}

	entries, err := loadReconciliationLedger(ctx, tx, id)
	if err != nil {
		return ReconciliationResult{}, err
	}

	calculatedBalance, ledgerValid, err := calculateLedgerBalance(
		entries,
		domain.Currency(wallet.Currency),
	)
	if err != nil {
		return ReconciliationResult{}, err
	}

	storedMoney := domain.NewMoney(
		wallet.Balance,
		domain.Currency(wallet.Currency),
	)
	calculatedMoney := domain.NewMoney(
		calculatedBalance,
		domain.Currency(wallet.Currency),
	)
	difference, err := storedMoney.Subtract(calculatedMoney)
	if err != nil {
		return ReconciliationResult{}, fmt.Errorf(
			"calculate reconciliation difference: %w",
			err,
		)
	}

	result := ReconciliationResult{
		WalletID:          wallet.WalletID,
		Currency:          wallet.Currency,
		StoredBalance:     wallet.Balance,
		CalculatedBalance: calculatedBalance,
		Difference:        difference.Amount(),
		CheckedEntries:    len(entries),
		Consistent:        ledgerValid && difference.IsZero(),
	}

	if err := tx.Commit(ctx); err != nil {
		return ReconciliationResult{}, fmt.Errorf(
			"commit reconciliation transaction: %w",
			err,
		)
	}

	if !result.Consistent {
		s.metrics.IncReconciliationDivergences()
		s.logger.Warn(
			"wallet reconciliation divergence",
			slog.String("walletId", result.WalletID),
			slog.String("storedBalance", storedMoney.String()),
			slog.String("calculatedBalance", calculatedMoney.String()),
			slog.String("difference", difference.String()),
			slog.Int("checkedEntries", result.CheckedEntries),
		)
	}

	return result, nil
}

func loadReconciliationWallet(
	ctx context.Context,
	tx pgx.Tx,
	walletID uuid.UUID,
) (WalletResult, error) {
	var result WalletResult
	err := tx.QueryRow(
		ctx,
		`
		SELECT id, player_id, currency, balance, version
		FROM wallets
		WHERE id = $1
		`,
		walletID,
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
			"query reconciliation wallet: %w",
			err,
		)
	}
	return result, nil
}

func loadReconciliationLedger(
	ctx context.Context,
	tx pgx.Tx,
	walletID uuid.UUID,
) ([]LedgerEntryResult, error) {
	rows, err := tx.Query(
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
		walletID,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"query reconciliation ledger: %w",
			err,
		)
	}
	defer rows.Close()

	return scanLedgerEntries(rows)
}

func calculateLedgerBalance(
	entries []LedgerEntryResult,
	currency domain.Currency,
) (int64, bool, error) {
	if len(entries) == 0 {
		return 0, true, nil
	}

	calculated := domain.NewMoney(entries[0].BalanceBefore, currency)
	valid := true

	for _, entry := range entries {
		if entry.BalanceBefore != calculated.Amount() {
			valid = false
		}

		amount := domain.NewMoney(entry.Amount, currency)
		var expected domain.Money
		var err error

		switch entry.Direction {
		case "CREDIT":
			expected, err = domain.NewMoney(entry.BalanceBefore, currency).Add(amount)
		case "DEBIT":
			expected, err = domain.NewMoney(entry.BalanceBefore, currency).Subtract(amount)
		default:
			return 0, false, fmt.Errorf(
				"unknown ledger direction: %s",
				entry.Direction,
			)
		}
		if err != nil {
			return 0, false, fmt.Errorf(
				"calculate ledger entry %s: %w",
				entry.ID,
				err,
			)
		}
		if expected.Amount() != entry.BalanceAfter {
			valid = false
		}

		calculated = domain.NewMoney(entry.BalanceAfter, currency)
	}

	return calculated.Amount(), valid, nil
}

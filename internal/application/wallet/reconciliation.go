package wallet

import (
	"context"
	"fmt"
)

type ReconciliationResult struct {
	WalletID      string
	Currency      string
	WalletBalance int64
	LedgerBalance int64
	EntryCount    int
	Consistent    bool
}

func (s *Service) Reconcile(
	ctx context.Context,
	walletID string,
) (ReconciliationResult, error) {
	wallet, err := s.Get(ctx, walletID)
	if err != nil {
		return ReconciliationResult{}, err
	}

	entries, err := s.Ledger(ctx, walletID)
	if err != nil {
		return ReconciliationResult{}, err
	}

	consistent := true

	var ledgerBalance int64

	if len(entries) > 0 {
		ledgerBalance = entries[0].BalanceBefore

		for _, entry := range entries {
			if entry.BalanceBefore != ledgerBalance {
				consistent = false
			}

			expectedBalance := entry.BalanceBefore

			switch entry.Direction {
			case "CREDIT":
				expectedBalance += entry.Amount

			case "DEBIT":
				expectedBalance -= entry.Amount

			default:
				return ReconciliationResult{}, fmt.Errorf(
					"unknown ledger direction: %s",
					entry.Direction,
				)
			}

			if expectedBalance != entry.BalanceAfter {
				consistent = false
			}

			ledgerBalance = entry.BalanceAfter
		}
	}

	if ledgerBalance != wallet.Balance {
		consistent = false
	}

	if !consistent {
		s.metrics.IncReconciliationDivergences()
	}

	return ReconciliationResult{
		WalletID:      wallet.WalletID,
		Currency:      wallet.Currency,
		WalletBalance: wallet.Balance,
		LedgerBalance: ledgerBalance,
		EntryCount:    len(entries),
		Consistent:    consistent,
	}, nil
}

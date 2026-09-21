package wagering

import (
	"errors"

	"github.com/Tharik/wagering-platform/internal/domain"
)

var ErrReversalInsufficientFunds = errors.New(
	"insufficient funds to reverse referenced transaction",
)

type movementDirection string

const (
	movementCredit movementDirection = "CREDIT"
	movementDebit  movementDirection = "DEBIT"
)

func reversalDirection(
	kind domain.WagerKind,
	reference referencedTransaction,
) (movementDirection, error) {
	switch kind {
	case domain.WagerKindRefund:
		// REFUND reverses a BET:
		// original BET debited the wallet, therefore REFUND credits it.
		if reference.Kind != domain.WagerKindBet {
			return "", ErrInvalidReferenceKind
		}

		return movementCredit, nil

	case domain.WagerKindRollback:
		switch reference.Kind {
		case domain.WagerKindBet:
			// BET originally debited the wallet.
			return movementCredit, nil

		case domain.WagerKindWin:
			// WIN originally credited the wallet.
			return movementDebit, nil

		case domain.WagerKindRefund:
			// REFUND originally credited the wallet.
			return movementDebit, nil

		default:
			return "", ErrInvalidReferenceKind
		}

	default:
		return "", ErrInvalidReferenceKind
	}
}

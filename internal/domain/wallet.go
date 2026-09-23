package domain

import (
	"errors"
	"math"
	"time"
)

var (
	ErrInsufficientFunds       = errors.New("insufficient funds")
	ErrInvalidAmount           = errors.New("amount must be greater than zero")
	ErrInvalidWalletID         = errors.New("wallet ID is required")
	ErrInvalidWalletPlayerID   = errors.New("wallet player ID is required")
	ErrInvalidWalletCurrency   = errors.New("wallet currency is required")
	ErrInvalidWalletBalance    = errors.New("wallet balance cannot be negative")
	ErrInvalidWalletVersion    = errors.New("wallet version must be positive")
	ErrInvalidWalletTimestamps = errors.New("wallet timestamps are required")
	ErrWalletVersionOverflow   = errors.New("wallet version overflow")
)

type Wallet struct {
	ID        string
	PlayerID  string
	Balance   Money
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

func NewWallet(
	id string,
	playerID string,
	initialBalance Money,
	now time.Time,
) (Wallet, error) {
	wallet := Wallet{
		ID:        id,
		PlayerID:  playerID,
		Balance:   initialBalance,
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := validateWallet(wallet); err != nil {
		return Wallet{}, err
	}
	return wallet, nil
}

func ReconstituteWallet(
	id string,
	playerID string,
	balance Money,
	version int64,
	createdAt time.Time,
	updatedAt time.Time,
) (Wallet, error) {
	wallet := Wallet{
		ID:        id,
		PlayerID:  playerID,
		Balance:   balance,
		Version:   version,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}
	if err := validateWallet(wallet); err != nil {
		return Wallet{}, err
	}
	return wallet, nil
}

func (w *Wallet) Credit(amount Money, now time.Time) error {
	if amount.Amount() <= 0 {
		return ErrInvalidAmount
	}

	newBalance, err := w.Balance.Add(amount)
	if err != nil {
		return err
	}
	if err := w.validateVersionIncrement(); err != nil {
		return err
	}

	w.Balance = newBalance
	w.Version++
	w.UpdatedAt = now

	return nil
}

func (w *Wallet) Debit(amount Money, now time.Time) error {
	if amount.Amount() <= 0 {
		return ErrInvalidAmount
	}

	comparison, err := w.Balance.Compare(amount)
	if err != nil {
		return err
	}

	if comparison < 0 {
		return ErrInsufficientFunds
	}

	newBalance, err := w.Balance.Subtract(amount)
	if err != nil {
		return err
	}
	if err := w.validateVersionIncrement(); err != nil {
		return err
	}

	w.Balance = newBalance
	w.Version++
	w.UpdatedAt = now

	return nil
}

func validateWallet(wallet Wallet) error {
	switch {
	case wallet.ID == "":
		return ErrInvalidWalletID
	case wallet.PlayerID == "":
		return ErrInvalidWalletPlayerID
	case wallet.Balance.Currency() == "":
		return ErrInvalidWalletCurrency
	case wallet.Balance.Amount() < 0:
		return ErrInvalidWalletBalance
	case wallet.Version <= 0:
		return ErrInvalidWalletVersion
	case wallet.CreatedAt.IsZero() || wallet.UpdatedAt.IsZero():
		return ErrInvalidWalletTimestamps
	default:
		return nil
	}
}

func (w Wallet) validateVersionIncrement() error {
	if w.Version <= 0 {
		return ErrInvalidWalletVersion
	}
	if w.Version == math.MaxInt64 {
		return ErrWalletVersionOverflow
	}
	return nil
}

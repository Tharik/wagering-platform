package domain

import (
	"errors"
	"time"
)

var (
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrInvalidAmount     = errors.New("amount must be greater than zero")
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
) Wallet {
	return Wallet{
		ID:        id,
		PlayerID:  playerID,
		Balance:   initialBalance,
		Version:   1,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

func (w *Wallet) Credit(amount Money, now time.Time) error {
	if amount.Amount() <= 0 {
		return ErrInvalidAmount
	}

	newBalance, err := w.Balance.Add(amount)
	if err != nil {
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

	w.Balance = newBalance
	w.Version++
	w.UpdatedAt = now

	return nil
}

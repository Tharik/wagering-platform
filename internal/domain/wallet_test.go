package domain

import (
	"errors"
	"testing"
	"time"
)

func TestWalletDebit(t *testing.T) {
	now := time.Now()

	wallet := NewWallet(
		"wallet-1",
		"player-1",
		NewMoney(10000, BRL),
		now,
	)

	err := wallet.Debit(NewMoney(8000, BRL), now)
	if err != nil {
		t.Fatal(err)
	}

	if wallet.Balance.Amount() != 2000 {
		t.Fatalf(
			"expected balance 2000, got %d",
			wallet.Balance.Amount(),
		)
	}

	if wallet.Version != 2 {
		t.Fatalf(
			"expected version 2, got %d",
			wallet.Version,
		)
	}
}

func TestWalletCannotGoNegative(t *testing.T) {
	now := time.Now()

	wallet := NewWallet(
		"wallet-1",
		"player-1",
		NewMoney(10000, BRL),
		now,
	)

	err := wallet.Debit(NewMoney(10001, BRL), now)

	if !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf(
			"expected insufficient funds, got %v",
			err,
		)
	}

	if wallet.Balance.Amount() != 10000 {
		t.Fatalf(
			"balance changed after rejected debit: %d",
			wallet.Balance.Amount(),
		)
	}

	if wallet.Version != 1 {
		t.Fatalf(
			"version changed after rejected debit: %d",
			wallet.Version,
		)
	}
}

func TestWalletCredit(t *testing.T) {
	now := time.Now()

	wallet := NewWallet(
		"wallet-1",
		"player-1",
		NewMoney(10000, BRL),
		now,
	)

	err := wallet.Credit(NewMoney(2500, BRL), now)
	if err != nil {
		t.Fatal(err)
	}

	if wallet.Balance.Amount() != 12500 {
		t.Fatalf(
			"expected balance 12500, got %d",
			wallet.Balance.Amount(),
		)
	}

	if wallet.Version != 2 {
		t.Fatalf(
			"expected version 2, got %d",
			wallet.Version,
		)
	}
}

func TestWalletRejectsCurrencyMismatch(t *testing.T) {
	now := time.Now()

	wallet := NewWallet(
		"wallet-1",
		"player-1",
		NewMoney(10000, BRL),
		now,
	)

	err := wallet.Debit(
		NewMoney(1000, Currency("USD")),
		now,
	)

	if !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf(
			"expected currency mismatch, got %v",
			err,
		)
	}
}

func TestWalletRejectsZeroDebit(t *testing.T) {
	now := time.Now()

	wallet := NewWallet(
		"wallet-1",
		"player-1",
		NewMoney(10000, BRL),
		now,
	)

	err := wallet.Debit(Zero(BRL), now)

	if !errors.Is(err, ErrInvalidAmount) {
		t.Fatalf(
			"expected invalid amount, got %v",
			err,
		)
	}
}

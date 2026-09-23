package domain

import (
	"errors"
	"math"
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

func TestWalletCreditReachesMaximumBalance(t *testing.T) {
	now := time.Now()
	wallet := NewWallet("wallet-1", "player-1", NewMoney(math.MaxInt64-1, BRL), now)

	if err := wallet.Credit(NewMoney(1, BRL), now); err != nil {
		t.Fatal(err)
	}
	if wallet.Balance.Amount() != math.MaxInt64 || wallet.Version != 2 {
		t.Fatalf("expected maximum balance/version 2, got %d/%d", wallet.Balance.Amount(), wallet.Version)
	}
}

func TestWalletCreditOverflowDoesNotMutate(t *testing.T) {
	now := time.Now()
	wallet := NewWallet("wallet-1", "player-1", NewMoney(math.MaxInt64, BRL), now)

	err := wallet.Credit(NewMoney(1, BRL), now)
	if !errors.Is(err, ErrMoneyOverflow) {
		t.Fatalf("expected money overflow, got %v", err)
	}
	if wallet.Balance.Amount() != math.MaxInt64 || wallet.Version != 1 {
		t.Fatalf("wallet mutated after overflow: balance=%d version=%d", wallet.Balance.Amount(), wallet.Version)
	}
}

func TestWalletDebitExactBalanceCannotUnderflow(t *testing.T) {
	now := time.Now()
	wallet := NewWallet("wallet-1", "player-1", NewMoney(math.MaxInt64, BRL), now)

	if err := wallet.Debit(NewMoney(math.MaxInt64, BRL), now); err != nil {
		t.Fatal(err)
	}
	if wallet.Balance.Amount() != 0 || wallet.Version != 2 {
		t.Fatalf("expected zero balance/version 2, got %d/%d", wallet.Balance.Amount(), wallet.Version)
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

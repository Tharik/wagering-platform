package domain

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestWalletDebit(t *testing.T) {
	now := time.Now()

	wallet := mustNewWallet(t,
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
	attemptedAt := now.Add(time.Hour)

	wallet := mustNewWallet(t,
		"wallet-1",
		"player-1",
		NewMoney(10000, BRL),
		now,
	)

	err := wallet.Debit(NewMoney(10001, BRL), attemptedAt)

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
	if !wallet.UpdatedAt.Equal(now) {
		t.Fatalf("updated time changed after rejected debit: %s", wallet.UpdatedAt)
	}
}

func TestWalletCredit(t *testing.T) {
	now := time.Now()

	wallet := mustNewWallet(t,
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
	if !wallet.UpdatedAt.Equal(now) {
		t.Fatalf("expected updated time %s, got %s", now, wallet.UpdatedAt)
	}
}

func TestWalletCreditReachesMaximumBalance(t *testing.T) {
	now := time.Now()
	wallet := mustNewWallet(t, "wallet-1", "player-1", NewMoney(math.MaxInt64-1, BRL), now)

	if err := wallet.Credit(NewMoney(1, BRL), now); err != nil {
		t.Fatal(err)
	}
	if wallet.Balance.Amount() != math.MaxInt64 || wallet.Version != 2 {
		t.Fatalf("expected maximum balance/version 2, got %d/%d", wallet.Balance.Amount(), wallet.Version)
	}
}

func TestWalletCreditOverflowDoesNotMutate(t *testing.T) {
	now := time.Now()
	attemptedAt := now.Add(time.Hour)
	wallet := mustNewWallet(t, "wallet-1", "player-1", NewMoney(math.MaxInt64, BRL), now)

	err := wallet.Credit(NewMoney(1, BRL), attemptedAt)
	if !errors.Is(err, ErrMoneyOverflow) {
		t.Fatalf("expected money overflow, got %v", err)
	}
	if wallet.Balance.Amount() != math.MaxInt64 || wallet.Version != 1 || !wallet.UpdatedAt.Equal(now) {
		t.Fatalf("wallet mutated after overflow: balance=%d version=%d", wallet.Balance.Amount(), wallet.Version)
	}
}

func TestWalletDebitExactBalanceCannotUnderflow(t *testing.T) {
	now := time.Now()
	wallet := mustNewWallet(t, "wallet-1", "player-1", NewMoney(math.MaxInt64, BRL), now)

	if err := wallet.Debit(NewMoney(math.MaxInt64, BRL), now); err != nil {
		t.Fatal(err)
	}
	if wallet.Balance.Amount() != 0 || wallet.Version != 2 {
		t.Fatalf("expected zero balance/version 2, got %d/%d", wallet.Balance.Amount(), wallet.Version)
	}
}

func TestWalletRejectsCurrencyMismatch(t *testing.T) {
	now := time.Now()
	attemptedAt := now.Add(time.Hour)

	wallet := mustNewWallet(t,
		"wallet-1",
		"player-1",
		NewMoney(10000, BRL),
		now,
	)

	err := wallet.Debit(
		NewMoney(1000, Currency("USD")),
		attemptedAt,
	)

	if !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf(
			"expected currency mismatch, got %v",
			err,
		)
	}
	if wallet.Balance.Amount() != 10000 || wallet.Version != 1 || !wallet.UpdatedAt.Equal(now) {
		t.Fatalf("wallet mutated after currency mismatch: %+v", wallet)
	}
}

func TestWalletRejectsZeroDebit(t *testing.T) {
	now := time.Now()
	attemptedAt := now.Add(time.Hour)

	wallet := mustNewWallet(t,
		"wallet-1",
		"player-1",
		NewMoney(10000, BRL),
		now,
	)

	err := wallet.Debit(Zero(BRL), attemptedAt)

	if !errors.Is(err, ErrInvalidAmount) {
		t.Fatalf(
			"expected invalid amount, got %v",
			err,
		)
	}
	if wallet.Balance.Amount() != 10000 || wallet.Version != 1 || !wallet.UpdatedAt.Equal(now) {
		t.Fatalf("wallet mutated after invalid amount: %+v", wallet)
	}
}

func TestWalletCreditFailuresDoNotMutate(t *testing.T) {
	tests := []struct {
		name   string
		amount Money
		err    error
	}{
		{"invalid amount", Zero(BRL), ErrInvalidAmount},
		{"currency mismatch", NewMoney(1, Currency("USD")), ErrCurrencyMismatch},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			originalUpdatedAt := time.Now().UTC().Add(-time.Hour)
			wallet := mustNewWallet(t, "wallet-1", "player-1", NewMoney(1000, BRL), originalUpdatedAt)

			err := wallet.Credit(tt.amount, originalUpdatedAt.Add(time.Hour))
			if !errors.Is(err, tt.err) {
				t.Fatalf("expected %v, got %v", tt.err, err)
			}
			if wallet.Balance.Amount() != 1000 || wallet.Version != 1 || !wallet.UpdatedAt.Equal(originalUpdatedAt) {
				t.Fatalf("wallet mutated after failed credit: %+v", wallet)
			}
		})
	}
}

func TestNewWalletValidatesCreationInvariants(t *testing.T) {
	now := time.Now().UTC()
	wallet, err := NewWallet("wallet-1", "player-1", NewMoney(1000, BRL), now)
	if err != nil {
		t.Fatal(err)
	}
	if wallet.Version != 1 || !wallet.CreatedAt.Equal(now) || !wallet.UpdatedAt.Equal(now) {
		t.Fatalf("unexpected new wallet: %+v", wallet)
	}

	tests := []struct {
		name     string
		id       string
		playerID string
		balance  Money
		now      time.Time
		err      error
	}{
		{"empty wallet ID", "", "player-1", NewMoney(0, BRL), now, ErrInvalidWalletID},
		{"empty player ID", "wallet-1", "", NewMoney(0, BRL), now, ErrInvalidWalletPlayerID},
		{"negative balance", "wallet-1", "player-1", NewMoney(-1, BRL), now, ErrInvalidWalletBalance},
		{"empty currency", "wallet-1", "player-1", NewMoney(0, ""), now, ErrInvalidWalletCurrency},
		{"zero timestamp", "wallet-1", "player-1", NewMoney(0, BRL), time.Time{}, ErrInvalidWalletTimestamps},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewWallet(tt.id, tt.playerID, tt.balance, tt.now)
			if !errors.Is(err, tt.err) {
				t.Fatalf("expected %v, got %v", tt.err, err)
			}
		})
	}
}

func TestReconstituteWalletPreservesValidPersistedState(t *testing.T) {
	createdAt := time.Now().UTC().Add(-time.Hour)
	updatedAt := createdAt.Add(30 * time.Minute)
	wallet, err := ReconstituteWallet("wallet-1", "player-1", NewMoney(2500, BRL), 7, createdAt, updatedAt)
	if err != nil {
		t.Fatal(err)
	}
	if wallet.Balance.Amount() != 2500 || wallet.Version != 7 || !wallet.CreatedAt.Equal(createdAt) || !wallet.UpdatedAt.Equal(updatedAt) {
		t.Fatalf("persisted wallet was not preserved: %+v", wallet)
	}
}

func TestReconstituteWalletRejectsInvalidPersistedState(t *testing.T) {
	now := time.Now().UTC()
	tests := []struct {
		name      string
		id        string
		playerID  string
		balance   Money
		version   int64
		createdAt time.Time
		updatedAt time.Time
		err       error
	}{
		{"empty wallet ID", "", "player-1", NewMoney(0, BRL), 1, now, now, ErrInvalidWalletID},
		{"empty player ID", "wallet-1", "", NewMoney(0, BRL), 1, now, now, ErrInvalidWalletPlayerID},
		{"negative balance", "wallet-1", "player-1", NewMoney(-1, BRL), 1, now, now, ErrInvalidWalletBalance},
		{"empty currency", "wallet-1", "player-1", NewMoney(0, ""), 1, now, now, ErrInvalidWalletCurrency},
		{"zero version", "wallet-1", "player-1", NewMoney(0, BRL), 0, now, now, ErrInvalidWalletVersion},
		{"zero created at", "wallet-1", "player-1", NewMoney(0, BRL), 1, time.Time{}, now, ErrInvalidWalletTimestamps},
		{"zero updated at", "wallet-1", "player-1", NewMoney(0, BRL), 1, now, time.Time{}, ErrInvalidWalletTimestamps},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ReconstituteWallet(tt.id, tt.playerID, tt.balance, tt.version, tt.createdAt, tt.updatedAt)
			if !errors.Is(err, tt.err) {
				t.Fatalf("expected %v, got %v", tt.err, err)
			}
		})
	}
}

func TestWalletVersionOverflowDoesNotMutate(t *testing.T) {
	for _, operation := range []struct {
		name string
		run  func(*Wallet, time.Time) error
	}{
		{"credit", func(wallet *Wallet, now time.Time) error { return wallet.Credit(NewMoney(1, BRL), now) }},
		{"debit", func(wallet *Wallet, now time.Time) error { return wallet.Debit(NewMoney(1, BRL), now) }},
	} {
		t.Run(operation.name, func(t *testing.T) {
			originalUpdatedAt := time.Now().UTC().Add(-time.Hour)
			wallet, err := ReconstituteWallet("wallet-1", "player-1", NewMoney(1000, BRL), math.MaxInt64, originalUpdatedAt, originalUpdatedAt)
			if err != nil {
				t.Fatal(err)
			}
			err = operation.run(&wallet, originalUpdatedAt.Add(time.Hour))
			if !errors.Is(err, ErrWalletVersionOverflow) {
				t.Fatalf("expected wallet version overflow, got %v", err)
			}
			if wallet.Balance.Amount() != 1000 || wallet.Version != math.MaxInt64 || !wallet.UpdatedAt.Equal(originalUpdatedAt) {
				t.Fatalf("wallet mutated after version overflow: %+v", wallet)
			}
		})
	}
}

func TestWalletSuccessfulMutationsUpdateTimestampAndVersionOnce(t *testing.T) {
	for _, operation := range []struct {
		name            string
		run             func(*Wallet, time.Time) error
		expectedBalance int64
	}{
		{"credit", func(wallet *Wallet, now time.Time) error { return wallet.Credit(NewMoney(250, BRL), now) }, 1250},
		{"debit", func(wallet *Wallet, now time.Time) error { return wallet.Debit(NewMoney(250, BRL), now) }, 750},
	} {
		t.Run(operation.name, func(t *testing.T) {
			createdAt := time.Now().UTC().Add(-time.Hour)
			updatedAt := createdAt.Add(time.Hour)
			wallet := mustNewWallet(t, "wallet-1", "player-1", NewMoney(1000, BRL), createdAt)

			if err := operation.run(&wallet, updatedAt); err != nil {
				t.Fatal(err)
			}
			if wallet.Balance.Amount() != operation.expectedBalance || wallet.Version != 2 || !wallet.UpdatedAt.Equal(updatedAt) {
				t.Fatalf("unexpected successful mutation: %+v", wallet)
			}
			if !wallet.CreatedAt.Equal(createdAt) {
				t.Fatalf("created time changed: %s", wallet.CreatedAt)
			}
		})
	}
}

func mustNewWallet(t *testing.T, id, playerID string, balance Money, now time.Time) Wallet {
	t.Helper()
	wallet, err := NewWallet(id, playerID, balance, now)
	if err != nil {
		t.Fatal(err)
	}
	return wallet
}

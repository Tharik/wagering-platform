package domain

import (
	"errors"
	"testing"
)

func TestParseMoney(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int64
	}{
		{"whole amount", "25", 2500},
		{"one decimal", "25.5", 2550},
		{"two decimals", "25.50", 2550},
		{"zero", "0.00", 0},
		{"one cent", "0.01", 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			money, err := ParseMoney(tt.input, BRL)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if money.Amount() != tt.expected {
				t.Fatalf(
					"expected %d, got %d",
					tt.expected,
					money.Amount(),
				)
			}
		})
	}
}

func TestParseMoneyRejectsInvalidValues(t *testing.T) {
	values := []string{
		"",
		"-1.00",
		"1.001",
		"1e3",
		"NaN",
		"Infinity",
		".50",
		"1.",
		"abc",
	}

	for _, value := range values {
		t.Run(value, func(t *testing.T) {
			_, err := ParseMoney(value, BRL)

			if !errors.Is(err, ErrInvalidMoneyFormat) {
				t.Fatalf(
					"expected ErrInvalidMoneyFormat for %q, got %v",
					value,
					err,
				)
			}
		})
	}
}

func TestMoneyAdd(t *testing.T) {
	a := NewMoney(1000, BRL)
	b := NewMoney(500, BRL)

	result, err := a.Add(b)
	if err != nil {
		t.Fatal(err)
	}

	if result.Amount() != 1500 {
		t.Fatalf("expected 1500, got %d", result.Amount())
	}
}

func TestMoneyRejectsCurrencyMismatch(t *testing.T) {
	brl := NewMoney(1000, BRL)
	usd := NewMoney(1000, Currency("USD"))

	_, err := brl.Add(usd)

	if !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("expected currency mismatch, got %v", err)
	}
}

func TestMoneySubtract(t *testing.T) {
	a := NewMoney(1000, BRL)
	b := NewMoney(250, BRL)

	result, err := a.Subtract(b)
	if err != nil {
		t.Fatal(err)
	}

	if result.Amount() != 750 {
		t.Fatalf("expected 750, got %d", result.Amount())
	}
}

func TestMoneyCompare(t *testing.T) {
	a := NewMoney(1000, BRL)
	b := NewMoney(500, BRL)

	result, err := a.Compare(b)
	if err != nil {
		t.Fatal(err)
	}

	if result != 1 {
		t.Fatalf("expected 1, got %d", result)
	}
}

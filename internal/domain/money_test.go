package domain

import (
	"errors"
	"math"
	"strings"
	"testing"
)

func TestParseMoney(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int64
	}{
		{"zero", "0.00", 0},
		{"one cent", "0.01", 1},
		{"normal amount", "25.50", 2550},
		{"maximum", "92233720368547758.07", math.MaxInt64},
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
	tests := []struct {
		name  string
		value string
		err   error
	}{
		{"empty", "", ErrInvalidMoneyFormat},
		{"missing decimal", "1", ErrInvalidMoneyFormat},
		{"one decimal", "1.2", ErrInvalidMoneyFormat},
		{"extra decimal", "1.234", ErrInvalidMoneyFormat},
		{"missing whole", ".50", ErrInvalidMoneyFormat},
		{"missing fraction", "1.", ErrInvalidMoneyFormat},
		{"leading plus", "+1.00", ErrInvalidMoneyFormat},
		{"negative", "-1.00", ErrInvalidMoneyFormat},
		{"leading whitespace", " 1.00", ErrInvalidMoneyFormat},
		{"trailing whitespace", "1.00 ", ErrInvalidMoneyFormat},
		{"lower scientific", "1e2", ErrInvalidMoneyFormat},
		{"upper scientific", "1E2", ErrInvalidMoneyFormat},
		{"not a number", "NaN", ErrInvalidMoneyFormat},
		{"infinity", "Infinity", ErrInvalidMoneyFormat},
		{"max plus one cent", "92233720368547758.08", ErrMoneyOverflow},
		{"next whole amount", "92233720368547759.00", ErrMoneyOverflow},
		{"huge amount", "999999999999999999999999999999999999.99", ErrMoneyOverflow},
		{"thousand digits", strings.Repeat("9", 1000) + ".99", ErrMoneyOverflow},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseMoney(tt.value, BRL)

			if !errors.Is(err, tt.err) {
				t.Fatalf(
					"expected %v for %q, got %v",
					tt.err,
					tt.value,
					err,
				)
			}
		})
	}
}

func TestMoneyAddBoundaries(t *testing.T) {
	tests := []struct {
		name     string
		left     int64
		right    int64
		expected int64
		err      error
	}{
		{"safe positive", 1000, 500, 1500, nil},
		{"exact maximum", math.MaxInt64 - 1, 1, math.MaxInt64, nil},
		{"positive overflow", math.MaxInt64, 1, 0, ErrMoneyOverflow},
		{"safe negative", -100, -50, -150, nil},
		{"negative underflow", math.MinInt64, -1, 0, ErrMoneyOverflow},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := NewMoney(tt.left, BRL).Add(NewMoney(tt.right, BRL))
			if !errors.Is(err, tt.err) {
				t.Fatalf("expected error %v, got %v", tt.err, err)
			}
			if err == nil && result.Amount() != tt.expected {
				t.Fatalf("expected %d, got %d", tt.expected, result.Amount())
			}
		})
	}
}

func TestMoneyRejectsCurrencyMismatch(t *testing.T) {
	brl := NewMoney(1000, BRL)
	usd := NewMoney(1000, Currency("USD"))

	_, err := brl.Add(usd)

	if !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("expected currency mismatch, got %v", err)
	}

	_, err = brl.Subtract(usd)
	if !errors.Is(err, ErrCurrencyMismatch) {
		t.Fatalf("expected subtraction currency mismatch, got %v", err)
	}
}

func TestMoneySubtractBoundaries(t *testing.T) {
	tests := []struct {
		name     string
		left     int64
		right    int64
		expected int64
		err      error
	}{
		{"safe subtraction", 1000, 250, 750, nil},
		{"exact maximum", math.MaxInt64 - 1, -1, math.MaxInt64, nil},
		{"positive overflow", math.MaxInt64, -1, 0, ErrMoneyOverflow},
		{"negative underflow", math.MinInt64, 1, 0, ErrMoneyOverflow},
		{"subtract minimum", 0, math.MinInt64, 0, ErrMoneyOverflow},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := NewMoney(tt.left, BRL).Subtract(NewMoney(tt.right, BRL))
			if !errors.Is(err, tt.err) {
				t.Fatalf("expected error %v, got %v", tt.err, err)
			}
			if err == nil && result.Amount() != tt.expected {
				t.Fatalf("expected %d, got %d", tt.expected, result.Amount())
			}
		})
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

func TestMoneyString(t *testing.T) {
	tests := []struct {
		name     string
		amount   int64
		expected string
	}{
		{"zero", 0, "0.00"},
		{"one cent", 1, "0.01"},
		{"one unit", 100, "1.00"},
		{"representative", 1234, "12.34"},
		{"maximum int64", math.MaxInt64, "92233720368547758.07"},
		{"negative one cent", -1, "-0.01"},
		{"negative one unit", -100, "-1.00"},
		{"minimum int64", math.MinInt64, "-92233720368547758.08"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			money := NewMoney(tt.amount, BRL)

			if got := money.String(); got != tt.expected {
				t.Fatalf(
					"expected %q, got %q",
					tt.expected,
					got,
				)
			}
		})
	}
}

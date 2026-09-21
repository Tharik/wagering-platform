package domain

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

var (
	ErrInvalidMoneyFormat = errors.New("invalid money format")
	ErrCurrencyMismatch   = errors.New("currency mismatch")
	ErrMoneyOverflow      = errors.New("money overflow")
)

type Currency string

const (
	BRL Currency = "BRL"
)

type Money struct {
	amount   int64
	currency Currency
}

func NewMoney(amount int64, currency Currency) Money {
	return Money{
		amount:   amount,
		currency: currency,
	}
}

func ParseMoney(value string, currency Currency) (Money, error) {
	if value == "" || strings.ContainsAny(value, "eE") {
		return Money{}, ErrInvalidMoneyFormat
	}

	// External monetary amounts cannot be negative.
	if strings.HasPrefix(value, "-") {
		return Money{}, ErrInvalidMoneyFormat
	}

	parts := strings.Split(value, ".")
	if len(parts) > 2 {
		return Money{}, ErrInvalidMoneyFormat
	}

	whole := parts[0]
	if whole == "" {
		return Money{}, ErrInvalidMoneyFormat
	}

	fraction := "00"

	if len(parts) == 2 {
		switch len(parts[1]) {
		case 1:
			fraction = parts[1] + "0"
		case 2:
			fraction = parts[1]
		default:
			return Money{}, ErrInvalidMoneyFormat
		}
	}

	if !allDigits(whole) || !allDigits(fraction) {
		return Money{}, ErrInvalidMoneyFormat
	}

	wholeValue, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return Money{}, ErrMoneyOverflow
	}

	fractionValue, err := strconv.ParseInt(fraction, 10, 64)
	if err != nil {
		return Money{}, ErrInvalidMoneyFormat
	}

	if wholeValue > (math.MaxInt64-fractionValue)/100 {
		return Money{}, ErrMoneyOverflow
	}

	return Money{
		amount:   wholeValue*100 + fractionValue,
		currency: currency,
	}, nil
}

func Zero(currency Currency) Money {
	return Money{
		amount:   0,
		currency: currency,
	}
}

func (m Money) Amount() int64 {
	return m.amount
}

func (m Money) Currency() Currency {
	return m.currency
}

func (m Money) IsZero() bool {
	return m.amount == 0
}

func (m Money) Add(other Money) (Money, error) {
	if m.currency != other.currency {
		return Money{}, ErrCurrencyMismatch
	}

	if other.amount > 0 && m.amount > math.MaxInt64-other.amount {
		return Money{}, ErrMoneyOverflow
	}

	if other.amount < 0 && m.amount < math.MinInt64-other.amount {
		return Money{}, ErrMoneyOverflow
	}

	return NewMoney(m.amount+other.amount, m.currency), nil
}

func (m Money) Subtract(other Money) (Money, error) {
	if m.currency != other.currency {
		return Money{}, ErrCurrencyMismatch
	}

	if other.amount == math.MinInt64 {
		return Money{}, ErrMoneyOverflow
	}

	return m.Add(NewMoney(-other.amount, other.currency))
}

func (m Money) Negate() (Money, error) {
	if m.amount == math.MinInt64 {
		return Money{}, ErrMoneyOverflow
	}

	return NewMoney(-m.amount, m.currency), nil
}

func (m Money) Compare(other Money) (int, error) {
	if m.currency != other.currency {
		return 0, ErrCurrencyMismatch
	}

	switch {
	case m.amount < other.amount:
		return -1, nil
	case m.amount > other.amount:
		return 1, nil
	default:
		return 0, nil
	}
}

func (m Money) String() string {
	return fmt.Sprintf("%d.%02d", m.amount/100, m.amount%100)
}

func allDigits(value string) bool {
	if value == "" {
		return false
	}

	for _, r := range value {
		if r < '0' || r > '9' {
			return false
		}
	}

	return true
}

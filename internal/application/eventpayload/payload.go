package eventpayload

import "github.com/Tharik/wagering-platform/internal/domain"

type Money struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func NewMoney(value domain.Money) Money {
	return Money{
		Amount:   value.String(),
		Currency: string(value.Currency()),
	}
}

type WagerTransactionProcessedData struct {
	TransactionID string `json:"transactionId"`
	WalletID      string `json:"walletId"`
	Kind          string `json:"kind"`
}

type WagerTransactionRejectedData struct {
	TransactionID string `json:"transactionId"`
	WalletID      string `json:"walletId"`
	ProviderID    string `json:"providerId"`
	Kind          string `json:"kind"`
	FailureCode   string `json:"failureCode"`
}

type WagerTransactionPendingReferenceData struct {
	TransactionID                  string `json:"transactionId"`
	ProviderID                     string `json:"providerId"`
	ExternalTransactionID          string `json:"externalTransactionId"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId"`
}

type WalletBalanceChangedData struct {
	WalletID      string `json:"walletId"`
	TransactionID string `json:"transactionId"`
	Direction     string `json:"direction"`
	Money         Money  `json:"money"`
	BalanceBefore Money  `json:"balanceBefore"`
	BalanceAfter  Money  `json:"balanceAfter"`
	WalletVersion int64  `json:"walletVersion"`
}

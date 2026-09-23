package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
)

type WagerKind string

const (
	WagerKindBet      WagerKind = "BET"
	WagerKindWin      WagerKind = "WIN"
	WagerKindLoss     WagerKind = "LOSS"
	WagerKindRefund   WagerKind = "REFUND"
	WagerKindRollback WagerKind = "ROLLBACK"
)

type WagerState string

const (
	// WagerStatePending is reserved for persisted unresolved wagers. The current
	// runtime does not create it, and readers treat it as non-terminal.
	WagerStatePending          WagerState = "PENDING"
	WagerStatePendingReference WagerState = "PENDING_REFERENCE"
	WagerStateProcessed        WagerState = "PROCESSED"
	WagerStateRejected         WagerState = "REJECTED"
	// WagerStateFailed is reserved for persisted terminal unsuccessful wagers.
	// Readers treat it as terminal unsuccessful. The current runtime rolls back
	// infrastructure failures instead of creating it.
	WagerStateFailed WagerState = "FAILED"
)

var (
	ErrInvalidWagerKind = errors.New("invalid wager kind")
)

type WagerRequest struct {
	ProviderID                     string
	ExternalTransactionID          string
	PlayerID                       string
	WalletID                       string
	RoundID                        string
	GameID                         string
	Kind                           WagerKind
	Amount                         Money
	ReferenceExternalTransactionID string
}

// CanonicalPayload contains only business fields.
//
// Transport metadata such as HTTP headers, SQS message IDs and the
// idempotency key itself are deliberately excluded.
type CanonicalPayload struct {
	ProviderID                     string `json:"providerId"`
	ExternalTransactionID          string `json:"externalTransactionId"`
	PlayerID                       string `json:"playerId"`
	WalletID                       string `json:"walletId"`
	RoundID                        string `json:"roundId"`
	GameID                         string `json:"gameId"`
	Kind                           string `json:"kind"`
	Amount                         string `json:"amount"`
	Currency                       string `json:"currency"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId,omitempty"`
}

func (r WagerRequest) PayloadHash() (string, error) {
	payload := CanonicalPayload{
		ProviderID:                     r.ProviderID,
		ExternalTransactionID:          r.ExternalTransactionID,
		PlayerID:                       r.PlayerID,
		WalletID:                       r.WalletID,
		RoundID:                        r.RoundID,
		GameID:                         r.GameID,
		Kind:                           string(r.Kind),
		Amount:                         r.Amount.String(),
		Currency:                       string(r.Amount.Currency()),
		ReferenceExternalTransactionID: r.ReferenceExternalTransactionID,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	hash := sha256.Sum256(data)

	return hex.EncodeToString(hash[:]), nil
}

func (k WagerKind) IsValidExternalKind() bool {
	switch k {
	case WagerKindBet,
		WagerKindWin,
		WagerKindLoss,
		WagerKindRefund,
		WagerKindRollback:
		return true
	default:
		return false
	}
}

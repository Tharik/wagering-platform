package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
)

func TestPayloadHashMatchesCanonicalContractWithoutReference(t *testing.T) {
	const canonicalJSON = `{"providerId":"provider-a","externalTransactionId":"ext-123","playerId":"player-1","walletId":"wallet-1","roundId":"round-1","gameId":"game-1","kind":"BET","amount":"25.00","currency":"BRL"}`
	const goldenHash = "abb358a2852f6d689c183a14d94a0d4d5ac9eef391e35c85a7efc48c8bf11fd4"

	assertGoldenPayloadHash(t, canonicalWagerRequest(), canonicalJSON, goldenHash)
}

func TestPayloadHashMatchesCanonicalContractWithReference(t *testing.T) {
	const canonicalJSON = `{"providerId":"provider-a","externalTransactionId":"ext-123","playerId":"player-1","walletId":"wallet-1","roundId":"round-1","gameId":"game-1","kind":"BET","amount":"25.00","currency":"BRL","referenceExternalTransactionId":"original-bet"}`
	const goldenHash = "1dd51feb6e273737ce3d56f31149c9b8f1f897afff550b5bd1281e44fed8401c"

	request := canonicalWagerRequest()
	request.ReferenceExternalTransactionID = "original-bet"

	assertGoldenPayloadHash(t, request, canonicalJSON, goldenHash)
}

func TestPayloadHashTreatsMissingAndExplicitEmptyReferenceIdentically(t *testing.T) {
	missing := canonicalWagerRequest()
	explicitEmpty := canonicalWagerRequest()
	explicitEmpty.ReferenceExternalTransactionID = ""

	missingHash, err := missing.PayloadHash()
	if err != nil {
		t.Fatalf("hash missing reference: %v", err)
	}
	explicitEmptyHash, err := explicitEmpty.PayloadHash()
	if err != nil {
		t.Fatalf("hash explicit empty reference: %v", err)
	}

	if missingHash != explicitEmptyHash {
		t.Fatalf("expected missing and empty references to match: %s != %s", missingHash, explicitEmptyHash)
	}
}

func canonicalWagerRequest() WagerRequest {
	return WagerRequest{
		ProviderID:            "provider-a",
		ExternalTransactionID: "ext-123",
		PlayerID:              "player-1",
		WalletID:              "wallet-1",
		RoundID:               "round-1",
		GameID:                "game-1",
		Kind:                  WagerKindBet,
		Amount:                NewMoney(2500, BRL),
	}
}

func assertGoldenPayloadHash(t *testing.T, request WagerRequest, canonicalJSON, goldenHash string) {
	t.Helper()

	expectedSum := sha256.Sum256([]byte(canonicalJSON))
	if calculated := hex.EncodeToString(expectedSum[:]); calculated != goldenHash {
		t.Fatalf("golden hash does not match documented canonical JSON: %s != %s", calculated, goldenHash)
	}

	actual, err := request.PayloadHash()
	if err != nil {
		t.Fatalf("hash request: %v", err)
	}
	if actual != goldenHash {
		t.Fatalf("canonical payload contract changed: got %s, want %s", actual, goldenHash)
	}
}

func TestPayloadHashIsDeterministic(t *testing.T) {
	request := WagerRequest{
		ProviderID:            "provider-a",
		ExternalTransactionID: "bet-123",
		PlayerID:              "player-1",
		WalletID:              "wallet-1",
		RoundID:               "round-1",
		GameID:                "game-1",
		Kind:                  WagerKindBet,
		Amount:                NewMoney(2500, BRL),
	}

	first, err := request.PayloadHash()
	if err != nil {
		t.Fatal(err)
	}

	second, err := request.PayloadHash()
	if err != nil {
		t.Fatal(err)
	}

	if first != second {
		t.Fatalf(
			"expected deterministic hash, got %s and %s",
			first,
			second,
		)
	}
}

func TestPayloadHashChangesWhenBusinessPayloadChanges(t *testing.T) {
	first := WagerRequest{
		ProviderID:            "provider-a",
		ExternalTransactionID: "bet-123",
		PlayerID:              "player-1",
		WalletID:              "wallet-1",
		RoundID:               "round-1",
		GameID:                "game-1",
		Kind:                  WagerKindBet,
		Amount:                NewMoney(2500, BRL),
	}

	second := first
	second.Amount = NewMoney(2600, BRL)

	firstHash, err := first.PayloadHash()
	if err != nil {
		t.Fatal(err)
	}

	secondHash, err := second.PayloadHash()
	if err != nil {
		t.Fatal(err)
	}

	if firstHash == secondHash {
		t.Fatal("expected different hashes for different business payloads")
	}
}

func TestFixedDecimalMoneyProducesCanonicalPayloadHash(t *testing.T) {
	amount, err := ParseMoney("25.00", BRL)
	if err != nil {
		t.Fatal(err)
	}

	request := WagerRequest{
		ProviderID:            "provider-a",
		ExternalTransactionID: "bet-123",
		PlayerID:              "player-1",
		WalletID:              "wallet-1",
		RoundID:               "round-1",
		GameID:                "game-1",
		Kind:                  WagerKindBet,
		Amount:                amount,
	}

	firstHash, err := request.PayloadHash()
	if err != nil {
		t.Fatal(err)
	}
	secondHash, err := request.PayloadHash()
	if err != nil {
		t.Fatal(err)
	}

	if firstHash != secondHash {
		t.Fatal("fixed-decimal money must produce a deterministic hash")
	}
}

func TestInvalidMoneyCannotBecomeCanonicalPayload(t *testing.T) {
	_, err := ParseMoney("25.0", BRL)
	if !errors.Is(err, ErrInvalidMoneyFormat) {
		t.Fatalf("expected invalid money format before hashing, got %v", err)
	}
}

func TestOpeningIsNotAnExternalWagerKind(t *testing.T) {
	if WagerKind("OPENING").IsValidExternalKind() {
		t.Fatal("OPENING must not be accepted as an external wager kind")
	}
}

func TestWagerRequestValidateExternal(t *testing.T) {
	tests := []struct {
		name      string
		kind      WagerKind
		amount    int64
		reference string
		wantErr   error
	}{
		{name: "BET positive without reference", kind: WagerKindBet, amount: 1},
		{name: "BET zero", kind: WagerKindBet, wantErr: ErrInvalidAmount},
		{name: "BET negative", kind: WagerKindBet, amount: -1, wantErr: ErrInvalidAmount},
		{name: "BET with reference", kind: WagerKindBet, amount: 1, reference: "bet-1", wantErr: ErrWagerReferenceNotAllowed},
		{name: "WIN positive without reference", kind: WagerKindWin, amount: 1},
		{name: "WIN positive with reference", kind: WagerKindWin, amount: 1, reference: "bet-1"},
		{name: "WIN zero", kind: WagerKindWin, wantErr: ErrInvalidAmount},
		{name: "WIN negative", kind: WagerKindWin, amount: -1, wantErr: ErrInvalidAmount},
		{name: "LOSS zero without reference", kind: WagerKindLoss},
		{name: "LOSS positive", kind: WagerKindLoss, amount: 1, wantErr: ErrInvalidLossAmount},
		{name: "LOSS negative", kind: WagerKindLoss, amount: -1, wantErr: ErrInvalidLossAmount},
		{name: "LOSS with reference", kind: WagerKindLoss, reference: "bet-1", wantErr: ErrWagerReferenceNotAllowed},
		{name: "REFUND positive with reference", kind: WagerKindRefund, amount: 1, reference: "bet-1"},
		{name: "REFUND zero with reference", kind: WagerKindRefund, reference: "bet-1", wantErr: ErrInvalidAmount},
		{name: "REFUND negative with reference", kind: WagerKindRefund, amount: -1, reference: "bet-1", wantErr: ErrInvalidAmount},
		{name: "REFUND without reference", kind: WagerKindRefund, amount: 1, wantErr: ErrWagerReferenceRequired},
		{name: "ROLLBACK positive with reference", kind: WagerKindRollback, amount: 1, reference: "bet-1"},
		{name: "ROLLBACK zero with reference", kind: WagerKindRollback, reference: "bet-1", wantErr: ErrInvalidAmount},
		{name: "ROLLBACK negative with reference", kind: WagerKindRollback, amount: -1, reference: "bet-1", wantErr: ErrInvalidAmount},
		{name: "ROLLBACK without reference", kind: WagerKindRollback, amount: 1, wantErr: ErrWagerReferenceRequired},
		{name: "OPENING validates kind first", kind: WagerKind("OPENING"), amount: -1, reference: "bet-1", wantErr: ErrInvalidWagerKind},
		{name: "unknown validates kind first", kind: WagerKind("UNKNOWN"), amount: -1, reference: "bet-1", wantErr: ErrInvalidWagerKind},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := WagerRequest{
				Kind:                           tt.kind,
				Amount:                         NewMoney(tt.amount, BRL),
				ReferenceExternalTransactionID: tt.reference,
			}

			err := request.ValidateExternal()
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("expected valid request, got %v", err)
				}
				return
			}

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("expected %v, got %v", tt.wantErr, err)
			}
		})
	}
}

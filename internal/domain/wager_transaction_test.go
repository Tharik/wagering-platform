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

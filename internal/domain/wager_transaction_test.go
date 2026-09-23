package domain

import (
	"errors"
	"testing"
)

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

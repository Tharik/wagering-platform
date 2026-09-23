package eventpayload

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestEventConstructors(t *testing.T) {
	aggregateID := uuid.New()
	occurredAt := time.Date(2026, time.September, 8, 12, 0, 0, 123456789, time.UTC)

	tests := []struct {
		name      string
		eventType string
		data      any
		construct func() Event
	}{
		{
			name:      "processed",
			eventType: "WagerTransactionProcessed",
			data: WagerTransactionProcessedData{
				TransactionID: "transaction-processed",
				WalletID:      "wallet-1",
				Kind:          "BET",
			},
			construct: func() Event {
				return NewWagerTransactionProcessedEvent(
					aggregateID,
					"correlation-1",
					"causation-1",
					occurredAt,
					WagerTransactionProcessedData{
						TransactionID: "transaction-processed",
						WalletID:      "wallet-1",
						Kind:          "BET",
					},
				)
			},
		},
		{
			name:      "rejected",
			eventType: "WagerTransactionRejected",
			data: WagerTransactionRejectedData{
				TransactionID: "transaction-rejected",
				WalletID:      "wallet-1",
				ProviderID:    "provider-a",
				Kind:          "BET",
				FailureCode:   "INSUFFICIENT_FUNDS",
			},
			construct: func() Event {
				return NewWagerTransactionRejectedEvent(
					aggregateID,
					"correlation-1",
					"causation-1",
					occurredAt,
					WagerTransactionRejectedData{
						TransactionID: "transaction-rejected",
						WalletID:      "wallet-1",
						ProviderID:    "provider-a",
						Kind:          "BET",
						FailureCode:   "INSUFFICIENT_FUNDS",
					},
				)
			},
		},
		{
			name:      "pending reference",
			eventType: "WagerTransactionPendingReference",
			data: WagerTransactionPendingReferenceData{
				TransactionID:                  "transaction-pending",
				ProviderID:                     "provider-a",
				ExternalTransactionID:          "external-pending",
				ReferenceExternalTransactionID: "external-reference",
			},
			construct: func() Event {
				return NewWagerTransactionPendingReferenceEvent(
					aggregateID,
					"correlation-1",
					"causation-1",
					occurredAt,
					WagerTransactionPendingReferenceData{
						TransactionID:                  "transaction-pending",
						ProviderID:                     "provider-a",
						ExternalTransactionID:          "external-pending",
						ReferenceExternalTransactionID: "external-reference",
					},
				)
			},
		},
		{
			name:      "wallet balance changed",
			eventType: "WalletBalanceChanged",
			data: WalletBalanceChangedData{
				WalletID:      "wallet-1",
				TransactionID: "transaction-balance",
				Direction:     "CREDIT",
				Money:         Money{Amount: "10.00", Currency: "BRL"},
				BalanceBefore: Money{Amount: "90.00", Currency: "BRL"},
				BalanceAfter:  Money{Amount: "100.00", Currency: "BRL"},
				WalletVersion: 2,
			},
			construct: func() Event {
				return NewWalletBalanceChangedEvent(
					aggregateID,
					"correlation-1",
					"causation-1",
					occurredAt,
					WalletBalanceChangedData{
						WalletID:      "wallet-1",
						TransactionID: "transaction-balance",
						Direction:     "CREDIT",
						Money:         Money{Amount: "10.00", Currency: "BRL"},
						BalanceBefore: Money{Amount: "90.00", Currency: "BRL"},
						BalanceAfter:  Money{Amount: "100.00", Currency: "BRL"},
						WalletVersion: 2,
					},
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := tt.construct()
			if event.ID() == uuid.Nil {
				t.Fatal("expected non-zero event ID")
			}
			if event.Type() != tt.eventType {
				t.Fatalf("expected type %s, got %s", tt.eventType, event.Type())
			}
			if event.Version() != 1 {
				t.Fatalf("expected version 1, got %d", event.Version())
			}
			if event.AggregateID() != aggregateID {
				t.Fatalf("expected aggregate ID %s, got %s", aggregateID, event.AggregateID())
			}
			if !event.OccurredAt().Equal(occurredAt) {
				t.Fatalf("expected occurredAt %s, got %s", occurredAt, event.OccurredAt())
			}

			assertEventJSON(t, event, aggregateID, occurredAt, tt.eventType, "causation-1", tt.data)
		})
	}
}

func TestEventConstructorOmitsAbsentCausationID(t *testing.T) {
	event := NewWagerTransactionProcessedEvent(
		uuid.New(),
		"correlation-1",
		"",
		time.Date(2026, time.September, 8, 12, 0, 0, 0, time.UTC),
		WagerTransactionProcessedData{
			TransactionID: "transaction-1",
			WalletID:      "wallet-1",
			Kind:          "LOSS",
		},
	)

	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(payload, &envelope); err != nil {
		t.Fatalf("decode event: %v", err)
	}
	if _, exists := envelope["causationId"]; exists {
		t.Fatal("causationId must be omitted when absent")
	}
}

func assertEventJSON(
	t *testing.T,
	event Event,
	aggregateID uuid.UUID,
	occurredAt time.Time,
	eventType string,
	causationID string,
	expectedData any,
) {
	t.Helper()

	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(payload, &envelope); err != nil {
		t.Fatalf("decode event: %v", err)
	}
	expectedKeys := []string{
		"eventId",
		"eventType",
		"aggregateId",
		"correlationId",
		"causationId",
		"occurredAt",
		"version",
		"data",
	}
	if len(envelope) != len(expectedKeys) {
		t.Fatalf("expected %d envelope fields, got %d: %s", len(expectedKeys), len(envelope), payload)
	}
	for _, key := range expectedKeys {
		if _, exists := envelope[key]; !exists {
			t.Fatalf("missing envelope field %s: %s", key, payload)
		}
	}

	assertJSONString(t, envelope["eventId"], event.ID().String())
	assertJSONString(t, envelope["eventType"], eventType)
	assertJSONString(t, envelope["aggregateId"], aggregateID.String())
	assertJSONString(t, envelope["correlationId"], "correlation-1")
	assertJSONString(t, envelope["causationId"], causationID)
	assertJSONString(t, envelope["occurredAt"], occurredAt.Format(time.RFC3339Nano))

	var version int
	if err := json.Unmarshal(envelope["version"], &version); err != nil {
		t.Fatalf("decode version: %v", err)
	}
	if version != 1 {
		t.Fatalf("expected numeric version 1, got %s", envelope["version"])
	}

	var actualData any
	if err := json.Unmarshal(envelope["data"], &actualData); err != nil {
		t.Fatalf("decode event data: %v", err)
	}
	expectedPayload, err := json.Marshal(expectedData)
	if err != nil {
		t.Fatalf("marshal expected event data: %v", err)
	}
	var wantedData any
	if err := json.Unmarshal(expectedPayload, &wantedData); err != nil {
		t.Fatalf("decode expected event data: %v", err)
	}
	if !reflect.DeepEqual(actualData, wantedData) {
		t.Fatalf("unexpected event data: got %s want %s", envelope["data"], expectedPayload)
	}
}

func assertJSONString(t *testing.T, raw json.RawMessage, expected string) {
	t.Helper()
	var actual string
	if err := json.Unmarshal(raw, &actual); err != nil {
		t.Fatalf("decode JSON string: %v", err)
	}
	if actual != expected {
		t.Fatalf("expected %q, got %q", expected, actual)
	}
}

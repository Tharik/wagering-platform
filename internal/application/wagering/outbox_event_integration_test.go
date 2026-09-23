package wagering

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Tharik/wagering-platform/internal/application/eventpayload"
	"github.com/Tharik/wagering-platform/internal/application/wallet"
	"github.com/Tharik/wagering-platform/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type storedEventEnvelope struct {
	EventID       string          `json:"eventId"`
	EventType     string          `json:"eventType"`
	AggregateID   string          `json:"aggregateId"`
	CorrelationID string          `json:"correlationId"`
	CausationID   string          `json:"causationId,omitempty"`
	OccurredAt    string          `json:"occurredAt"`
	Version       int             `json:"version"`
	Data          json.RawMessage `json:"data"`
}

func TestWagerOutboxEventDataContracts(t *testing.T) {
	ctx, pool, walletID := setupWagerEventTest(t, "player-event-contract", 10000)
	service := NewService(pool)

	bet := processEventTestWager(t, ctx, service, ProcessCommand{
		IdempotencyKey: "event-bet", CorrelationID: "correlation-bet", CausationID: "causation-bet",
		Request: domain.WagerRequest{
			ProviderID: "provider-a", ExternalTransactionID: "event-bet", PlayerID: "player-event-contract",
			WalletID: walletID, RoundID: "round-1", GameID: "game-1", Kind: domain.WagerKindBet,
			Amount: domain.NewMoney(1000, domain.BRL),
		},
	})
	assertProcessedAndBalanceEvents(t, ctx, pool, bet.TransactionID, "correlation-bet", "causation-bet", eventpayload.WagerTransactionProcessedData{
		TransactionID: bet.TransactionID, WalletID: walletID, Kind: "BET",
	}, eventpayload.WalletBalanceChangedData{
		WalletID: walletID, TransactionID: bet.TransactionID, Direction: "DEBIT",
		Money:         eventpayload.Money{Amount: "10.00", Currency: "BRL"},
		BalanceBefore: eventpayload.Money{Amount: "100.00", Currency: "BRL"},
		BalanceAfter:  eventpayload.Money{Amount: "90.00", Currency: "BRL"}, WalletVersion: 2,
	})

	win := processEventTestWager(t, ctx, service, ProcessCommand{
		IdempotencyKey: "event-win", CorrelationID: "correlation-win", CausationID: "causation-win",
		Request: domain.WagerRequest{
			ProviderID: "provider-a", ExternalTransactionID: "event-win", PlayerID: "player-event-contract",
			WalletID: walletID, RoundID: "round-1", GameID: "game-1", Kind: domain.WagerKindWin,
			Amount: domain.NewMoney(2500, domain.BRL),
		},
	})
	assertProcessedAndBalanceEvents(t, ctx, pool, win.TransactionID, "correlation-win", "causation-win", eventpayload.WagerTransactionProcessedData{
		TransactionID: win.TransactionID, WalletID: walletID, Kind: "WIN",
	}, eventpayload.WalletBalanceChangedData{
		WalletID: walletID, TransactionID: win.TransactionID, Direction: "CREDIT",
		Money:         eventpayload.Money{Amount: "25.00", Currency: "BRL"},
		BalanceBefore: eventpayload.Money{Amount: "90.00", Currency: "BRL"},
		BalanceAfter:  eventpayload.Money{Amount: "115.00", Currency: "BRL"}, WalletVersion: 3,
	})

	rejected := processEventTestWager(t, ctx, service, ProcessCommand{
		IdempotencyKey: "event-rejected", CorrelationID: "correlation-rejected", CausationID: "causation-rejected",
		Request: domain.WagerRequest{
			ProviderID: "provider-a", ExternalTransactionID: "event-rejected", PlayerID: "player-event-contract",
			WalletID: walletID, RoundID: "round-1", GameID: "game-1", Kind: domain.WagerKindBet,
			Amount: domain.NewMoney(20000, domain.BRL),
		},
	})
	if rejected.State != domain.WagerStateRejected {
		t.Fatalf("expected rejected wager, got %s", rejected.State)
	}
	rejectedEnvelope := loadStoredEvent(t, ctx, pool, rejected.TransactionID, "WagerTransactionRejected")
	assertEventEnvelopeContract(t, rejectedEnvelope, rejected.TransactionID, "WagerTransactionRejected", "correlation-rejected", "causation-rejected")
	var rejectedData eventpayload.WagerTransactionRejectedData
	decodeExactEventData(t, rejectedEnvelope.Data, []string{"transactionId", "walletId", "providerId", "kind", "failureCode"}, &rejectedData)
	if rejectedData != (eventpayload.WagerTransactionRejectedData{
		TransactionID: rejected.TransactionID, WalletID: walletID, ProviderID: "provider-a", Kind: "BET", FailureCode: "INSUFFICIENT_FUNDS",
	}) {
		t.Fatalf("unexpected rejected event data: %+v", rejectedData)
	}

	pending := processEventTestWager(t, ctx, service, ProcessCommand{
		IdempotencyKey: "event-pending", CorrelationID: "correlation-pending", CausationID: "causation-pending",
		Request: domain.WagerRequest{
			ProviderID: "provider-a", ExternalTransactionID: "event-pending", PlayerID: "player-event-contract",
			WalletID: walletID, RoundID: "round-1", GameID: "game-1", Kind: domain.WagerKindRefund,
			Amount: domain.NewMoney(1000, domain.BRL), ReferenceExternalTransactionID: "missing-event-bet",
		},
	})
	pendingEnvelope := loadStoredEvent(t, ctx, pool, pending.TransactionID, "WagerTransactionPendingReference")
	assertEventEnvelopeContract(t, pendingEnvelope, pending.TransactionID, "WagerTransactionPendingReference", "correlation-pending", "causation-pending")
	var pendingData eventpayload.WagerTransactionPendingReferenceData
	decodeExactEventData(t, pendingEnvelope.Data, []string{"transactionId", "providerId", "externalTransactionId", "referenceExternalTransactionId"}, &pendingData)
	if pendingData != (eventpayload.WagerTransactionPendingReferenceData{
		TransactionID: pending.TransactionID, ProviderID: "provider-a", ExternalTransactionID: "event-pending", ReferenceExternalTransactionID: "missing-event-bet",
	}) {
		t.Fatalf("unexpected pending event data: %+v", pendingData)
	}

	loss := processEventTestWager(t, ctx, service, ProcessCommand{
		IdempotencyKey: "event-loss", CorrelationID: "correlation-loss",
		Request: domain.WagerRequest{
			ProviderID: "provider-a", ExternalTransactionID: "event-loss", PlayerID: "player-event-contract",
			WalletID: walletID, RoundID: "round-1", GameID: "game-1", Kind: domain.WagerKindLoss,
			Amount: domain.Zero(domain.BRL),
		},
	})
	var processedCount, balanceChangedCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FILTER (WHERE event_type = 'WagerTransactionProcessed'), COUNT(*) FILTER (WHERE event_type = 'WalletBalanceChanged') FROM outbox_events WHERE aggregate_id = $1`, loss.TransactionID).Scan(&processedCount, &balanceChangedCount); err != nil {
		t.Fatalf("count LOSS events: %v", err)
	}
	if processedCount != 1 || balanceChangedCount != 0 {
		t.Fatalf("expected LOSS processed event only, got processed=%d balance=%d", processedCount, balanceChangedCount)
	}
}

func TestPendingResolverUsesSharedProcessedAndRejectedEventContracts(t *testing.T) {
	t.Run("processed", func(t *testing.T) {
		ctx, pool, walletID := setupWagerEventTest(t, "player-resolved-event", 10000)
		service := NewService(pool)
		pending := processEventTestWager(t, ctx, service, ProcessCommand{
			IdempotencyKey: "resolved-rollback", CorrelationID: "correlation-resolved", CausationID: "causation-resolved",
			Request: domain.WagerRequest{
				ProviderID: "provider-a", ExternalTransactionID: "resolved-rollback", PlayerID: "player-resolved-event",
				WalletID: walletID, RoundID: "round-1", GameID: "game-1", Kind: domain.WagerKindRollback,
				Amount: domain.NewMoney(1000, domain.BRL), ReferenceExternalTransactionID: "late-resolver-bet",
			},
		})
		processEventTestWager(t, ctx, service, ProcessCommand{IdempotencyKey: "late-resolver-bet", Request: domain.WagerRequest{
			ProviderID: "provider-a", ExternalTransactionID: "late-resolver-bet", PlayerID: "player-resolved-event", WalletID: walletID,
			RoundID: "round-1", GameID: "game-1", Kind: domain.WagerKindBet, Amount: domain.NewMoney(1000, domain.BRL),
		}})
		makeEventTestPendingDue(t, ctx, pool, pending.TransactionID)
		handled, err := NewPendingReferenceResolver(pool).ResolveOne(ctx)
		if err != nil || !handled {
			t.Fatalf("resolve pending wager: handled=%v err=%v", handled, err)
		}
		assertProcessedAndBalanceEvents(t, ctx, pool, pending.TransactionID, "correlation-resolved", "causation-resolved", eventpayload.WagerTransactionProcessedData{
			TransactionID: pending.TransactionID, WalletID: walletID, Kind: "ROLLBACK",
		}, eventpayload.WalletBalanceChangedData{
			WalletID: walletID, TransactionID: pending.TransactionID, Direction: "CREDIT",
			Money:         eventpayload.Money{Amount: "10.00", Currency: "BRL"},
			BalanceBefore: eventpayload.Money{Amount: "90.00", Currency: "BRL"},
			BalanceAfter:  eventpayload.Money{Amount: "100.00", Currency: "BRL"}, WalletVersion: 3,
		})
	})

	t.Run("rejected", func(t *testing.T) {
		ctx, pool, walletID := setupWagerEventTest(t, "player-rejected-event", 10000)
		pending := processEventTestWager(t, ctx, NewService(pool), ProcessCommand{
			IdempotencyKey: "expired-refund", CorrelationID: "correlation-expired", CausationID: "causation-expired",
			Request: domain.WagerRequest{
				ProviderID: "provider-a", ExternalTransactionID: "expired-refund", PlayerID: "player-rejected-event",
				WalletID: walletID, RoundID: "round-1", GameID: "game-1", Kind: domain.WagerKindRefund,
				Amount: domain.NewMoney(1000, domain.BRL), ReferenceExternalTransactionID: "never-arrives",
			},
		})
		if _, err := pool.Exec(ctx, `UPDATE wager_transactions SET reference_next_attempt_at = NOW(), reference_expires_at = NOW() - INTERVAL '1 second' WHERE id = $1`, pending.TransactionID); err != nil {
			t.Fatalf("expire pending wager: %v", err)
		}
		handled, err := NewPendingReferenceResolver(pool).ResolveOne(ctx)
		if err != nil || !handled {
			t.Fatalf("reject expired wager: handled=%v err=%v", handled, err)
		}
		envelope := loadStoredEvent(t, ctx, pool, pending.TransactionID, "WagerTransactionRejected")
		assertEventEnvelopeContract(t, envelope, pending.TransactionID, "WagerTransactionRejected", "correlation-expired", "causation-expired")
		var data eventpayload.WagerTransactionRejectedData
		decodeExactEventData(t, envelope.Data, []string{"transactionId", "walletId", "providerId", "kind", "failureCode"}, &data)
		if data.FailureCode != "REFERENCE_EXPIRED" || data.TransactionID != pending.TransactionID || data.WalletID != walletID {
			t.Fatalf("unexpected resolver rejection event: %+v", data)
		}
	})
}

func assertProcessedAndBalanceEvents(t *testing.T, ctx context.Context, pool *pgxpool.Pool, transactionID, correlationID, causationID string, expectedProcessed eventpayload.WagerTransactionProcessedData, expectedBalance eventpayload.WalletBalanceChangedData) {
	t.Helper()
	processed := loadStoredEvent(t, ctx, pool, transactionID, "WagerTransactionProcessed")
	assertEventEnvelopeContract(t, processed, transactionID, "WagerTransactionProcessed", correlationID, causationID)
	var processedData eventpayload.WagerTransactionProcessedData
	decodeExactEventData(t, processed.Data, []string{"transactionId", "walletId", "kind"}, &processedData)
	if processedData != expectedProcessed {
		t.Fatalf("unexpected processed event data: %+v", processedData)
	}

	balance := loadStoredEvent(t, ctx, pool, transactionID, "WalletBalanceChanged")
	assertEventEnvelopeContract(t, balance, transactionID, "WalletBalanceChanged", correlationID, causationID)
	var balanceData eventpayload.WalletBalanceChangedData
	raw := decodeExactEventData(t, balance.Data, []string{"walletId", "transactionId", "direction", "money", "balanceBefore", "balanceAfter", "walletVersion"}, &balanceData)
	if _, exists := raw["amount"]; exists {
		t.Fatal("WalletBalanceChanged must not contain legacy amount")
	}
	if _, exists := raw["currency"]; exists {
		t.Fatal("WalletBalanceChanged must not contain legacy currency")
	}
	if balanceData != expectedBalance {
		t.Fatalf("unexpected WalletBalanceChanged data: %+v", balanceData)
	}
}

func loadStoredEvent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, aggregateID, eventType string) storedEventEnvelope {
	t.Helper()
	var payload []byte
	if err := pool.QueryRow(ctx, `SELECT payload FROM outbox_events WHERE aggregate_id = $1 AND event_type = $2`, aggregateID, eventType).Scan(&payload); err != nil {
		t.Fatalf("load %s event: %v", eventType, err)
	}
	var envelope storedEventEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		t.Fatalf("decode %s envelope: %v", eventType, err)
	}
	return envelope
}

func assertEventEnvelopeContract(t *testing.T, envelope storedEventEnvelope, aggregateID, eventType, correlationID, causationID string) {
	t.Helper()
	if _, err := uuid.Parse(envelope.EventID); err != nil {
		t.Fatalf("invalid eventId %q: %v", envelope.EventID, err)
	}
	if envelope.EventType != eventType || envelope.AggregateID != aggregateID || envelope.CorrelationID != correlationID || envelope.CausationID != causationID || envelope.Version != 1 {
		t.Fatalf("unexpected event envelope: %+v", envelope)
	}
	if _, err := time.Parse(time.RFC3339Nano, envelope.OccurredAt); err != nil {
		t.Fatalf("invalid occurredAt %q: %v", envelope.OccurredAt, err)
	}
}

func decodeExactEventData(t *testing.T, payload []byte, fields []string, target any) map[string]json.RawMessage {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		t.Fatalf("decode typed event data: %v", err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(payload, &raw); err != nil {
		t.Fatalf("decode raw event data: %v", err)
	}
	if len(raw) != len(fields) {
		t.Fatalf("expected %d event data fields, got %d: %s", len(fields), len(raw), payload)
	}
	for _, field := range fields {
		if _, ok := raw[field]; !ok {
			t.Fatalf("missing event data field %s: %s", field, payload)
		}
	}
	return raw
}

func setupWagerEventTest(t *testing.T, playerID string, initialBalance int64) (context.Context, *pgxpool.Pool, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	pool, err := pgxpool.New(ctx, "postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable")
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	t.Cleanup(pool.Close)
	cleanDatabase(t, ctx, pool)
	created, err := wallet.NewService(pool).Create(ctx, wallet.CreateWalletCommand{PlayerID: playerID, InitialBalance: domain.NewMoney(initialBalance, domain.BRL)})
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE TABLE outbox_events`); err != nil {
		t.Fatalf("clear opening events: %v", err)
	}
	return ctx, pool, created.WalletID
}

func processEventTestWager(t *testing.T, ctx context.Context, service *Service, command ProcessCommand) ProcessResult {
	t.Helper()
	result, err := service.Process(ctx, command)
	if err != nil {
		t.Fatalf("process %s: %v", command.Request.ExternalTransactionID, err)
	}
	return result
}

func makeEventTestPendingDue(t *testing.T, ctx context.Context, pool *pgxpool.Pool, transactionID string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `UPDATE wager_transactions SET reference_next_attempt_at = NOW() WHERE id = $1`, transactionID); err != nil {
		t.Fatalf("make pending transaction due: %v", err)
	}
}

package eventpayload

import (
	"time"

	"github.com/Tharik/wagering-platform/internal/domain"
	"github.com/google/uuid"
)

const eventVersion = 1

type Event interface {
	ID() uuid.UUID
	Type() string
	AggregateID() uuid.UUID
	OccurredAt() time.Time
	Version() int
}

type eventEnvelope[T any] struct {
	EventIDValue       uuid.UUID `json:"eventId"`
	EventTypeValue     string    `json:"eventType"`
	AggregateIDValue   uuid.UUID `json:"aggregateId"`
	CorrelationIDValue string    `json:"correlationId"`
	CausationIDValue   *string   `json:"causationId,omitempty"`
	OccurredAtValue    time.Time `json:"occurredAt"`
	VersionValue       int       `json:"version"`
	Data               T         `json:"data"`
}

func (e eventEnvelope[T]) ID() uuid.UUID {
	return e.EventIDValue
}

func (e eventEnvelope[T]) Type() string {
	return e.EventTypeValue
}

func (e eventEnvelope[T]) AggregateID() uuid.UUID {
	return e.AggregateIDValue
}

func (e eventEnvelope[T]) OccurredAt() time.Time {
	return e.OccurredAtValue
}

func (e eventEnvelope[T]) Version() int {
	return e.VersionValue
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

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

func NewWagerTransactionProcessedEvent(
	aggregateID uuid.UUID,
	correlationID string,
	causationID string,
	occurredAt time.Time,
	data WagerTransactionProcessedData,
) Event {
	return eventEnvelope[WagerTransactionProcessedData]{
		EventIDValue:       uuid.New(),
		EventTypeValue:     "WagerTransactionProcessed",
		AggregateIDValue:   aggregateID,
		CorrelationIDValue: correlationID,
		CausationIDValue:   optionalString(causationID),
		OccurredAtValue:    occurredAt,
		VersionValue:       eventVersion,
		Data:               data,
	}
}

func NewWagerTransactionRejectedEvent(
	aggregateID uuid.UUID,
	correlationID string,
	causationID string,
	occurredAt time.Time,
	data WagerTransactionRejectedData,
) Event {
	return eventEnvelope[WagerTransactionRejectedData]{
		EventIDValue:       uuid.New(),
		EventTypeValue:     "WagerTransactionRejected",
		AggregateIDValue:   aggregateID,
		CorrelationIDValue: correlationID,
		CausationIDValue:   optionalString(causationID),
		OccurredAtValue:    occurredAt,
		VersionValue:       eventVersion,
		Data:               data,
	}
}

func NewWagerTransactionPendingReferenceEvent(
	aggregateID uuid.UUID,
	correlationID string,
	causationID string,
	occurredAt time.Time,
	data WagerTransactionPendingReferenceData,
) Event {
	return eventEnvelope[WagerTransactionPendingReferenceData]{
		EventIDValue:       uuid.New(),
		EventTypeValue:     "WagerTransactionPendingReference",
		AggregateIDValue:   aggregateID,
		CorrelationIDValue: correlationID,
		CausationIDValue:   optionalString(causationID),
		OccurredAtValue:    occurredAt,
		VersionValue:       eventVersion,
		Data:               data,
	}
}

func NewWalletBalanceChangedEvent(
	aggregateID uuid.UUID,
	correlationID string,
	causationID string,
	occurredAt time.Time,
	data WalletBalanceChangedData,
) Event {
	return eventEnvelope[WalletBalanceChangedData]{
		EventIDValue:       uuid.New(),
		EventTypeValue:     "WalletBalanceChanged",
		AggregateIDValue:   aggregateID,
		CorrelationIDValue: correlationID,
		CausationIDValue:   optionalString(causationID),
		OccurredAtValue:    occurredAt,
		VersionValue:       eventVersion,
		Data:               data,
	}
}

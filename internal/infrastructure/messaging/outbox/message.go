package outbox

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Event struct {
	ID          uuid.UUID
	AggregateID uuid.UUID
	EventType   string
	Payload     json.RawMessage
	OccurredAt  time.Time
	Attempts    int
}

type MessagePublisher interface {
	Publish(ctx context.Context, event Event) error
}

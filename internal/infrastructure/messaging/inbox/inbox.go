package inbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

var ErrPayloadConflict = errors.New(
	"message id already exists with different payload",
)

type Message struct {
	ConsumerName string
	MessageID    string
	PayloadHash  string
	ReceivedAt   time.Time
	CompletedAt  *time.Time
}

func PayloadHash(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func Register(
	ctx context.Context,
	tx pgx.Tx,
	consumerName string,
	messageID string,
	payload []byte,
) (alreadyCompleted bool, err error) {
	hash := PayloadHash(payload)

	var existingHash string
	var completedAt *time.Time

	err = tx.QueryRow(
		ctx,
		`
		SELECT
			payload_hash,
			completed_at
		FROM inbox_messages
		WHERE consumer_name = $1
		  AND message_id = $2
		FOR UPDATE
		`,
		consumerName,
		messageID,
	).Scan(
		&existingHash,
		&completedAt,
	)

	switch {
	case err == nil:
		if existingHash != hash {
			return false, ErrPayloadConflict
		}

		return completedAt != nil, nil

	case !errors.Is(err, pgx.ErrNoRows):
		return false, fmt.Errorf(
			"query inbox message: %w",
			err,
		)
	}

	_, err = tx.Exec(
		ctx,
		`
		INSERT INTO inbox_messages (
			consumer_name,
			message_id,
			payload_hash,
			received_at
		)
		VALUES ($1, $2, $3, NOW())
		`,
		consumerName,
		messageID,
		hash,
	)
	if err != nil {
		return false, fmt.Errorf(
			"register inbox message: %w",
			err,
		)
	}

	return false, nil
}

func Complete(
	ctx context.Context,
	tx pgx.Tx,
	consumerName string,
	messageID string,
) error {
	result, err := tx.Exec(
		ctx,
		`
		UPDATE inbox_messages
		SET completed_at = NOW()
		WHERE consumer_name = $1
		  AND message_id = $2
		  AND completed_at IS NULL
		`,
		consumerName,
		messageID,
	)
	if err != nil {
		return fmt.Errorf(
			"complete inbox message: %w",
			err,
		)
	}

	if result.RowsAffected() != 1 {
		return fmt.Errorf(
			"expected to complete one inbox message, updated %d",
			result.RowsAffected(),
		)
	}

	return nil
}

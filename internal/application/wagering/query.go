package wagering

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

var ErrWagerNotFound = errors.New("wager transaction not found")

type WagerResult struct {
	TransactionID                  string
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	WalletID                       string
	PlayerID                       string
	RoundID                        string
	GameID                         string
	Kind                           string
	State                          string
	Amount                         int64
	Currency                       string
	ReferenceExternalTransactionID string
	ReferencedTransactionID        string
	FailureCode                    string
	ResultBalance                  *int64
	CreatedAt                      time.Time
	UpdatedAt                      time.Time
}

func (s *Service) Get(
	ctx context.Context,
	transactionID string,
) (WagerResult, error) {
	var result WagerResult

	var (
		providerID                     *string
		externalTransactionID          *string
		idempotencyKey                 *string
		roundID                        *string
		gameID                         *string
		referenceExternalTransactionID *string
		referencedTransactionID        *string
		failureCode                    *string
	)

	err := s.pool.QueryRow(
		ctx,
		`
		SELECT
			id::text,
			provider_id,
			external_transaction_id,
			idempotency_key,
			wallet_id::text,
			player_id,
			round_id,
			game_id,
			kind::text,
			state::text,
			amount,
			currency,
			reference_external_transaction_id,
			referenced_transaction_id::text,
			failure_code,
			result_balance,
			created_at,
			updated_at
		FROM wager_transactions
		WHERE id = $1
		`,
		transactionID,
	).Scan(
		&result.TransactionID,
		&providerID,
		&externalTransactionID,
		&idempotencyKey,
		&result.WalletID,
		&result.PlayerID,
		&roundID,
		&gameID,
		&result.Kind,
		&result.State,
		&result.Amount,
		&result.Currency,
		&referenceExternalTransactionID,
		&referencedTransactionID,
		&failureCode,
		&result.ResultBalance,
		&result.CreatedAt,
		&result.UpdatedAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return WagerResult{}, ErrWagerNotFound
	}

	if err != nil {
		return WagerResult{}, fmt.Errorf(
			"query wager transaction: %w",
			err,
		)
	}

	result.ProviderID = stringValue(providerID)
	result.ExternalTransactionID = stringValue(externalTransactionID)
	result.IdempotencyKey = stringValue(idempotencyKey)
	result.RoundID = stringValue(roundID)
	result.GameID = stringValue(gameID)
	result.ReferenceExternalTransactionID =
		stringValue(referenceExternalTransactionID)
	result.ReferencedTransactionID =
		stringValue(referencedTransactionID)
	result.FailureCode = stringValue(failureCode)

	return result, nil
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}

	return *value
}

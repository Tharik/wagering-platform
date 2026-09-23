package wallet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/Tharik/wagering-platform/internal/application/eventpayload"
	"github.com/Tharik/wagering-platform/internal/domain"
	"github.com/Tharik/wagering-platform/internal/observability"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrWalletAlreadyExists = errors.New("wallet already exists")

const walletPlayerCurrencyUniqueConstraint = "wallets_player_currency_unique"

type CreateWalletCommand struct {
	PlayerID       string
	InitialBalance domain.Money
}

type CreateWalletResult struct {
	WalletID string
	Balance  domain.Money
	Version  int64
}

type Service struct {
	pool    *pgxpool.Pool
	metrics *observability.Metrics
	logger  *slog.Logger
}

func NewService(pool *pgxpool.Pool) *Service {
	return &Service{
		pool:    pool,
		metrics: observability.NewMetrics(),
		logger:  newDiscardLogger(),
	}
}

func NewServiceWithMetrics(
	pool *pgxpool.Pool,
	metrics *observability.Metrics,
) *Service {
	return &Service{
		pool:    pool,
		metrics: metrics,
		logger:  newDiscardLogger(),
	}
}

func NewServiceWithMetricsAndLogger(
	pool *pgxpool.Pool,
	metrics *observability.Metrics,
	logger *slog.Logger,
) *Service {
	return &Service{
		pool:    pool,
		metrics: metrics,
		logger:  logger,
	}
}

func newDiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func (s *Service) Create(
	ctx context.Context,
	cmd CreateWalletCommand,
) (CreateWalletResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return CreateWalletResult{}, fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	now := time.Now().UTC()
	walletID := uuid.New()
	correlationID := uuid.NewString()
	currency := string(cmd.InitialBalance.Currency())

	_, err = tx.Exec(
		ctx,
		`
		INSERT INTO wallets (
			id,
			player_id,
			currency,
			balance,
			version,
			created_at,
			updated_at
		)
		VALUES ($1, $2, $3, $4, 1, $5, $5)
		`,
		walletID,
		cmd.PlayerID,
		currency,
		cmd.InitialBalance.Amount(),
		now,
	)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) &&
			pgErr.Code == "23505" &&
			pgErr.ConstraintName == walletPlayerCurrencyUniqueConstraint {
			return CreateWalletResult{}, ErrWalletAlreadyExists
		}

		return CreateWalletResult{}, fmt.Errorf("insert wallet: %w", err)
	}

	// A wallet with zero initial balance does not create an
	// OPENING transaction, ledger entry, or events.
	if !cmd.InitialBalance.IsZero() {
		if err := createOpeningTransaction(
			ctx,
			tx,
			walletID,
			cmd.PlayerID,
			cmd.InitialBalance,
			correlationID,
			now,
		); err != nil {
			return CreateWalletResult{}, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return CreateWalletResult{}, fmt.Errorf("commit transaction: %w", err)
	}

	return CreateWalletResult{
		WalletID: walletID.String(),
		Balance:  cmd.InitialBalance,
		Version:  1,
	}, nil
}

func createOpeningTransaction(
	ctx context.Context,
	tx pgx.Tx,
	walletID uuid.UUID,
	playerID string,
	initialBalance domain.Money,
	correlationID string,
	now time.Time,
) error {
	transactionID := uuid.New()

	_, err := tx.Exec(
		ctx,
		`
		INSERT INTO wager_transactions (
			id,
			wallet_id,
			player_id,
			kind,
			state,
			amount,
			currency,
			result_balance,
			created_at,
			updated_at
		)
		VALUES (
			$1, $2, $3,
			'OPENING',
			'PROCESSED',
			$4, $5, $4,
			$6, $6
		)
		`,
		transactionID,
		walletID,
		playerID,
		initialBalance.Amount(),
		string(initialBalance.Currency()),
		now,
	)
	if err != nil {
		return fmt.Errorf("insert opening transaction: %w", err)
	}

	_, err = tx.Exec(
		ctx,
		`
		INSERT INTO ledger_entries (
			id,
			wallet_id,
			transaction_id,
			direction,
			amount,
			balance_before,
			balance_after,
			created_at
		)
		VALUES (
			$1, $2, $3,
			'CREDIT',
			$4,
			0,
			$4,
			$5
		)
		`,
		uuid.New(),
		walletID,
		transactionID,
		initialBalance.Amount(),
		now,
	)
	if err != nil {
		return fmt.Errorf("insert opening ledger entry: %w", err)
	}

	if err := insertOutboxEvent(
		ctx,
		tx,
		walletID,
		"WagerTransactionProcessed",
		correlationID,
		eventpayload.WagerTransactionProcessedData{
			TransactionID: transactionID.String(),
			WalletID:      walletID.String(),
			Kind:          "OPENING",
		},
		now,
	); err != nil {
		return err
	}

	if err := insertOutboxEvent(
		ctx,
		tx,
		walletID,
		"WalletBalanceChanged",
		correlationID,
		eventpayload.WalletBalanceChangedData{
			WalletID:      walletID.String(),
			TransactionID: transactionID.String(),
			Direction:     "CREDIT",
			Money:         eventpayload.NewMoney(initialBalance),
			BalanceBefore: eventpayload.NewMoney(domain.Zero(initialBalance.Currency())),
			BalanceAfter:  eventpayload.NewMoney(initialBalance),
			WalletVersion: 1,
		},
		now,
	); err != nil {
		return err
	}

	return nil
}

func insertOutboxEvent(
	ctx context.Context,
	tx pgx.Tx,
	aggregateID uuid.UUID,
	eventType string,
	correlationID string,
	data any,
	now time.Time,
) error {
	eventID := uuid.New()

	envelope := map[string]any{
		"eventId":       eventID.String(),
		"eventType":     eventType,
		"aggregateId":   aggregateID.String(),
		"correlationId": correlationID,
		"occurredAt":    now.Format(time.RFC3339Nano),
		"version":       1,
		"data":          data,
	}

	payload, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal outbox event: %w", err)
	}

	_, err = tx.Exec(
		ctx,
		`
		INSERT INTO outbox_events (
			id,
			aggregate_id,
			event_type,
			payload,
			occurred_at,
			attempts,
			next_attempt_at
		)
		VALUES ($1, $2, $3, $4::jsonb, $5, 0, $5)
		`,
		eventID,
		aggregateID,
		eventType,
		payload,
		now,
	)
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}

	return nil
}

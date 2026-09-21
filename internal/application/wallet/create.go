package wallet

import (
	"context"
	"fmt"
	"time"

	"github.com/Tharik/wagering-platform/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

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
	pool *pgxpool.Pool
}

func NewService(pool *pgxpool.Pool) *Service {
	return &Service{
		pool: pool,
	}
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
		uuid.New(),
		walletID,
		"WagerTransactionProcessed",
		fmt.Sprintf(
			`{"transactionId":%q,"walletId":%q,"kind":"OPENING"}`,
			transactionID.String(),
			walletID.String(),
		),
		now,
	); err != nil {
		return err
	}

	if err := insertOutboxEvent(
		ctx,
		tx,
		uuid.New(),
		walletID,
		"WalletBalanceChanged",
		fmt.Sprintf(
			`{"walletId":%q,"transactionId":%q,"direction":"CREDIT","amount":%q,"balanceBefore":"0.00","balanceAfter":%q,"walletVersion":1}`,
			walletID.String(),
			transactionID.String(),
			initialBalance.String(),
			initialBalance.String(),
		),
		now,
	); err != nil {
		return err
	}

	return nil
}

func insertOutboxEvent(
	ctx context.Context,
	tx pgx.Tx,
	eventID uuid.UUID,
	aggregateID uuid.UUID,
	eventType string,
	payload string,
	now time.Time,
) error {
	_, err := tx.Exec(
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

package wagering

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Tharik/wagering-platform/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrIdempotencyConflict       = errors.New("idempotency key already used with different payload")
	ErrExternalTransactionExists = errors.New("external transaction already exists")
	ErrWalletNotFound            = errors.New("wallet not found")
	ErrWalletPlayerMismatch      = errors.New("wallet does not belong to player")
)

type ProcessCommand struct {
	IdempotencyKey string
	Request        domain.WagerRequest
}

type ProcessResult struct {
	TransactionID    string
	State            domain.WagerState
	Balance          domain.Money
	IdempotentReplay bool
	FailureCode      string
}

type Service struct {
	pool *pgxpool.Pool
}

func NewService(pool *pgxpool.Pool) *Service {
	return &Service{pool: pool}
}

func (s *Service) Process(
	ctx context.Context,
	cmd ProcessCommand,
) (ProcessResult, error) {
	if cmd.IdempotencyKey == "" {
		return ProcessResult{}, errors.New("idempotency key is required")
	}

	if !cmd.Request.Kind.IsValidExternalKind() {
		return ProcessResult{}, domain.ErrInvalidWagerKind
	}

	// For this first implementation slice we support BET.
	// Other transaction kinds will be added through the same use case.
	if cmd.Request.Kind != domain.WagerKindBet {
		return ProcessResult{}, fmt.Errorf(
			"wager kind %s not implemented yet",
			cmd.Request.Kind,
		)
	}

	payloadHash, err := cmd.Request.PayloadHash()
	if err != nil {
		return ProcessResult{}, fmt.Errorf("calculate payload hash: %w", err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ProcessResult{}, fmt.Errorf("begin transaction: %w", err)
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	replay, found, err := findIdempotentReplay(
		ctx,
		tx,
		cmd.Request.ProviderID,
		cmd.IdempotencyKey,
		payloadHash,
	)
	if err != nil {
		return ProcessResult{}, err
	}

	if found {
		if err := tx.Commit(ctx); err != nil {
			return ProcessResult{}, fmt.Errorf("commit replay transaction: %w", err)
		}

		return replay, nil
	}

	exists, err := externalTransactionExists(
		ctx,
		tx,
		cmd.Request.ProviderID,
		cmd.Request.ExternalTransactionID,
	)
	if err != nil {
		return ProcessResult{}, err
	}

	if exists {
		return ProcessResult{}, ErrExternalTransactionExists
	}

	wallet, err := lockWallet(
		ctx,
		tx,
		cmd.Request.WalletID,
	)
	if err != nil {
		return ProcessResult{}, err
	}

	if wallet.PlayerID != cmd.Request.PlayerID {
		return ProcessResult{}, ErrWalletPlayerMismatch
	}

	if wallet.Balance.Currency() != cmd.Request.Amount.Currency() {
		return ProcessResult{}, domain.ErrCurrencyMismatch
	}

	balanceBefore := wallet.Balance

	err = wallet.Debit(cmd.Request.Amount, time.Now().UTC())
	if err != nil {
		if errors.Is(err, domain.ErrInsufficientFunds) {
			return persistRejectedBet(
				ctx,
				tx,
				cmd,
				payloadHash,
				wallet,
				"INSUFFICIENT_FUNDS",
			)
		}

		return ProcessResult{}, err
	}

	transactionID := uuid.New()
	now := time.Now().UTC()

	_, err = tx.Exec(
		ctx,
		`
		INSERT INTO wager_transactions (
			id,
			provider_id,
			external_transaction_id,
			idempotency_key,
			payload_hash,
			wallet_id,
			player_id,
			round_id,
			game_id,
			kind,
			state,
			amount,
			currency,
			result_balance,
			created_at,
			updated_at
		)
		VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9,
			'BET', 'PROCESSED',
			$10, $11, $12,
			$13, $13
		)
		`,
		transactionID,
		cmd.Request.ProviderID,
		cmd.Request.ExternalTransactionID,
		cmd.IdempotencyKey,
		payloadHash,
		cmd.Request.WalletID,
		cmd.Request.PlayerID,
		cmd.Request.RoundID,
		cmd.Request.GameID,
		cmd.Request.Amount.Amount(),
		string(cmd.Request.Amount.Currency()),
		wallet.Balance.Amount(),
		now,
	)
	if err != nil {
		return ProcessResult{}, fmt.Errorf("insert wager transaction: %w", err)
	}

	_, err = tx.Exec(
		ctx,
		`
		UPDATE wallets
		SET
			balance = $2,
			version = $3,
			updated_at = $4
		WHERE id = $1
		`,
		wallet.ID,
		wallet.Balance.Amount(),
		wallet.Version,
		now,
	)
	if err != nil {
		return ProcessResult{}, fmt.Errorf("update wallet: %w", err)
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
		VALUES ($1, $2, $3, 'DEBIT', $4, $5, $6, $7)
		`,
		uuid.New(),
		wallet.ID,
		transactionID,
		cmd.Request.Amount.Amount(),
		balanceBefore.Amount(),
		wallet.Balance.Amount(),
		now,
	)
	if err != nil {
		return ProcessResult{}, fmt.Errorf("insert ledger entry: %w", err)
	}

	if err := insertProcessedEvents(
		ctx,
		tx,
		transactionID,
		wallet,
		cmd.Request.Amount,
		balanceBefore,
		now,
	); err != nil {
		return ProcessResult{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return ProcessResult{}, fmt.Errorf("commit wager transaction: %w", err)
	}

	return ProcessResult{
		TransactionID: transactionID.String(),
		State:         domain.WagerStateProcessed,
		Balance:       wallet.Balance,
	}, nil
}

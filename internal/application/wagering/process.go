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
	ErrInvalidLossAmount         = errors.New("LOSS amount must be zero")
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

	switch cmd.Request.Kind {
	case domain.WagerKindBet,
		domain.WagerKindWin,
		domain.WagerKindLoss,
		domain.WagerKindRefund,
		domain.WagerKindRollback:
		// Supported below.

	default:
		return ProcessResult{}, domain.ErrInvalidWagerKind
	}

	if cmd.Request.Kind == domain.WagerKindLoss &&
		!cmd.Request.Amount.IsZero() {
		return ProcessResult{}, ErrInvalidLossAmount
	}

	if cmd.Request.Kind != domain.WagerKindLoss &&
		cmd.Request.Amount.Amount() <= 0 {
		return ProcessResult{}, domain.ErrInvalidAmount
	}

	payloadHash, err := cmd.Request.PayloadHash()
	if err != nil {
		return ProcessResult{}, fmt.Errorf(
			"calculate payload hash: %w",
			err,
		)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ProcessResult{}, fmt.Errorf(
			"begin transaction: %w",
			err,
		)
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	// Fast idempotency path.
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
			return ProcessResult{}, fmt.Errorf(
				"commit replay transaction: %w",
				err,
			)
		}

		return replay, nil
	}

	// Serialize all financial decisions for this wallet.
	wallet, err := lockWallet(
		ctx,
		tx,
		cmd.Request.WalletID,
	)
	if err != nil {
		return ProcessResult{}, err
	}

	// A concurrent request may have committed while this transaction
	// was waiting for the wallet lock.
	replay, found, err = findIdempotentReplay(
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
			return ProcessResult{}, fmt.Errorf(
				"commit replay transaction after wallet lock: %w",
				err,
			)
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

	if wallet.PlayerID != cmd.Request.PlayerID {
		return ProcessResult{}, ErrWalletPlayerMismatch
	}

	if wallet.Balance.Currency() != cmd.Request.Amount.Currency() {
		return ProcessResult{}, domain.ErrCurrencyMismatch
	}

	var reference *referencedTransaction
	var reversalMovement movementDirection

	if cmd.Request.Kind == domain.WagerKindRefund ||
		cmd.Request.Kind == domain.WagerKindRollback {

		foundReference, found, err := findReferencedTransaction(
			ctx,
			tx,
			cmd.Request,
		)
		if err != nil {
			return ProcessResult{}, err
		}

		if !found {
			// Temporary behavior.
			// This becomes PENDING_REFERENCE in the next stage.
			return ProcessResult{}, errors.New(
				"referenced transaction not found",
			)
		}

		if err := validateReference(
			cmd.Request,
			foundReference,
		); err != nil {
			return ProcessResult{}, err
		}

		reversalMovement, err = reversalDirection(
			cmd.Request.Kind,
			foundReference,
		)
		if err != nil {
			return ProcessResult{}, err
		}

		reference = &foundReference
	}

	balanceBefore := wallet.Balance
	now := time.Now().UTC()

	switch cmd.Request.Kind {
	case domain.WagerKindBet:
		if err := wallet.Debit(cmd.Request.Amount, now); err != nil {
			if errors.Is(err, domain.ErrInsufficientFunds) {
				return persistRejectedTransaction(
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

	case domain.WagerKindWin:
		if err := wallet.Credit(cmd.Request.Amount, now); err != nil {
			return ProcessResult{}, err
		}

	case domain.WagerKindLoss:
		// LOSS has no financial movement.

	case domain.WagerKindRefund,
		domain.WagerKindRollback:

		switch reversalMovement {
		case movementCredit:
			if err := wallet.Credit(cmd.Request.Amount, now); err != nil {
				return ProcessResult{}, err
			}

		case movementDebit:
			if err := wallet.Debit(cmd.Request.Amount, now); err != nil {
				if errors.Is(err, domain.ErrInsufficientFunds) {
					return persistRejectedTransaction(
						ctx,
						tx,
						cmd,
						payloadHash,
						wallet,
						"REVERSAL_INSUFFICIENT_FUNDS",
					)
				}

				return ProcessResult{}, err
			}

		default:
			return ProcessResult{}, ErrInvalidReferenceKind
		}
	}

	transactionID := uuid.New()

	var referencedTransactionID any

	if reference != nil {
		referencedTransactionID = reference.ID
	}

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
			reference_external_transaction_id,
			referenced_transaction_id,
			result_balance,
			created_at,
			updated_at
		)
		VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9,
			$10, 'PROCESSED',
			$11, $12, $13, $14,
			$15, $16, $16
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
		string(cmd.Request.Kind),
		cmd.Request.Amount.Amount(),
		string(cmd.Request.Amount.Currency()),
		cmd.Request.ReferenceExternalTransactionID,
		referencedTransactionID,
		wallet.Balance.Amount(),
		now,
	)
	if err != nil {
		return ProcessResult{}, fmt.Errorf(
			"insert wager transaction: %w",
			err,
		)
	}

	if cmd.Request.Kind != domain.WagerKindLoss {
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
			return ProcessResult{}, fmt.Errorf(
				"update wallet: %w",
				err,
			)
		}

		direction := "CREDIT"

		switch cmd.Request.Kind {
		case domain.WagerKindBet:
			direction = "DEBIT"

		case domain.WagerKindRollback:
			if reversalMovement == movementDebit {
				direction = "DEBIT"
			}
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
				$1, $2, $3, $4,
				$5, $6, $7, $8
			)
			`,
			uuid.New(),
			wallet.ID,
			transactionID,
			direction,
			cmd.Request.Amount.Amount(),
			balanceBefore.Amount(),
			wallet.Balance.Amount(),
			now,
		)
		if err != nil {
			return ProcessResult{}, fmt.Errorf(
				"insert ledger entry: %w",
				err,
			)
		}
	}

	if err := insertProcessedEvents(
		ctx,
		tx,
		transactionID,
		cmd.Request.Kind,
		wallet,
		cmd.Request.Amount,
		balanceBefore,
		now,
	); err != nil {
		return ProcessResult{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return ProcessResult{}, fmt.Errorf(
			"commit wager transaction: %w",
			err,
		)
	}

	return ProcessResult{
		TransactionID: transactionID.String(),
		State:         domain.WagerStateProcessed,
		Balance:       wallet.Balance,
	}, nil
}

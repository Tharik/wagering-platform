package wallet

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var (
	ErrWalletNotFound      = errors.New("wallet not found")
	ErrInvalidLedgerCursor = errors.New("invalid ledger cursor")
)

const (
	DefaultLedgerPageSize = 50
	MaxLedgerPageSize     = 100
)

type WalletResult struct {
	WalletID string
	PlayerID string
	Currency string
	Balance  int64
	Version  int64
}

type LedgerEntryResult struct {
	ID            string
	TransactionID string
	Direction     string
	Amount        int64
	BalanceBefore int64
	BalanceAfter  int64
	CreatedAt     time.Time
}

type LedgerPage struct {
	Entries    []LedgerEntryResult
	NextCursor string
}

type ledgerCursor struct {
	CreatedAt time.Time `json:"createdAt"`
	ID        string    `json:"id"`
}

func (s *Service) Get(
	ctx context.Context,
	walletID string,
) (WalletResult, error) {
	id, err := uuid.Parse(walletID)
	if err != nil {
		return WalletResult{}, ErrWalletNotFound
	}

	var result WalletResult

	err = s.pool.QueryRow(
		ctx,
		`
		SELECT
			id,
			player_id,
			currency,
			balance,
			version
		FROM wallets
		WHERE id = $1
		`,
		id,
	).Scan(
		&result.WalletID,
		&result.PlayerID,
		&result.Currency,
		&result.Balance,
		&result.Version,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return WalletResult{}, ErrWalletNotFound
	}

	if err != nil {
		return WalletResult{}, fmt.Errorf(
			"query wallet: %w",
			err,
		)
	}

	return result, nil
}

// Ledger keeps the original application API for callers that need the complete
// ledger, such as reconciliation. The HTTP API uses LedgerPage so it never
// exposes an unbounded result set.
func (s *Service) Ledger(
	ctx context.Context,
	walletID string,
) ([]LedgerEntryResult, error) {
	id, err := parseExistingWallet(ctx, s, walletID)
	if err != nil {
		return nil, err
	}

	rows, err := s.pool.Query(
		ctx,
		`
		SELECT
			id,
			transaction_id,
			direction,
			amount,
			balance_before,
			balance_after,
			created_at
		FROM ledger_entries
		WHERE wallet_id = $1
		ORDER BY created_at, id
		`,
		id,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"query wallet ledger: %w",
			err,
		)
	}
	defer rows.Close()

	return scanLedgerEntries(rows)
}

func (s *Service) LedgerPage(
	ctx context.Context,
	walletID string,
	limit int,
	cursor string,
) (LedgerPage, error) {
	id, err := parseExistingWallet(ctx, s, walletID)
	if err != nil {
		return LedgerPage{}, err
	}

	if limit <= 0 {
		limit = DefaultLedgerPageSize
	}
	if limit > MaxLedgerPageSize {
		limit = MaxLedgerPageSize
	}

	var cursorValue *ledgerCursor
	if cursor != "" {
		decoded, err := decodeLedgerCursor(cursor)
		if err != nil {
			return LedgerPage{}, err
		}
		cursorValue = &decoded
	}

	query := `
		SELECT
			id,
			transaction_id,
			direction,
			amount,
			balance_before,
			balance_after,
			created_at
		FROM ledger_entries
		WHERE wallet_id = $1
	`
	args := []any{id}

	if cursorValue != nil {
		query += `
		  AND (
			created_at > $2
			OR (created_at = $2 AND id > $3)
		  )
		`
		args = append(
			args,
			cursorValue.CreatedAt,
			cursorValue.ID,
		)
	}

	// Read one extra row so we can determine whether another page exists
	// without issuing a second COUNT query.
	query += fmt.Sprintf(
		" ORDER BY created_at, id LIMIT $%d",
		len(args)+1,
	)
	args = append(args, limit+1)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return LedgerPage{}, fmt.Errorf(
			"query wallet ledger page: %w",
			err,
		)
	}
	defer rows.Close()

	entries, err := scanLedgerEntries(rows)
	if err != nil {
		return LedgerPage{}, err
	}

	page := LedgerPage{
		Entries: entries,
	}

	if len(entries) > limit {
		page.Entries = entries[:limit]

		last := page.Entries[len(page.Entries)-1]
		nextCursor, err := encodeLedgerCursor(
			ledgerCursor{
				CreatedAt: last.CreatedAt,
				ID:        last.ID,
			},
		)
		if err != nil {
			return LedgerPage{}, fmt.Errorf(
				"encode ledger cursor: %w",
				err,
			)
		}

		page.NextCursor = nextCursor
	}

	return page, nil
}

func parseExistingWallet(
	ctx context.Context,
	s *Service,
	walletID string,
) (uuid.UUID, error) {
	id, err := uuid.Parse(walletID)
	if err != nil {
		return uuid.Nil, ErrWalletNotFound
	}

	var exists bool

	err = s.pool.QueryRow(
		ctx,
		`
		SELECT EXISTS (
			SELECT 1
			FROM wallets
			WHERE id = $1
		)
		`,
		id,
	).Scan(&exists)
	if err != nil {
		return uuid.Nil, fmt.Errorf(
			"check wallet existence: %w",
			err,
		)
	}

	if !exists {
		return uuid.Nil, ErrWalletNotFound
	}

	return id, nil
}

func scanLedgerEntries(rows pgx.Rows) ([]LedgerEntryResult, error) {
	entries := make([]LedgerEntryResult, 0)

	for rows.Next() {
		var entry LedgerEntryResult

		if err := rows.Scan(
			&entry.ID,
			&entry.TransactionID,
			&entry.Direction,
			&entry.Amount,
			&entry.BalanceBefore,
			&entry.BalanceAfter,
			&entry.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf(
				"scan ledger entry: %w",
				err,
			)
		}

		entries = append(entries, entry)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate ledger entries: %w",
			err,
		)
	}

	return entries, nil
}

func encodeLedgerCursor(cursor ledgerCursor) (string, error) {
	payload, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func decodeLedgerCursor(value string) (ledgerCursor, error) {
	payload, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return ledgerCursor{}, ErrInvalidLedgerCursor
	}

	var cursor ledgerCursor
	if err := json.Unmarshal(payload, &cursor); err != nil {
		return ledgerCursor{}, ErrInvalidLedgerCursor
	}

	if cursor.CreatedAt.IsZero() {
		return ledgerCursor{}, ErrInvalidLedgerCursor
	}

	if _, err := uuid.Parse(cursor.ID); err != nil {
		return ledgerCursor{}, ErrInvalidLedgerCursor
	}

	return cursor, nil
}

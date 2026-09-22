package wallet_test

import (
	"context"
	"testing"
	"time"

	wageringapp "github.com/Tharik/wagering-platform/internal/application/wagering"
	walletapp "github.com/Tharik/wagering-platform/internal/application/wallet"
	"github.com/Tharik/wagering-platform/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

const paginationTestDatabaseURL = "postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable"

func TestLedgerPageUsesOpaqueStableCursor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, paginationTestDatabaseURL)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping postgres: %v", err)
	}

	cleanPaginationTestDatabase(t, ctx, pool)

	walletService := walletapp.NewService(pool)
	wagerService := wageringapp.NewService(pool)

	created, err := walletService.Create(
		ctx,
		walletapp.CreateWalletCommand{
			PlayerID:       "player-ledger-pagination",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	processPaginationWager(
		t,
		ctx,
		wagerService,
		created.WalletID,
		"pagination-bet",
		"pagination-bet-external",
		domain.WagerKindBet,
		1000,
	)

	processPaginationWager(
		t,
		ctx,
		wagerService,
		created.WalletID,
		"pagination-win",
		"pagination-win-external",
		domain.WagerKindWin,
		500,
	)

	firstPage, err := walletService.LedgerPage(
		ctx,
		created.WalletID,
		2,
		"",
	)
	if err != nil {
		t.Fatalf("get first ledger page: %v", err)
	}

	if len(firstPage.Entries) != 2 {
		t.Fatalf(
			"expected 2 entries on first page, got %d",
			len(firstPage.Entries),
		)
	}

	if firstPage.NextCursor == "" {
		t.Fatal("expected nextCursor on first page")
	}

	secondPage, err := walletService.LedgerPage(
		ctx,
		created.WalletID,
		2,
		firstPage.NextCursor,
	)
	if err != nil {
		t.Fatalf("get second ledger page: %v", err)
	}

	if len(secondPage.Entries) != 1 {
		t.Fatalf(
			"expected 1 entry on second page, got %d",
			len(secondPage.Entries),
		)
	}

	if secondPage.NextCursor != "" {
		t.Fatalf(
			"expected empty nextCursor on final page, got %q",
			secondPage.NextCursor,
		)
	}

	allIDs := map[string]struct{}{}

	for _, entry := range append(firstPage.Entries, secondPage.Entries...) {
		if _, exists := allIDs[entry.ID]; exists {
			t.Fatalf("duplicate ledger entry across pages: %s", entry.ID)
		}
		allIDs[entry.ID] = struct{}{}
	}

	if len(allIDs) != 3 {
		t.Fatalf("expected exactly 3 distinct ledger entries, got %d", len(allIDs))
	}

	assertLedgerOrder(
		t,
		firstPage.Entries[0],
		firstPage.Entries[1],
	)

	assertLedgerOrder(
		t,
		firstPage.Entries[1],
		secondPage.Entries[0],
	)

	_, err = walletService.LedgerPage(
		ctx,
		created.WalletID,
		2,
		"definitely-not-a-valid-cursor",
	)
	if err != walletapp.ErrInvalidLedgerCursor {
		t.Fatalf(
			"expected ErrInvalidLedgerCursor, got %v",
			err,
		)
	}
}

func processPaginationWager(
	t *testing.T,
	ctx context.Context,
	service *wageringapp.Service,
	walletID string,
	idempotencyKey string,
	externalTransactionID string,
	kind domain.WagerKind,
	amount int64,
) {
	t.Helper()

	result, err := service.Process(
		ctx,
		wageringapp.ProcessCommand{
			IdempotencyKey: idempotencyKey,
			Request: domain.WagerRequest{
				ProviderID:            "provider-pagination-test",
				ExternalTransactionID: externalTransactionID,
				PlayerID:              "player-ledger-pagination",
				WalletID:              walletID,
				RoundID:               "round-ledger-pagination",
				GameID:                "game-ledger-pagination",
				Kind:                  kind,
				Amount:                domain.NewMoney(amount, domain.BRL),
			},
		},
	)
	if err != nil {
		t.Fatalf("process %s: %v", kind, err)
	}

	if result.State != domain.WagerStateProcessed {
		t.Fatalf(
			"expected %s to be PROCESSED, got %s",
			kind,
			result.State,
		)
	}
}

func assertLedgerOrder(
	t *testing.T,
	left walletapp.LedgerEntryResult,
	right walletapp.LedgerEntryResult,
) {
	t.Helper()

	if left.CreatedAt.After(right.CreatedAt) {
		t.Fatalf(
			"ledger is not ordered by created_at: %s > %s",
			left.CreatedAt,
			right.CreatedAt,
		)
	}

	if left.CreatedAt.Equal(right.CreatedAt) && left.ID >= right.ID {
		t.Fatalf(
			"ledger tie-break ordering is not stable: %s >= %s",
			left.ID,
			right.ID,
		)
	}
}

func cleanPaginationTestDatabase(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
) {
	t.Helper()

	_, err := pool.Exec(
		ctx,
		`
		TRUNCATE TABLE
			outbox_events,
			inbox_messages,
			ledger_entries,
			wager_transactions,
			wallets
		CASCADE
		`,
	)
	if err != nil {
		t.Fatalf("clean database: %v", err)
	}
}

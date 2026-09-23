package wagering

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Tharik/wagering-platform/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestLockWalletRejectsInvalidPersistedState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, "postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	cleanDatabase(t, ctx, pool)

	walletID := uuid.New()
	now := time.Now().UTC()
	_, err = pool.Exec(ctx, `
		INSERT INTO wallets (id, player_id, currency, balance, version, created_at, updated_at)
		VALUES ($1, '', 'BRL', 0, 1, $2, $2)
	`, walletID, now)
	if err != nil {
		t.Fatal(err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	_, err = lockWallet(ctx, tx, walletID.String())
	if !errors.Is(err, domain.ErrInvalidWalletPlayerID) {
		t.Fatalf("expected invalid persisted player ID, got %v", err)
	}
}

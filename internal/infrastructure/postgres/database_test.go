package postgres

import (
	"context"
	"testing"
	"time"
)

func TestDatabaseConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	db, err := NewDatabase(
		ctx,
		"postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable",
	)
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}
	defer db.Close()

	var result int

	err = db.Pool.QueryRow(
		ctx,
		"SELECT 1",
	).Scan(&result)

	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if result != 1 {
		t.Fatalf("expected 1, got %d", result)
	}
}

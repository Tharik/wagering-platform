package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	walletapp "github.com/Tharik/wagering-platform/internal/application/wallet"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestUnexpectedDatabaseFailureRemainsInternalServerError(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), testDatabaseURL)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	pool.Close()

	handler := NewWalletHandler(walletapp.NewService(pool))
	request := httptest.NewRequest(http.MethodGet, "/wallets/00000000-0000-0000-0000-000000000001", nil)
	request.SetPathValue("id", "00000000-0000-0000-0000-000000000001")
	recorder := httptest.NewRecorder()

	handler.Get(recorder, request)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d: %s", recorder.Code, recorder.Body.String())
	}
	var response map[string]string
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response["error"] != "failed to get wallet" {
		t.Fatalf("unexpected error response: %q", response["error"])
	}
}

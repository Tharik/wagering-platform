package httpapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	wageringapp "github.com/Tharik/wagering-platform/internal/application/wagering"
	walletapp "github.com/Tharik/wagering-platform/internal/application/wallet"
	"github.com/Tharik/wagering-platform/internal/observability"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestWalletLedgerHTTPPagination(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, testDatabaseURL)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping postgres: %v", err)
	}

	cleanHTTPTestDatabase(t, ctx, pool)

	auth, err := NewAuthMiddleware(ctx, testOIDCIssuer)
	if err != nil {
		t.Fatalf("create OIDC middleware: %v", err)
	}

	walletService := walletapp.NewService(pool)
	wagerService := wageringapp.NewService(pool)

	sqsClient := sqs.New(sqs.Options{
		Region: "us-east-1",
		Credentials: aws.NewCredentialsCache(
			credentials.NewStaticCredentialsProvider(
				"test",
				"test",
				"",
			),
		),
		BaseEndpoint: aws.String("http://localhost:4566"),
	})

	server := NewServer(
		NewWalletHandler(walletService),
		NewWagerHandler(wagerService),
		NewHealthHandler(pool, sqsClient, testCommandsQueueURL),
		NewMetricsHandler(observability.NewMetrics()),
		auth,
		testDiscardLogger(),
	)

	testServer := httptest.NewServer(server.Handler())
	defer testServer.Close()

	internalToken := getClientCredentialsToken(
		t,
		ctx,
		"wagering-internal",
		"internal-secret",
	)

	providerToken := getClientCredentialsToken(
		t,
		ctx,
		"provider-a",
		"provider-a-secret",
	)

	walletID := createPaginationWalletHTTP(
		t,
		ctx,
		testServer.URL,
		internalToken,
	)

	createPaginationBetHTTP(
		t,
		ctx,
		testServer.URL,
		providerToken,
		walletID,
		"ledger-page-bet-1",
		"ledger-page-external-1",
	)

	createPaginationBetHTTP(
		t,
		ctx,
		testServer.URL,
		providerToken,
		walletID,
		"ledger-page-bet-2",
		"ledger-page-external-2",
	)

	firstPage := getLedgerPageHTTP(
		t,
		ctx,
		testServer.URL,
		internalToken,
		walletID,
		2,
		"",
		http.StatusOK,
	)

	if len(firstPage.Entries) != 2 {
		t.Fatalf(
			"expected 2 entries on first page, got %d",
			len(firstPage.Entries),
		)
	}

	if firstPage.NextCursor == "" {
		t.Fatal("expected nextCursor on first page")
	}

	secondPage := getLedgerPageHTTP(
		t,
		ctx,
		testServer.URL,
		internalToken,
		walletID,
		2,
		firstPage.NextCursor,
		http.StatusOK,
	)

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

	seen := make(map[string]struct{}, 3)

	for _, entry := range append(firstPage.Entries, secondPage.Entries...) {
		if _, exists := seen[entry.ID]; exists {
			t.Fatalf("duplicate ledger entry across pages: %s", entry.ID)
		}

		seen[entry.ID] = struct{}{}
	}

	if len(seen) != 3 {
		t.Fatalf(
			"expected exactly 3 distinct ledger entries, got %d",
			len(seen),
		)
	}

	getLedgerPageHTTP(
		t,
		ctx,
		testServer.URL,
		internalToken,
		walletID,
		2,
		"not-a-valid-cursor",
		http.StatusBadRequest,
	)

	response := doRequest(
		t,
		ctx,
		http.MethodGet,
		testServer.URL+"/wallets/"+walletID+"/ledger?limit=0",
		internalToken,
		nil,
	)
	defer response.Body.Close()

	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf(
			"expected invalid limit to return 400, got %d: %s",
			response.StatusCode,
			readBody(t, response),
		)
	}
}

type ledgerPageHTTPResponse struct {
	Entries []struct {
		ID            string `json:"id"`
		TransactionID string `json:"transactionId"`
		Direction     string `json:"direction"`
		Amount        string `json:"amount"`
		BalanceBefore string `json:"balanceBefore"`
		BalanceAfter  string `json:"balanceAfter"`
		CreatedAt     string `json:"createdAt"`
	} `json:"entries"`
	NextCursor string `json:"nextCursor"`
}

func createPaginationWalletHTTP(
	t *testing.T,
	ctx context.Context,
	baseURL string,
	token string,
) string {
	t.Helper()

	response := doRequest(
		t,
		ctx,
		http.MethodPost,
		baseURL+"/wallets",
		token,
		map[string]any{
			"playerId":       "player-ledger-http-pagination",
			"initialBalance": "100.00",
			"currency":       "BRL",
		},
	)
	defer response.Body.Close()

	if response.StatusCode != http.StatusCreated {
		t.Fatalf(
			"create wallet: expected 201, got %d: %s",
			response.StatusCode,
			readBody(t, response),
		)
	}

	var result struct {
		WalletID string `json:"walletId"`
	}

	decodeJSON(t, response, &result)

	if result.WalletID == "" {
		t.Fatal("create wallet: expected walletId")
	}

	return result.WalletID
}

func createPaginationBetHTTP(
	t *testing.T,
	ctx context.Context,
	baseURL string,
	token string,
	walletID string,
	idempotencyKey string,
	externalTransactionID string,
) {
	t.Helper()

	body := map[string]any{
		"providerId":            "provider-a",
		"externalTransactionId": externalTransactionID,
		"playerId":              "player-ledger-http-pagination",
		"walletId":              walletID,
		"roundId":               "round-ledger-http-pagination",
		"gameId":                "game-ledger-http-pagination",
		"kind":                  "BET",
		"money": map[string]any{
			"amount":   "10.00",
			"currency": "BRL",
		},
	}

	response := doWagerRequest(
		t,
		ctx,
		baseURL+"/wagering/transactions",
		token,
		idempotencyKey,
		body,
	)
	defer response.Body.Close()

	if response.StatusCode != http.StatusCreated {
		t.Fatalf(
			"create BET: expected 201, got %d: %s",
			response.StatusCode,
			readBody(t, response),
		)
	}
}

func getLedgerPageHTTP(
	t *testing.T,
	ctx context.Context,
	baseURL string,
	token string,
	walletID string,
	limit int,
	cursor string,
	expectedStatus int,
) ledgerPageHTTPResponse {
	t.Helper()

	target := baseURL +
		"/wallets/" +
		walletID +
		"/ledger?limit=" +
		url.QueryEscape(stringInt(limit))

	if cursor != "" {
		target += "&cursor=" + url.QueryEscape(cursor)
	}

	response := doRequest(
		t,
		ctx,
		http.MethodGet,
		target,
		token,
		nil,
	)
	defer response.Body.Close()

	if response.StatusCode != expectedStatus {
		t.Fatalf(
			"ledger request: expected %d, got %d: %s",
			expectedStatus,
			response.StatusCode,
			readBody(t, response),
		)
	}

	if expectedStatus != http.StatusOK {
		return ledgerPageHTTPResponse{}
	}

	var result ledgerPageHTTPResponse
	decodeJSON(t, response, &result)

	return result
}

func stringInt(value int) string {
	if value == 0 {
		return "0"
	}

	const digits = "0123456789"

	var buffer [20]byte
	index := len(buffer)

	for value > 0 {
		index--
		buffer[index] = digits[value%10]
		value /= 10
	}

	return string(buffer[index:])
}

func testDiscardLogger() *slog.Logger {
	return slog.New(
		slog.NewJSONHandler(io.Discard, nil),
	)
}

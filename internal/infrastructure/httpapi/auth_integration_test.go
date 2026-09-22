package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
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

const (
	testDatabaseURL      = "postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable"
	testOIDCIssuer       = "http://localhost:8081/realms/wagering"
	testTokenURL         = testOIDCIssuer + "/protocol/openid-connect/token"
	testCommandsQueueURL = "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/wager-transactions.fifo"
)

func TestOIDCAuthenticationAndProviderIsolation(t *testing.T) {
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

	walletHandler := NewWalletHandler(walletService)
	wagerHandler := NewWagerHandler(wagerService)

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

	healthHandler := NewHealthHandler(
		pool,
		sqsClient,
		testCommandsQueueURL,
	)

	metrics := observability.NewMetrics()
	metricsHandler := NewMetricsHandler(metrics)

	logger := slog.New(
		slog.NewJSONHandler(io.Discard, nil),
	)

	server := NewServer(
		walletHandler,
		wagerHandler,
		healthHandler,
		metricsHandler,
		auth,
		logger,
	)

	testServer := httptest.NewServer(server.Handler())
	defer testServer.Close()

	internalToken := getClientCredentialsToken(
		t,
		ctx,
		"wagering-internal",
		"internal-secret",
	)

	providerAToken := getClientCredentialsToken(
		t,
		ctx,
		"provider-a",
		"provider-a-secret",
	)

	providerBToken := getClientCredentialsToken(
		t,
		ctx,
		"provider-b",
		"provider-b-secret",
	)

	t.Run("wallet endpoint rejects missing token", func(t *testing.T) {
		response := doRequest(
			t,
			ctx,
			http.MethodGet,
			testServer.URL+"/wallets/00000000-0000-0000-0000-000000000001",
			"",
			nil,
		)
		defer response.Body.Close()

		if response.StatusCode != http.StatusUnauthorized {
			t.Fatalf(
				"expected 401, got %d: %s",
				response.StatusCode,
				readBody(t, response),
			)
		}
	})

	t.Run("invalid token is rejected", func(t *testing.T) {
		response := doRequest(
			t,
			ctx,
			http.MethodGet,
			testServer.URL+"/wallets/00000000-0000-0000-0000-000000000001",
			"this-is-not-a-valid-jwt",
			nil,
		)
		defer response.Body.Close()

		if response.StatusCode != http.StatusUnauthorized {
			t.Fatalf(
				"expected 401, got %d: %s",
				response.StatusCode,
				readBody(t, response),
			)
		}
	})

	t.Run("provider cannot access internal wallet endpoint", func(t *testing.T) {
		response := doRequest(
			t,
			ctx,
			http.MethodGet,
			testServer.URL+"/wallets/00000000-0000-0000-0000-000000000001",
			providerAToken,
			nil,
		)
		defer response.Body.Close()

		if response.StatusCode != http.StatusForbidden {
			t.Fatalf(
				"expected 403, got %d: %s",
				response.StatusCode,
				readBody(t, response),
			)
		}
	})

	var walletID string

	t.Run("internal client can create wallet", func(t *testing.T) {
		body := map[string]any{
			"playerId":       "player-auth-integration",
			"initialBalance": "100.00",
			"currency":       "BRL",
		}

		response := doRequest(
			t,
			ctx,
			http.MethodPost,
			testServer.URL+"/wallets",
			internalToken,
			body,
		)
		defer response.Body.Close()

		if response.StatusCode != http.StatusCreated {
			t.Fatalf(
				"expected 201, got %d: %s",
				response.StatusCode,
				readBody(t, response),
			)
		}

		var result struct {
			WalletID string `json:"walletId"`
		}

		decodeJSON(t, response, &result)

		if result.WalletID == "" {
			t.Fatal("expected walletId")
		}

		walletID = result.WalletID
	})

	t.Run("internal client cannot access provider wager endpoint", func(t *testing.T) {
		body := wagerBody(
			"internal-must-fail",
			"internal-must-fail",
			walletID,
		)

		response := doRequest(
			t,
			ctx,
			http.MethodPost,
			testServer.URL+"/wagering/transactions",
			internalToken,
			body,
		)
		defer response.Body.Close()

		if response.StatusCode != http.StatusForbidden {
			t.Fatalf(
				"expected 403, got %d: %s",
				response.StatusCode,
				readBody(t, response),
			)
		}
	})

	const providerAExternalTransactionID = "provider-a-auth-external"

	var wagerID string

	t.Run("provider A creates wager and identity comes from token", func(t *testing.T) {
		body := wagerBody(
			"provider-a-auth-test",
			providerAExternalTransactionID,
			walletID,
		)

		response := doRequest(
			t,
			ctx,
			http.MethodPost,
			testServer.URL+"/wagering/transactions",
			providerAToken,
			body,
		)
		defer response.Body.Close()

		if response.StatusCode != http.StatusCreated {
			t.Fatalf(
				"expected 201, got %d: %s",
				response.StatusCode,
				readBody(t, response),
			)
		}

		var result struct {
			TransactionID string `json:"transactionId"`
		}

		decodeJSON(t, response, &result)

		if result.TransactionID == "" {
			t.Fatal("expected transactionId")
		}

		wagerID = result.TransactionID

		var providerID string

		err := pool.QueryRow(
			ctx,
			`
			SELECT provider_id
			FROM wager_transactions
			WHERE id = $1
			`,
			wagerID,
		).Scan(&providerID)
		if err != nil {
			t.Fatalf("query wager provider: %v", err)
		}

		if providerID != "provider-a" {
			t.Fatalf(
				"expected persisted provider provider-a, got %s",
				providerID,
			)
		}
	})

	t.Run("internal client cannot read provider wager", func(t *testing.T) {
		response := doRequest(
			t,
			ctx,
			http.MethodGet,
			testServer.URL+"/wagering/transactions/"+wagerID,
			internalToken,
			nil,
		)
		defer response.Body.Close()

		if response.StatusCode != http.StatusForbidden {
			t.Fatalf(
				"expected 403, got %d: %s",
				response.StatusCode,
				readBody(t, response),
			)
		}
	})

	t.Run("provider A can read its own wager", func(t *testing.T) {
		response := doRequest(
			t,
			ctx,
			http.MethodGet,
			testServer.URL+"/wagering/transactions/"+wagerID,
			providerAToken,
			nil,
		)
		defer response.Body.Close()

		if response.StatusCode != http.StatusOK {
			t.Fatalf(
				"expected 200, got %d: %s",
				response.StatusCode,
				readBody(t, response),
			)
		}

		var result struct {
			ProviderID string `json:"providerId"`
		}

		decodeJSON(t, response, &result)

		if result.ProviderID != "provider-a" {
			t.Fatalf(
				"expected providerId provider-a, got %s",
				result.ProviderID,
			)
		}
	})

	t.Run("provider B cannot read provider A wager", func(t *testing.T) {
		response := doRequest(
			t,
			ctx,
			http.MethodGet,
			testServer.URL+"/wagering/transactions/"+wagerID,
			providerBToken,
			nil,
		)
		defer response.Body.Close()

		if response.StatusCode != http.StatusNotFound {
			t.Fatalf(
				"expected 404, got %d: %s",
				response.StatusCode,
				readBody(t, response),
			)
		}
	})

	t.Run("provider A can read its wager by external transaction ID", func(t *testing.T) {
		response := doRequest(
			t,
			ctx,
			http.MethodGet,
			testServer.URL+
				"/providers/provider-a/wagering/transactions/"+
				providerAExternalTransactionID,
			providerAToken,
			nil,
		)
		defer response.Body.Close()

		if response.StatusCode != http.StatusOK {
			t.Fatalf(
				"expected 200, got %d: %s",
				response.StatusCode,
				readBody(t, response),
			)
		}

		var result struct {
			TransactionID         string `json:"transactionId"`
			ProviderID            string `json:"providerId"`
			ExternalTransactionID string `json:"externalTransactionId"`
		}

		decodeJSON(t, response, &result)

		if result.TransactionID != wagerID {
			t.Fatalf(
				"expected transactionId %s, got %s",
				wagerID,
				result.TransactionID,
			)
		}

		if result.ProviderID != "provider-a" {
			t.Fatalf(
				"expected providerId provider-a, got %s",
				result.ProviderID,
			)
		}

		if result.ExternalTransactionID != providerAExternalTransactionID {
			t.Fatalf(
				"expected externalTransactionId %s, got %s",
				providerAExternalTransactionID,
				result.ExternalTransactionID,
			)
		}
	})

	t.Run("provider A cannot use provider B namespace", func(t *testing.T) {
		response := doRequest(
			t,
			ctx,
			http.MethodGet,
			testServer.URL+
				"/providers/provider-b/wagering/transactions/"+
				providerAExternalTransactionID,
			providerAToken,
			nil,
		)
		defer response.Body.Close()

		if response.StatusCode != http.StatusForbidden {
			t.Fatalf(
				"expected 403, got %d: %s",
				response.StatusCode,
				readBody(t, response),
			)
		}
	})

	t.Run("provider B cannot find provider A external transaction in its namespace", func(t *testing.T) {
		response := doRequest(
			t,
			ctx,
			http.MethodGet,
			testServer.URL+
				"/providers/provider-b/wagering/transactions/"+
				providerAExternalTransactionID,
			providerBToken,
			nil,
		)
		defer response.Body.Close()

		if response.StatusCode != http.StatusNotFound {
			t.Fatalf(
				"expected 404, got %d: %s",
				response.StatusCode,
				readBody(t, response),
			)
		}
	})

	t.Run("provider cannot spoof providerId in request body", func(t *testing.T) {
		body := wagerBody(
			"provider-spoof-test",
			"provider-spoof-external",
			walletID,
		)

		body["providerId"] = "provider-b"

		response := doRequest(
			t,
			ctx,
			http.MethodPost,
			testServer.URL+"/wagering/transactions",
			providerAToken,
			body,
		)
		defer response.Body.Close()

		if response.StatusCode != http.StatusBadRequest {
			t.Fatalf(
				"expected 400, got %d: %s",
				response.StatusCode,
				readBody(t, response),
			)
		}
	})

	// Financial sanity check:
	// opening 100.00 - exactly one successful BET 10.00 = 90.00.
	t.Run("authorization failures did not create financial side effects", func(t *testing.T) {
		var balance int64
		var betCount int

		err := pool.QueryRow(
			ctx,
			`
			SELECT balance
			FROM wallets
			WHERE id = $1
			`,
			walletID,
		).Scan(&balance)
		if err != nil {
			t.Fatalf("query final balance: %v", err)
		}

		if balance != 9000 {
			t.Fatalf("expected final balance 9000, got %d", balance)
		}

		err = pool.QueryRow(
			ctx,
			`
			SELECT COUNT(*)
			FROM wager_transactions
			WHERE wallet_id = $1
			  AND kind = 'BET'
			`,
			walletID,
		).Scan(&betCount)
		if err != nil {
			t.Fatalf("count BET transactions: %v", err)
		}

		if betCount != 1 {
			t.Fatalf("expected exactly 1 BET, got %d", betCount)
		}
	})
}

func wagerBody(
	idempotencyKey string,
	externalTransactionID string,
	walletID string,
) map[string]any {
	return map[string]any{
		"idempotencyKey":        idempotencyKey,
		"externalTransactionId": externalTransactionID,
		"playerId":              "player-auth-integration",
		"walletId":              walletID,
		"roundId":               "round-auth-integration",
		"gameId":                "game-auth-integration",
		"kind":                  "BET",
		"amount":                "10.00",
		"currency":              "BRL",
	}
}

func getClientCredentialsToken(
	t *testing.T,
	ctx context.Context,
	clientID string,
	clientSecret string,
) string {
	t.Helper()

	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", clientID)
	form.Set("client_secret", clientSecret)

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		testTokenURL,
		strings.NewReader(form.Encode()),
	)
	if err != nil {
		t.Fatalf("create token request for %s: %v", clientID, err)
	}

	request.Header.Set(
		"Content-Type",
		"application/x-www-form-urlencoded",
	)

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf(
			"request token for %s; is Keycloak running?: %v",
			clientID,
			err,
		)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)

		t.Fatalf(
			"request token for %s: expected 200, got %d: %s",
			clientID,
			response.StatusCode,
			string(body),
		)
	}

	var result struct {
		AccessToken string `json:"access_token"`
	}

	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatalf("decode token response for %s: %v", clientID, err)
	}

	if result.AccessToken == "" {
		t.Fatalf("Keycloak returned empty access token for %s", clientID)
	}

	return result.AccessToken
}

func doRequest(
	t *testing.T,
	ctx context.Context,
	method string,
	target string,
	token string,
	body any,
) *http.Response {
	t.Helper()

	var requestBody io.Reader

	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}

		requestBody = bytes.NewReader(payload)
	}

	request, err := http.NewRequestWithContext(
		ctx,
		method,
		target,
		requestBody,
	)
	if err != nil {
		t.Fatalf("create HTTP request: %v", err)
	}

	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}

	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("execute HTTP request: %v", err)
	}

	return response
}

func decodeJSON(
	t *testing.T,
	response *http.Response,
	target any,
) {
	t.Helper()

	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		t.Fatalf("decode response: %v", err)
	}
}

func readBody(t *testing.T, response *http.Response) string {
	t.Helper()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}

	return string(body)
}

func cleanHTTPTestDatabase(
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

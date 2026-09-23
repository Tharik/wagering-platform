package multiinstance

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const providerID = "provider-a"

type harness struct {
	t             *testing.T
	ctx           context.Context
	client        *http.Client
	pool          *pgxpool.Pool
	apps          []string
	internalToken string
	providerToken string
	prefix        string
}

type walletResponse struct {
	ID      string `json:"id"`
	Version int64  `json:"version"`
	Balance money  `json:"balance"`
}

type wagerResponse struct {
	TransactionID    string `json:"transactionId"`
	Status           string `json:"status"`
	Balance          money  `json:"balance"`
	IdempotentReplay bool   `json:"idempotentReplay"`
	FailureCode      string `json:"failureCode"`
}

type money struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type wagerRequest struct {
	ProviderID            string `json:"providerId"`
	ExternalTransactionID string `json:"externalTransactionId"`
	PlayerID              string `json:"playerId"`
	WalletID              string `json:"walletId"`
	RoundID               string `json:"roundId"`
	GameID                string `json:"gameId"`
	Kind                  string `json:"kind"`
	Money                 money  `json:"money"`
}

type wagerOutcome struct {
	statusCode int
	response   wagerResponse
	errorText  string
}

func TestMultiInstanceCorrectness(t *testing.T) {
	if os.Getenv("APP_URLS") == "" {
		t.Skip("multi-instance harness requires the Compose test environment")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	h := newHarness(t, ctx)
	defer h.pool.Close()

	h.verifyIndependentEndpoints()
	h.testTwoEightyBets()
	h.testThreeWayIdempotency()
	h.testIdempotencyConflict()
	h.testDifferentWallets()
}

func newHarness(t *testing.T, ctx context.Context) *harness {
	t.Helper()

	apps := strings.Split(requiredEnv(t, "APP_URLS"), ",")
	if len(apps) != 3 {
		t.Fatalf("expected exactly three APP_URLS, got %d", len(apps))
	}

	pool, err := pgxpool.New(ctx, requiredEnv(t, "DATABASE_URL"))
	if err != nil {
		t.Fatalf("connect to PostgreSQL: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping PostgreSQL: %v", err)
	}

	client := &http.Client{Timeout: 15 * time.Second}
	internalToken := clientCredentialsToken(t, ctx, client, "wagering-internal", "internal-secret")
	providerToken := clientCredentialsToken(t, ctx, client, providerID, "provider-a-secret")
	assertTokenIssuer(t, providerToken, requiredEnv(t, "EXPECTED_OIDC_ISSUER"))

	return &harness{
		t:             t,
		ctx:           ctx,
		client:        client,
		pool:          pool,
		apps:          apps,
		internalToken: internalToken,
		providerToken: providerToken,
		prefix:        "multi-" + uuid.NewString(),
	}
}

func (h *harness) verifyIndependentEndpoints() {
	for index, app := range h.apps {
		request, err := http.NewRequestWithContext(h.ctx, http.MethodGet, app+"/health/ready", nil)
		if err != nil {
			h.t.Fatalf("build readiness request: %v", err)
		}
		response, err := h.client.Do(request)
		if err != nil {
			h.t.Fatalf("app%d readiness: %v", index+1, err)
		}
		body := readBody(h.t, response)
		if response.StatusCode != http.StatusOK {
			h.t.Fatalf("app%d readiness returned %d: %s", index+1, response.StatusCode, body)
		}
		h.t.Logf("app%d healthy at %s", index+1, app)
	}
}

func (h *harness) testTwoEightyBets() {
	playerID := h.prefix + "-80-player"
	roundID := h.prefix + "-80-round"
	wallet := h.createWallet(h.apps[0], playerID, "100.00")
	requests := []wagerRequest{
		h.betRequest(playerID, wallet.ID, roundID, h.prefix+"-80-a", "80.00"),
		h.betRequest(playerID, wallet.ID, roundID, h.prefix+"-80-b", "80.00"),
	}
	outcomes := h.concurrentWagers(
		[]string{h.apps[0], h.apps[1]},
		[]string{h.prefix + "-80-idem-a", h.prefix + "-80-idem-b"},
		requests,
	)

	processed, rejected := 0, 0
	for _, outcome := range outcomes {
		h.requireNoTransportError(outcome)
		if outcome.statusCode != http.StatusCreated {
			h.t.Fatalf("80/80 expected 201, got %d: %s", outcome.statusCode, outcome.errorText)
		}
		switch outcome.response.Status {
		case "PROCESSED":
			processed++
		case "REJECTED":
			rejected++
			if outcome.response.FailureCode != "INSUFFICIENT_FUNDS" {
				h.t.Fatalf("80/80 rejection code: %s", outcome.response.FailureCode)
			}
		default:
			h.t.Fatalf("80/80 unexpected status: %s", outcome.response.Status)
		}
	}
	if processed != 1 || rejected != 1 {
		h.t.Fatalf("80/80 expected one processed and one rejected, got %d/%d", processed, rejected)
	}

	var balance, version int64
	h.queryRow(`SELECT balance, version FROM wallets WHERE id = $1`, wallet.ID).Scan(&balance, &version)
	if balance != 2000 || version != 2 {
		h.t.Fatalf("80/80 wallet expected 2000/version 2, got %d/%d", balance, version)
	}
	var total, dbProcessed, dbRejected, insufficient int
	h.queryRow(`SELECT COUNT(*), COUNT(*) FILTER (WHERE state = 'PROCESSED'), COUNT(*) FILTER (WHERE state = 'REJECTED'), COUNT(*) FILTER (WHERE failure_code = 'INSUFFICIENT_FUNDS') FROM wager_transactions WHERE provider_id = $1 AND round_id = $2 AND kind = 'BET'`, providerID, roundID).Scan(&total, &dbProcessed, &dbRejected, &insufficient)
	if total != 2 || dbProcessed != 1 || dbRejected != 1 || insufficient != 1 {
		h.t.Fatalf("80/80 wager counts total=%d processed=%d rejected=%d insufficient=%d", total, dbProcessed, dbRejected, insufficient)
	}
	var entries, debits, unsafeEntries int
	h.queryRow(`SELECT COUNT(*), COUNT(*) FILTER (WHERE l.direction = 'DEBIT' AND l.amount = 8000), COUNT(*) FILTER (WHERE l.balance_before < 0 OR l.balance_after < 0) FROM ledger_entries l JOIN wager_transactions w ON w.id = l.transaction_id WHERE w.provider_id = $1 AND w.round_id = $2`, providerID, roundID).Scan(&entries, &debits, &unsafeEntries)
	if entries != 1 || debits != 1 || unsafeEntries != 0 {
		h.t.Fatalf("80/80 ledger expected exactly one debit and no negatives, got entries=%d debits=%d unsafe=%d", entries, debits, unsafeEntries)
	}
	h.t.Log("80/80: one PROCESSED, one REJECTED, balance 20.00, one debit")
}

func (h *harness) testThreeWayIdempotency() {
	playerID := h.prefix + "-idem-player"
	roundID := h.prefix + "-idem-round"
	wallet := h.createWallet(h.apps[1], playerID, "100.00")
	request := h.betRequest(playerID, wallet.ID, roundID, h.prefix+"-idem-external", "30.00")
	idempotencyKey := h.prefix + "-idem-key"
	outcomes := h.concurrentWagers(
		h.apps,
		[]string{idempotencyKey, idempotencyKey, idempotencyKey},
		[]wagerRequest{request, request, request},
	)

	created, replayed := 0, 0
	transactionID := ""
	for _, outcome := range outcomes {
		h.requireNoTransportError(outcome)
		if outcome.statusCode >= 500 {
			h.t.Fatalf("idempotency returned %d: %s", outcome.statusCode, outcome.errorText)
		}
		if outcome.response.Status != "PROCESSED" || outcome.response.Balance.Amount != "70.00" {
			h.t.Fatalf("unexpected idempotency response: %+v", outcome.response)
		}
		if transactionID == "" {
			transactionID = outcome.response.TransactionID
		} else if transactionID != outcome.response.TransactionID {
			h.t.Fatalf("idempotency transaction IDs diverged: %s vs %s", transactionID, outcome.response.TransactionID)
		}
		switch outcome.statusCode {
		case http.StatusCreated:
			created++
			if outcome.response.IdempotentReplay {
				h.t.Fatal("original response marked as replay")
			}
		case http.StatusOK:
			replayed++
			if !outcome.response.IdempotentReplay {
				h.t.Fatal("replay response was not marked as replay")
			}
		default:
			h.t.Fatalf("unexpected idempotency HTTP status %d", outcome.statusCode)
		}
	}
	if created != 1 || replayed != 2 {
		h.t.Fatalf("expected one 201 and two 200 responses, got %d/%d", created, replayed)
	}

	var balance, version int64
	h.queryRow(`SELECT balance, version FROM wallets WHERE id = $1`, wallet.ID).Scan(&balance, &version)
	if balance != 7000 || version != 2 {
		h.t.Fatalf("idempotency wallet expected 7000/version 2, got %d/%d", balance, version)
	}
	var wagers, debits int
	h.queryRow(`SELECT COUNT(*) FROM wager_transactions WHERE provider_id = $1 AND idempotency_key = $2`, providerID, idempotencyKey).Scan(&wagers)
	h.queryRow(`SELECT COUNT(*) FROM ledger_entries WHERE transaction_id = $1 AND direction = 'DEBIT' AND amount = 3000`, transactionID).Scan(&debits)
	if wagers != 1 || debits != 1 {
		h.t.Fatalf("idempotency expected one wager/debit, got %d/%d", wagers, debits)
	}
	var processedEvents, balanceEvents int
	h.queryRow(`SELECT COUNT(*) FILTER (WHERE event_type = 'WagerTransactionProcessed'), COUNT(*) FILTER (WHERE event_type = 'WalletBalanceChanged') FROM outbox_events WHERE aggregate_id = $1`, transactionID).Scan(&processedEvents, &balanceEvents)
	if processedEvents != 1 || balanceEvents != 1 {
		h.t.Fatalf("idempotency expected one processed/balance event, got %d/%d", processedEvents, balanceEvents)
	}
	h.t.Logf("three-way idempotency: transaction %s, one original and two replays", transactionID)
}

func (h *harness) testIdempotencyConflict() {
	playerID := h.prefix + "-conflict-player"
	roundID := h.prefix + "-conflict-round"
	wallet := h.createWallet(h.apps[2], playerID, "100.00")
	idempotencyKey := h.prefix + "-conflict-key"
	requests := []wagerRequest{
		h.betRequest(playerID, wallet.ID, roundID, h.prefix+"-conflict-a", "10.00"),
		h.betRequest(playerID, wallet.ID, roundID, h.prefix+"-conflict-b", "20.00"),
	}
	outcomes := h.concurrentWagers(
		[]string{h.apps[0], h.apps[2]},
		[]string{idempotencyKey, idempotencyKey},
		requests,
	)

	created, conflicts := 0, 0
	for _, outcome := range outcomes {
		h.requireNoTransportError(outcome)
		switch outcome.statusCode {
		case http.StatusCreated:
			created++
			if outcome.response.Status != "PROCESSED" {
				h.t.Fatalf("winning conflict request was not processed: %+v", outcome.response)
			}
		case http.StatusConflict:
			conflicts++
			if outcome.errorText != "idempotency conflict" {
				h.t.Fatalf("unexpected conflict body: %q", outcome.errorText)
			}
		default:
			h.t.Fatalf("conflict scenario returned %d: %s", outcome.statusCode, outcome.errorText)
		}
	}
	if created != 1 || conflicts != 1 {
		h.t.Fatalf("expected one creation and one conflict, got %d/%d", created, conflicts)
	}
	var wagers, debits int
	h.queryRow(`SELECT COUNT(*) FROM wager_transactions WHERE provider_id = $1 AND idempotency_key = $2`, providerID, idempotencyKey).Scan(&wagers)
	h.queryRow(`SELECT COUNT(*) FROM ledger_entries l JOIN wager_transactions w ON w.id = l.transaction_id WHERE w.provider_id = $1 AND w.idempotency_key = $2 AND l.direction = 'DEBIT'`, providerID, idempotencyKey).Scan(&debits)
	if wagers != 1 || debits != 1 {
		h.t.Fatalf("conflict expected one wager/debit, got %d/%d", wagers, debits)
	}
	h.t.Log("idempotency conflict: one canonical transaction, one HTTP 409, no duplicate debit")
}

func (h *harness) testDifferentWallets() {
	playerA := h.prefix + "-parallel-a"
	playerB := h.prefix + "-parallel-b"
	walletA := h.createWallet(h.apps[0], playerA, "100.00")
	walletB := h.createWallet(h.apps[1], playerB, "100.00")
	requestA := h.betRequest(playerA, walletA.ID, h.prefix+"-parallel-round-a", h.prefix+"-parallel-external-a", "30.00")
	requestB := h.betRequest(playerB, walletB.ID, h.prefix+"-parallel-round-b", h.prefix+"-parallel-external-b", "40.00")
	outcomes := h.concurrentWagers(
		[]string{h.apps[1], h.apps[2]},
		[]string{h.prefix + "-parallel-idem-a", h.prefix + "-parallel-idem-b"},
		[]wagerRequest{requestA, requestB},
	)
	for _, outcome := range outcomes {
		h.requireNoTransportError(outcome)
		if outcome.statusCode != http.StatusCreated || outcome.response.Status != "PROCESSED" {
			h.t.Fatalf("parallel wallet request failed: status=%d response=%+v error=%s", outcome.statusCode, outcome.response, outcome.errorText)
		}
	}
	h.assertWalletMovement(walletA.ID, 7000, 3000)
	h.assertWalletMovement(walletB.ID, 6000, 4000)
	h.t.Log("different wallets: both processed with independent debits and version 2")
}

func (h *harness) createWallet(app, playerID, amount string) walletResponse {
	payload := map[string]any{
		"playerId":       playerID,
		"initialBalance": map[string]string{"amount": amount, "currency": "BRL"},
	}
	var result walletResponse
	status, errorText := h.doJSON(http.MethodPost, app+"/wallets", h.internalToken, "", payload, &result)
	if status != http.StatusCreated {
		h.t.Fatalf("create wallet %s returned %d: %s", playerID, status, errorText)
	}
	if result.Version != 1 || result.Balance.Amount != amount {
		h.t.Fatalf("unexpected created wallet: %+v", result)
	}
	return result
}

func (h *harness) betRequest(playerID, walletID, roundID, externalID, amount string) wagerRequest {
	return wagerRequest{
		ProviderID: providerID, ExternalTransactionID: externalID,
		PlayerID: playerID, WalletID: walletID, RoundID: roundID,
		GameID: "multi-instance-game", Kind: "BET",
		Money: money{Amount: amount, Currency: "BRL"},
	}
}

func (h *harness) concurrentWagers(apps, idempotencyKeys []string, requests []wagerRequest) []wagerOutcome {
	if len(apps) != len(idempotencyKeys) || len(apps) != len(requests) {
		h.t.Fatal("concurrent wager input lengths differ")
	}
	start := make(chan struct{})
	outcomes := make([]wagerOutcome, len(apps))
	var wg sync.WaitGroup
	for index := range apps {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			var response wagerResponse
			status, errorText := h.doJSON(http.MethodPost, apps[index]+"/wagering/transactions", h.providerToken, idempotencyKeys[index], requests[index], &response)
			outcomes[index] = wagerOutcome{statusCode: status, response: response, errorText: errorText}
		}(index)
	}
	close(start)
	wg.Wait()
	return outcomes
}

func (h *harness) doJSON(method, endpoint, token, idempotencyKey string, payload, target any) (int, string) {
	body, err := json.Marshal(payload)
	if err != nil {
		h.t.Fatalf("marshal request: %v", err)
	}
	request, err := http.NewRequestWithContext(h.ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		h.t.Fatalf("build request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		request.Header.Set("Idempotency-Key", idempotencyKey)
	}
	response, err := h.client.Do(request)
	if err != nil {
		return 0, err.Error()
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return response.StatusCode, err.Error()
	}
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		if err := json.Unmarshal(data, target); err != nil {
			return response.StatusCode, "decode response: " + err.Error()
		}
		return response.StatusCode, ""
	}
	var apiError struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(data, &apiError) == nil && apiError.Error != "" {
		return response.StatusCode, apiError.Error
	}
	return response.StatusCode, string(data)
}

func (h *harness) assertWalletMovement(walletID string, expectedBalance, expectedAmount int64) {
	var balance, version int64
	h.queryRow(`SELECT balance, version FROM wallets WHERE id = $1`, walletID).Scan(&balance, &version)
	if balance != expectedBalance || version != 2 {
		h.t.Fatalf("wallet %s expected %d/version 2, got %d/%d", walletID, expectedBalance, balance, version)
	}
	var debits int
	h.queryRow(`SELECT COUNT(*) FROM ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT' AND amount = $2`, walletID, expectedAmount).Scan(&debits)
	if debits != 1 {
		h.t.Fatalf("wallet %s expected one debit of %d, got %d", walletID, expectedAmount, debits)
	}
}

func (h *harness) queryRow(query string, args ...any) rowScanner {
	return checkedRow{t: h.t, row: h.pool.QueryRow(h.ctx, query, args...)}
}

type rowScanner interface{ Scan(dest ...any) }

type pgxRow interface{ Scan(dest ...any) error }

type checkedRow struct {
	t   *testing.T
	row pgxRow
}

func (r checkedRow) Scan(dest ...any) {
	r.t.Helper()
	if err := r.row.Scan(dest...); err != nil {
		r.t.Fatalf("query invariant: %v", err)
	}
}

func (h *harness) requireNoTransportError(outcome wagerOutcome) {
	h.t.Helper()
	if outcome.statusCode == 0 {
		h.t.Fatalf("wager transport failed: %s", outcome.errorText)
	}
}

func clientCredentialsToken(t *testing.T, ctx context.Context, client *http.Client, clientID, secret string) string {
	t.Helper()
	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {clientID},
		"client_secret": {secret},
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, requiredEnv(t, "KEYCLOAK_TOKEN_URL"), strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("build token request: %v", err)
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("request %s token: %v", clientID, err)
	}
	defer response.Body.Close()
	data := readBody(t, response)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("request %s token returned %d: %s", clientID, response.StatusCode, data)
	}
	var tokenResponse struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal([]byte(data), &tokenResponse); err != nil || tokenResponse.AccessToken == "" {
		t.Fatalf("decode %s token: %v", clientID, err)
	}
	return tokenResponse.AccessToken
}

func assertTokenIssuer(t *testing.T, token, expected string) {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatal("access token is not a JWT")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode token payload: %v", err)
	}
	var claims struct {
		Issuer string `json:"iss"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		t.Fatalf("decode token claims: %v", err)
	}
	if claims.Issuer != expected {
		t.Fatalf("expected issuer %q, got %q", expected, claims.Issuer)
	}
	t.Logf("OIDC issuer verified: %s", claims.Issuer)
}

func readBody(t *testing.T, response *http.Response) string {
	t.Helper()
	data, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return string(data)
}

func requiredEnv(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required", name)
	}
	return value
}

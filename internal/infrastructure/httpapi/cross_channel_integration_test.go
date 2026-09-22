package httpapi_test

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
	httpapi "github.com/Tharik/wagering-platform/internal/infrastructure/httpapi"
	sqsmessaging "github.com/Tharik/wagering-platform/internal/infrastructure/messaging/sqs"
	"github.com/Tharik/wagering-platform/internal/observability"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	crossChannelDatabaseURL = "postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable"

	crossChannelOIDCIssuer = "http://localhost:8081/realms/wagering"
	crossChannelTokenURL   = crossChannelOIDCIssuer + "/protocol/openid-connect/token"

	crossChannelQueueURL = "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/wager-transactions.fifo"
)

func TestSameWagerAcrossHTTPAndSQSIsProcessedExactlyOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, crossChannelDatabaseURL)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping postgres: %v", err)
	}

	cleanCrossChannelDatabase(t, ctx, pool)

	sqsClient := newCrossChannelSQSClient()

	_, err = sqsClient.PurgeQueue(
		ctx,
		&awssqs.PurgeQueueInput{
			QueueUrl: aws.String(crossChannelQueueURL),
		},
	)
	if err != nil {
		t.Fatalf("purge SQS queue: %v", err)
	}

	auth, err := httpapi.NewAuthMiddleware(ctx, crossChannelOIDCIssuer)
	if err != nil {
		t.Fatalf("create OIDC middleware: %v", err)
	}

	walletService := walletapp.NewService(pool)
	wagerService := wageringapp.NewService(pool)

	walletHandler := httpapi.NewWalletHandler(walletService)
	wagerHandler := httpapi.NewWagerHandler(wagerService)

	healthHandler := httpapi.NewHealthHandler(
		pool,
		sqsClient,
		crossChannelQueueURL,
	)

	metrics := observability.NewMetrics()
	metricsHandler := httpapi.NewMetricsHandler(metrics)

	logger := slog.New(
		slog.NewJSONHandler(io.Discard, nil),
	)

	server := httpapi.NewServer(
		walletHandler,
		wagerHandler,
		healthHandler,
		metricsHandler,
		auth,
		logger,
	)

	testServer := httptest.NewServer(server.Handler())
	defer testServer.Close()

	internalToken := getCrossChannelToken(
		t,
		ctx,
		"wagering-internal",
		"internal-secret",
	)

	providerToken := getCrossChannelToken(
		t,
		ctx,
		"provider-a",
		"provider-a-secret",
	)

	// Create the wallet through the real HTTP internal API.
	createWalletResponse := crossChannelRequest(
		t,
		ctx,
		http.MethodPost,
		testServer.URL+"/wallets",
		internalToken,
		map[string]any{
			"playerId":       "player-cross-channel",
			"initialBalance": "100.00",
			"currency":       "BRL",
		},
	)
	defer createWalletResponse.Body.Close()

	if createWalletResponse.StatusCode != http.StatusCreated {
		t.Fatalf(
			"create wallet: expected 201, got %d: %s",
			createWalletResponse.StatusCode,
			readCrossChannelBody(t, createWalletResponse),
		)
	}

	var walletResult struct {
		WalletID string `json:"walletId"`
	}

	if err := json.NewDecoder(createWalletResponse.Body).Decode(&walletResult); err != nil {
		t.Fatalf("decode wallet response: %v", err)
	}

	if walletResult.WalletID == "" {
		t.Fatal("expected walletId")
	}

	idempotencyKey := "cross-channel-idempotency-" + uuid.NewString()
	externalTransactionID := "cross-channel-external-" + uuid.NewString()

	// First delivery: HTTP.
	httpResponse := crossChannelRequest(
		t,
		ctx,
		http.MethodPost,
		testServer.URL+"/wagering/transactions",
		providerToken,
		map[string]any{
			"idempotencyKey":        idempotencyKey,
			"externalTransactionId": externalTransactionID,
			"playerId":              "player-cross-channel",
			"walletId":              walletResult.WalletID,
			"roundId":               "round-cross-channel",
			"gameId":                "game-cross-channel",
			"kind":                  "BET",
			"amount":                "30.00",
			"currency":              "BRL",
		},
	)
	defer httpResponse.Body.Close()

	if httpResponse.StatusCode != http.StatusCreated {
		t.Fatalf(
			"HTTP wager: expected 201, got %d: %s",
			httpResponse.StatusCode,
			readCrossChannelBody(t, httpResponse),
		)
	}

	var httpResult struct {
		TransactionID string `json:"transactionId"`
	}

	if err := json.NewDecoder(httpResponse.Body).Decode(&httpResult); err != nil {
		t.Fatalf("decode HTTP wager response: %v", err)
	}

	if httpResult.TransactionID == "" {
		t.Fatal("expected HTTP transactionId")
	}

	// Second delivery: exactly the same logical wager through SQS.
	command := sqsmessaging.CommandMessage{
		MessageID:  "cross-channel-message-" + uuid.NewString(),
		Type:       "WAGER_TRANSACTION",
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Data: sqsmessaging.WagerCommandData{
			IdempotencyKey:        idempotencyKey,
			ProviderID:            "provider-a",
			ExternalTransactionID: externalTransactionID,
			PlayerID:              "player-cross-channel",
			WalletID:              walletResult.WalletID,
			RoundID:               "round-cross-channel",
			GameID:                "game-cross-channel",
			Kind:                  "BET",
			Amount:                "30.00",
			Currency:              "BRL",
		},
	}

	payload, err := json.Marshal(command)
	if err != nil {
		t.Fatalf("marshal SQS command: %v", err)
	}

	_, err = sqsClient.SendMessage(
		ctx,
		&awssqs.SendMessageInput{
			QueueUrl:               aws.String(crossChannelQueueURL),
			MessageBody:            aws.String(string(payload)),
			MessageGroupId:         aws.String(walletResult.WalletID),
			MessageDeduplicationId: aws.String(command.MessageID),
		},
	)
	if err != nil {
		t.Fatalf("send SQS command: %v", err)
	}

	messageProcessor := wageringapp.NewMessageProcessor(
		pool,
		wagerService,
	)

	consumer := sqsmessaging.NewConsumer(
		sqsClient,
		messageProcessor,
		crossChannelQueueURL,
	)

	processed, err := consumer.ConsumeOnce(ctx)
	if err != nil {
		t.Fatalf("consume cross-channel replay: %v", err)
	}

	if processed != 1 {
		t.Fatalf(
			"expected consumer to handle 1 SQS delivery, got %d",
			processed,
		)
	}

	// The important part:
	// HTTP + SQS represented the SAME logical transaction.
	// Money must have moved exactly once.
	var (
		balance int64
		version int64
	)

	err = pool.QueryRow(
		ctx,
		`
		SELECT balance, version
		FROM wallets
		WHERE id = $1
		`,
		walletResult.WalletID,
	).Scan(&balance, &version)
	if err != nil {
		t.Fatalf("query final wallet: %v", err)
	}

	if balance != 7000 {
		t.Fatalf(
			"expected balance 7000 after cross-channel replay, got %d",
			balance,
		)
	}

	// Opening = version 1.
	// The BET must increment it exactly once.
	if version != 2 {
		t.Fatalf(
			"expected wallet version 2, got %d",
			version,
		)
	}

	var wagerCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM wager_transactions
		WHERE provider_id = 'provider-a'
		  AND external_transaction_id = $1
		`,
		externalTransactionID,
	).Scan(&wagerCount)
	if err != nil {
		t.Fatalf("count wager transactions: %v", err)
	}

	if wagerCount != 1 {
		t.Fatalf(
			"expected exactly 1 wager transaction, got %d",
			wagerCount,
		)
	}

	var transactionID string

	err = pool.QueryRow(
		ctx,
		`
		SELECT id::text
		FROM wager_transactions
		WHERE provider_id = 'provider-a'
		  AND external_transaction_id = $1
		`,
		externalTransactionID,
	).Scan(&transactionID)
	if err != nil {
		t.Fatalf("query persisted transaction: %v", err)
	}

	if transactionID != httpResult.TransactionID {
		t.Fatalf(
			"expected SQS replay to preserve original HTTP transaction %s, got %s",
			httpResult.TransactionID,
			transactionID,
		)
	}

	var debitCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM ledger_entries
		WHERE transaction_id = $1
		  AND direction = 'DEBIT'
		`,
		httpResult.TransactionID,
	).Scan(&debitCount)
	if err != nil {
		t.Fatalf("count ledger debits: %v", err)
	}

	if debitCount != 1 {
		t.Fatalf(
			"expected exactly 1 debit ledger entry, got %d",
			debitCount,
		)
	}

	// Even though the business transaction was an idempotent replay,
	// the SQS delivery itself must be durably recorded and completed.
	var inboxCompletedAt *time.Time

	err = pool.QueryRow(
		ctx,
		`
		SELECT completed_at
		FROM inbox_messages
		WHERE message_id = $1
		`,
		command.MessageID,
	).Scan(&inboxCompletedAt)
	if err != nil {
		t.Fatalf("query SQS inbox entry: %v", err)
	}

	if inboxCompletedAt == nil {
		t.Fatal("expected cross-channel SQS inbox entry to be completed")
	}

	// The message must also have been deleted after the successful commit.
	remaining, err := sqsClient.ReceiveMessage(
		ctx,
		&awssqs.ReceiveMessageInput{
			QueueUrl:            aws.String(crossChannelQueueURL),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     1,
		},
	)
	if err != nil {
		t.Fatalf("receive SQS after processing: %v", err)
	}

	if len(remaining.Messages) != 0 {
		t.Fatalf(
			"expected SQS queue to be empty, got %d messages",
			len(remaining.Messages),
		)
	}
}

func newCrossChannelSQSClient() *awssqs.Client {
	return awssqs.New(
		awssqs.Options{
			Region: "us-east-1",
			Credentials: aws.NewCredentialsCache(
				credentials.NewStaticCredentialsProvider(
					"test",
					"test",
					"",
				),
			),
			BaseEndpoint: aws.String("http://localhost:4566"),
		},
	)
}

func getCrossChannelToken(
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
		crossChannelTokenURL,
		strings.NewReader(form.Encode()),
	)
	if err != nil {
		t.Fatalf("create token request: %v", err)
	}

	request.Header.Set(
		"Content-Type",
		"application/x-www-form-urlencoded",
	)

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("request Keycloak token: %v", err)
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
		t.Fatalf("decode token response: %v", err)
	}

	if result.AccessToken == "" {
		t.Fatal("expected access token")
	}

	return result.AccessToken
}

func crossChannelRequest(
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

func readCrossChannelBody(
	t *testing.T,
	response *http.Response,
) string {
	t.Helper()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}

	return string(body)
}

func cleanCrossChannelDatabase(
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

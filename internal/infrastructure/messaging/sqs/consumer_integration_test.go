package sqs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Tharik/wagering-platform/internal/application/wagering"
	"github.com/Tharik/wagering-platform/internal/application/wallet"
	"github.com/Tharik/wagering-platform/internal/domain"
	"github.com/Tharik/wagering-platform/internal/observability"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	awstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

const testCommandsQueueURL = "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/wager-commands.fifo"
const testCommandsDLQURL = "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/wager-commands-dlq.fifo"

func TestConsumerProcessesBetFromSQSAndDeletesMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Second,
	)
	defer cancel()

	pool, err := pgxpool.New(
		ctx,
		"postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable",
	)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	cleanConsumerDatabase(t, ctx, pool)

	client := newTestSQSClient()
	purgeCommandsQueue(t, ctx, client)

	walletService := wallet.NewService(pool)

	createdWallet, err := walletService.Create(
		ctx,
		wallet.CreateWalletCommand{
			PlayerID:       "player-sqs-consumer",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	service := wagering.NewService(pool)
	processor := wagering.NewMessageProcessor(pool, service)

	consumer := NewConsumer(
		client,
		processor,
		testCommandsQueueURL,
	)

	command := CommandMessage{
		MessageID:             "sqs-message-" + uuid.NewString(),
		IdempotencyKey:        "sqs-idempotency-" + uuid.NewString(),
		ProviderID:            "provider-a",
		ExternalTransactionID: "sqs-bet-" + uuid.NewString(),
		PlayerID:              "player-sqs-consumer",
		WalletID:              createdWallet.WalletID,
		RoundID:               "round-sqs-1",
		GameID:                "game-1",
		Kind:                  "BET",
		Amount:                "30.00",
		Currency:              "BRL",
	}

	payload, err := json.Marshal(command)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}

	_, err = client.SendMessage(
		ctx,
		&awssqs.SendMessageInput{
			QueueUrl:               aws.String(testCommandsQueueURL),
			MessageBody:            aws.String(string(payload)),
			MessageGroupId:         aws.String(createdWallet.WalletID),
			MessageDeduplicationId: aws.String(command.MessageID),
		},
	)
	if err != nil {
		t.Fatalf("send command to SQS: %v", err)
	}

	processed, err := consumer.ConsumeOnce(ctx)
	if err != nil {
		t.Fatalf("consume message: %v", err)
	}

	if processed != 1 {
		t.Fatalf(
			"expected 1 processed message, got %d",
			processed,
		)
	}

	var balance int64
	var version int64

	err = pool.QueryRow(
		ctx,
		`
		SELECT balance, version
		FROM wallets
		WHERE id = $1
		`,
		createdWallet.WalletID,
	).Scan(&balance, &version)
	if err != nil {
		t.Fatalf("query wallet: %v", err)
	}

	if balance != 7000 {
		t.Fatalf(
			"expected balance 7000, got %d",
			balance,
		)
	}

	if version != 2 {
		t.Fatalf(
			"expected wallet version 2, got %d",
			version,
		)
	}

	var completedAt *time.Time

	err = pool.QueryRow(
		ctx,
		`
		SELECT completed_at
		FROM inbox_messages
		WHERE consumer_name = $1
		  AND message_id = $2
		`,
		defaultConsumerName,
		command.MessageID,
	).Scan(&completedAt)
	if err != nil {
		t.Fatalf("query inbox: %v", err)
	}

	if completedAt == nil {
		t.Fatal("expected inbox message to be completed")
	}

	var betCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM wager_transactions
		WHERE wallet_id = $1
		  AND kind = 'BET'
		  AND state = 'PROCESSED'
		`,
		createdWallet.WalletID,
	).Scan(&betCount)
	if err != nil {
		t.Fatalf("count BET transactions: %v", err)
	}

	if betCount != 1 {
		t.Fatalf(
			"expected exactly 1 processed BET, got %d",
			betCount,
		)
	}

	var debitCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM ledger_entries
		WHERE wallet_id = $1
		  AND direction = 'DEBIT'
		`,
		createdWallet.WalletID,
	).Scan(&debitCount)
	if err != nil {
		t.Fatalf("count debit entries: %v", err)
	}

	if debitCount != 1 {
		t.Fatalf(
			"expected exactly 1 debit, got %d",
			debitCount,
		)
	}

	// ConsumeOnce deletes the SQS message only after the database
	// transaction has committed successfully.
	result, err := client.ReceiveMessage(
		ctx,
		&awssqs.ReceiveMessageInput{
			QueueUrl:            aws.String(testCommandsQueueURL),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     1,
		},
	)
	if err != nil {
		t.Fatalf("receive after processing: %v", err)
	}

	if len(result.Messages) != 0 {
		t.Fatalf(
			"expected command queue to be empty, got %d messages",
			len(result.Messages),
		)
	}
}

func newTestSQSClient() *awssqs.Client {
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
			BaseEndpoint: aws.String(
				"http://localhost:4566",
			),
		},
	)
}

func TestConsumerDoesNotProcessBetAgainWhenDeleteFailsAfterCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Second,
	)
	defer cancel()

	pool, err := pgxpool.New(
		ctx,
		"postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable",
	)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	cleanConsumerDatabase(t, ctx, pool)

	realClient := newTestSQSClient()
	purgeCommandsQueue(t, ctx, realClient)

	walletService := wallet.NewService(pool)

	createdWallet, err := walletService.Create(
		ctx,
		wallet.CreateWalletCommand{
			PlayerID:       "player-sqs-redelivery",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	service := wagering.NewService(pool)
	processor := wagering.NewMessageProcessor(pool, service)

	client := &failFirstDeleteClient{
		Client: realClient,
	}

	consumer := NewConsumer(
		client,
		processor,
		testCommandsQueueURL,
	)

	command := CommandMessage{
		MessageID:             "sqs-redelivery-message-" + uuid.NewString(),
		IdempotencyKey:        "sqs-redelivery-idempotency-" + uuid.NewString(),
		ProviderID:            "provider-a",
		ExternalTransactionID: "sqs-redelivery-bet-" + uuid.NewString(),
		PlayerID:              "player-sqs-redelivery",
		WalletID:              createdWallet.WalletID,
		RoundID:               "round-sqs-redelivery",
		GameID:                "game-1",
		Kind:                  "BET",
		Amount:                "30.00",
		Currency:              "BRL",
	}

	payload, err := json.Marshal(command)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}

	_, err = realClient.SendMessage(
		ctx,
		&awssqs.SendMessageInput{
			QueueUrl:               aws.String(testCommandsQueueURL),
			MessageBody:            aws.String(string(payload)),
			MessageGroupId:         aws.String(createdWallet.WalletID),
			MessageDeduplicationId: aws.String(command.MessageID),
		},
	)
	if err != nil {
		t.Fatalf("send command: %v", err)
	}

	// Financial processing succeeds and commits, but deletion fails.
	_, err = consumer.ConsumeOnce(ctx)
	if err == nil {
		t.Fatal("expected first consume to fail deleting SQS message")
	}

	assertConsumerWalletState(
		t,
		ctx,
		pool,
		createdWallet.WalletID,
		7000,
		1,
		1,
	)

	// Make the same message immediately visible again instead of waiting
	// for the queue's visibility timeout.
	message := client.lastReceivedMessage
	if message == nil || message.ReceiptHandle == nil {
		t.Fatal("expected first delivery receipt handle")
	}

	_, err = realClient.ChangeMessageVisibility(
		ctx,
		&awssqs.ChangeMessageVisibilityInput{
			QueueUrl:          aws.String(testCommandsQueueURL),
			ReceiptHandle:     message.ReceiptHandle,
			VisibilityTimeout: 0,
		},
	)
	if err != nil {
		t.Fatalf("make message visible again: %v", err)
	}

	// Second delivery must hit the completed Inbox entry.
	processed, err := consumer.ConsumeOnce(ctx)
	if err != nil {
		t.Fatalf("consume redelivery: %v", err)
	}

	if processed != 1 {
		t.Fatalf(
			"expected 1 redelivered message, got %d",
			processed,
		)
	}

	// Most important assertion:
	// the same BET was NOT applied a second time.
	assertConsumerWalletState(
		t,
		ctx,
		pool,
		createdWallet.WalletID,
		7000,
		1,
		1,
	)

	result, err := realClient.ReceiveMessage(
		ctx,
		&awssqs.ReceiveMessageInput{
			QueueUrl:            aws.String(testCommandsQueueURL),
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     1,
		},
	)
	if err != nil {
		t.Fatalf("receive after successful redelivery: %v", err)
	}

	if len(result.Messages) != 0 {
		t.Fatalf(
			"expected queue to be empty after successful redelivery, got %d messages",
			len(result.Messages),
		)
	}
}

type failFirstDeleteClient struct {
	*awssqs.Client

	deleteAttempts      int
	lastReceivedMessage *awstypes.Message
}

type recordingReceiveClient struct {
	*awssqs.Client

	lastReceivedMessage *awstypes.Message
}

func (c *recordingReceiveClient) ReceiveMessage(
	ctx context.Context,
	input *awssqs.ReceiveMessageInput,
	optFns ...func(*awssqs.Options),
) (*awssqs.ReceiveMessageOutput, error) {
	output, err := c.Client.ReceiveMessage(
		ctx,
		input,
		optFns...,
	)
	if err != nil {
		return nil, err
	}

	if len(output.Messages) > 0 {
		message := output.Messages[0]
		c.lastReceivedMessage = &message
	}

	return output, nil
}

func (c *failFirstDeleteClient) ReceiveMessage(
	ctx context.Context,
	input *awssqs.ReceiveMessageInput,
	optFns ...func(*awssqs.Options),
) (*awssqs.ReceiveMessageOutput, error) {
	output, err := c.Client.ReceiveMessage(
		ctx,
		input,
		optFns...,
	)
	if err != nil {
		return nil, err
	}

	if len(output.Messages) > 0 {
		message := output.Messages[0]
		c.lastReceivedMessage = &message
	}

	return output, nil
}

func (c *failFirstDeleteClient) DeleteMessage(
	ctx context.Context,
	input *awssqs.DeleteMessageInput,
	optFns ...func(*awssqs.Options),
) (*awssqs.DeleteMessageOutput, error) {
	c.deleteAttempts++

	if c.deleteAttempts == 1 {
		return nil, errors.New(
			"simulated delete failure after database commit",
		)
	}

	return c.Client.DeleteMessage(
		ctx,
		input,
		optFns...,
	)
}

func TestConsumerRetriesInvalidMessageAndMovesItToDLQ(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		30*time.Second,
	)
	defer cancel()

	pool, err := pgxpool.New(
		ctx,
		"postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable",
	)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	cleanConsumerDatabase(t, ctx, pool)

	realClient := newTestSQSClient()

	purgeCommandsQueue(t, ctx, realClient)
	purgeCommandsDLQ(t, ctx, realClient)

	walletService := wallet.NewService(pool)

	createdWallet, err := walletService.Create(
		ctx,
		wallet.CreateWalletCommand{
			PlayerID:       "player-sqs-dlq",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	service := wagering.NewService(pool)
	processor := wagering.NewMessageProcessor(pool, service)

	metrics := observability.NewMetrics()

	client := &recordingReceiveClient{
		Client: realClient,
	}

	consumer := NewConsumerWithMetrics(
		client,
		processor,
		testCommandsQueueURL,
		metrics,
	)

	command := CommandMessage{
		MessageID:             "sqs-invalid-" + uuid.NewString(),
		IdempotencyKey:        "sqs-invalid-idempotency-" + uuid.NewString(),
		ProviderID:            "provider-a",
		ExternalTransactionID: "sqs-invalid-tx-" + uuid.NewString(),
		PlayerID:              "player-sqs-dlq",
		WalletID:              createdWallet.WalletID,
		RoundID:               "round-sqs-dlq",
		GameID:                "game-1",
		Kind:                  "BET",
		Amount:                "10.00",

		// Currency intentionally omitted.
		// decodeCommand must fail and the message must not be deleted.
	}

	payload, err := json.Marshal(command)
	if err != nil {
		t.Fatalf("marshal invalid command: %v", err)
	}

	_, err = realClient.SendMessage(
		ctx,
		&awssqs.SendMessageInput{
			QueueUrl:               aws.String(testCommandsQueueURL),
			MessageBody:            aws.String(string(payload)),
			MessageGroupId:         aws.String(createdWallet.WalletID),
			MessageDeduplicationId: aws.String(command.MessageID),
		},
	)
	if err != nil {
		t.Fatalf("send invalid command: %v", err)
	}

	// The source queue has maxReceiveCount=3.
	//
	// Each attempt must fail. Since a failed message is not deleted,
	// we explicitly reset its visibility to zero so the test does not
	// wait for the 30-second visibility timeout.
	for attempt := 1; attempt <= 3; attempt++ {
		client.lastReceivedMessage = nil

		processed, err := consumer.ConsumeOnce(ctx)
		if err == nil {
			t.Fatalf(
				"expected attempt %d to fail",
				attempt,
			)
		}

		if processed != 0 {
			t.Fatalf(
				"expected 0 processed messages on attempt %d, got %d",
				attempt,
				processed,
			)
		}

		if client.lastReceivedMessage == nil ||
			client.lastReceivedMessage.ReceiptHandle == nil {
			t.Fatalf(
				"expected receipt handle on attempt %d",
				attempt,
			)
		}

		if attempt < 3 {
			_, err = realClient.ChangeMessageVisibility(
				ctx,
				&awssqs.ChangeMessageVisibilityInput{
					QueueUrl:          aws.String(testCommandsQueueURL),
					ReceiptHandle:     client.lastReceivedMessage.ReceiptHandle,
					VisibilityTimeout: 0,
				},
			)
			if err != nil {
				t.Fatalf(
					"make message visible after attempt %d: %v",
					attempt,
					err,
				)
			}
		}
	}

	// After the third failed delivery, make it visible once more.
	// The next receive causes LocalStack/SQS to apply the redrive policy.
	_, err = realClient.ChangeMessageVisibility(
		ctx,
		&awssqs.ChangeMessageVisibilityInput{
			QueueUrl:          aws.String(testCommandsQueueURL),
			ReceiptHandle:     client.lastReceivedMessage.ReceiptHandle,
			VisibilityTimeout: 0,
		},
	)
	if err != nil {
		t.Fatalf(
			"make message visible for redrive: %v",
			err,
		)
	}

	deadline := time.Now().Add(5 * time.Second)

	var dlqMessage *awstypes.Message

	for time.Now().Before(deadline) {
		// Trigger source-queue receive so the redrive policy is evaluated.
		_, _ = realClient.ReceiveMessage(
			ctx,
			&awssqs.ReceiveMessageInput{
				QueueUrl:            aws.String(testCommandsQueueURL),
				MaxNumberOfMessages: 1,
				WaitTimeSeconds:     0,
			},
		)

		output, receiveErr := realClient.ReceiveMessage(
			ctx,
			&awssqs.ReceiveMessageInput{
				QueueUrl:            aws.String(testCommandsDLQURL),
				MaxNumberOfMessages: 1,
				WaitTimeSeconds:     1,
				MessageSystemAttributeNames: []awstypes.MessageSystemAttributeName{
					awstypes.MessageSystemAttributeNameApproximateReceiveCount,
				},
			},
		)
		if receiveErr != nil {
			t.Fatalf(
				"receive DLQ message: %v",
				receiveErr,
			)
		}

		if len(output.Messages) == 1 {
			message := output.Messages[0]
			dlqMessage = &message
			break
		}
	}

	if dlqMessage == nil {
		t.Fatal(
			"expected invalid message to be moved to DLQ",
		)
	}

	if dlqMessage.Body == nil {
		t.Fatal("expected DLQ message body")
	}

	if *dlqMessage.Body != string(payload) {
		t.Fatal(
			"expected DLQ payload to match original message",
		)
	}

	// No financial transaction must have been created from the poison message.
	var betCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM wager_transactions
		WHERE wallet_id = $1
		AND kind = 'BET'
		`,
		createdWallet.WalletID,
	).Scan(&betCount)

	if err != nil {
		t.Fatalf(
			"count BET transactions: %v",
			err,
		)
	}

	if betCount != 0 {
		t.Fatalf(
			"expected poison message to create no BET transactions, got %d",
			betCount,
		)
	}

	var balance int64
	var version int64

	err = pool.QueryRow(
		ctx,
		`
		SELECT balance, version
		FROM wallets
		WHERE id = $1
		`,
		createdWallet.WalletID,
	).Scan(&balance, &version)
	if err != nil {
		t.Fatalf(
			"query wallet after poison message: %v",
			err,
		)
	}

	if balance != 10000 {
		t.Fatalf(
			"expected balance to remain 10000, got %d",
			balance,
		)
	}

	if version != 1 {
		t.Fatalf(
			"expected wallet version to remain 1, got %d",
			version,
		)
	}

	// Verify that retry deliveries were actually observed by our metrics.
	var metricsOutput bytes.Buffer

	if err := metrics.WritePrometheus(&metricsOutput); err != nil {
		t.Fatalf(
			"write metrics: %v",
			err,
		)
	}

	if !strings.Contains(
		metricsOutput.String(),
		"wagering_sqs_retries_total 2",
	) {
		t.Fatalf(
			"expected exactly 2 SQS retries, metrics:\n%s",
			metricsOutput.String(),
		)
	}
}

func assertConsumerWalletState(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	walletID string,
	expectedBalance int64,
	expectedBetCount int,
	expectedDebitCount int,
) {
	t.Helper()

	var balance int64

	err := pool.QueryRow(
		ctx,
		"SELECT balance FROM wallets WHERE id = $1",
		walletID,
	).Scan(&balance)
	if err != nil {
		t.Fatalf("query wallet balance: %v", err)
	}

	if balance != expectedBalance {
		t.Fatalf(
			"expected balance %d, got %d",
			expectedBalance,
			balance,
		)
	}

	var betCount int

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

	if betCount != expectedBetCount {
		t.Fatalf(
			"expected %d BET transactions, got %d",
			expectedBetCount,
			betCount,
		)
	}

	var debitCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM ledger_entries
		WHERE wallet_id = $1
		  AND direction = 'DEBIT'
		`,
		walletID,
	).Scan(&debitCount)
	if err != nil {
		t.Fatalf("count debit entries: %v", err)
	}

	if debitCount != expectedDebitCount {
		t.Fatalf(
			"expected %d debit entries, got %d",
			expectedDebitCount,
			debitCount,
		)
	}
}

func purgeCommandsDLQ(
	t *testing.T,
	ctx context.Context,
	client *awssqs.Client,
) {
	t.Helper()

	_, err := client.PurgeQueue(
		ctx,
		&awssqs.PurgeQueueInput{
			QueueUrl: aws.String(testCommandsDLQURL),
		},
	)
	if err != nil {
		t.Fatalf(
			"purge commands DLQ: %v",
			err,
		)
	}
}

func purgeCommandsQueue(
	t *testing.T,
	ctx context.Context,
	client *awssqs.Client,
) {
	t.Helper()

	_, err := client.PurgeQueue(
		ctx,
		&awssqs.PurgeQueueInput{
			QueueUrl: aws.String(testCommandsQueueURL),
		},
	)
	if err != nil {
		t.Fatalf("purge commands queue: %v", err)
	}
}

func cleanConsumerDatabase(
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

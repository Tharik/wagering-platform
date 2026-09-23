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

func TestDecodeCommandValidatesOfficialContract(t *testing.T) {
	valid := CommandMessage{
		MessageID:  "msg-123",
		Type:       wagerTransactionRequestedType,
		OccurredAt: "2026-09-08T12:00:00.000Z",
		Data: WagerCommandData{
			ProviderID:            "provider-a",
			ExternalTransactionID: "transaction-123",
			IdempotencyKey:        "provider-a:transaction-123",
			PlayerID:              "player-id",
			WalletID:              "wallet-id",
			RoundID:               "round-987",
			GameID:                "fortune-chimp",
			Kind:                  "BET",
			Money: MoneyDTO{
				Amount:   "25.00",
				Currency: "BRL",
			},
		},
	}

	payload, err := json.Marshal(valid)
	if err != nil {
		t.Fatalf("marshal valid command: %v", err)
	}

	decoded, err := decodeCommand(payload)
	if err != nil {
		t.Fatalf("decode valid official command: %v", err)
	}
	if decoded.money.Amount() != 2500 || decoded.money.Currency() != domain.BRL {
		t.Fatalf("unexpected decoded money: %s %s", decoded.money.String(), decoded.money.Currency())
	}

	tests := []struct {
		name          string
		mutate        func(*CommandMessage)
		expectedError string
	}{
		{
			name: "missing type",
			mutate: func(command *CommandMessage) {
				command.Type = ""
			},
			expectedError: "type is required",
		},
		{
			name: "unsupported type",
			mutate: func(command *CommandMessage) {
				command.Type = "WAGER_TRANSACTION"
			},
			expectedError: "unsupported message type",
		},
		{
			name: "missing nested money",
			mutate: func(command *CommandMessage) {
				command.Data.Money = MoneyDTO{}
			},
			expectedError: "money.amount is required",
		},
		{
			name: "invalid money",
			mutate: func(command *CommandMessage) {
				command.Data.Money.Amount = "not-money"
			},
			expectedError: "parse amount",
		},
		{
			name: "missing providerId",
			mutate: func(command *CommandMessage) {
				command.Data.ProviderID = ""
			},
			expectedError: "providerId is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			command := valid
			tt.mutate(&command)

			payload, err := json.Marshal(command)
			if err != nil {
				t.Fatalf("marshal command: %v", err)
			}

			_, err = decodeCommand(payload)
			if err == nil || !strings.Contains(err.Error(), tt.expectedError) {
				t.Fatalf("expected error containing %q, got %v", tt.expectedError, err)
			}
		})
	}
}

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
	commandsQueueURL := createIsolatedCommandsQueue(t, ctx, client)
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
		commandsQueueURL,
	)
	command := CommandMessage{
		MessageID:  "sqs-message-" + uuid.NewString(),
		Type:       wagerTransactionRequestedType,
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Data: WagerCommandData{
			IdempotencyKey:        "sqs-idempotency-" + uuid.NewString(),
			ProviderID:            "provider-a",
			ExternalTransactionID: "sqs-bet-" + uuid.NewString(),
			PlayerID:              "player-sqs-consumer",
			WalletID:              createdWallet.WalletID,
			RoundID:               "round-sqs-1",
			GameID:                "game-1",
			Kind:                  "BET",
			Money: MoneyDTO{
				Amount:   "30.00",
				Currency: "BRL",
			},
		},
	}
	payload, err := json.Marshal(command)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	_, err = client.SendMessage(
		ctx,
		&awssqs.SendMessageInput{
			QueueUrl:               aws.String(commandsQueueURL),
			MessageBody:            aws.String(string(payload)),
			MessageGroupId:         aws.String(createdWallet.WalletID),
			MessageDeduplicationId: aws.String(command.MessageID),
		},
	)
	if err != nil {
		t.Fatalf("send command to SQS: %v", err)
	}
	for {
		processed, consumeErr := consumer.ConsumeOnce(ctx)
		if consumeErr != nil {
			t.Fatalf("consume message: %v", consumeErr)
		}
		if processed == 1 {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("timed out waiting for SQS delivery: %v", ctx.Err())
		}
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
			QueueUrl:            aws.String(commandsQueueURL),
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

func TestConsumerCommitsAndDeletesDurablyRejectedReference(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, "postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable")
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()
	cleanConsumerDatabase(t, ctx, pool)

	createdWallet, err := wallet.NewService(pool).Create(ctx, wallet.CreateWalletCommand{
		PlayerID:       "player-sqs-invalid-reference",
		InitialBalance: domain.NewMoney(10000, domain.BRL),
	})
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	service := wagering.NewService(pool)
	_, err = service.Process(ctx, wagering.ProcessCommand{
		IdempotencyKey: "sqs-reference-win",
		Request: domain.WagerRequest{
			ProviderID: "provider-a", ExternalTransactionID: "sqs-reference-win", PlayerID: "player-sqs-invalid-reference",
			WalletID: createdWallet.WalletID, RoundID: "round-1", GameID: "game-1", Kind: domain.WagerKindWin,
			Amount: domain.NewMoney(3000, domain.BRL),
		},
	})
	if err != nil {
		t.Fatalf("process reference WIN: %v", err)
	}

	client := newTestSQSClient()
	queueURL := createIsolatedCommandsQueue(t, ctx, client)
	consumer := NewConsumer(client, wagering.NewMessageProcessor(pool, service), queueURL)
	command := CommandMessage{
		MessageID: "sqs-invalid-reference-" + uuid.NewString(), Type: wagerTransactionRequestedType,
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Data: WagerCommandData{
			ProviderID: "provider-a", ExternalTransactionID: "sqs-invalid-refund", IdempotencyKey: "sqs-invalid-refund",
			PlayerID: "player-sqs-invalid-reference", WalletID: createdWallet.WalletID, RoundID: "round-1", GameID: "game-1",
			Kind: "REFUND", Money: MoneyDTO{Amount: "30.00", Currency: "BRL"},
			ReferenceExternalTransactionID: "sqs-reference-win",
		},
	}
	payload, err := json.Marshal(command)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	_, err = client.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl: aws.String(queueURL), MessageBody: aws.String(string(payload)),
		MessageGroupId: aws.String(createdWallet.WalletID), MessageDeduplicationId: aws.String(command.MessageID),
	})
	if err != nil {
		t.Fatalf("send command: %v", err)
	}

	for {
		processed, consumeErr := consumer.ConsumeOnce(ctx)
		if consumeErr != nil {
			t.Fatalf("consume rejected command: %v", consumeErr)
		}
		if processed == 1 {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("wait for rejected command: %v", ctx.Err())
		}
	}

	var (
		state       string
		failureCode string
		completedAt *time.Time
		balance     int64
		version     int64
	)
	if err := pool.QueryRow(ctx, `SELECT state, failure_code FROM wager_transactions WHERE provider_id = $1 AND external_transaction_id = $2`, "provider-a", "sqs-invalid-refund").Scan(&state, &failureCode); err != nil {
		t.Fatalf("query rejected wager: %v", err)
	}
	if state != string(domain.WagerStateRejected) || failureCode != "INVALID_REFERENCE_KIND" {
		t.Fatalf("expected durable invalid-kind rejection, got state=%s code=%s", state, failureCode)
	}
	if err := pool.QueryRow(ctx, `SELECT completed_at FROM inbox_messages WHERE consumer_name = $1 AND message_id = $2`, defaultConsumerName, command.MessageID).Scan(&completedAt); err != nil {
		t.Fatalf("query inbox: %v", err)
	}
	if completedAt == nil {
		t.Fatal("expected rejected message inbox entry to be complete")
	}
	if err := pool.QueryRow(ctx, `SELECT balance, version FROM wallets WHERE id = $1`, createdWallet.WalletID).Scan(&balance, &version); err != nil {
		t.Fatalf("query wallet: %v", err)
	}
	if balance != 13000 || version != 2 {
		t.Fatalf("expected rejected refund not to move wallet, got %d/version %d", balance, version)
	}

	output, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 1, WaitTimeSeconds: 1})
	if err != nil {
		t.Fatalf("receive after rejection: %v", err)
	}
	if len(output.Messages) != 0 {
		t.Fatalf("expected rejected business message to be deleted, got %d messages", len(output.Messages))
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
	commandsQueueURL := createIsolatedCommandsQueue(t, ctx, realClient)
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
		commandsQueueURL,
	)
	command := CommandMessage{
		MessageID:  "sqs-redelivery-message-" + uuid.NewString(),
		Type:       wagerTransactionRequestedType,
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Data: WagerCommandData{
			IdempotencyKey:        "sqs-redelivery-idempotency-" + uuid.NewString(),
			ProviderID:            "provider-a",
			ExternalTransactionID: "sqs-redelivery-bet-" + uuid.NewString(),
			PlayerID:              "player-sqs-redelivery",
			WalletID:              createdWallet.WalletID,
			RoundID:               "round-sqs-redelivery",
			GameID:                "game-1",
			Kind:                  "BET",
			Money: MoneyDTO{
				Amount:   "30.00",
				Currency: "BRL",
			},
		},
	}
	payload, err := json.Marshal(command)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	_, err = realClient.SendMessage(
		ctx,
		&awssqs.SendMessageInput{
			QueueUrl:               aws.String(commandsQueueURL),
			MessageBody:            aws.String(string(payload)),
			MessageGroupId:         aws.String(createdWallet.WalletID),
			MessageDeduplicationId: aws.String(command.MessageID),
		},
	)
	if err != nil {
		t.Fatalf("send command: %v", err)
	}
	// Financial processing succeeds and commits, but deletion fails.
	//
	// SQS/LocalStack delivery is asynchronous. A receive is allowed to return
	// no messages even immediately after SendMessage, so keep polling until
	// the message is actually delivered or the test context expires.
	for {
		_, err = consumer.ConsumeOnce(ctx)
		if err != nil {
			break
		}
		if client.lastReceivedMessage != nil {
			t.Fatal("expected first delivered message to fail during deletion")
		}
		if ctx.Err() != nil {
			t.Fatalf(
				"timed out waiting for first SQS delivery: %v",
				ctx.Err(),
			)
		}
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
			QueueUrl:          aws.String(commandsQueueURL),
			ReceiptHandle:     message.ReceiptHandle,
			VisibilityTimeout: 0,
		},
	)
	if err != nil {
		t.Fatalf("make message visible again: %v", err)
	}
	// Second delivery must hit the completed Inbox entry.
	for {
		processed, consumeErr := consumer.ConsumeOnce(ctx)
		if consumeErr != nil {
			t.Fatalf("consume redelivery: %v", consumeErr)
		}
		if processed == 1 {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("timed out waiting for SQS redelivery: %v", ctx.Err())
		}
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
			QueueUrl:            aws.String(commandsQueueURL),
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
	queues := createIsolatedCommandsQueueWithDLQ(t, ctx, realClient)
	commandsQueueURL := queues.commandsURL
	commandsDLQURL := queues.dlqURL
	queueAttributes, err := realClient.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl: aws.String(commandsQueueURL),
		AttributeNames: []awstypes.QueueAttributeName{
			awstypes.QueueAttributeNameVisibilityTimeout,
			awstypes.QueueAttributeNameReceiveMessageWaitTimeSeconds,
			awstypes.QueueAttributeNameRedrivePolicy,
		},
	})
	if err != nil {
		t.Fatalf("get source queue attributes: %v", err)
	}
	if queueAttributes.Attributes[string(awstypes.QueueAttributeNameVisibilityTimeout)] != "60" {
		t.Fatalf("expected 60-second visibility timeout, got %q", queueAttributes.Attributes[string(awstypes.QueueAttributeNameVisibilityTimeout)])
	}
	if queueAttributes.Attributes[string(awstypes.QueueAttributeNameReceiveMessageWaitTimeSeconds)] != "10" {
		t.Fatalf("expected 10-second receive wait, got %q", queueAttributes.Attributes[string(awstypes.QueueAttributeNameReceiveMessageWaitTimeSeconds)])
	}
	var redrivePolicy map[string]string
	if err := json.Unmarshal([]byte(queueAttributes.Attributes[string(awstypes.QueueAttributeNameRedrivePolicy)]), &redrivePolicy); err != nil {
		t.Fatalf("decode redrive policy: %v", err)
	}
	if redrivePolicy["maxReceiveCount"] != "3" {
		t.Fatalf("expected maxReceiveCount 3, got %q", redrivePolicy["maxReceiveCount"])
	}
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
		commandsQueueURL,
		metrics,
	)
	command := CommandMessage{
		MessageID:  "sqs-invalid-" + uuid.NewString(),
		Type:       wagerTransactionRequestedType,
		OccurredAt: time.Now().UTC().Format(time.RFC3339),
		Data: WagerCommandData{
			IdempotencyKey:        "sqs-invalid-idempotency-" + uuid.NewString(),
			ProviderID:            "provider-a",
			ExternalTransactionID: "sqs-invalid-tx-" + uuid.NewString(),
			PlayerID:              "player-sqs-dlq",
			WalletID:              createdWallet.WalletID,
			RoundID:               "round-sqs-dlq",
			GameID:                "game-1",
			Kind:                  "BET",
			// Money intentionally omitted. decodeCommand must fail and the
			// message must remain available for retry and eventual redrive.
		},
	}
	payload, err := json.Marshal(command)
	if err != nil {
		t.Fatalf("marshal invalid command: %v", err)
	}
	_, err = realClient.SendMessage(
		ctx,
		&awssqs.SendMessageInput{
			QueueUrl:               aws.String(commandsQueueURL),
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
	// Each attempt must fail. The consumer schedules the production retry
	// delay; the test resets visibility to zero only to avoid waiting for it.
	for attempt := 1; attempt <= 3; attempt++ {
		client.lastReceivedMessage = nil
		for {
			processed, consumeErr := consumer.ConsumeOnce(ctx)
			if consumeErr != nil {
				if processed != 0 {
					t.Fatalf("expected 0 processed messages on attempt %d, got %d", attempt, processed)
				}
				if client.lastReceivedMessage == nil || client.lastReceivedMessage.ReceiptHandle == nil {
					t.Fatalf("expected receipt handle on attempt %d", attempt)
				}
				break
			}
			if ctx.Err() != nil {
				t.Fatalf("timed out waiting for poison-message attempt %d: %v", attempt, ctx.Err())
			}
		}
		if attempt < 3 {
			_, err = realClient.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{
				QueueUrl: aws.String(commandsQueueURL), ReceiptHandle: client.lastReceivedMessage.ReceiptHandle, VisibilityTimeout: 0,
			})
			if err != nil {
				t.Fatalf("make message visible after attempt %d: %v", attempt, err)
			}
		}
	}
	// After the third failed delivery, make it visible once more.
	// The next receive causes LocalStack/SQS to apply the redrive policy.
	_, err = realClient.ChangeMessageVisibility(
		ctx,
		&awssqs.ChangeMessageVisibilityInput{
			QueueUrl:          aws.String(commandsQueueURL),
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
				QueueUrl:            aws.String(commandsQueueURL),
				MaxNumberOfMessages: 1,
				WaitTimeSeconds:     0,
			},
		)
		output, receiveErr := realClient.ReceiveMessage(
			ctx,
			&awssqs.ReceiveMessageInput{
				QueueUrl:            aws.String(commandsDLQURL),
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

type isolatedTestQueues struct {
	commandsURL string
	dlqURL      string
}

func createIsolatedCommandsQueue(t *testing.T, ctx context.Context, client *awssqs.Client) string {
	t.Helper()
	result, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName:  aws.String("consumer-test-" + uuid.NewString() + ".fifo"),
		Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "false", "VisibilityTimeout": "60", "ReceiveMessageWaitTimeSeconds": "10"},
	})
	if err != nil {
		t.Fatalf("create isolated commands queue: %v", err)
	}
	if result.QueueUrl == nil || *result.QueueUrl == "" {
		t.Fatal("create isolated commands queue returned empty URL")
	}
	queueURL := *result.QueueUrl
	t.Cleanup(func() { deleteTestQueue(t, client, queueURL) })
	return queueURL
}
func createIsolatedCommandsQueueWithDLQ(t *testing.T, ctx context.Context, client *awssqs.Client) isolatedTestQueues {
	t.Helper()
	dlqResult, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName:  aws.String("consumer-dlq-" + uuid.NewString() + ".fifo"),
		Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "false", "VisibilityTimeout": "60"},
	})
	if err != nil {
		t.Fatalf("create isolated DLQ: %v", err)
	}
	if dlqResult.QueueUrl == nil || *dlqResult.QueueUrl == "" {
		t.Fatal("create isolated DLQ returned empty URL")
	}
	dlqURL := *dlqResult.QueueUrl
	attributes, err := client.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl: aws.String(dlqURL), AttributeNames: []awstypes.QueueAttributeName{awstypes.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatalf("get isolated DLQ ARN: %v", err)
	}
	dlqARN := attributes.Attributes[string(awstypes.QueueAttributeNameQueueArn)]
	if dlqARN == "" {
		t.Fatal("isolated DLQ returned empty ARN")
	}
	redrivePolicy, err := json.Marshal(map[string]string{"deadLetterTargetArn": dlqARN, "maxReceiveCount": "3"})
	if err != nil {
		t.Fatalf("marshal redrive policy: %v", err)
	}
	commandsResult, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{
		QueueName:  aws.String("consumer-source-" + uuid.NewString() + ".fifo"),
		Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "false", "VisibilityTimeout": "60", "ReceiveMessageWaitTimeSeconds": "10", "RedrivePolicy": string(redrivePolicy)},
	})
	if err != nil {
		t.Fatalf("create isolated commands queue with DLQ: %v", err)
	}
	if commandsResult.QueueUrl == nil || *commandsResult.QueueUrl == "" {
		t.Fatal("create isolated commands queue returned empty URL")
	}
	commandsURL := *commandsResult.QueueUrl
	t.Cleanup(func() { deleteTestQueue(t, client, commandsURL); deleteTestQueue(t, client, dlqURL) })
	return isolatedTestQueues{commandsURL: commandsURL, dlqURL: dlqURL}
}
func deleteTestQueue(t *testing.T, client *awssqs.Client, queueURL string) {
	t.Helper()
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := client.DeleteQueue(cleanupCtx, &awssqs.DeleteQueueInput{QueueUrl: aws.String(queueURL)})
	if err != nil {
		t.Errorf("delete isolated SQS queue %s: %v", queueURL, err)
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

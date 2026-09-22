package sqs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/Tharik/wagering-platform/internal/application/wagering"
	"github.com/Tharik/wagering-platform/internal/domain"
	"github.com/Tharik/wagering-platform/internal/observability"
	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	awstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const defaultConsumerName = "wager-commands"

type ConsumerClient interface {
	ReceiveMessage(
		ctx context.Context,
		params *awssqs.ReceiveMessageInput,
		optFns ...func(*awssqs.Options),
	) (*awssqs.ReceiveMessageOutput, error)

	DeleteMessage(
		ctx context.Context,
		params *awssqs.DeleteMessageInput,
		optFns ...func(*awssqs.Options),
	) (*awssqs.DeleteMessageOutput, error)
}

type MessageProcessor interface {
	Process(
		ctx context.Context,
		cmd wagering.MessageProcessCommand,
	) (wagering.MessageProcessResult, error)
}

type Consumer struct {
	client       ConsumerClient
	processor    MessageProcessor
	queueURL     string
	consumerName string
	metrics      *observability.Metrics
}

type CommandMessage struct {
	MessageID                      string `json:"messageId"`
	IdempotencyKey                 string `json:"idempotencyKey"`
	ProviderID                     string `json:"providerId"`
	ExternalTransactionID          string `json:"externalTransactionId"`
	PlayerID                       string `json:"playerId"`
	WalletID                       string `json:"walletId"`
	RoundID                        string `json:"roundId"`
	GameID                         string `json:"gameId"`
	Kind                           string `json:"kind"`
	Amount                         string `json:"amount"`
	Currency                       string `json:"currency"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId,omitempty"`
}

func NewConsumer(
	client ConsumerClient,
	processor MessageProcessor,
	queueURL string,
) *Consumer {
	return &Consumer{
		client:       client,
		processor:    processor,
		queueURL:     queueURL,
		consumerName: defaultConsumerName,
		metrics:      observability.NewMetrics(),
	}
}

func NewConsumerWithMetrics(
	client ConsumerClient,
	processor MessageProcessor,
	queueURL string,
	metrics *observability.Metrics,
) *Consumer {
	return &Consumer{
		client:       client,
		processor:    processor,
		queueURL:     queueURL,
		consumerName: defaultConsumerName,
		metrics:      metrics,
	}
}

func (c *Consumer) ConsumeOnce(ctx context.Context) (int, error) {
	output, err := c.client.ReceiveMessage(
		ctx,
		&awssqs.ReceiveMessageInput{
			QueueUrl:            aws.String(c.queueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     10,
			MessageSystemAttributeNames: []awstypes.MessageSystemAttributeName{
				awstypes.MessageSystemAttributeNameApproximateReceiveCount,
			},
		},
	)
	if err != nil {
		return 0, fmt.Errorf(
			"receive SQS messages: %w",
			err,
		)
	}

	processed := 0

	for _, message := range output.Messages {
		c.recordRetry(message)

		if err := c.processMessage(ctx, message); err != nil {
			return processed, err
		}

		processed++
	}

	return processed, nil
}

func (c *Consumer) recordRetry(message awstypes.Message) {
	rawReceiveCount, ok := message.Attributes[string(
		awstypes.MessageSystemAttributeNameApproximateReceiveCount,
	)]
	if !ok {
		return
	}

	receiveCount, err := strconv.Atoi(rawReceiveCount)
	if err != nil {
		return
	}

	if receiveCount > 1 {
		c.metrics.IncSQSRetries()
	}
}

func (c *Consumer) processMessage(
	ctx context.Context,
	message awstypes.Message,
) error {
	if message.Body == nil {
		return errors.New("SQS message body is required")
	}

	if message.ReceiptHandle == nil {
		return errors.New("SQS receipt handle is required")
	}

	rawPayload := []byte(*message.Body)

	command, err := decodeCommand(rawPayload)
	if err != nil {
		return fmt.Errorf(
			"decode SQS command: %w",
			err,
		)
	}

	_, err = c.processor.Process(
		ctx,
		wagering.MessageProcessCommand{
			ConsumerName: c.consumerName,
			MessageID:    command.MessageID,
			RawPayload:   rawPayload,
			Command: wagering.ProcessCommand{
				IdempotencyKey: command.IdempotencyKey,
				Request: domain.WagerRequest{
					ProviderID:                     command.ProviderID,
					ExternalTransactionID:          command.ExternalTransactionID,
					PlayerID:                       command.PlayerID,
					WalletID:                       command.WalletID,
					RoundID:                        command.RoundID,
					GameID:                         command.GameID,
					Kind:                           domain.WagerKind(command.Kind),
					Amount:                         command.money,
					ReferenceExternalTransactionID: command.ReferenceExternalTransactionID,
				},
			},
		},
	)
	if err != nil {
		return fmt.Errorf(
			"process SQS command %s: %w",
			command.MessageID,
			err,
		)
	}

	// Delete only after MessageProcessor has successfully committed:
	// Inbox + Wager + Wallet + Ledger + Outbox.
	_, err = c.client.DeleteMessage(
		ctx,
		&awssqs.DeleteMessageInput{
			QueueUrl:      aws.String(c.queueURL),
			ReceiptHandle: message.ReceiptHandle,
		},
	)
	if err != nil {
		return fmt.Errorf(
			"delete SQS command %s: %w",
			command.MessageID,
			err,
		)
	}

	return nil
}

type decodedCommand struct {
	CommandMessage
	money domain.Money
}

func decodeCommand(payload []byte) (decodedCommand, error) {
	var message CommandMessage

	if err := json.Unmarshal(payload, &message); err != nil {
		return decodedCommand{}, fmt.Errorf(
			"unmarshal command: %w",
			err,
		)
	}

	if message.MessageID == "" {
		return decodedCommand{}, errors.New(
			"messageId is required",
		)
	}

	if message.IdempotencyKey == "" {
		return decodedCommand{}, errors.New(
			"idempotencyKey is required",
		)
	}

	if message.Currency == "" {
		return decodedCommand{}, errors.New(
			"currency is required",
		)
	}

	currency := domain.Currency(message.Currency)

	if currency != domain.BRL {
		return decodedCommand{}, fmt.Errorf(
			"unsupported currency: %s",
			message.Currency,
		)
	}

	money, err := domain.ParseMoney(
		message.Amount,
		currency,
	)
	if err != nil {
		return decodedCommand{}, fmt.Errorf(
			"parse amount: %w",
			err,
		)
	}

	return decodedCommand{
		CommandMessage: message,
		money:          money,
	}, nil
}

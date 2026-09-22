package sqs

import (
	"context"

	"encoding/json"

	"errors"

	"fmt"
	"log/slog"

	"strconv"

	"time"

	"github.com/Tharik/wagering-platform/internal/application/wagering"

	"github.com/Tharik/wagering-platform/internal/domain"

	"github.com/Tharik/wagering-platform/internal/observability"

	"github.com/aws/aws-sdk-go-v2/aws"

	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"

	awstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const (
	defaultConsumerName           = "wager-transactions"
	wagerTransactionRequestedType = "WagerTransactionRequested"
)

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
	client ConsumerClient

	processor MessageProcessor

	queueURL string

	consumerName string

	metrics *observability.Metrics
	logger  *slog.Logger
}

type CommandMessage struct {
	MessageID string `json:"messageId"`

	Type string `json:"type"`

	OccurredAt string `json:"occurredAt"`

	Data WagerCommandData `json:"data"`
}

type WagerCommandData struct {
	IdempotencyKey string `json:"idempotencyKey"`

	ProviderID string `json:"providerId"`

	ExternalTransactionID string `json:"externalTransactionId"`

	PlayerID string `json:"playerId"`

	WalletID string `json:"walletId"`

	RoundID string `json:"roundId"`

	GameID string `json:"gameId"`

	Kind string `json:"kind"`

	Money MoneyDTO `json:"money"`

	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId,omitempty"`
}

type MoneyDTO struct {
	Amount string `json:"amount"`

	Currency string `json:"currency"`
}

func NewConsumer(

	client ConsumerClient,

	processor MessageProcessor,

	queueURL string,

) *Consumer {

	return &Consumer{

		client: client,

		processor: processor,

		queueURL: queueURL,

		consumerName: defaultConsumerName,

		metrics: observability.NewMetrics(),
		logger:  slog.Default(),
	}

}

func NewConsumerWithMetrics(

	client ConsumerClient,

	processor MessageProcessor,

	queueURL string,

	metrics *observability.Metrics,

) *Consumer {

	return &Consumer{

		client: client,

		processor: processor,

		queueURL: queueURL,

		consumerName: defaultConsumerName,

		metrics: metrics,
		logger:  slog.Default(),
	}

}

func NewConsumerWithMetricsAndLogger(
	client ConsumerClient,
	processor MessageProcessor,
	queueURL string,
	metrics *observability.Metrics,
	logger *slog.Logger,
) *Consumer {
	return &Consumer{
		client:       client,
		processor:    processor,
		queueURL:     queueURL,
		consumerName: defaultConsumerName,
		metrics:      metrics,
		logger: logger.With(
			slog.String("component", "sqs_consumer"),
		),
	}
}

func (c *Consumer) ConsumeOnce(ctx context.Context) (int, error) {

	output, err := c.client.ReceiveMessage(

		ctx,

		&awssqs.ReceiveMessageInput{

			QueueUrl: aws.String(c.queueURL),

			MaxNumberOfMessages: 10,

			WaitTimeSeconds: 10,

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

	result, err := c.processor.Process(

		ctx,

		wagering.MessageProcessCommand{

			ConsumerName: c.consumerName,

			MessageID: command.MessageID,

			RawPayload: rawPayload,

			Command: wagering.ProcessCommand{

				IdempotencyKey: command.Data.IdempotencyKey,

				CorrelationID: command.MessageID,

				CausationID: command.MessageID,

				Request: domain.WagerRequest{

					ProviderID: command.Data.ProviderID,

					ExternalTransactionID: command.Data.ExternalTransactionID,

					PlayerID: command.Data.PlayerID,

					WalletID: command.Data.WalletID,

					RoundID: command.Data.RoundID,

					GameID: command.Data.GameID,

					Kind: domain.WagerKind(command.Data.Kind),

					Amount: command.money,

					ReferenceExternalTransactionID: command.Data.ReferenceExternalTransactionID,
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

	attrs := []any{
		slog.String("messageId", command.MessageID),
		slog.String("correlationId", command.MessageID),
		slog.String("providerId", command.Data.ProviderID),
		slog.String("walletId", command.Data.WalletID),
		slog.String("externalTransactionId", command.Data.ExternalTransactionID),
		slog.String("kind", command.Data.Kind),
		slog.Bool("inboxReplay", result.InboxReplay),
		slog.Bool("idempotentReplay", result.IdempotentReplay),
	}

	if result.Result.TransactionID != "" {
		attrs = append(attrs, slog.String("transactionId", result.Result.TransactionID))
	}

	if result.Result.State != "" {
		attrs = append(attrs, slog.String("status", string(result.Result.State)))
	}

	c.logger.Info("SQS command committed", attrs...)

	// Delete only after MessageProcessor has successfully committed:

	// Inbox + Wager + Wallet + Ledger + Outbox.

	_, err = c.client.DeleteMessage(

		ctx,

		&awssqs.DeleteMessageInput{

			QueueUrl: aws.String(c.queueURL),

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

	if message.Type == "" {

		return decodedCommand{}, errors.New(

			"type is required",
		)

	}

	if message.Type != wagerTransactionRequestedType {

		return decodedCommand{}, fmt.Errorf(

			"unsupported message type: %s",

			message.Type,
		)

	}

	if message.OccurredAt == "" {

		return decodedCommand{}, errors.New(

			"occurredAt is required",
		)

	}

	if _, err := time.Parse(time.RFC3339, message.OccurredAt); err != nil {

		return decodedCommand{}, errors.New(

			"occurredAt must be RFC3339",
		)

	}

	if message.Data.ProviderID == "" {

		return decodedCommand{}, errors.New(

			"providerId is required",
		)

	}

	if message.Data.ExternalTransactionID == "" {

		return decodedCommand{}, errors.New(

			"externalTransactionId is required",
		)

	}

	if message.Data.IdempotencyKey == "" {

		return decodedCommand{}, errors.New(

			"idempotencyKey is required",
		)

	}

	if message.Data.PlayerID == "" {

		return decodedCommand{}, errors.New(

			"playerId is required",
		)

	}

	if message.Data.WalletID == "" {

		return decodedCommand{}, errors.New(

			"walletId is required",
		)

	}

	if message.Data.RoundID == "" {

		return decodedCommand{}, errors.New(

			"roundId is required",
		)

	}

	if message.Data.GameID == "" {

		return decodedCommand{}, errors.New(

			"gameId is required",
		)

	}

	if message.Data.Kind == "" {

		return decodedCommand{}, errors.New(

			"kind is required",
		)

	}

	if message.Data.Money.Amount == "" {

		return decodedCommand{}, errors.New(

			"money.amount is required",
		)

	}

	if message.Data.Money.Currency == "" {

		return decodedCommand{}, errors.New(

			"money.currency is required",
		)

	}

	currency := domain.Currency(message.Data.Money.Currency)

	if currency != domain.BRL {

		return decodedCommand{}, fmt.Errorf(

			"unsupported currency: %s",

			message.Data.Money.Currency,
		)

	}

	money, err := domain.ParseMoney(

		message.Data.Money.Amount,

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

		money: money,
	}, nil

}

package sqs

import (
	"context"
	"fmt"

	"github.com/Tharik/wagering-platform/internal/infrastructure/messaging/outbox"
	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
)

type Client interface {
	SendMessage(
		ctx context.Context,
		params *awssqs.SendMessageInput,
		optFns ...func(*awssqs.Options),
	) (*awssqs.SendMessageOutput, error)
}

type Publisher struct {
	client   Client
	queueURL string
}

func NewPublisher(
	client Client,
	queueURL string,
) *Publisher {
	return &Publisher{
		client:   client,
		queueURL: queueURL,
	}
}

func (p *Publisher) Publish(
	ctx context.Context,
	event outbox.Event,
) error {
	_, err := p.client.SendMessage(
		ctx,
		&awssqs.SendMessageInput{
			QueueUrl:               aws.String(p.queueURL),
			MessageBody:            aws.String(string(event.Payload)),
			MessageGroupId:         aws.String(event.AggregateID.String()),
			MessageDeduplicationId: aws.String(event.ID.String()),
		},
	)
	if err != nil {
		return fmt.Errorf(
			"send outbox event %s to SQS: %w",
			event.ID,
			err,
		)
	}

	return nil
}

package sqs

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/Tharik/wagering-platform/internal/observability"
	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	awstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const dlqMonitorInterval = 5 * time.Second

type DLQAttributesClient interface {
	GetQueueAttributes(
		ctx context.Context,
		params *awssqs.GetQueueAttributesInput,
		optFns ...func(*awssqs.Options),
	) (*awssqs.GetQueueAttributesOutput, error)
}

type DLQMonitor struct {
	client   DLQAttributesClient
	queueURL string
	metrics  *observability.Metrics
	logger   *slog.Logger
}

func NewDLQMonitor(
	client DLQAttributesClient,
	queueURL string,
	metrics *observability.Metrics,
	logger *slog.Logger,
) *DLQMonitor {
	return &DLQMonitor{
		client:   client,
		queueURL: queueURL,
		metrics:  metrics,
		logger: logger.With(
			slog.String("component", "sqs_dlq_monitor"),
		),
	}
}

func (m *DLQMonitor) Run(ctx context.Context) {
	m.logger.Info("worker started")

	m.refresh(ctx)

	ticker := time.NewTicker(dlqMonitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.logger.Info("worker stopped")
			return
		case <-ticker.C:
			m.refresh(ctx)
		}
	}
}

func (m *DLQMonitor) refresh(ctx context.Context) {
	output, err := m.client.GetQueueAttributes(
		ctx,
		&awssqs.GetQueueAttributesInput{
			QueueUrl: aws.String(m.queueURL),
			AttributeNames: []awstypes.QueueAttributeName{
				awstypes.QueueAttributeNameApproximateNumberOfMessages,
			},
		},
	)
	if err != nil {
		if ctx.Err() != nil {
			return
		}

		m.logger.Error(
			"failed to read DLQ attributes",
			slog.Any("error", err),
		)
		return
	}

	rawCount, ok := output.Attributes[string(
		awstypes.QueueAttributeNameApproximateNumberOfMessages,
	)]
	if !ok {
		m.logger.Error("DLQ message count attribute missing")
		return
	}

	count, err := strconv.ParseUint(rawCount, 10, 64)
	if err != nil {
		m.logger.Error(
			"invalid DLQ message count",
			slog.String("value", rawCount),
			slog.Any("error", fmt.Errorf("parse DLQ message count: %w", err)),
		)
		return
	}

	m.metrics.SetSQSDLQMessages(count)
}

package wagering

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Tharik/wagering-platform/internal/infrastructure/messaging/inbox"
	"github.com/jackc/pgx/v5/pgxpool"
)

type MessageProcessor struct {
	pool    *pgxpool.Pool
	service *Service
}

type MessageProcessCommand struct {
	ConsumerName string
	MessageID    string
	RawPayload   []byte
	Command      ProcessCommand
}

type MessageProcessResult struct {
	Result           ProcessResult
	InboxReplay      bool
	IdempotentReplay bool
}

func NewMessageProcessor(
	pool *pgxpool.Pool,
	service *Service,
) *MessageProcessor {
	return &MessageProcessor{
		pool:    pool,
		service: service,
	}
}

func (p *MessageProcessor) Process(
	ctx context.Context,
	cmd MessageProcessCommand,
) (MessageProcessResult, error) {
	startedAt := time.Now()

	defer func() {
		p.service.metrics.ObserveProcessingDuration(
			time.Since(startedAt),
		)
	}()

	if cmd.ConsumerName == "" {
		return MessageProcessResult{}, errors.New(
			"consumer name is required",
		)
	}

	if cmd.MessageID == "" {
		return MessageProcessResult{}, errors.New(
			"message id is required",
		)
	}

	if len(cmd.RawPayload) == 0 {
		return MessageProcessResult{}, errors.New(
			"raw payload is required",
		)
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return MessageProcessResult{}, fmt.Errorf(
			"begin message transaction: %w",
			err,
		)
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	alreadyCompleted, err := inbox.Register(
		ctx,
		tx,
		cmd.ConsumerName,
		cmd.MessageID,
		cmd.RawPayload,
	)
	if err != nil {
		return MessageProcessResult{}, err
	}

	if alreadyCompleted {
		if err := tx.Commit(ctx); err != nil {
			return MessageProcessResult{}, fmt.Errorf(
				"commit inbox replay transaction: %w",
				err,
			)
		}

		return MessageProcessResult{
			InboxReplay: true,
		}, nil
	}

	result, err := p.service.ProcessTx(
		ctx,
		tx,
		cmd.Command,
	)
	if err != nil {
		return MessageProcessResult{}, err
	}

	if err := inbox.Complete(
		ctx,
		tx,
		cmd.ConsumerName,
		cmd.MessageID,
	); err != nil {
		return MessageProcessResult{}, fmt.Errorf(
			"complete inbox message: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return MessageProcessResult{}, fmt.Errorf(
			"commit message transaction: %w",
			err,
		)
	}

	p.service.recordProcessResult(result)

	return MessageProcessResult{
		Result:           result,
		IdempotentReplay: result.IdempotentReplay,
	}, nil
}

package httpapi

import (
	"context"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jackc/pgx/v5/pgxpool"
)

const readinessTimeout = 2 * time.Second

type HealthHandler struct {
	pool             *pgxpool.Pool
	sqsClient        *sqs.Client
	commandsQueueURL string
}

func NewHealthHandler(
	pool *pgxpool.Pool,
	sqsClient *sqs.Client,
	commandsQueueURL string,
) *HealthHandler {
	return &HealthHandler{
		pool:             pool,
		sqsClient:        sqsClient,
		commandsQueueURL: commandsQueueURL,
	}
}

func (h *HealthHandler) Live(
	w http.ResponseWriter,
	r *http.Request,
) {
	writeJSON(
		w,
		http.StatusOK,
		map[string]string{
			"status": "UP",
		},
	)
}

func (h *HealthHandler) Ready(
	w http.ResponseWriter,
	r *http.Request,
) {
	ctx, cancel := context.WithTimeout(
		r.Context(),
		readinessTimeout,
	)
	defer cancel()

	checks := map[string]string{
		"postgres": "UP",
		"sqs":      "UP",
	}

	if err := h.pool.Ping(ctx); err != nil {
		checks["postgres"] = "DOWN"
	}

	if _, err := h.sqsClient.GetQueueAttributes(
		ctx,
		&sqs.GetQueueAttributesInput{
			QueueUrl: &h.commandsQueueURL,
			AttributeNames: []types.QueueAttributeName{
				types.QueueAttributeNameQueueArn,
			},
		},
	); err != nil {
		checks["sqs"] = "DOWN"
	}

	status := http.StatusOK
	overall := "UP"

	if checks["postgres"] != "UP" || checks["sqs"] != "UP" {
		status = http.StatusServiceUnavailable
		overall = "DOWN"
	}

	writeJSON(
		w,
		status,
		map[string]any{
			"status": overall,
			"checks": checks,
		},
	)
}

package httpapi

import (
	"context"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const readinessTimeout = 2 * time.Second

type HealthHandler struct {
	pool *pgxpool.Pool
}

func NewHealthHandler(
	pool *pgxpool.Pool,
) *HealthHandler {
	return &HealthHandler{
		pool: pool,
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

	if err := h.pool.Ping(ctx); err != nil {
		writeJSON(
			w,
			http.StatusServiceUnavailable,
			map[string]string{
				"status": "DOWN",
			},
		)
		return
	}

	writeJSON(
		w,
		http.StatusOK,
		map[string]string{
			"status": "UP",
		},
	)
}

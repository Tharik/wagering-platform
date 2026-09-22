package httpapi

import (
	"net/http"

	"github.com/Tharik/wagering-platform/internal/observability"
)

type MetricsHandler struct {
	metrics *observability.Metrics
}

func NewMetricsHandler(
	metrics *observability.Metrics,
) *MetricsHandler {
	return &MetricsHandler{
		metrics: metrics,
	}
}

func (h *MetricsHandler) ServeHTTP(
	w http.ResponseWriter,
	r *http.Request,
) {
	w.Header().Set(
		"Content-Type",
		"text/plain; version=0.0.4; charset=utf-8",
	)

	if err := h.metrics.WritePrometheus(w); err != nil {
		return
	}
}

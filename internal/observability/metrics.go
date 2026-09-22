package observability

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

type Metrics struct {
	sqsMessagesProcessed atomic.Uint64
	sqsErrors            atomic.Uint64
	sqsRetries           atomic.Uint64
	sqsDLQMessages       atomic.Uint64

	outboxEventsPublished atomic.Uint64
	outboxErrors          atomic.Uint64
	outboxRetries         atomic.Uint64

	wagersProcessed        atomic.Uint64
	wagersRejected         atomic.Uint64
	wagersPendingReference atomic.Uint64

	idempotentReplays    atomic.Uint64
	concurrencyConflicts atomic.Uint64

	reconciliationDivergences atomic.Uint64

	processingCount             atomic.Uint64
	processingDurationMicros    atomic.Uint64
	outboxPublishedDelayMicros  atomic.Uint64
	outboxPublishedDelaySamples atomic.Uint64
}

func NewMetrics() *Metrics {
	return &Metrics{}
}

func (m *Metrics) IncSQSMessagesProcessed(count uint64) {
	m.sqsMessagesProcessed.Add(count)
}

func (m *Metrics) IncSQSErrors() {
	m.sqsErrors.Add(1)
}

func (m *Metrics) IncSQSRetries() {
	m.sqsRetries.Add(1)
}

func (m *Metrics) IncSQSDLQMessages() {
	m.sqsDLQMessages.Add(1)
}

func (m *Metrics) IncOutboxEventsPublished(count uint64) {
	m.outboxEventsPublished.Add(count)
}

func (m *Metrics) IncOutboxErrors() {
	m.outboxErrors.Add(1)
}

func (m *Metrics) IncOutboxRetries() {
	m.outboxRetries.Add(1)
}

func (m *Metrics) IncWagersProcessed() {
	m.wagersProcessed.Add(1)
}

func (m *Metrics) IncWagersRejected() {
	m.wagersRejected.Add(1)
}

func (m *Metrics) IncWagersPendingReference() {
	m.wagersPendingReference.Add(1)
}

func (m *Metrics) IncIdempotentReplays() {
	m.idempotentReplays.Add(1)
}

func (m *Metrics) IncConcurrencyConflicts() {
	m.concurrencyConflicts.Add(1)
}

func (m *Metrics) IncReconciliationDivergences() {
	m.reconciliationDivergences.Add(1)
}

func (m *Metrics) ObserveProcessingDuration(duration time.Duration) {
	if duration < 0 {
		return
	}

	m.processingCount.Add(1)
	m.processingDurationMicros.Add(uint64(duration.Microseconds()))
}

func (m *Metrics) ObserveOutboxPublishedDelay(duration time.Duration) {
	if duration < 0 {
		return
	}

	m.outboxPublishedDelaySamples.Add(1)
	m.outboxPublishedDelayMicros.Add(uint64(duration.Microseconds()))
}

func (m *Metrics) WritePrometheus(w io.Writer) error {
	counters := []struct {
		name  string
		value uint64
	}{
		{
			"wagering_sqs_messages_processed_total",
			m.sqsMessagesProcessed.Load(),
		},
		{
			"wagering_sqs_errors_total",
			m.sqsErrors.Load(),
		},
		{
			"wagering_sqs_retries_total",
			m.sqsRetries.Load(),
		},
		{
			"wagering_sqs_dlq_messages_total",
			m.sqsDLQMessages.Load(),
		},
		{
			"wagering_outbox_events_published_total",
			m.outboxEventsPublished.Load(),
		},
		{
			"wagering_outbox_errors_total",
			m.outboxErrors.Load(),
		},
		{
			"wagering_outbox_retries_total",
			m.outboxRetries.Load(),
		},
		{
			"wagering_wagers_processed_total",
			m.wagersProcessed.Load(),
		},
		{
			"wagering_wagers_rejected_total",
			m.wagersRejected.Load(),
		},
		{
			"wagering_wagers_pending_reference_total",
			m.wagersPendingReference.Load(),
		},
		{
			"wagering_idempotent_replays_total",
			m.idempotentReplays.Load(),
		},
		{
			"wagering_concurrency_conflicts_total",
			m.concurrencyConflicts.Load(),
		},
		{
			"wagering_reconciliation_divergences_total",
			m.reconciliationDivergences.Load(),
		},
	}

	for _, metric := range counters {
		if _, err := fmt.Fprintf(
			w,
			"# TYPE %s counter\n%s %d\n",
			metric.name,
			metric.name,
			metric.value,
		); err != nil {
			return err
		}
	}

	processingCount := m.processingCount.Load()
	processingDurationMicros := m.processingDurationMicros.Load()

	if _, err := fmt.Fprintf(
		w,
		"# TYPE wagering_processing_duration_seconds summary\n"+
			"wagering_processing_duration_seconds_count %d\n"+
			"wagering_processing_duration_seconds_sum %.6f\n",
		processingCount,
		float64(processingDurationMicros)/1_000_000,
	); err != nil {
		return err
	}

	outboxDelaySamples := m.outboxPublishedDelaySamples.Load()
	outboxDelayMicros := m.outboxPublishedDelayMicros.Load()

	if _, err := fmt.Fprintf(
		w,
		"# TYPE wagering_outbox_publish_delay_seconds summary\n"+
			"wagering_outbox_publish_delay_seconds_count %d\n"+
			"wagering_outbox_publish_delay_seconds_sum %.6f\n",
		outboxDelaySamples,
		float64(outboxDelayMicros)/1_000_000,
	); err != nil {
		return err
	}

	return nil
}

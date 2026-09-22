package wallet

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Tharik/wagering-platform/internal/application/wagering"
	"github.com/Tharik/wagering-platform/internal/domain"
	"github.com/Tharik/wagering-platform/internal/observability"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestReconciliationDetectsConsistentAndDivergentWallet(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cancel()

	const databaseURL = "postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable"

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	cleanReconciliationDatabase(t, ctx, pool)

	metrics := observability.NewMetrics()
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	service := NewServiceWithMetricsAndLogger(pool, metrics, logger)

	created, err := service.Create(
		ctx,
		CreateWalletCommand{
			PlayerID:       "player-reconciliation",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	//
	// 1. Healthy wallet.
	//
	// Wallet creation with 100.00 creates the OPENING ledger entry.
	// Reconciliation must reconstruct exactly the same balance.
	//

	result, err := service.Reconcile(
		ctx,
		created.WalletID,
	)
	if err != nil {
		t.Fatalf("reconcile healthy wallet: %v", err)
	}

	if !result.Consistent {
		t.Fatal("expected newly created wallet to be consistent")
	}

	if result.StoredBalance != 10000 {
		t.Fatalf(
			"expected wallet balance 10000, got %d",
			result.StoredBalance,
		)
	}

	if result.CalculatedBalance != 10000 {
		t.Fatalf(
			"expected ledger balance 10000, got %d",
			result.CalculatedBalance,
		)
	}

	if result.Difference != 0 {
		t.Fatalf("expected zero difference, got %d", result.Difference)
	}

	if result.CheckedEntries != 1 {
		t.Fatalf(
			"expected exactly 1 opening ledger entry, got %d",
			result.CheckedEntries,
		)
	}

	if result.Currency != string(domain.BRL) {
		t.Fatalf(
			"expected BRL currency, got %s",
			result.Currency,
		)
	}

	assertReconciliationDivergenceMetric(t, metrics, 0)
	if logs.Len() != 0 {
		t.Fatalf("healthy reconciliation must not log a divergence: %s", logs.String())
	}

	//
	// 2. Simulate database corruption / operational divergence.
	//
	// We intentionally modify only the materialized wallet balance.
	// The immutable ledger remains untouched.
	//

	_, err = pool.Exec(
		ctx,
		`
		UPDATE wallets
		SET balance = 12345
		WHERE id = $1
		`,
		created.WalletID,
	)
	if err != nil {
		t.Fatalf("corrupt wallet balance: %v", err)
	}

	divergent, err := service.Reconcile(
		ctx,
		created.WalletID,
	)
	if err != nil {
		t.Fatalf("reconcile divergent wallet: %v", err)
	}

	if divergent.Consistent {
		t.Fatal(
			"expected reconciliation to detect wallet/ledger divergence",
		)
	}

	if divergent.StoredBalance != 12345 {
		t.Fatalf(
			"expected corrupted wallet balance 12345, got %d",
			divergent.StoredBalance,
		)
	}

	if divergent.CalculatedBalance != 10000 {
		t.Fatalf(
			"expected immutable ledger balance 10000, got %d",
			divergent.CalculatedBalance,
		)
	}

	if divergent.Difference != 2345 {
		t.Fatalf("expected difference 2345, got %d", divergent.Difference)
	}

	if divergent.CheckedEntries != 1 {
		t.Fatalf(
			"expected ledger entry count to remain 1, got %d",
			divergent.CheckedEntries,
		)
	}

	assertReconciliationDivergenceMetric(t, metrics, 1)
	assertReconciliationDivergenceLog(t, logs.Bytes(), divergent)

	//
	// 3. Reconciliation is diagnostic only.
	//
	// It must report the divergence, never silently repair financial state.
	//

	var persistedBalance int64

	err = pool.QueryRow(
		ctx,
		`
		SELECT balance
		FROM wallets
		WHERE id = $1
		`,
		created.WalletID,
	).Scan(&persistedBalance)
	if err != nil {
		t.Fatalf("query wallet after reconciliation: %v", err)
	}

	if persistedBalance != 12345 {
		t.Fatalf(
			"expected reconciliation not to mutate wallet; got balance %d",
			persistedBalance,
		)
	}

	var ledgerBalanceAfter int64

	err = pool.QueryRow(
		ctx,
		`
		SELECT balance_after
		FROM ledger_entries
		WHERE wallet_id = $1
		ORDER BY created_at DESC, id DESC
		LIMIT 1
		`,
		created.WalletID,
	).Scan(&ledgerBalanceAfter)
	if err != nil {
		t.Fatalf("query ledger after reconciliation: %v", err)
	}

	if ledgerBalanceAfter != 10000 {
		t.Fatalf(
			"expected reconciliation not to mutate ledger; got %d",
			ledgerBalanceAfter,
		)
	}
}

func TestReconciliationWithZeroLedgerEntries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, reconciliationDatabaseURL)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()
	cleanReconciliationDatabase(t, ctx, pool)

	service := NewService(pool)
	created, err := service.Create(
		ctx,
		CreateWalletCommand{
			PlayerID:       "player-zero-ledger-reconciliation",
			InitialBalance: domain.Zero(domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create zero-balance wallet: %v", err)
	}

	result, err := service.Reconcile(ctx, created.WalletID)
	if err != nil {
		t.Fatalf("reconcile zero-balance wallet: %v", err)
	}
	if !result.Consistent || result.StoredBalance != 0 ||
		result.CalculatedBalance != 0 || result.Difference != 0 ||
		result.CheckedEntries != 0 {
		t.Fatalf("unexpected zero-ledger reconciliation: %+v", result)
	}
}

func TestReconcileUsesOneRepeatableReadSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	setupPool, err := pgxpool.New(ctx, reconciliationDatabaseURL)
	if err != nil {
		t.Fatalf("connect setup postgres pool: %v", err)
	}
	defer setupPool.Close()
	cleanReconciliationDatabase(t, ctx, setupPool)

	creator := NewService(setupPool)
	created, err := creator.Create(
		ctx,
		CreateWalletCommand{
			PlayerID:       "player-snapshot-reconciliation",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create snapshot wallet: %v", err)
	}

	tracer := newLedgerQueryBarrier()
	config, err := pgxpool.ParseConfig(reconciliationDatabaseURL)
	if err != nil {
		t.Fatalf("parse traced pool config: %v", err)
	}
	config.ConnConfig.Tracer = tracer
	reconciliationPool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("create traced reconciliation pool: %v", err)
	}
	defer reconciliationPool.Close()

	service := NewService(reconciliationPool)
	tracer.arm()

	type reconciliationOutcome struct {
		result ReconciliationResult
		err    error
	}
	outcome := make(chan reconciliationOutcome, 1)
	go func() {
		result, err := service.Reconcile(ctx, created.WalletID)
		outcome <- reconciliationOutcome{result: result, err: err}
	}()

	select {
	case <-tracer.ledgerQueryStarted:
	case <-ctx.Done():
		t.Fatalf("wait for reconciliation ledger query: %v", ctx.Err())
	}

	wagerService := wagering.NewService(setupPool)
	mutation, err := wagerService.Process(
		ctx,
		wagering.ProcessCommand{
			IdempotencyKey: "snapshot-reconciliation-bet",
			Request: domain.WagerRequest{
				ProviderID:            "provider-a",
				ExternalTransactionID: "snapshot-reconciliation-bet",
				PlayerID:              "player-snapshot-reconciliation",
				WalletID:              created.WalletID,
				RoundID:               "round-snapshot-reconciliation",
				GameID:                "game-snapshot-reconciliation",
				Kind:                  domain.WagerKindBet,
				Amount:                domain.NewMoney(1000, domain.BRL),
			},
		},
	)
	if err != nil {
		t.Fatalf("commit concurrent financial mutation: %v", err)
	}
	if mutation.State != domain.WagerStateProcessed {
		t.Fatalf("expected concurrent mutation to be processed, got %s", mutation.State)
	}

	tracer.release()

	var reconciled reconciliationOutcome
	select {
	case reconciled = <-outcome:
	case <-ctx.Done():
		t.Fatalf("wait for reconciliation result: %v", ctx.Err())
	}
	if reconciled.err != nil {
		t.Fatalf("reconcile concurrent wallet: %v", reconciled.err)
	}
	if !reconciled.result.Consistent || reconciled.result.StoredBalance != 10000 ||
		reconciled.result.CalculatedBalance != 10000 ||
		reconciled.result.CheckedEntries != 1 {
		t.Fatalf("reconciliation mixed database snapshots: %+v", reconciled.result)
	}

	current, err := creator.Get(ctx, created.WalletID)
	if err != nil {
		t.Fatalf("query wallet after concurrent mutation: %v", err)
	}
	if current.Balance != 9000 {
		t.Fatalf("expected committed current balance 9000, got %d", current.Balance)
	}
}

type ledgerQueryBarrier struct {
	mu                 sync.Mutex
	armed              bool
	ledgerQueryStarted chan struct{}
	continueLedger     chan struct{}
	startedOnce        sync.Once
	releaseOnce        sync.Once
}

func newLedgerQueryBarrier() *ledgerQueryBarrier {
	return &ledgerQueryBarrier{
		ledgerQueryStarted: make(chan struct{}),
		continueLedger:     make(chan struct{}),
	}
}

func (b *ledgerQueryBarrier) arm() {
	b.mu.Lock()
	b.armed = true
	b.mu.Unlock()
}

func (b *ledgerQueryBarrier) release() {
	b.releaseOnce.Do(func() {
		close(b.continueLedger)
	})
}

func (b *ledgerQueryBarrier) TraceQueryStart(
	ctx context.Context,
	_ *pgx.Conn,
	data pgx.TraceQueryStartData,
) context.Context {
	b.mu.Lock()
	armed := b.armed
	b.mu.Unlock()
	if !armed || !strings.Contains(data.SQL, "FROM ledger_entries") {
		return ctx
	}

	b.startedOnce.Do(func() {
		close(b.ledgerQueryStarted)
	})
	select {
	case <-b.continueLedger:
	case <-ctx.Done():
	}
	return ctx
}

func (b *ledgerQueryBarrier) TraceQueryEnd(
	context.Context,
	*pgx.Conn,
	pgx.TraceQueryEndData,
) {
}

const reconciliationDatabaseURL = "postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable"

func assertReconciliationDivergenceMetric(
	t *testing.T,
	metrics *observability.Metrics,
	expected uint64,
) {
	t.Helper()

	var output bytes.Buffer

	if err := metrics.WritePrometheus(&output); err != nil {
		t.Fatalf("write prometheus metrics: %v", err)
	}

	expectedLine := "wagering_reconciliation_divergences_total " +
		uintToString(expected)

	if !strings.Contains(output.String(), expectedLine) {
		t.Fatalf(
			"expected metrics output to contain %q, got:\n%s",
			expectedLine,
			output.String(),
		)
	}
}

func assertReconciliationDivergenceLog(
	t *testing.T,
	data []byte,
	result ReconciliationResult,
) {
	t.Helper()

	var entry map[string]any
	if err := json.Unmarshal(data, &entry); err != nil {
		t.Fatalf("decode reconciliation log: %v", err)
	}

	expected := map[string]any{
		"msg":               "wallet reconciliation divergence",
		"walletId":          result.WalletID,
		"storedBalance":     "123.45",
		"calculatedBalance": "100.00",
		"difference":        "23.45",
		"checkedEntries":    float64(1),
	}
	for key, value := range expected {
		if entry[key] != value {
			t.Fatalf("expected log field %s=%v, got %v in %s", key, value, entry[key], string(data))
		}
	}
}

func uintToString(value uint64) string {
	if value == 0 {
		return "0"
	}

	const digits = "0123456789"

	var buffer [20]byte
	index := len(buffer)

	for value > 0 {
		index--
		buffer[index] = digits[value%10]
		value /= 10
	}

	return string(buffer[index:])
}

func cleanReconciliationDatabase(
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

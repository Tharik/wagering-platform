package wagering

import (
	"context"
	"testing"
	"time"

	"github.com/Tharik/wagering-platform/internal/application/wallet"
	"github.com/Tharik/wagering-platform/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMessageProcessorProcessesBetAndCompletesInboxAtomically(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.New(
		ctx,
		"postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable",
	)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	cleanDatabase(t, ctx, pool)

	walletService := wallet.NewService(pool)

	createdWallet, err := walletService.Create(
		ctx,
		wallet.CreateWalletCommand{
			PlayerID:       "player-message",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	service := NewService(pool)
	processor := NewMessageProcessor(pool, service)

	payload := []byte(`{
		"messageId":"message-1",
		"externalTransactionId":"external-message-bet-1",
		"kind":"BET",
		"amount":"30.00"
	}`)

	command := MessageProcessCommand{
		ConsumerName: "wager-consumer",
		MessageID:    "message-1",
		RawPayload:   payload,
		Command: ProcessCommand{
			IdempotencyKey: "message-bet-1",
			Request: domain.WagerRequest{
				ProviderID:            "provider-a",
				ExternalTransactionID: "external-message-bet-1",
				PlayerID:              "player-message",
				WalletID:              createdWallet.WalletID,
				RoundID:               "round-message-1",
				GameID:                "game-1",
				Kind:                  domain.WagerKindBet,
				Amount:                domain.NewMoney(3000, domain.BRL),
			},
		},
	}

	result, err := processor.Process(ctx, command)
	if err != nil {
		t.Fatalf("process message: %v", err)
	}

	if result.InboxReplay {
		t.Fatal("first delivery must not be an inbox replay")
	}

	if result.Result.State != domain.WagerStateProcessed {
		t.Fatalf(
			"expected PROCESSED, got %s",
			result.Result.State,
		)
	}

	if result.Result.Balance.Amount() != 7000 {
		t.Fatalf(
			"expected balance 7000, got %d",
			result.Result.Balance.Amount(),
		)
	}

	var balance int64
	var version int64

	err = pool.QueryRow(
		ctx,
		`
		SELECT balance, version
		FROM wallets
		WHERE id = $1
		`,
		createdWallet.WalletID,
	).Scan(&balance, &version)
	if err != nil {
		t.Fatalf("query wallet: %v", err)
	}

	if balance != 7000 {
		t.Fatalf("expected persisted balance 7000, got %d", balance)
	}

	if version != 2 {
		t.Fatalf("expected wallet version 2, got %d", version)
	}

	var completedAt *time.Time

	err = pool.QueryRow(
		ctx,
		`
		SELECT completed_at
		FROM inbox_messages
		WHERE consumer_name = $1
		  AND message_id = $2
		`,
		"wager-consumer",
		"message-1",
	).Scan(&completedAt)
	if err != nil {
		t.Fatalf("query inbox: %v", err)
	}

	if completedAt == nil {
		t.Fatal("expected inbox message to be completed")
	}

	var betCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM wager_transactions
		WHERE wallet_id = $1
		  AND kind = 'BET'
		  AND state = 'PROCESSED'
		`,
		createdWallet.WalletID,
	).Scan(&betCount)
	if err != nil {
		t.Fatalf("count BET transactions: %v", err)
	}

	if betCount != 1 {
		t.Fatalf("expected exactly 1 BET, got %d", betCount)
	}

	var debitCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM ledger_entries
		WHERE wallet_id = $1
		  AND direction = 'DEBIT'
		`,
		createdWallet.WalletID,
	).Scan(&debitCount)
	if err != nil {
		t.Fatalf("count debit ledger entries: %v", err)
	}

	if debitCount != 1 {
		t.Fatalf("expected exactly 1 debit, got %d", debitCount)
	}
}

func TestMessageProcessorDuplicateDeliveryDoesNotProcessBetAgain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.New(
		ctx,
		"postgres://wagering:wagering@localhost:5432/wagering?sslmode=disable",
	)
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	cleanDatabase(t, ctx, pool)

	walletService := wallet.NewService(pool)

	createdWallet, err := walletService.Create(
		ctx,
		wallet.CreateWalletCommand{
			PlayerID:       "player-message-replay",
			InitialBalance: domain.NewMoney(10000, domain.BRL),
		},
	)
	if err != nil {
		t.Fatalf("create wallet: %v", err)
	}

	service := NewService(pool)
	processor := NewMessageProcessor(pool, service)

	payload := []byte(`{"messageId":"duplicate-message","kind":"BET"}`)

	command := MessageProcessCommand{
		ConsumerName: "wager-consumer",
		MessageID:    "duplicate-message",
		RawPayload:   payload,
		Command: ProcessCommand{
			IdempotencyKey: "duplicate-message-bet",
			Request: domain.WagerRequest{
				ProviderID:            "provider-a",
				ExternalTransactionID: "external-duplicate-message",
				PlayerID:              "player-message-replay",
				WalletID:              createdWallet.WalletID,
				RoundID:               "round-message-replay",
				GameID:                "game-1",
				Kind:                  domain.WagerKindBet,
				Amount:                domain.NewMoney(3000, domain.BRL),
			},
		},
	}

	first, err := processor.Process(ctx, command)
	if err != nil {
		t.Fatalf("first delivery: %v", err)
	}

	if first.InboxReplay {
		t.Fatal("first delivery must not be inbox replay")
	}

	second, err := processor.Process(ctx, command)
	if err != nil {
		t.Fatalf("duplicate delivery: %v", err)
	}

	if !second.InboxReplay {
		t.Fatal("expected duplicate delivery to be inbox replay")
	}

	var balance int64

	err = pool.QueryRow(
		ctx,
		"SELECT balance FROM wallets WHERE id = $1",
		createdWallet.WalletID,
	).Scan(&balance)
	if err != nil {
		t.Fatalf("query wallet: %v", err)
	}

	if balance != 7000 {
		t.Fatalf(
			"expected balance to remain 7000, got %d",
			balance,
		)
	}

	var betCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM wager_transactions
		WHERE wallet_id = $1
		  AND kind = 'BET'
		`,
		createdWallet.WalletID,
	).Scan(&betCount)
	if err != nil {
		t.Fatalf("count BET transactions: %v", err)
	}

	if betCount != 1 {
		t.Fatalf(
			"expected exactly 1 BET after duplicate delivery, got %d",
			betCount,
		)
	}

	var debitCount int

	err = pool.QueryRow(
		ctx,
		`
		SELECT COUNT(*)
		FROM ledger_entries
		WHERE wallet_id = $1
		  AND direction = 'DEBIT'
		`,
		createdWallet.WalletID,
	).Scan(&debitCount)
	if err != nil {
		t.Fatalf("count debit entries: %v", err)
	}

	if debitCount != 1 {
		t.Fatalf(
			"expected exactly 1 debit after duplicate delivery, got %d",
			debitCount,
		)
	}
}

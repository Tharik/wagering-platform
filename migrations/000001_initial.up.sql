CREATE TABLE wallets (
    id UUID PRIMARY KEY,
    player_id VARCHAR(100) NOT NULL,
    currency CHAR(3) NOT NULL,
    balance BIGINT NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,

    CONSTRAINT wallets_balance_non_negative
        CHECK (balance >= 0),

    CONSTRAINT wallets_version_positive
        CHECK (version >= 1),

    CONSTRAINT wallets_player_currency_unique
        UNIQUE (player_id, currency)
);

CREATE TYPE wager_transaction_kind AS ENUM (
    'OPENING',
    'BET',
    'WIN',
    'LOSS',
    'REFUND',
    'ROLLBACK'
);

CREATE TYPE wager_transaction_state AS ENUM (
    'PENDING',
    'PENDING_REFERENCE',
    'PROCESSED',
    'REJECTED',
    'FAILED'
);

CREATE TABLE wager_transactions (
    id UUID PRIMARY KEY,

    provider_id VARCHAR(100),
    external_transaction_id VARCHAR(100),
    idempotency_key VARCHAR(255),
    payload_hash CHAR(64),

    wallet_id UUID NOT NULL REFERENCES wallets(id),
    player_id VARCHAR(100) NOT NULL,
    round_id VARCHAR(100),
    game_id VARCHAR(100),

    kind wager_transaction_kind NOT NULL,
    state wager_transaction_state NOT NULL,

    amount BIGINT NOT NULL,
    currency CHAR(3) NOT NULL,

    reference_external_transaction_id VARCHAR(100),
    referenced_transaction_id UUID REFERENCES wager_transactions(id),

    failure_code VARCHAR(100),
    result_balance BIGINT,

    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,

    CONSTRAINT wager_external_identity_unique
        UNIQUE (provider_id, external_transaction_id),

    CONSTRAINT wager_idempotency_unique
        UNIQUE (provider_id, idempotency_key),

    CONSTRAINT wager_external_amount_non_negative
        CHECK (
            kind = 'OPENING'
            OR amount >= 0
        )
);

CREATE TYPE ledger_direction AS ENUM (
    'DEBIT',
    'CREDIT'
);

CREATE TABLE ledger_entries (
    id UUID PRIMARY KEY,
    wallet_id UUID NOT NULL REFERENCES wallets(id),
    transaction_id UUID NOT NULL REFERENCES wager_transactions(id),

    direction ledger_direction NOT NULL,
    amount BIGINT NOT NULL CHECK (amount > 0),

    balance_before BIGINT NOT NULL CHECK (balance_before >= 0),
    balance_after BIGINT NOT NULL CHECK (balance_after >= 0),

    created_at TIMESTAMPTZ NOT NULL,

    CONSTRAINT ledger_wallet_transaction_unique
        UNIQUE (wallet_id, transaction_id),

    CONSTRAINT ledger_arithmetic_valid CHECK (
        (
            direction = 'DEBIT'
            AND balance_after = balance_before - amount
        )
        OR
        (
            direction = 'CREDIT'
            AND balance_after = balance_before + amount
        )
    )
);

CREATE TABLE inbox_messages (
    consumer_name VARCHAR(100) NOT NULL,
    message_id VARCHAR(255) NOT NULL,
    payload_hash CHAR(64) NOT NULL,

    received_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,

    PRIMARY KEY (consumer_name, message_id)
);

CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,

    occurred_at TIMESTAMPTZ NOT NULL,

    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,

    CONSTRAINT outbox_attempts_non_negative
        CHECK (attempts >= 0)
);

CREATE INDEX outbox_pending_idx
    ON outbox_events (next_attempt_at)
    WHERE published_at IS NULL;

CREATE INDEX wager_pending_reference_idx
    ON wager_transactions (updated_at)
    WHERE state = 'PENDING_REFERENCE';

CREATE INDEX ledger_wallet_created_idx
    ON ledger_entries (wallet_id, created_at, id);
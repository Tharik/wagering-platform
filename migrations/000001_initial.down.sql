DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS inbox_messages;
DROP TABLE IF EXISTS ledger_entries;
DROP TABLE IF EXISTS wager_transactions;
DROP TABLE IF EXISTS wallets;

DROP TYPE IF EXISTS ledger_direction;
DROP TYPE IF EXISTS wager_transaction_state;
DROP TYPE IF EXISTS wager_transaction_kind;
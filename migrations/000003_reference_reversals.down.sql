DROP INDEX IF EXISTS ux_wager_transactions_wallet_opening;
DROP INDEX IF EXISTS ix_wager_transactions_pending_reference_retry;
DROP INDEX IF EXISTS ux_wager_transactions_processed_rollback;
DROP INDEX IF EXISTS ux_wager_transactions_processed_refund;

ALTER TABLE wager_transactions
    DROP CONSTRAINT IF EXISTS wager_transactions_reference_kind_check,
    DROP CONSTRAINT IF EXISTS wager_transactions_reference_attempts_non_negative;

ALTER TABLE wager_transactions
    DROP COLUMN IF EXISTS reference_expires_at,
    DROP COLUMN IF EXISTS reference_next_attempt_at,
    DROP COLUMN IF EXISTS reference_attempts;
-- Metadata used to retry operations whose referenced transaction
-- has not arrived yet.
ALTER TABLE wager_transactions
    ADD COLUMN reference_attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN reference_next_attempt_at TIMESTAMPTZ,
    ADD COLUMN reference_expires_at TIMESTAMPTZ;

ALTER TABLE wager_transactions
    ADD CONSTRAINT wager_transactions_reference_attempts_non_negative
        CHECK (reference_attempts >= 0);

-- Only reversal operations may reference another wager transaction.
ALTER TABLE wager_transactions
    ADD CONSTRAINT wager_transactions_reference_kind_check
        CHECK (
            referenced_transaction_id IS NULL
            OR kind IN ('REFUND', 'ROLLBACK')
        );

-- Once the referenced transaction has been resolved, prevent two
-- successfully processed REFUNDs for the same transaction.
CREATE UNIQUE INDEX ux_wager_transactions_processed_refund
    ON wager_transactions (referenced_transaction_id)
    WHERE kind = 'REFUND'
      AND state = 'PROCESSED'
      AND referenced_transaction_id IS NOT NULL;

-- Likewise, prevent two successfully processed ROLLBACKs for the
-- same referenced transaction.
CREATE UNIQUE INDEX ux_wager_transactions_processed_rollback
    ON wager_transactions (referenced_transaction_id)
    WHERE kind = 'ROLLBACK'
      AND state = 'PROCESSED'
      AND referenced_transaction_id IS NOT NULL;

-- Worker lookup for unresolved references.
CREATE INDEX ix_wager_transactions_pending_reference_retry
    ON wager_transactions (
        reference_next_attempt_at,
        reference_expires_at
    )
    WHERE state = 'PENDING_REFERENCE';

-- An OPENING transaction is an internal representation of the
-- wallet's initial funding. A wallet must never have two of them.
CREATE UNIQUE INDEX ux_wager_transactions_wallet_opening
    ON wager_transactions (wallet_id)
    WHERE kind = 'OPENING';
DROP INDEX IF EXISTS wager_single_processed_reversal_idx;

CREATE UNIQUE INDEX ux_wager_transactions_processed_refund
    ON wager_transactions (referenced_transaction_id)
    WHERE kind = 'REFUND'
      AND state = 'PROCESSED'
      AND referenced_transaction_id IS NOT NULL;

CREATE UNIQUE INDEX ux_wager_transactions_processed_rollback
    ON wager_transactions (referenced_transaction_id)
    WHERE kind = 'ROLLBACK'
      AND state = 'PROCESSED'
      AND referenced_transaction_id IS NOT NULL;
CREATE UNIQUE INDEX wager_single_processed_reversal_idx
    ON wager_transactions (referenced_transaction_id)
    WHERE state = 'PROCESSED'
      AND kind IN ('REFUND', 'ROLLBACK')
      AND referenced_transaction_id IS NOT NULL;

DROP INDEX IF EXISTS ux_wager_transactions_processed_refund;
DROP INDEX IF EXISTS ux_wager_transactions_processed_rollback;
ALTER TABLE wager_transactions
    DROP CONSTRAINT wager_transactions_reference_kind_check;

ALTER TABLE wager_transactions
    ADD CONSTRAINT wager_transactions_reference_kind_check
        CHECK (
            referenced_transaction_id IS NULL
            OR kind IN ('REFUND', 'ROLLBACK')
        );

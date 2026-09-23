ALTER TABLE wager_transactions
    DROP CONSTRAINT wager_transactions_result_balance_non_negative,
    DROP CONSTRAINT wager_transactions_reference_applicability_check,
    DROP CONSTRAINT wager_transactions_amount_by_kind_check,
    DROP CONSTRAINT wager_transactions_origin_metadata_check;

ALTER TABLE wager_transactions
    ADD CONSTRAINT wager_transactions_origin_metadata_check
        CHECK (
            (
                kind = 'OPENING'
                AND state = 'PROCESSED'
                AND provider_id IS NULL
                AND external_transaction_id IS NULL
                AND idempotency_key IS NULL
                AND payload_hash IS NULL
                AND round_id IS NULL
                AND game_id IS NULL
                AND reference_external_transaction_id IS NULL
                AND referenced_transaction_id IS NULL
                AND failure_code IS NULL
                AND result_balance IS NOT NULL
                AND result_balance = amount
                AND player_id <> ''
                AND BTRIM(currency) <> ''
            )
            OR
            (
                kind IN ('BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')
                AND provider_id IS NOT NULL
                AND provider_id <> ''
                AND external_transaction_id IS NOT NULL
                AND external_transaction_id <> ''
                AND idempotency_key IS NOT NULL
                AND idempotency_key <> ''
                AND payload_hash IS NOT NULL
                AND BTRIM(payload_hash) <> ''
                AND player_id <> ''
                AND round_id IS NOT NULL
                AND round_id <> ''
                AND game_id IS NOT NULL
                AND game_id <> ''
                AND BTRIM(currency) <> ''
            )
        ),
    ADD CONSTRAINT wager_transactions_amount_by_kind_check
        CHECK (
            (kind = 'LOSS' AND amount = 0)
            OR
            (
                kind IN ('OPENING', 'BET', 'WIN', 'REFUND', 'ROLLBACK')
                AND amount > 0
            )
        ),
    ADD CONSTRAINT wager_transactions_reference_applicability_check
        CHECK (
            (
                kind = 'OPENING'
                AND reference_external_transaction_id IS NULL
                AND referenced_transaction_id IS NULL
            )
            OR
            (
                kind IN ('BET', 'LOSS')
                AND COALESCE(reference_external_transaction_id, '') = ''
                AND referenced_transaction_id IS NULL
            )
            OR
            (
                kind = 'WIN'
                AND (
                    referenced_transaction_id IS NULL
                    OR (
                        reference_external_transaction_id IS NOT NULL
                        AND reference_external_transaction_id <> ''
                    )
                )
            )
            OR
            (
                kind IN ('REFUND', 'ROLLBACK')
                AND reference_external_transaction_id IS NOT NULL
                AND reference_external_transaction_id <> ''
            )
        ),
    ADD CONSTRAINT wager_transactions_result_balance_non_negative
        CHECK (
            result_balance IS NULL
            OR result_balance >= 0
        );

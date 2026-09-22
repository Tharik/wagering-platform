-- Preserve transport correlation metadata for transactions that may be
-- completed asynchronously by the pending-reference resolver.
--
-- These fields are intentionally nullable because internal transactions
-- such as OPENING do not necessarily have transport causation metadata,
-- and existing rows predate this migration.

ALTER TABLE wager_transactions
    ADD COLUMN correlation_id VARCHAR(255),
    ADD COLUMN causation_id VARCHAR(255);

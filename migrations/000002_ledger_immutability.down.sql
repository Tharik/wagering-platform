DROP TRIGGER IF EXISTS ledger_entries_prevent_update
    ON ledger_entries;

DROP TRIGGER IF EXISTS ledger_entries_prevent_delete
    ON ledger_entries;

DROP FUNCTION IF EXISTS prevent_ledger_mutation();
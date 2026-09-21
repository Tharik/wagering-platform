CREATE OR REPLACE FUNCTION prevent_ledger_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'ledger entries are immutable';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ledger_entries_prevent_update
BEFORE UPDATE ON ledger_entries
FOR EACH ROW
EXECUTE FUNCTION prevent_ledger_mutation();

CREATE TRIGGER ledger_entries_prevent_delete
BEFORE DELETE ON ledger_entries
FOR EACH ROW
EXECUTE FUNCTION prevent_ledger_mutation();
-- Down: drop lifecycle.contracts table
DROP TABLE IF EXISTS lifecycle.contracts CASCADE;
DROP FUNCTION IF EXISTS lifecycle.contracts_audit_timestamp() CASCADE;

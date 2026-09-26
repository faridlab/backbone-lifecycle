-- Down: drop lifecycle.discipline_records table
DROP TABLE IF EXISTS lifecycle.discipline_records CASCADE;
DROP FUNCTION IF EXISTS lifecycle.discipline_records_audit_timestamp() CASCADE;

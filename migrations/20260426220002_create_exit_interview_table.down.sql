-- Down: drop lifecycle.exit_interviews table
DROP TABLE IF EXISTS lifecycle.exit_interviews CASCADE;
DROP FUNCTION IF EXISTS lifecycle.exit_interviews_audit_timestamp() CASCADE;

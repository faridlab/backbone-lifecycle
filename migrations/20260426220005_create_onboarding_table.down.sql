-- Down: drop lifecycle.onboardings table
DROP TABLE IF EXISTS lifecycle.onboardings CASCADE;
DROP FUNCTION IF EXISTS lifecycle.onboardings_audit_timestamp() CASCADE;

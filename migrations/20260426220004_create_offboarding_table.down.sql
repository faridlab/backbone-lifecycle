-- Down: drop lifecycle.offboardings table
DROP TABLE IF EXISTS lifecycle.offboardings CASCADE;
DROP FUNCTION IF EXISTS lifecycle.offboardings_audit_timestamp() CASCADE;

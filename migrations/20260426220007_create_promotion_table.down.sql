-- Down: drop lifecycle.promotions table
DROP TABLE IF EXISTS lifecycle.promotions CASCADE;
DROP FUNCTION IF EXISTS lifecycle.promotions_audit_timestamp() CASCADE;

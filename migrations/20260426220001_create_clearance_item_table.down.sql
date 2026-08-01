-- Down: drop lifecycle.clearance_items table
DROP TABLE IF EXISTS lifecycle.clearance_items CASCADE;
DROP FUNCTION IF EXISTS lifecycle.clearance_items_audit_timestamp() CASCADE;

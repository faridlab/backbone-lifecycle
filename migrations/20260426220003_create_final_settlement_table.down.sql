-- Down: drop lifecycle.final_settlements table
DROP TABLE IF EXISTS lifecycle.final_settlements CASCADE;
DROP FUNCTION IF EXISTS lifecycle.final_settlements_audit_timestamp() CASCADE;

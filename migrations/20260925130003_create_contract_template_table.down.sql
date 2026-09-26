-- Down: drop lifecycle.contract_templates table
DROP TABLE IF EXISTS lifecycle.contract_templates CASCADE;
DROP FUNCTION IF EXISTS lifecycle.contract_templates_audit_timestamp() CASCADE;

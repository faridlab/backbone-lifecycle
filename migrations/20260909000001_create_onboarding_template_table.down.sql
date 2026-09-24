-- Down: drop lifecycle.onboarding_templates table
DROP TABLE IF EXISTS lifecycle.onboarding_templates CASCADE;
DROP FUNCTION IF EXISTS lifecycle.onboarding_templates_audit_timestamp() CASCADE;

-- Down: drop lifecycle.onboarding_template_tasks table
DROP TABLE IF EXISTS lifecycle.onboarding_template_tasks CASCADE;
DROP FUNCTION IF EXISTS lifecycle.onboarding_template_tasks_audit_timestamp() CASCADE;

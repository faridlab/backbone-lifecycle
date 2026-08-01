-- Down: drop lifecycle.onboarding_tasks table
DROP TABLE IF EXISTS lifecycle.onboarding_tasks CASCADE;
DROP FUNCTION IF EXISTS lifecycle.onboarding_tasks_audit_timestamp() CASCADE;

-- Down: drop enum types for lifecycle module
DROP TYPE IF EXISTS promotion_status CASCADE;
DROP TYPE IF EXISTS promotion_type CASCADE;
DROP TYPE IF EXISTS task_status CASCADE;
DROP TYPE IF EXISTS task_category CASCADE;
DROP TYPE IF EXISTS onboarding_status CASCADE;
DROP TYPE IF EXISTS offboarding_status CASCADE;
DROP TYPE IF EXISTS offboarding_reason CASCADE;
DROP TYPE IF EXISTS settlement_status CASCADE;
DROP TYPE IF EXISTS clearance_status CASCADE;

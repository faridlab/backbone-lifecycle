-- Probation confirmation on onboardings + GL linkage on final settlements.
--
-- Two related changes:
--
-- 1) onboardings gains the probation plan and its confirmation stamp. The
--    confirmation verb is allowed on/after probation_end_date (or early with
--    an operator override) and stamps confirmed_at exactly once — the stamp
--    doubles as the producer-side idempotency guard for the emitted event.
--
-- 2) final_settlements gains the accounting linkage columns filled by the
--    confirm verb (the GL post + journal the confirmation produced), plus a
--    partial unique index enforcing one live settlement per offboarding:
--    double-drafting the same exit is a client error, not a second row.

ALTER TABLE lifecycle.onboardings ADD COLUMN IF NOT EXISTS probation_end_date DATE;
ALTER TABLE lifecycle.onboardings ADD COLUMN IF NOT EXISTS confirmed_at TIMESTAMPTZ;

ALTER TABLE lifecycle.final_settlements ADD COLUMN IF NOT EXISTS accounting_post_id UUID;
ALTER TABLE lifecycle.final_settlements ADD COLUMN IF NOT EXISTS journal_id UUID;

DROP INDEX IF EXISTS lifecycle.uq_final_settlements_offboarding;
CREATE UNIQUE INDEX uq_final_settlements_offboarding
    ON lifecycle.final_settlements (company_id, offboarding_id)
    WHERE (metadata->>'deleted_at') IS NULL;

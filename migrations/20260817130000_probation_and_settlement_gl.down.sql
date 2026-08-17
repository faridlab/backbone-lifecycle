-- Revert the probation/settlement-GL increment: drop the settlement
-- uniqueness, the GL linkage columns, and the probation columns. Rows that
-- carried confirmation stamps lose them — the revert is lossy by design (the
-- increment was never deployed with data).

DROP INDEX IF EXISTS lifecycle.uq_final_settlements_offboarding;

ALTER TABLE lifecycle.final_settlements DROP COLUMN IF EXISTS journal_id;
ALTER TABLE lifecycle.final_settlements DROP COLUMN IF EXISTS accounting_post_id;

ALTER TABLE lifecycle.onboardings DROP COLUMN IF EXISTS confirmed_at;
ALTER TABLE lifecycle.onboardings DROP COLUMN IF EXISTS probation_end_date;

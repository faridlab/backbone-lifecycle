-- Hand-authored (user-owned). Not regenerated.
--
-- Best-effort restore sketch for the tenancy strip (ADR-0029). This is a breaking module
-- release against dev-stage databases: the down re-adds the company_id column as nullable
-- with its plain indexes and the company isolation policy shape, but restores NO data —
-- rows written after the strip (or after the decorator re-keyed them) carry org_unit_id
-- only. The composing service's tenancy decorator remains the live fence; treat this
-- down as a schema-shape sketch for archaeology, not a usable rollback.

ALTER TABLE lifecycle.clearance_items   ADD COLUMN IF NOT EXISTS company_id uuid;
ALTER TABLE lifecycle.exit_interviews   ADD COLUMN IF NOT EXISTS company_id uuid;
ALTER TABLE lifecycle.final_settlements ADD COLUMN IF NOT EXISTS company_id uuid;
ALTER TABLE lifecycle.offboardings      ADD COLUMN IF NOT EXISTS company_id uuid;
ALTER TABLE lifecycle.onboardings       ADD COLUMN IF NOT EXISTS company_id uuid;
ALTER TABLE lifecycle.onboarding_tasks  ADD COLUMN IF NOT EXISTS company_id uuid;
ALTER TABLE lifecycle.promotions        ADD COLUMN IF NOT EXISTS company_id uuid;

-- The strip's restored tenant-free settlement unique goes away again (the
-- company-leading variant would need company data this sketch does not restore).
DROP INDEX IF EXISTS lifecycle.uq_final_settlements_offboarding;

CREATE INDEX IF NOT EXISTS idx_clearance_items_company_id   ON lifecycle.clearance_items (company_id);
CREATE INDEX IF NOT EXISTS idx_exit_interviews_company_id   ON lifecycle.exit_interviews (company_id);
CREATE INDEX IF NOT EXISTS idx_final_settlements_company_id ON lifecycle.final_settlements (company_id);
CREATE INDEX IF NOT EXISTS idx_offboardings_company_id      ON lifecycle.offboardings (company_id);
CREATE INDEX IF NOT EXISTS idx_onboardings_company_id       ON lifecycle.onboardings (company_id);
CREATE INDEX IF NOT EXISTS idx_onboarding_tasks_company_id  ON lifecycle.onboarding_tasks (company_id);
CREATE INDEX IF NOT EXISTS idx_promotions_company_id        ON lifecycle.promotions (company_id);

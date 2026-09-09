-- Hand-authored (user-owned). Not regenerated.
--
-- Strip every company-fence artifact from the lifecycle tables (ADR-0029): the module is
-- tenant-agnostic; org scoping is installed by the COMPOSING service's tenancy decorator,
-- never by the module. Dropped here, per table: the company-leading indexes, the
-- <table>_company_isolation RLS policy, and the company_id column itself.
--
-- Ordering guard (the decorator must run FIRST on any database with data): the module
-- never moves tenancy data. A table is safe to strip when EITHER
--   a) it carries org_unit_id with no NULLs — the decorator backfilled it from company_id —
--      or b) it is empty (a fresh database: the earlier chain files created it empty).
-- Otherwise the strip RAISEs, naming the decorator step, rather than dropping a column
-- that still holds the only tenancy key. The file is re-runnable (every drop is IF EXISTS
-- and the tracker has no checksums), so a failed run retries cleanly after the decorator
-- lands.
--
-- RLS enable/force flags are deliberately NOT touched: the decorator owns those now.
-- `lifecycle.outbox_events` / `lifecycle.inbox_consumed` are framework relay
-- infrastructure and keep their company axis by design — untouched here.

DO $$
DECLARE
    t text;
    has_org boolean;
    org_nulls bigint;
    total bigint;
    offenders text := '';
BEGIN
    FOREACH t IN ARRAY ARRAY['clearance_items', 'exit_interviews', 'final_settlements', 'offboardings', 'onboardings', 'onboarding_tasks', 'promotions']
    LOOP
        IF to_regclass(format('lifecycle.%I', t)) IS NULL THEN
            CONTINUE; -- chain not fully applied on this database; nothing to strip
        END IF;

        SELECT EXISTS (
                   SELECT 1 FROM information_schema.columns
                   WHERE table_schema = 'lifecycle' AND table_name = t AND column_name = 'org_unit_id'
               )
        INTO has_org;

        EXECUTE format('SELECT count(*) FROM lifecycle.%I', t) INTO total;

        IF has_org THEN
            EXECUTE format(
                'SELECT count(*) FROM lifecycle.%I WHERE org_unit_id IS NULL', t)
            INTO org_nulls;
        ELSE
            org_nulls := total; -- no org column: every row's only tenancy key is company_id
        END IF;

        IF has_org AND org_nulls = 0 THEN
            CONTINUE; -- decorator backfilled: safe
        END IF;
        IF total = 0 THEN
            CONTINUE; -- empty table (fresh database): safe
        END IF;
        offenders := offenders || format(' lifecycle.%s (%s rows, %s rows not covered by org_unit_id);', t, total, org_nulls);
    END LOOP;

    IF offenders <> '' THEN
        RAISE EXCEPTION 'refusing to strip company_id — these tables are not yet covered by the tenancy decorator:%. Apply the composing service''s tenancy decorator (it backfills org_unit_id from company_id) and re-run; it is the only step that moves tenancy data.', offenders;
    END IF;
END $$;

-- ── clearance_items ────────────────────────────────────────────────────────────
DROP INDEX IF EXISTS lifecycle.idx_clearance_items_company_id;
DROP POLICY IF EXISTS clearance_items_company_isolation ON lifecycle.clearance_items;
ALTER TABLE lifecycle.clearance_items DROP COLUMN IF EXISTS company_id;

-- ── exit_interviews ────────────────────────────────────────────────────────────
DROP INDEX IF EXISTS lifecycle.idx_exit_interviews_company_id;
DROP INDEX IF EXISTS lifecycle.idx_exit_interviews_company_id_employee_id;
DROP POLICY IF EXISTS exit_interviews_company_isolation ON lifecycle.exit_interviews;
ALTER TABLE lifecycle.exit_interviews DROP COLUMN IF EXISTS company_id;

-- ── final_settlements ──────────────────────────────────────────────────────────
DROP INDEX IF EXISTS lifecycle.idx_final_settlements_company_id;
DROP INDEX IF EXISTS lifecycle.idx_final_settlements_company_id_employee_id;
DROP INDEX IF EXISTS lifecycle.uq_final_settlements_offboarding;
DROP POLICY IF EXISTS final_settlements_company_isolation ON lifecycle.final_settlements;
ALTER TABLE lifecycle.final_settlements DROP COLUMN IF EXISTS company_id;

-- ── offboardings ───────────────────────────────────────────────────────────────
DROP INDEX IF EXISTS lifecycle.idx_offboardings_company_id;
DROP INDEX IF EXISTS lifecycle.idx_offboardings_company_id_employee_id;
DROP POLICY IF EXISTS offboardings_company_isolation ON lifecycle.offboardings;
ALTER TABLE lifecycle.offboardings DROP COLUMN IF EXISTS company_id;

-- ── onboardings ────────────────────────────────────────────────────────────────
DROP INDEX IF EXISTS lifecycle.idx_onboardings_company_id;
DROP INDEX IF EXISTS lifecycle.idx_onboardings_company_id_employee_id;
DROP POLICY IF EXISTS onboardings_company_isolation ON lifecycle.onboardings;
ALTER TABLE lifecycle.onboardings DROP COLUMN IF EXISTS company_id;

-- ── onboarding_tasks ───────────────────────────────────────────────────────────
DROP INDEX IF EXISTS lifecycle.idx_onboarding_tasks_company_id;
DROP POLICY IF EXISTS onboarding_tasks_company_isolation ON lifecycle.onboarding_tasks;
ALTER TABLE lifecycle.onboarding_tasks DROP COLUMN IF EXISTS company_id;

-- ── promotions ─────────────────────────────────────────────────────────────────
DROP INDEX IF EXISTS lifecycle.idx_promotions_company_id_employee_id_effective_date;
DROP POLICY IF EXISTS promotions_company_isolation ON lifecycle.promotions;
ALTER TABLE lifecycle.promotions DROP COLUMN IF EXISTS company_id;

-- ── Restore the domain one-settlement-per-offboarding unique (tenant-free) ─────
-- One live settlement per offboarding is a DOMAIN invariant, not a tenancy posture:
-- an offboarding is one row (id is the PK) living in exactly one unit under any
-- deployment, so the per-offboarding unique needs no tenant column. It had only ever
-- existed in company-leading form (added with the settlement/GL work); this restores
-- it under the same name and soft-delete predicate in tenant-free shape — the exact
-- conflict target the draft verb's ON CONFLICT clause names.
CREATE UNIQUE INDEX IF NOT EXISTS uq_final_settlements_offboarding
    ON lifecycle.final_settlements (offboarding_id)
    WHERE (metadata ->> 'deleted_at') IS NULL;

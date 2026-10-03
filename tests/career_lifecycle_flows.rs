//! Integration tests for the three career-lifecycle compound events (ADR-005).
//!
//! Proves the full producer→relay→consumer flow + idempotency for each, over real Postgres:
//!
//! 1. `promotion.effective`  → employee.employment_histories + payroll.compensation_changes
//! 2. `onboarding.completed` → employee.employments.status='active'
//! 3. `offboarding.closed`   → employee.employments.status='inactive' + payroll.compensation_changes
//!
//! Each test: seeds the lifecycle row → calls the PRODUCER (state change + `lifecycle.outbox_events`
//! stage in one tx) → runs the RELAY ([`backbone_outbox::relay::drain_once`]) through an
//! [`IntegrationEventBus`] carrying the consumer handler(s), exactly as the composer's bus does →
//! asserts the target effects → replays the SAME event id (redelivery) and asserts NO second effect
//! (inbox dedup makes the effect exactly-once).
//!
//! Hermetic about schema: builds the minimal DDL each flow touches inline (the producer/consumer SQL
//! is schema-pinned, so the real module tables are exercised). SKIPS (not fails) when no DB is
//! reachable; set `DATABASE_URL` to run it for real.
//!
//! Tenancy (ADR-0029): the module is tenant-agnostic — its tables carry no scoping column, and
//! every producer verb reads the company id its still-company-keyed seams need (the outbox
//! record, the event payload's `company_id`, the cross-module pesangon input reads) off the
//! ambient org request scope. The suite binds that scope around each producer call with
//! [`scoped`] — exactly what the composing service's middleware does in production. The consumer
//! tables (employee / payroll / timeoff) keep their `company_id` columns and seeds: those
//! siblings are not stripped and their handlers read the payload's `company_id`.

use backbone_lifecycle::application::service::{
    OffboardingWriteService, OnboardingWriteService, PromotionWriteService,
};
use backbone_messaging::{IntegrationEventBus, IntegrationEventEnvelope, IntegrationEventHandler};
use backbone_orm::org_scope::{with_org_request_scope, OrgScope};
use backbone_outbox::{inbox, outbox, relay, OutboxRecord};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::{PgPool, Row};
use uuid::Uuid;

/// Run a producer verb under an org request scope bound to `company_id` — the composing
/// service's middleware does the same in production. The scope carries the legacy company
/// the producer hands to its still-company-keyed seams.
async fn scoped<R>(
    pool: &PgPool,
    company_id: Uuid,
    f: impl std::future::Future<Output = R>,
) -> Result<R, sqlx::Error> {
    with_org_request_scope(pool, OrgScope::for_company_unit(company_id), f).await
}

/// Connect to a scratch DB this test owns, or `None` to skip.
///
/// The suite builds its own minimal DDL (see [`setup`]), which must NOT run against a
/// fully-migrated database: the real migrations carry stricter constraints than the
/// best-effort shapes here, and the two disagree (a migrated `promotions.title`-style
/// NOT NULL, for instance, breaks the hermetic seeds). So each test provisions a
/// dedicated database it drops and recreates on every run — hermetic by construction.
///
/// One database PER TEST, named after the test: cargo runs the tests of this file in
/// parallel, and a single shared scratch database made them drop each other's database
/// and truncate each other's rows mid-flight (the losers then skipped, which read as
/// green). The test harness names each test's thread after the test, and a
/// `#[tokio::test]` body runs on that thread, so the name is available here.
///
/// Set `LIFECYCLE_REQUIRE_DB=1` (CI does) to turn any skip into a hard failure —
/// a silently-skipped suite must never read as "tests green" on a machine that
/// simply lost its database.
async fn connect() -> Option<PgPool> {
    fn db_required() -> bool {
        std::env::var("LIFECYCLE_REQUIRE_DB").map(|v| v != "0").unwrap_or(false)
    }
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/backbone_hr".into());
    let Some((prefix, _)) = url.trim_end_matches('/').rsplit_once('/') else {
        eprintln!("skip career_lifecycle_flows: DATABASE_URL has no database segment");
        if db_required() {
            panic!("career_lifecycle_flows would skip but LIFECYCLE_REQUIRE_DB is set");
        }
        return None;
    };
    let admin = match PgPool::connect(&format!("{prefix}/postgres")).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("skip career_lifecycle_flows: no admin connection from `{prefix}` ({e})");
            if db_required() {
                panic!("career_lifecycle_flows would skip but LIFECYCLE_REQUIRE_DB is set");
            }
            return None;
        }
    };
    let scratch = scratch_db_name();
    let scratch = scratch.as_str();
    let _ = sqlx::query(&format!(
        r#"DROP DATABASE IF EXISTS "{scratch}" WITH (FORCE)"#
    ))
    .execute(&admin)
    .await;
    if let Err(e) = sqlx::query(&format!(r#"CREATE DATABASE "{scratch}""#))
        .execute(&admin)
        .await
    {
        eprintln!("skip career_lifecycle_flows: could not create `{scratch}` ({e})");
        if db_required() {
            panic!("career_lifecycle_flows would skip but LIFECYCLE_REQUIRE_DB is set");
        }
        return None;
    }
    admin.close().await;
    match PgPool::connect(&format!("{prefix}/{scratch}")).await {
        Ok(p) => Some(p),
        Err(e) => {
            eprintln!("skip career_lifecycle_flows: could not connect to `{scratch}` ({e})");
            if db_required() {
                panic!("career_lifecycle_flows would skip but LIFECYCLE_REQUIRE_DB is set");
            }
            None
        }
    }
}

/// `lifecycle_flows_<test name>`, cut to Postgres' 63-byte identifier limit with a hash of
/// the full name so two long names never collide.
fn scratch_db_name() -> String {
    use std::hash::{Hash, Hasher};
    let test = std::thread::current()
        .name()
        .unwrap_or("main")
        .rsplit("::")
        .next()
        .unwrap_or("main")
        .to_string();
    let mut h = std::collections::hash_map::DefaultHasher::new();
    test.hash(&mut h);
    let short: String = test
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_lowercase() } else { '_' })
        .take(36)
        .collect();
    format!("lifecycle_flows_{short}_{:08x}", h.finish() as u32)
}

/// Run the framework outbox migration, retrying the narrow race where a sibling test's concurrent
/// `CREATE TYPE ... IF NOT EXISTS` slips between our check and insert and the loser gets a pg_type
/// duplicate-key (23505). The loser's next attempt sees everything existing and succeeds — any other
/// error is real.
async fn migrate_outbox_with_race_retry(pool: &PgPool, schema: &str) {
    for attempt in 0..3 {
        match outbox::migrate(pool, schema).await {
            Ok(()) => return,
            Err(e) => {
                let is_race = matches!(&e, backbone_outbox::OutboxError::Db(sqlx::Error::Database(db)) if db.code().as_deref() == Some("23505"));
                if !is_race || attempt == 2 {
                    panic!("outbox migrate {schema}: {e}");
                }
                tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            }
        }
    }
}

/// Build the minimal schema the three flows exercise. Idempotent (CREATE ... IF NOT EXISTS).
///
/// Every shape-creating statement here is run **best-effort** (its error is ignored): cargo runs the
/// tests in parallel, and `CREATE SCHEMA/TABLE/TYPE IF NOT EXISTS` is not atomic against itself — two
/// concurrent setup() calls can both see "not exists", both try to create, and the loser raises a
/// duplicate-key error even though the shape ends up existing. Ignoring the result is the right call
/// because every statement is an "ensure-exists" — a real DDL bug surfaces later as a clear
/// missing-table error in the seed/assert phase. The framework `outbox::migrate` calls get the same
/// treatment via a narrow retry (`migrate_outbox_with_race_retry`) because their internal
/// catch-duplicates logic can still lose the same race on `pg_type`.
async fn setup(pool: &PgPool) -> sqlx::Result<()> {
    let shape_ddl = [
        // Enum types the producer/consumer SQL depends on.
        "CREATE TYPE promotion_type AS ENUM ('promotion','transfer','demotion','lateral')",
        "CREATE TYPE promotion_status AS ENUM ('draft','pending','approved','rejected','effective','cancelled')",
        "CREATE TYPE onboarding_status AS ENUM ('pending','in_progress','completed','abandoned')",
        "CREATE TYPE task_category AS ENUM ('document','equipment','account_access','policy_ack','induction')",
        "CREATE TYPE task_status AS ENUM ('pending','done','skipped','blocked')",
        "CREATE TYPE offboarding_reason AS ENUM ('resignation','termination','end_of_contract','retirement','death','merger_acquisition','efficiency','force_majeure','misconduct')",
        "CREATE TYPE offboarding_status AS ENUM ('initiated','in_progress','cleared','closed')",
        "CREATE TYPE clearance_status AS ENUM ('pending','cleared','blocked')",
        "CREATE TYPE settlement_status AS ENUM ('draft','confirmed','paid','disputed')",
        "CREATE TYPE employment_status AS ENUM ('permanent','contract','probation','associate')",
        "CREATE TYPE employment_state AS ENUM ('active','inactive')",
        "CREATE TYPE employment_action AS ENUM ('hire','transfer','promotion','demotion','role_change','reinstatement')",
        "CREATE TYPE compensation_change_type AS ENUM ('hire','promotion','transfer','adjustment','offboarding')",
        // Schemas.
        "CREATE SCHEMA IF NOT EXISTS lifecycle",
        "CREATE SCHEMA IF NOT EXISTS employee",
        "CREATE SCHEMA IF NOT EXISTS payroll",
        // ── lifecycle.* (the producers read/write these) ──
        r#"CREATE TABLE IF NOT EXISTS lifecycle.promotions (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               employee_id UUID NOT NULL,
               promotion_type promotion_type NOT NULL DEFAULT 'promotion',
               position_id_from UUID, position_id_to UUID,
               level_id_from UUID, level_id_to UUID,
               department_id_from UUID, department_id_to UUID,
               proposed_salary NUMERIC(18,2),
               effective_date DATE NOT NULL,
               status promotion_status NOT NULL DEFAULT 'draft',
               requested_by UUID,
               approved_by UUID,
               reason TEXT,
               metadata JSONB NOT NULL DEFAULT '{}'::jsonb
           )"#,
        r#"CREATE TABLE IF NOT EXISTS lifecycle.onboardings (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               employee_id UUID NOT NULL,
               start_date DATE NOT NULL,
               status onboarding_status NOT NULL DEFAULT 'pending',
               completed_at TIMESTAMPTZ,
               probation_end_date DATE,
               confirmed_at TIMESTAMPTZ,
               template_id UUID,
               metadata JSONB NOT NULL DEFAULT '{}'::jsonb
           )"#,
        r#"CREATE TABLE IF NOT EXISTS lifecycle.onboarding_tasks (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               onboarding_id UUID NOT NULL,
               title TEXT NOT NULL,
               category task_category,
               owner_employee_id UUID,
               due_date DATE,
               status task_status NOT NULL DEFAULT 'pending',
               metadata JSONB NOT NULL DEFAULT '{}'::jsonb
           )"#,
        r#"CREATE TABLE IF NOT EXISTS lifecycle.offboardings (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               employee_id UUID NOT NULL,
               reason offboarding_reason NOT NULL DEFAULT 'resignation',
               notice_date DATE NOT NULL,
               last_working_day DATE NOT NULL,
               status offboarding_status NOT NULL DEFAULT 'initiated',
               metadata JSONB NOT NULL DEFAULT '{}'::jsonb
           )"#,
        r#"CREATE TABLE IF NOT EXISTS lifecycle.clearance_items (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               offboarding_id UUID NOT NULL,
               title TEXT NOT NULL,
               clearer_employee_id UUID,
               status clearance_status NOT NULL DEFAULT 'pending',
               metadata JSONB NOT NULL DEFAULT '{}'::jsonb
           )"#,
        r#"CREATE TABLE IF NOT EXISTS lifecycle.final_settlements (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               employee_id UUID NOT NULL,
               offboarding_id UUID NOT NULL,
               period TEXT NOT NULL,
               base_pay NUMERIC(18,2) NOT NULL,
               unused_leave_payout NUMERIC(18,2),
               pesangon_amount NUMERIC(18,2),
               tax_deduction NUMERIC(18,2),
               net_payable NUMERIC(18,2) NOT NULL,
               status settlement_status NOT NULL DEFAULT 'draft',
               accounting_post_id UUID,
               journal_id UUID,
               metadata JSONB NOT NULL DEFAULT '{}'::jsonb
           )"#,
        // One live settlement per offboarding — the idempotency the draft verb surfaces as 409.
        // Tenant-free (ADR-0029): the domain invariant needs no tenant column.
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_final_settlements_offboarding ON lifecycle.final_settlements (offboarding_id) WHERE (metadata->>'deleted_at') IS NULL",
        // ── employee.* (the employee consumers write these). The employments shape is a SUPERSET of
        //    backbone-recruitment's hire_flow test (department_id/position_id) so the two hermetic
        //    suites coexist on a shared DB — CREATE TABLE IF NOT EXISTS no-ops on whichever runs second,
        //    so the union of columns must satisfy both consumers. ──
        r#"CREATE TABLE IF NOT EXISTS employee.employments (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               company_id UUID NOT NULL,
               employee_id UUID NOT NULL,
               employment_status employment_status NOT NULL DEFAULT 'permanent',
               join_date DATE NOT NULL,
               department_id UUID,
               level_id UUID,
               position_id UUID,
               status employment_state NOT NULL DEFAULT 'active',
               date_of_exit DATE,
               metadata JSONB NOT NULL DEFAULT '{}'::jsonb
           )"#,
        r#"CREATE TABLE IF NOT EXISTS employee.employment_histories (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               company_id UUID NOT NULL,
               employee_id UUID NOT NULL,
               effective_date DATE NOT NULL,
               action employment_action NOT NULL,
               position_id_from UUID, position_id_to UUID,
               level_id_from UUID, level_id_to UUID,
               department_id_from UUID, department_id_to UUID,
               reference_id UUID,
               note TEXT,
               metadata JSONB NOT NULL DEFAULT '{}'::jsonb
           )"#,
        // ── employee.employees (the people master). The ADR-005 onboarding-enroll consumer reads the
        //    joiner's starting salary off `base_salary` here (a pool-backed port — no Cargo dep on
        //    backbone-employee). The shape matches the canonical migration + the ADR-005 base_salary
        //    column (nullable: NULL → the consumer claim-but-skips). ──
        r#"CREATE TABLE IF NOT EXISTS employee.employees (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               company_id UUID NOT NULL,
               employee_number TEXT NOT NULL,
               first_name TEXT NOT NULL,
               base_salary NUMERIC(18,2),
               metadata JSONB NOT NULL DEFAULT '{}'::jsonb
           )"#,
        // ── payroll.* (the payroll consumers write these) ──
        r#"CREATE TABLE IF NOT EXISTS payroll.compensation_changes (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               company_id UUID NOT NULL,
               employee_id UUID NOT NULL,
               change_type compensation_change_type NOT NULL,
               new_amount NUMERIC(18,2),
               effective_date DATE,
               reference_id UUID,
               note TEXT,
               metadata JSONB NOT NULL DEFAULT '{}'::jsonb
           )"#,
        // ── timeoff.* (the pesangon producer reads remaining leave from here at close time). ──
        "CREATE SCHEMA IF NOT EXISTS timeoff",
        r#"CREATE TABLE IF NOT EXISTS timeoff.timeoff_balances (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               company_id UUID NOT NULL,
               timeoff_type_id UUID NOT NULL,
               employee_id UUID NOT NULL,
               period TEXT NOT NULL,
               allocated NUMERIC(18,2) NOT NULL CHECK (allocated >= 0),
               used NUMERIC(18,2) NOT NULL DEFAULT 0 CHECK (used >= 0),
               metadata JSONB NOT NULL DEFAULT '{}'::jsonb
           )"#,
    ];
    for stmt in shape_ddl {
        let _ = sqlx::query(stmt).execute(pool).await;
    }

    // The sibling tables are org-unit keyed in production (the settlement input reads fence on
    // `org_unit_id`, and the payroll consumer writes it). The seeds here name a company; a
    // company's own unit carries the company's id, so a fill trigger keeps the two columns equal
    // whichever one an INSERT supplies — and, like the production fill trigger, falls back to
    // the acting unit of the bound org scope when an INSERT supplies neither.
    sqlx::raw_sql(
        r#"CREATE OR REPLACE FUNCTION public.test_fill_org_unit() RETURNS trigger AS $$
           BEGIN
             NEW.org_unit_id := COALESCE(NEW.org_unit_id, NEW.company_id,
                                         NULLIF(current_setting('app.acting_unit_id', true), '')::uuid);
             NEW.company_id := COALESCE(NEW.company_id, NEW.org_unit_id);
             RETURN NEW;
           END $$ LANGUAGE plpgsql"#,
    )
    .execute(pool)
    .await?;
    for table in [
        "employee.employments",
        "employee.employment_histories",
        "employee.employees",
        "payroll.compensation_changes",
        "timeoff.timeoff_balances",
    ] {
        for stmt in [
            format!("ALTER TABLE {table} ADD COLUMN IF NOT EXISTS org_unit_id UUID"),
            format!("ALTER TABLE {table} ALTER COLUMN company_id DROP NOT NULL"),
            format!(
                "CREATE OR REPLACE TRIGGER fill_org_unit BEFORE INSERT ON {table} \
                 FOR EACH ROW EXECUTE FUNCTION public.test_fill_org_unit()"
            ),
        ] {
            sqlx::query(&stmt).execute(pool).await?;
        }
    }

    // Leave types: only annual leave is paid out, and only while unexpired.
    for stmt in [
        r#"CREATE TABLE IF NOT EXISTS timeoff.timeoff_types (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               name TEXT NOT NULL DEFAULT '',
               code TEXT,
               metadata JSONB NOT NULL DEFAULT '{}'::jsonb
           )"#,
        "ALTER TABLE timeoff.timeoff_balances ADD COLUMN IF NOT EXISTS expired_at TIMESTAMPTZ",
        // The composing platform's settings store (the settlement reads its group).
        "CREATE SCHEMA IF NOT EXISTS platform",
        r#"CREATE TABLE IF NOT EXISTS platform.sysparams (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               group_name TEXT NOT NULL,
               key TEXT NOT NULL,
               value TEXT NOT NULL,
               status TEXT NOT NULL DEFAULT 'active',
               UNIQUE (group_name, key)
           )"#,
    ] {
        let _ = sqlx::query(stmt).execute(pool).await;
    }

    // The module's own hand-written migrations for the settlement: the widened reason, the
    // effective-dated PP 35/2021 tables with their seeded law data, and the itemised
    // settlement columns — the shipped files, not copies.
    for sql in [
        include_str!("../migrations/20261003100000_widen_offboarding_reason.up.sql"),
        include_str!("../migrations/20261003100001_create_severance_statutory_tables.up.sql"),
        include_str!("../migrations/20261003100002_add_final_settlement_breakdown.up.sql"),
    ] {
        sqlx::raw_sql(sql).execute(pool).await?;
    }

    // Outbox + inbox tables (framework DDL) in every schema the flows touch. `timeoff` is a
    // CONSUMER schema (its outbox_events is unused) — we migrate it so its `inbox_consumed` exists for
    // the offboarding-encash handler's `inbox::once`.
    for schema in ["lifecycle", "employee", "payroll", "timeoff"] {
        migrate_outbox_with_race_retry(pool, schema).await;
    }

    Ok(())
}

/// Seed a leave type with the given code and return its id (`ANNUAL` is what the settlement pays
/// out by default).
async fn seed_leave_type(pool: &PgPool, code: &str) -> sqlx::Result<Uuid> {
    sqlx::query_scalar("INSERT INTO timeoff.timeoff_types (name, code) VALUES ($1, $1) RETURNING id")
        .bind(code)
        .fetch_one(pool)
        .await
}

/// Isolate a flow from any prior data in the shared shapes.
async fn truncate_all(pool: &PgPool) -> sqlx::Result<()> {
    for stmt in [
        "TRUNCATE lifecycle.promotions, lifecycle.onboardings, lifecycle.onboarding_tasks, lifecycle.offboardings, lifecycle.clearance_items, lifecycle.final_settlements",
        "TRUNCATE employee.employment_histories, employee.employments, employee.employees",
        "TRUNCATE payroll.compensation_changes",
        "TRUNCATE timeoff.timeoff_balances, timeoff.timeoff_types, platform.sysparams",
        "TRUNCATE lifecycle.outbox_events, employee.inbox_consumed, payroll.inbox_consumed, timeoff.inbox_consumed",
    ] {
        sqlx::query(stmt).execute(pool).await?;
    }
    Ok(())
}

/// Drain `lifecycle.outbox_events` through a bus carrying the registered handlers — exactly as the
/// composer's relay wiring does (envelope id = outbox row id = consumer dedup key).
async fn drain_lifecycle(
    pool: &PgPool,
    bus: IntegrationEventBus,
) -> Result<usize, backbone_outbox::OutboxError> {
    relay::drain_once(pool, "lifecycle", 10, move |rec: OutboxRecord| {
        let bus = bus.clone();
        async move {
            let envelope = IntegrationEventEnvelope {
                id: rec.id.to_string(),
                event_type: rec.event_type.clone(),
                source_context: rec.aggregate_type.clone(),
                aggregate_id: rec.aggregate_id.clone(),
                occurred_at: rec.occurred_at,
                published_at: Utc::now(),
                version: rec.version as u32,
                correlation_id: rec.correlation_id.clone(),
                causation_id: rec.causation_id.clone(),
                payload: rec.payload.clone(),
            };
            bus.publish_envelope(envelope)
                .await
                .map_err(|e| backbone_outbox::OutboxError::Publish(format!("lifecycle: {e}")))
        }
    })
    .await
}

/// Re-fetch the staged outbox row and rebuild the envelope — a faithful redelivery for the
/// idempotency replay.
async fn fetch_envelope(pool: &PgPool, event_id: Uuid) -> sqlx::Result<IntegrationEventEnvelope> {
    let row = sqlx::query(
        "SELECT event_type, aggregate_type, aggregate_id, payload, occurred_at, version
         FROM lifecycle.outbox_events WHERE id=$1",
    )
    .bind(event_id)
    .fetch_one(pool)
    .await?;
    Ok(IntegrationEventEnvelope {
        id: event_id.to_string(),
        event_type: row.get("event_type"),
        source_context: row.get("aggregate_type"),
        aggregate_id: row.get("aggregate_id"),
        occurred_at: row.get::<DateTime<Utc>, _>("occurred_at"),
        published_at: Utc::now(),
        version: row.get::<i32, _>("version") as u32,
        correlation_id: None,
        causation_id: None,
        payload: row.get("payload"),
    })
}

// ─────────────────────────────────────────────────────────────────────────────────────────────
// Flow 1: promotion.effective → employee.employment_histories + payroll.compensation_changes
// ─────────────────────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn promotion_effective_flow_applies_both_targets_and_is_idempotent(
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = match connect().await {
        Some(p) => p,
        None => return Ok(()),
    };
    setup(&pool).await?;
    truncate_all(&pool).await?;

    let company_id = Uuid::new_v4();
    let employee_id = Uuid::new_v4();
    let position_from = Uuid::new_v4();
    let position_to = Uuid::new_v4();
    let salary = Decimal::new(8_500_000, 0);

    let promotion_id: Uuid = sqlx::query(
        r#"INSERT INTO lifecycle.promotions
               (employee_id, promotion_type, position_id_from, position_id_to,
                proposed_salary, effective_date, status)
           VALUES ($1,'promotion',$2,$3,$4,NOW(),'approved') RETURNING id"#,
    )
    .bind(employee_id)
    .bind(position_from)
    .bind(position_to)
    .bind(salary)
    .fetch_one(&pool)
    .await?
    .get("id");

    // ── 1. PRODUCER: effect() flips approved→effective + stages promotion.effective, in one tx. ──
    let svc = PromotionWriteService::new(pool.clone());
    let event_id = scoped(&pool, company_id, svc.effect(promotion_id))
        .await??
        .expect("fresh effect stages an event");

    let promo_status: String =
        sqlx::query_scalar("SELECT status::text FROM lifecycle.promotions WHERE id=$1")
            .bind(promotion_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(
        promo_status, "effective",
        "promotion is effective after effect()"
    );
    assert_eq!(
        outbox::pending_count(&pool, "lifecycle").await?,
        1,
        "one event staged"
    );

    // ── 2. RELAY → CONSUMERS: drain through a bus carrying BOTH target handlers. ───────────────
    let bus = IntegrationEventBus::new();
    bus.register_handler(std::sync::Arc::new(
        backbone_employee::application::PromotionEffectiveHandler::new(pool.clone()),
    ))
    .await;
    bus.register_handler(std::sync::Arc::new(
        backbone_payroll::application::PromotionSalaryHandler::new(pool.clone()),
    ))
    .await;

    let published = drain_lifecycle(&pool, bus).await?;
    assert_eq!(published, 1, "relay drained + the bus acked the event");
    assert_eq!(
        outbox::pending_count(&pool, "lifecycle").await?,
        0,
        "outbox drained"
    );

    // ── 3. ASSERT: employment_histories (employee target) + compensation_changes (payroll target). ─
    let eh_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM employee.employment_histories WHERE reference_id=$1",
    )
    .bind(promotion_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        eh_count, 1,
        "exactly one employment_history row for this promotion"
    );

    let eh = sqlx::query(
        "SELECT action::text AS action, position_id_from, position_id_to
         FROM employee.employment_histories WHERE reference_id=$1",
    )
    .bind(promotion_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        eh.get::<String, _>("action"),
        "promotion",
        "action mapped from promotion_type"
    );
    assert_eq!(
        eh.get::<Option<Uuid>, _>("position_id_from"),
        Some(position_from)
    );
    assert_eq!(
        eh.get::<Option<Uuid>, _>("position_id_to"),
        Some(position_to)
    );

    let cc = sqlx::query(
        "SELECT change_type::text AS ct, new_amount, reference_id
         FROM payroll.compensation_changes WHERE reference_id=$1",
    )
    .bind(promotion_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        cc.get::<String, _>("ct"),
        "promotion",
        "compensation change_type='promotion'"
    );
    assert_eq!(
        cc.get::<Decimal, _>("new_amount"),
        salary,
        "new_amount = proposed_salary"
    );

    // Both consumers recorded the apply in their own inboxes.
    assert!(
        inbox::was_consumed(&pool, "employee", "promotion.role", event_id).await?,
        "employee inbox recorded the consumption"
    );
    assert!(
        inbox::was_consumed(&pool, "payroll", "promotion.salary", event_id).await?,
        "payroll inbox recorded the consumption"
    );

    // ── 4. IDEMPOTENCY: replay the SAME event id to BOTH handlers — no second rows. ────────────
    let replay = fetch_envelope(&pool, event_id).await?;
    let h1 = backbone_employee::application::PromotionEffectiveHandler::new(pool.clone());
    let h2 = backbone_payroll::application::PromotionSalaryHandler::new(pool.clone());
    h1.handle(replay.clone())
        .await
        .expect("employee replay is Ok (no-op)");
    h2.handle(replay)
        .await
        .expect("payroll replay is Ok (no-op)");

    let eh_after: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM employee.employment_histories WHERE reference_id=$1",
    )
    .bind(promotion_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        eh_after, 1,
        "replay did not create a second employment_history"
    );

    let cc_after: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM payroll.compensation_changes WHERE reference_id=$1",
    )
    .bind(promotion_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        cc_after, 1,
        "replay did not create a second compensation_change"
    );

    Ok(())
}

#[tokio::test]
async fn promotion_effective_is_idempotent_at_the_producer(
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = match connect().await {
        Some(p) => p,
        None => return Ok(()),
    };
    setup(&pool).await?;
    truncate_all(&pool).await?;

    let company_id = Uuid::new_v4();
    let promotion_id: Uuid = sqlx::query(
        r#"INSERT INTO lifecycle.promotions (employee_id, effective_date, status)
           VALUES ($1,NOW(),'approved') RETURNING id"#,
    )
    .bind(Uuid::new_v4())
    .fetch_one(&pool)
    .await?
    .get("id");

    let svc = PromotionWriteService::new(pool.clone());
    let first = scoped(&pool, company_id, svc.effect(promotion_id))
        .await??
        .expect("first effect stages an event");
    let second = scoped(&pool, company_id, svc.effect(promotion_id)).await??;
    assert!(
        second.is_none(),
        "re-effect of an effective promotion stages no second event"
    );
    assert_eq!(
        outbox::pending_count(&pool, "lifecycle").await?,
        1,
        "still one event — id {first}"
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────────────────────
// Flow 2: onboarding.completed → employee.employments.status='active'
// ─────────────────────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn onboarding_completed_flow_activates_employment_and_is_idempotent(
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = match connect().await {
        Some(p) => p,
        None => return Ok(()),
    };
    setup(&pool).await?;
    truncate_all(&pool).await?;

    let company_id = Uuid::new_v4();
    let employee_id = Uuid::new_v4();

    // Seed an employment in 'inactive' so the activation is observable. No tasks → completable.
    sqlx::query(
        r#"INSERT INTO employee.employments (company_id, employee_id, join_date, status)
           VALUES ($1,$2,NOW(),'inactive')"#,
    )
    .bind(company_id)
    .bind(employee_id)
    .execute(&pool)
    .await?;

    let onboarding_id: Uuid = sqlx::query(
        r#"INSERT INTO lifecycle.onboardings (employee_id, start_date, status)
           VALUES ($1,NOW(),'in_progress') RETURNING id"#,
    )
    .bind(employee_id)
    .fetch_one(&pool)
    .await?
    .get("id");

    // ── 1. PRODUCER ───────────────────────────────────────────────────────────────────────────
    let svc = OnboardingWriteService::new(pool.clone());
    let event_id = scoped(&pool, company_id, svc.complete(onboarding_id))
        .await??
        .expect("fresh complete stages an event");

    let ob = sqlx::query(
        "SELECT status::text AS status, completed_at FROM lifecycle.onboardings WHERE id=$1",
    )
    .bind(onboarding_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        ob.get::<String, _>("status"),
        "completed",
        "onboarding is completed"
    );
    assert!(
        ob.get::<Option<DateTime<Utc>>, _>("completed_at").is_some(),
        "completed_at stamped"
    );
    assert_eq!(
        outbox::pending_count(&pool, "lifecycle").await?,
        1,
        "one event staged"
    );

    // ── 2. RELAY → CONSUMER ───────────────────────────────────────────────────────────────────
    let bus = IntegrationEventBus::new();
    bus.register_handler(std::sync::Arc::new(
        backbone_employee::application::OnboardingCompletedHandler::new(pool.clone()),
    ))
    .await;
    let published = drain_lifecycle(&pool, bus).await?;
    assert_eq!(published, 1);
    assert_eq!(
        outbox::pending_count(&pool, "lifecycle").await?,
        0,
        "outbox drained"
    );

    // ── 3. ASSERT: employment flipped to active. ──────────────────────────────────────────────
    let emp_status: String =
        sqlx::query_scalar("SELECT status::text FROM employee.employments WHERE employee_id=$1")
            .bind(employee_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(
        emp_status, "active",
        "employment activated on onboarding completion"
    );

    assert!(
        inbox::was_consumed(&pool, "employee", "onboarding.active", event_id).await?,
        "inbox recorded the consumption"
    );

    // ── 4. IDEMPOTENCY: replay — no error, status stays 'active'. ─────────────────────────────
    let replay = fetch_envelope(&pool, event_id).await?;
    let h = backbone_employee::application::OnboardingCompletedHandler::new(pool.clone());
    h.handle(replay).await.expect("replay is Ok (a no-op)");
    let emp_status_after: String =
        sqlx::query_scalar("SELECT status::text FROM employee.employments WHERE employee_id=$1")
            .bind(employee_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(
        emp_status_after, "active",
        "replay left the employment active (no-op)"
    );
    Ok(())
}

#[tokio::test]
async fn onboarding_complete_rejects_open_tasks() -> Result<(), Box<dyn std::error::Error>> {
    let pool = match connect().await {
        Some(p) => p,
        None => return Ok(()),
    };
    setup(&pool).await?;
    truncate_all(&pool).await?;

    let company_id = Uuid::new_v4();
    let onboarding_id: Uuid = sqlx::query(
        r#"INSERT INTO lifecycle.onboardings (employee_id, start_date, status)
           VALUES ($1,NOW(),'in_progress') RETURNING id"#,
    )
    .bind(Uuid::new_v4())
    .fetch_one(&pool)
    .await?
    .get("id");

    sqlx::query(
        r#"INSERT INTO lifecycle.onboarding_tasks (onboarding_id, title, status)
           VALUES ($1,'collect docs','pending')"#,
    )
    .bind(onboarding_id)
    .execute(&pool)
    .await?;

    let svc = OnboardingWriteService::new(pool.clone());
    let res = scoped(&pool, company_id, svc.complete(onboarding_id)).await?;
    assert!(
        res.is_err(),
        "complete() rejects when a task is still pending"
    );
    assert_eq!(
        outbox::pending_count(&pool, "lifecycle").await?,
        0,
        "no event staged on rejection"
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────────────────────
// Flow 3: offboarding.closed → employee.employments.status='inactive' + payroll.compensation_changes
// ─────────────────────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn offboarding_closed_flow_deactivates_and_settles_and_is_idempotent(
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = match connect().await {
        Some(p) => p,
        None => return Ok(()),
    };
    setup(&pool).await?;
    truncate_all(&pool).await?;

    let company_id = Uuid::new_v4();
    let employee_id = Uuid::new_v4();

    // ── Seed the three settlement inputs ───────────────────────────────────────────────────
    // join_date 2020-01-01 → last_working_day 2024-01-01 (inclusive) is four completed years.
    sqlx::query(
        r#"INSERT INTO employee.employments (company_id, employee_id, join_date, status)
           VALUES ($1,$2,'2020-01-01','active')"#,
    )
    .bind(company_id)
    .bind(employee_id)
    .execute(&pool)
    .await?;

    // Current monthly salary = 22,000,000 (daily wage 22M / 21 = 1,047,619.05, five-day week).
    sqlx::query(
        r#"INSERT INTO payroll.compensation_changes
               (company_id, employee_id, change_type, new_amount, effective_date)
           VALUES ($1,$2,'hire'::compensation_change_type, 22000000, '2020-01-01')"#,
    )
    .bind(company_id)
    .bind(employee_id)
    .execute(&pool)
    .await?;

    // Remaining leave = allocated(10) - used(5) = 5 days.
    sqlx::query(
        r#"INSERT INTO timeoff.timeoff_balances
               (company_id, employee_id, timeoff_type_id, period, allocated, used)
           VALUES ($1,$2,$3,'2024',10,5)"#,
    )
    .bind(company_id)
    .bind(employee_id)
    .bind(seed_leave_type(&pool, "ANNUAL").await?)
    .execute(&pool)
    .await?;

    let offboarding_id: Uuid = sqlx::query(
        r#"INSERT INTO lifecycle.offboardings
               (employee_id, reason, notice_date, last_working_day, status)
           VALUES ($1,'efficiency','2024-01-01','2024-01-01','cleared') RETURNING id"#,
    )
    .bind(employee_id)
    .fetch_one(&pool)
    .await?
    .get("id");

    // ── 1. PRODUCER ───────────────────────────────────────────────────────────────────────────
    // with_pool = pool-backed inputs; the statutory set comes from the seeded tables (the same wiring the lifecycle
    // module builder uses by default).
    let svc = OffboardingWriteService::with_pool(pool.clone());
    let event_id = scoped(&pool, company_id, svc.close(offboarding_id))
        .await??
        .expect("fresh close stages an event");

    let ob_status: String =
        sqlx::query_scalar("SELECT status::text FROM lifecycle.offboardings WHERE id=$1")
            .bind(offboarding_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(ob_status, "closed", "offboarding is closed");
    assert_eq!(
        outbox::pending_count(&pool, "lifecycle").await?,
        1,
        "one event staged"
    );

    // The staged payload carries the full breakdown (self-auditing event). `->>` yields TEXT, so
    // parse the string back into a Decimal (NUMERIC decoding does not apply to a JSON text fetch).
    let carried_total_str: String = sqlx::query_scalar(
        "SELECT payload->'pesangon_breakdown'->>'total' FROM lifecycle.outbox_events WHERE id=$1",
    )
    .bind(event_id)
    .fetch_one(&pool)
    .await?;
    let carried_total = carried_total_str.parse::<Decimal>().expect("total parses");
    assert_eq!(
        carried_total,
        Decimal::from_str_exact("159238095.25").unwrap(),
        "payload carries the settlement total"
    );

    // ── 2. RELAY → CONSUMERS (employee + payroll) ─────────────────────────────────────────────
    let bus = IntegrationEventBus::new();
    bus.register_handler(std::sync::Arc::new(
        backbone_employee::application::OffboardingClosedHandler::new(pool.clone()),
    ))
    .await;
    bus.register_handler(std::sync::Arc::new(
        backbone_payroll::application::OffboardingSettlementHandler::new(pool.clone()),
    ))
    .await;
    let published = drain_lifecycle(&pool, bus).await?;
    assert_eq!(published, 1);
    assert_eq!(
        outbox::pending_count(&pool, "lifecycle").await?,
        0,
        "outbox drained"
    );

    // ── 3. ASSERT: employment inactive + REAL settlement row. ─────────────────────────────────
    let emp_status: String =
        sqlx::query_scalar("SELECT status::text FROM employee.employments WHERE employee_id=$1")
            .bind(employee_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(emp_status, "inactive", "employment deactivated on close");

    // Hand-computed for `efficiency` (settled as PP 35/2021 Pasal 43(2): pesangon 1x, UPMK 1x),
    // four completed years, 22M wage, 5 unused annual-leave days:
    //   uang pesangon = Pasal 40(2) 4-<5 years = 5 months × 22M  = 110,000,000
    //   UPMK          = Pasal 40(3) 3-<6 years = 2 months × 22M  =  44,000,000
    //   leave         = 5 × round(22M / 21) = 5 × 1,047,619.05   =   5,238,095.25
    //   total         = 110M + 44M + 5,238,095.25               = 159,238,095.25
    let row: (String, Decimal) = sqlx::query_as(
        r#"SELECT change_type::text AS change_type, new_amount
             FROM payroll.compensation_changes WHERE reference_id=$1"#,
    )
    .bind(offboarding_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        row.0, "offboarding",
        "settlement row uses the 'offboarding' change type"
    );
    assert_eq!(
        row.1,
        Decimal::from_str_exact("159238095.25").unwrap(),
        "settlement new_amount is the carried total"
    );

    let cc_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM payroll.compensation_changes WHERE reference_id=$1",
    )
    .bind(offboarding_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        cc_count, 1,
        "exactly one settlement row for this offboarding"
    );

    assert!(
        inbox::was_consumed(&pool, "employee", "offboarding.role", event_id).await?,
        "employee inbox recorded the consumption"
    );
    assert!(
        inbox::was_consumed(&pool, "payroll", "offboarding.settlement", event_id).await?,
        "payroll inbox recorded the consumption"
    );

    // ── 4. IDEMPOTENCY: replay to BOTH handlers — no second effects. ──────────────────────────
    let replay = fetch_envelope(&pool, event_id).await?;
    let h1 = backbone_employee::application::OffboardingClosedHandler::new(pool.clone());
    let h2 = backbone_payroll::application::OffboardingSettlementHandler::new(pool.clone());
    h1.handle(replay.clone())
        .await
        .expect("employee replay is Ok (no-op)");
    h2.handle(replay)
        .await
        .expect("payroll replay is Ok (no-op)");

    let emp_after: String =
        sqlx::query_scalar("SELECT status::text FROM employee.employments WHERE employee_id=$1")
            .bind(employee_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(emp_after, "inactive", "replay left the employment inactive");

    let cc_after: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM payroll.compensation_changes WHERE reference_id=$1",
    )
    .bind(offboarding_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(cc_after, 1, "replay did not create a second settlement row");
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────────────────────
// Flow 3b: offboarding.closed → timeoff.timeoff_balances zeroed (the deferred encash consumer)
// ─────────────────────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn offboarding_closed_also_zeroes_leave_balance_and_is_idempotent(
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = match connect().await {
        Some(p) => p,
        None => return Ok(()),
    };
    setup(&pool).await?;
    truncate_all(&pool).await?;

    let company_id = Uuid::new_v4();
    let employee_id = Uuid::new_v4();

    // ── Seed the three pesangon inputs (close() needs them to compute the breakdown it carries). ──
    sqlx::query(
        r#"INSERT INTO employee.employments (company_id, employee_id, join_date, status)
           VALUES ($1,$2,'2020-01-01','active')"#,
    )
    .bind(company_id)
    .bind(employee_id)
    .execute(&pool)
    .await?;

    sqlx::query(
        r#"INSERT INTO payroll.compensation_changes
               (company_id, employee_id, change_type, new_amount, effective_date)
           VALUES ($1,$2,'hire'::compensation_change_type, 22000000, '2020-01-01')"#,
    )
    .bind(company_id)
    .bind(employee_id)
    .execute(&pool)
    .await?;

    // ── The balance this consumer zeroes: allocated(10) - used(5) = 5 unused days. ──────────────
    sqlx::query(
        r#"INSERT INTO timeoff.timeoff_balances
               (company_id, employee_id, timeoff_type_id, period, allocated, used)
           VALUES ($1,$2,$3,'2024',10,5)"#,
    )
    .bind(company_id)
    .bind(employee_id)
    .bind(seed_leave_type(&pool, "ANNUAL").await?)
    .execute(&pool)
    .await?;

    let offboarding_id: Uuid = sqlx::query(
        r#"INSERT INTO lifecycle.offboardings
               (employee_id, reason, notice_date, last_working_day, status)
           VALUES ($1,'efficiency','2024-01-01','2024-01-01','cleared') RETURNING id"#,
    )
    .bind(employee_id)
    .fetch_one(&pool)
    .await?
    .get("id");

    // ── 1. PRODUCER ───────────────────────────────────────────────────────────────────────────
    let svc = OffboardingWriteService::with_pool(pool.clone());
    let event_id = scoped(&pool, company_id, svc.close(offboarding_id))
        .await??
        .expect("fresh close stages an event");
    assert_eq!(
        outbox::pending_count(&pool, "lifecycle").await?,
        1,
        "one event staged"
    );

    // ── 2. RELAY → CONSUMER (the timeoff encash target). ───────────────────────────────────────
    let bus = IntegrationEventBus::new();
    bus.register_handler(std::sync::Arc::new(
        backbone_timeoff::application::OffboardingEncashHandler::new(pool.clone()),
    ))
    .await;
    let published = drain_lifecycle(&pool, bus).await?;
    assert_eq!(published, 1);
    assert_eq!(
        outbox::pending_count(&pool, "lifecycle").await?,
        0,
        "outbox drained"
    );

    // ── 3. ASSERT: remaining leave is zeroed — used is promoted to allocated (paid out). ────────
    let balance: (Decimal, Decimal) = sqlx::query_as(
        r#"SELECT allocated, used FROM timeoff.timeoff_balances WHERE employee_id=$1"#,
    )
    .bind(employee_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(balance.0, Decimal::from(10), "allocated unchanged");
    assert_eq!(
        balance.1, balance.0,
        "used promoted to allocated — the 5 unused days are now marked consumed (paid out)"
    );

    assert!(
        inbox::was_consumed(&pool, "timeoff", "offboarding.encash", event_id).await?,
        "timeoff inbox recorded the consumption"
    );

    // ── 4. IDEMPOTENCY: replay to the encash handler — balance stays zeroed. ───────────────────
    let replay = fetch_envelope(&pool, event_id).await?;
    let h = backbone_timeoff::application::OffboardingEncashHandler::new(pool.clone());
    h.handle(replay).await.expect("replay is Ok (a no-op)");

    let balance_after: (Decimal, Decimal) = sqlx::query_as(
        r#"SELECT allocated, used FROM timeoff.timeoff_balances WHERE employee_id=$1"#,
    )
    .bind(employee_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        balance_after.1, balance_after.0,
        "replay left the balance zeroed (no-op)"
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────────────────────
// Flow 2b: onboarding.completed → payroll initial CompensationChange (the deferred enroll consumer)
// ─────────────────────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn onboarding_completed_also_seeds_initial_compensation_and_is_idempotent(
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = match connect().await {
        Some(p) => p,
        None => return Ok(()),
    };
    setup(&pool).await?;
    truncate_all(&pool).await?;

    let company_id = Uuid::new_v4();
    let employee_id = Uuid::new_v4();
    let base_salary = Decimal::new(8_500_000, 0);

    // ── Seed the joiner with a starting salary on the people master. The `id` is pinned to the
    //    employee_id the onboarding event carries — that is the key the enroll handler reads by. ──
    sqlx::query(
        r#"INSERT INTO employee.employees (id, company_id, employee_number, first_name, base_salary)
           VALUES ($4,$1,$2,'Joinee',$3)"#,
    )
    .bind(company_id)
    .bind(format!("EMP-{employee_id}"))
    .bind(base_salary)
    .bind(employee_id)
    .execute(&pool)
    .await?;

    // An employment in 'inactive' so the activation is observable; no tasks → completable.
    sqlx::query(
        r#"INSERT INTO employee.employments (company_id, employee_id, join_date, status)
           VALUES ($1,$2,NOW(),'inactive')"#,
    )
    .bind(company_id)
    .bind(employee_id)
    .execute(&pool)
    .await?;

    let onboarding_id: Uuid = sqlx::query(
        r#"INSERT INTO lifecycle.onboardings (employee_id, start_date, status)
           VALUES ($1,NOW(),'in_progress') RETURNING id"#,
    )
    .bind(employee_id)
    .fetch_one(&pool)
    .await?
    .get("id");

    // ── 1. PRODUCER ───────────────────────────────────────────────────────────────────────────
    let svc = OnboardingWriteService::new(pool.clone());
    let event_id = scoped(&pool, company_id, svc.complete(onboarding_id))
        .await??
        .expect("fresh complete stages an event");
    assert_eq!(
        outbox::pending_count(&pool, "lifecycle").await?,
        1,
        "one event staged"
    );

    // ── 2. RELAY → CONSUMER (the payroll enroll target). ──────────────────────────────────────
    let bus = IntegrationEventBus::new();
    bus.register_handler(std::sync::Arc::new(
        backbone_payroll::application::OnboardingEnrolledHandler::new(pool.clone()),
    ))
    .await;
    let published = drain_lifecycle(&pool, bus).await?;
    assert_eq!(published, 1);
    assert_eq!(
        outbox::pending_count(&pool, "lifecycle").await?,
        0,
        "outbox drained"
    );

    // ── 3. ASSERT: an initial compensation row seeded from base_salary. ────────────────────────
    let cc: (String, Decimal) = sqlx::query_as(
        r#"SELECT change_type::text AS change_type, new_amount
             FROM payroll.compensation_changes WHERE reference_id=$1"#,
    )
    .bind(onboarding_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        cc.0, "hire",
        "initial compensation uses the 'hire' change_type"
    );
    assert_eq!(cc.1, base_salary, "new_amount = the joiner's base_salary");

    let cc_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM payroll.compensation_changes WHERE reference_id=$1",
    )
    .bind(onboarding_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        cc_count, 1,
        "exactly one initial compensation row for this onboarding"
    );

    assert!(
        inbox::was_consumed(&pool, "payroll", "onboarding.enroll", event_id).await?,
        "payroll inbox recorded the consumption"
    );

    // ── 4. IDEMPOTENCY: replay to the enroll handler — no second row. ──────────────────────────
    let replay = fetch_envelope(&pool, event_id).await?;
    let h = backbone_payroll::application::OnboardingEnrolledHandler::new(pool.clone());
    h.handle(replay).await.expect("replay is Ok (a no-op)");

    let cc_after: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM payroll.compensation_changes WHERE reference_id=$1",
    )
    .bind(onboarding_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        cc_after, 1,
        "replay did not create a second initial compensation row"
    );
    Ok(())
}

#[tokio::test]
async fn onboarding_enrolled_skips_when_no_starting_salary(
) -> Result<(), Box<dyn std::error::Error>> {
    // Claim-but-skip path: a joiner with NO base_salary (NULL) → the handler claims the event (so a
    // replay is a no-op) but writes NO compensation row.
    let pool = match connect().await {
        Some(p) => p,
        None => return Ok(()),
    };
    setup(&pool).await?;
    truncate_all(&pool).await?;

    let company_id = Uuid::new_v4();
    let employee_id = Uuid::new_v4();

    // No base_salary (NULL) — the claim-but-skip trigger. `id` pinned to the event's employee_id so
    // the read actually targets this row (and finds base_salary NULL), not "no row at all".
    sqlx::query(
        r#"INSERT INTO employee.employees (id, company_id, employee_number, first_name)
           VALUES ($3,$1,$2,'NoSalary')"#,
    )
    .bind(company_id)
    .bind(format!("EMP-{employee_id}"))
    .bind(employee_id)
    .execute(&pool)
    .await?;
    sqlx::query(
        r#"INSERT INTO employee.employments (company_id, employee_id, join_date, status)
           VALUES ($1,$2,NOW(),'inactive')"#,
    )
    .bind(company_id)
    .bind(employee_id)
    .execute(&pool)
    .await?;

    let onboarding_id: Uuid = sqlx::query(
        r#"INSERT INTO lifecycle.onboardings (employee_id, start_date, status)
           VALUES ($1,NOW(),'in_progress') RETURNING id"#,
    )
    .bind(employee_id)
    .fetch_one(&pool)
    .await?
    .get("id");

    let svc = OnboardingWriteService::new(pool.clone());
    let event_id = scoped(&pool, company_id, svc.complete(onboarding_id))
        .await??
        .expect("fresh complete stages an event");

    let bus = IntegrationEventBus::new();
    bus.register_handler(std::sync::Arc::new(
        backbone_payroll::application::OnboardingEnrolledHandler::new(pool.clone()),
    ))
    .await;
    drain_lifecycle(&pool, bus).await?;

    // Claim recorded, but NO compensation row.
    let cc_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM payroll.compensation_changes WHERE reference_id=$1",
    )
    .bind(onboarding_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        cc_count, 0,
        "no compensation row when base_salary is NULL (claim-but-skip)"
    );

    assert!(
        inbox::was_consumed(&pool, "payroll", "onboarding.enroll", event_id).await?,
        "the event was still CLAIMED — a replay is a no-op even with no salary"
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────────────────────
// Fakes for the outbound seams (the probes below wire these where production wires real adapters).
// ─────────────────────────────────────────────────────────────────────────────────────────────

/// Records every scheduled activity so a probe can assert the post-commit notify.
struct RecordingActivitySink(
    std::sync::Mutex<Vec<backbone_lifecycle::application::service::ActivityCommand>>,
);

#[async_trait::async_trait]
impl backbone_lifecycle::application::service::ActivitySink for RecordingActivitySink {
    async fn schedule(
        &self,
        cmd: backbone_lifecycle::application::service::ActivityCommand,
    ) -> Result<
        backbone_lifecycle::application::service::ActivityAck,
        backbone_lifecycle::application::service::ActivityRejected,
    > {
        self.0.lock().unwrap().push(cmd);
        Ok(backbone_lifecycle::application::service::ActivityAck {
            activity_id: Uuid::new_v4(),
        })
    }
}

/// Records the envelope and acks with fixed ids so a probe can assert the stamping.
struct AckingGlSink(std::sync::Mutex<Option<backbone_gl_posting::AccountingPostEnvelope>>);

#[async_trait::async_trait]
impl backbone_gl_posting::GlPostSink for AckingGlSink {
    async fn post(
        &self,
        envelope: &backbone_gl_posting::AccountingPostEnvelope,
    ) -> Result<backbone_gl_posting::GlPostAck, backbone_gl_posting::GlPostRejected> {
        *self.0.lock().unwrap() = Some(envelope.clone());
        Ok(backbone_gl_posting::GlPostAck {
            post_id: Uuid::new_v4(),
            journal_id: Uuid::new_v4(),
            idempotent_reuse: false,
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────────────────────
// Probation confirmation (the lifecycle.probation_confirmed producer)
// ─────────────────────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn probation_confirm_gates_on_completion_date_and_force_then_emits_once(
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = match connect().await {
        Some(p) => p,
        None => return Ok(()),
    };
    setup(&pool).await?;
    truncate_all(&pool).await?;

    let company_id = Uuid::new_v4();
    let employee_id = Uuid::new_v4();

    // In-flight onboarding: confirmation is refused (it runs on a finished journey).
    let in_flight: Uuid = sqlx::query(
        r#"INSERT INTO lifecycle.onboardings (employee_id, start_date, status)
           VALUES ($1,NOW(),'in_progress') RETURNING id"#,
    )
    .bind(employee_id)
    .fetch_one(&pool)
    .await?
    .get("id");

    // Completed with a FUTURE probation end: refused without force, allowed with it.
    let future_end = Utc::now().date_naive() + chrono::Duration::days(30);
    let gated: Uuid = sqlx::query(
        r#"INSERT INTO lifecycle.onboardings (employee_id, start_date, status, probation_end_date)
           VALUES ($1,NOW(),'completed',$2) RETURNING id"#,
    )
    .bind(employee_id)
    .bind(future_end)
    .fetch_one(&pool)
    .await?
    .get("id");

    // Completed with a PAST probation end: the happy path.
    let past_end = Utc::now().date_naive() - chrono::Duration::days(1);
    let ready: Uuid = sqlx::query(
        r#"INSERT INTO lifecycle.onboardings (employee_id, start_date, status, probation_end_date)
           VALUES ($1,NOW(),'completed',$2) RETURNING id"#,
    )
    .bind(employee_id)
    .bind(past_end)
    .fetch_one(&pool)
    .await?
    .get("id");

    let svc = OnboardingWriteService::new(pool.clone());

    // Gate 1: not completed.
    let err = scoped(&pool, company_id, svc.confirm(in_flight, false))
        .await?
        .expect_err("in-flight onboarding cannot confirm");
    assert!(
        matches!(
            err,
            backbone_lifecycle::application::service::OnboardingCompleteError::NotCompleted { .. }
        ),
        "in-flight gate is NotCompleted, got {err:?}"
    );

    // Gate 2: date not reached, no force.
    let err = scoped(&pool, company_id, svc.confirm(gated, false))
        .await?
        .expect_err("future probation end refuses without force");
    assert!(
        matches!(
            err,
            backbone_lifecycle::application::service::OnboardingCompleteError::ProbationNotEnded { .. }
        ),
        "date gate is ProbationNotEnded, got {err:?}"
    );
    assert_eq!(
        outbox::pending_count(&pool, "lifecycle").await?,
        0,
        "no event staged by a refused gate"
    );

    // Force overrides the date gate.
    let forced = scoped(&pool, company_id, svc.confirm(gated, true))
        .await??
        .expect("force confirms past the date gate");
    assert_eq!(
        sqlx::query_scalar::<_, String>(
            "SELECT event_type FROM lifecycle.outbox_events WHERE id=$1"
        )
        .bind(forced)
        .fetch_one(&pool)
        .await?,
        "lifecycle.probation_confirmed"
    );

    // Happy path + producer idempotency.
    let event_id = scoped(&pool, company_id, svc.confirm(ready, false))
        .await??
        .expect("past-end onboarding confirms");
    let replay = scoped(&pool, company_id, svc.confirm(ready, false)).await??;
    assert!(replay.is_none(), "re-confirm stages no second event");

    let confirmed_at: Option<DateTime<Utc>> =
        sqlx::query_scalar("SELECT confirmed_at FROM lifecycle.onboardings WHERE id=$1")
            .bind(ready)
            .fetch_one(&pool)
            .await?;
    assert!(confirmed_at.is_some(), "confirmed_at stamped exactly once");

    let payload_emp: String = sqlx::query_scalar(
        "SELECT payload->>'employee_id' FROM lifecycle.outbox_events WHERE id=$1",
    )
    .bind(event_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(
        payload_emp,
        employee_id.to_string(),
        "payload carries the employee id"
    );
    assert_eq!(
        outbox::pending_count(&pool, "lifecycle").await?,
        2,
        "forced + happy events staged"
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────────────────────
// Checkpoint creates (activity seam: fail-closed, silent, then recording)
// ─────────────────────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn checkpoint_creates_fail_closed_then_record_and_notify_after_commit(
) -> Result<(), Box<dyn std::error::Error>> {
    use backbone_lifecycle::application::service::{
        ClearanceItemWriteService, NewClearanceItem, NewOnboardingTask, OnboardingTaskWriteService,
        UnwiredActivitySink,
    };

    let pool = match connect().await {
        Some(p) => p,
        None => return Ok(()),
    };
    setup(&pool).await?;
    truncate_all(&pool).await?;

    let onboarding_id: Uuid = sqlx::query(
        r#"INSERT INTO lifecycle.onboardings (employee_id, start_date, status)
           VALUES ($1,NOW(),'in_progress') RETURNING id"#,
    )
    .bind(Uuid::new_v4())
    .fetch_one(&pool)
    .await?
    .get("id");
    let offboarding_id: Uuid = sqlx::query(
        r#"INSERT INTO lifecycle.offboardings
               (employee_id, reason, notice_date, last_working_day, status)
           VALUES ($1,'resignation',NOW(),NOW(),'in_progress') RETURNING id"#,
    )
    .bind(Uuid::new_v4())
    .fetch_one(&pool)
    .await?
    .get("id");

    // ── Fail-closed: an explicit notify against the unwired seam creates NO row. ──
    let unwired_tasks =
        OnboardingTaskWriteService::new(pool.clone(), std::sync::Arc::new(UnwiredActivitySink));
    let unwired_clearance =
        ClearanceItemWriteService::new(pool.clone(), std::sync::Arc::new(UnwiredActivitySink));

    let err = unwired_tasks
        .create_task(NewOnboardingTask {
            onboarding_id,
            title: "collect docs".into(),
            category: Some("document".into()),
            owner_employee_id: None,
            due_date: None,
            notify_user_id: Some(Uuid::new_v4()),
        })
        .await
        .expect_err("notify against the unwired seam fails closed");
    assert!(
        matches!(
            err,
            backbone_lifecycle::application::service::CheckpointError::ActivitySeamUnwired
        ),
        "unwired notify is ActivitySeamUnwired, got {err:?}"
    );
    let task_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM lifecycle.onboarding_tasks WHERE onboarding_id=$1",
    )
    .bind(onboarding_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(task_count, 0, "fail-closed notify wrote no row");

    let err = unwired_clearance
        .create_clearance_item(NewClearanceItem {
            offboarding_id,
            title: "return laptop".into(),
            clearer_employee_id: None,
            notify_user_id: Some(Uuid::new_v4()),
        })
        .await
        .expect_err("clearance notify against the unwired seam fails closed");
    assert!(
        matches!(
            err,
            backbone_lifecycle::application::service::CheckpointError::ActivitySeamUnwired
        ),
        "unwired clearance notify is ActivitySeamUnwired, got {err:?}"
    );

    // ── Silent create (no notify): the row records, nothing is scheduled. ──
    unwired_tasks
        .create_task(NewOnboardingTask {
            onboarding_id,
            title: "silent task".into(),
            category: None,
            owner_employee_id: None,
            due_date: None,
            notify_user_id: None,
        })
        .await?;
    let silent: Option<String> = sqlx::query_scalar(
        "SELECT status::text FROM lifecycle.onboarding_tasks WHERE onboarding_id=$1",
    )
    .bind(onboarding_id)
    .fetch_optional(&pool)
    .await?;
    assert_eq!(
        silent.as_deref(),
        Some("pending"),
        "silent create recorded the row"
    );

    // ── Wired sink: the create lands AND the notify fires with the checkpoint's own facts. ──
    let recorder = std::sync::Arc::new(RecordingActivitySink(std::sync::Mutex::new(Vec::new())));
    let wired_tasks = OnboardingTaskWriteService::new(pool.clone(), recorder.clone());
    let due = chrono::NaiveDate::from_ymd_opt(2026, 9, 1).unwrap();
    let user_id = Uuid::new_v4();
    wired_tasks
        .create_task(NewOnboardingTask {
            onboarding_id,
            title: "equipment handout".into(),
            category: Some("equipment".into()),
            owner_employee_id: None,
            due_date: Some(due),
            notify_user_id: Some(user_id),
        })
        .await?;

    let recorded = recorder.0.lock().unwrap().clone();
    assert_eq!(recorded.len(), 1, "exactly one activity scheduled");
    assert_eq!(recorded[0].res_model, "onboarding_task");
    assert_eq!(recorded[0].user_id, user_id);
    assert_eq!(
        recorded[0].deadline,
        Some(due),
        "deadline mirrors the task's due date"
    );
    assert!(recorded[0].summary.contains("equipment handout"));
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────────────────────
// Final settlement: draft (idempotent) → confirm (GL seam fail-closed → acking sink → idempotent)
// ─────────────────────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn settlement_draft_is_idempotent_and_confirm_stamps_only_after_the_ack(
) -> Result<(), Box<dyn std::error::Error>> {
    use backbone_lifecycle::application::service::{
        FinalSettlementError, FinalSettlementWriteService, SettlementAccounts,
    };

    let pool = match connect().await {
        Some(p) => p,
        None => return Ok(()),
    };
    setup(&pool).await?;
    truncate_all(&pool).await?;

    // Same seeds as the offboarding close flow: 4.000-year tenure, 22M salary, 5 unused days.
    let company_id = Uuid::new_v4();
    let employee_id = Uuid::new_v4();
    sqlx::query(
        r#"INSERT INTO employee.employments (company_id, employee_id, join_date, status)
           VALUES ($1,$2,'2020-01-01','active')"#,
    )
    .bind(company_id)
    .bind(employee_id)
    .execute(&pool)
    .await?;
    sqlx::query(
        r#"INSERT INTO payroll.compensation_changes
               (company_id, employee_id, change_type, new_amount, effective_date)
           VALUES ($1,$2,'hire'::compensation_change_type, 22000000, '2020-01-01')"#,
    )
    .bind(company_id)
    .bind(employee_id)
    .execute(&pool)
    .await?;
    sqlx::query(
        r#"INSERT INTO timeoff.timeoff_balances
               (company_id, employee_id, timeoff_type_id, period, allocated, used)
           VALUES ($1,$2,$3,'2024',10,5)"#,
    )
    .bind(company_id)
    .bind(employee_id)
    .bind(seed_leave_type(&pool, "ANNUAL").await?)
    .execute(&pool)
    .await?;
    let offboarding_id: Uuid = sqlx::query(
        r#"INSERT INTO lifecycle.offboardings
               (employee_id, reason, notice_date, last_working_day, status)
           VALUES ($1,'efficiency','2024-01-01','2024-01-01','cleared') RETURNING id"#,
    )
    .bind(employee_id)
    .fetch_one(&pool)
    .await?
    .get("id");

    // ── DRAFT: assembles from the same inputs the close verb used. ────────────────────────────
    let svc = FinalSettlementWriteService::with_pool(pool.clone());
    let settlement_id = scoped(&pool, company_id, svc.draft_from_offboarding(offboarding_id))
        .await??;

    // base_pay (last pay) = 1 working day (Monday 2024-01-01) × 1,047,619.05
    // pesangon_amount = 110M uang pesangon + 44M UPMK = 154,000,000 · leave = 5,238,095.25
    // net = 159,238,095.25 (severance items only — the last pay flows through the
    // payroll lane, never this settlement's envelope)
    let row = sqlx::query(
        r#"SELECT period, base_pay, unused_leave_payout, pesangon_amount, net_payable,
                  status::text AS status, accounting_post_id
             FROM lifecycle.final_settlements WHERE id=$1"#,
    )
    .bind(settlement_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(row.get::<String, _>("period"), "2024-01");
    assert_eq!(
        row.get::<Decimal, _>("base_pay"),
        Decimal::from_str_exact("1047619.05").unwrap()
    );
    assert_eq!(
        row.get::<Decimal, _>("pesangon_amount"),
        Decimal::new(154_000_000, 0)
    );
    assert_eq!(
        row.get::<Decimal, _>("unused_leave_payout"),
        Decimal::from_str_exact("5238095.25").unwrap()
    );
    assert_eq!(
        row.get::<Decimal, _>("net_payable"),
        Decimal::from_str_exact("159238095.25").unwrap()
    );
    assert_eq!(row.get::<String, _>("status"), "draft");
    assert!(row.get::<Option<Uuid>, _>("accounting_post_id").is_none());

    // Double draft → the collision surfaces the winner's id.
    let err = scoped(&pool, company_id, svc.draft_from_offboarding(offboarding_id))
        .await?
        .expect_err("second draft for the same offboarding is rejected");
    match err {
        FinalSettlementError::AlreadyDrafted {
            settlement_id: existing,
            ..
        } => {
            assert_eq!(
                existing, settlement_id,
                "409 carries the existing settlement id"
            );
        }
        other => panic!("expected AlreadyDrafted, got {other:?}"),
    }

    let accounts = SettlementAccounts {
        severance_expense_account_id: Uuid::new_v4(),
        leave_encashment_expense_account_id: Uuid::new_v4(),
        employee_payable_account_id: Uuid::new_v4(),
    };

    // ── CONFIRM, unwired: loud 422, row stays draft + unstamped. ──────────────────────────────
    let err = scoped(&pool, company_id, svc.confirm(settlement_id, accounts))
        .await?
        .expect_err("unwired GL seam refuses the confirm");
    match &err {
        FinalSettlementError::GlRejected { code, .. } => {
            assert_eq!(code, "gl_seam_unwired");
        }
        other => panic!("expected GlRejected, got {other:?}"),
    }
    assert_eq!(err.http_status(), 422);
    let still: (String, Option<Uuid>) = sqlx::query_as(
        "SELECT status::text AS status, accounting_post_id FROM lifecycle.final_settlements WHERE id=$1",
    )
    .bind(settlement_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(still.0, "draft", "rejected confirm left the row retryable");
    assert!(still.1.is_none(), "no post id stamped without an ack");

    // ── Tax gate: a drafted deduction refuses until a withholding account joins the seam. ─────
    sqlx::query("UPDATE lifecycle.final_settlements SET tax_deduction = 1000000 WHERE id=$1")
        .bind(settlement_id)
        .execute(&pool)
        .await?;
    let sink = std::sync::Arc::new(AckingGlSink(std::sync::Mutex::new(None)));
    let wired = FinalSettlementWriteService::with_pool(pool.clone()).with_gl_sink(sink.clone());
    let err = scoped(&pool, company_id, wired.confirm(settlement_id, accounts))
        .await?
        .expect_err("a drafted tax deduction refuses the confirm");
    assert!(
        matches!(err, FinalSettlementError::TaxRequiresAccount(_, t) if t == Decimal::new(1_000_000, 0)),
        "tax gate fired, got {err:?}"
    );
    sqlx::query("UPDATE lifecycle.final_settlements SET tax_deduction = NULL WHERE id=$1")
        .bind(settlement_id)
        .execute(&pool)
        .await?;

    // ── CONFIRM, wired: ack → stamp; envelope balanced + dedup-stable. ────────────────────────
    let ack = scoped(&pool, company_id, wired.confirm(settlement_id, accounts))
        .await??
        .expect("wired sink acks the confirm");
    let stamped: (String, Uuid, Uuid) = sqlx::query_as(
        "SELECT status::text AS status, accounting_post_id, journal_id FROM lifecycle.final_settlements WHERE id=$1",
    )
    .bind(settlement_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(stamped.0, "confirmed");
    assert_eq!(stamped.1, ack.post_id, "post id stamped from the ack");
    assert_eq!(stamped.2, ack.journal_id, "journal id stamped from the ack");

    let envelope = sink
        .0
        .lock()
        .unwrap()
        .clone()
        .expect("the sink saw the envelope");
    assert_eq!(
        envelope.idempotency_key,
        format!("final_settlement:{company_id}:{settlement_id}")
    );
    assert_eq!(envelope.source_type, "final_settlement");
    assert_eq!(envelope.source_id, settlement_id);
    assert!(envelope.is_balanced(), "debits equal credits");
    assert_eq!(
        envelope.lines.len(),
        3,
        "severance + leave debits, one payable credit"
    );
    let total_debit: Decimal = envelope.lines.iter().map(|l| l.debit).sum();
    assert_eq!(
        total_debit,
        Decimal::from_str_exact("159238095.25").unwrap(),
        "severance 154M + leave 5,238,095.25"
    );

    // Producer idempotency: a re-confirm sends no second envelope.
    let replay = scoped(&pool, company_id, wired.confirm(settlement_id, accounts)).await??;
    assert!(replay.is_none(), "re-confirm is a no-op");
    assert!(
        sink.0.lock().unwrap().is_some(),
        "sink still holds exactly the first envelope"
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────────────────────
// The clear gate in front of close (clearance-derived)
// ─────────────────────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn offboarding_clear_asserts_the_clearance_derivation(
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = match connect().await {
        Some(p) => p,
        None => return Ok(()),
    };
    setup(&pool).await?;
    truncate_all(&pool).await?;

    let offboarding_id: Uuid = sqlx::query(
        r#"INSERT INTO lifecycle.offboardings
               (employee_id, reason, notice_date, last_working_day, status)
           VALUES ($1,'resignation',NOW(),NOW(),'in_progress') RETURNING id"#,
    )
    .bind(Uuid::new_v4())
    .fetch_one(&pool)
    .await?
    .get("id");

    // A pending item blocks the clear.
    sqlx::query(
        r#"INSERT INTO lifecycle.clearance_items (offboarding_id, title, status)
           VALUES ($1,'revoke access','pending')"#,
    )
    .bind(offboarding_id)
    .execute(&pool)
    .await?;

    let svc = OffboardingWriteService::with_pool(pool.clone());
    let err = svc
        .clear(offboarding_id)
        .await
        .expect_err("open item blocks the clear");
    assert!(
        matches!(
            err,
            backbone_lifecycle::application::service::OffboardingCloseError::ClearanceOpen {
                open_count: 1,
                ..
            }
        ),
        "clearance gate fired with the open count, got {err:?}"
    );

    // Resolve the item → the clear lands; re-clear is a no-op.
    sqlx::query("UPDATE lifecycle.clearance_items SET status='cleared' WHERE offboarding_id=$1")
        .bind(offboarding_id)
        .execute(&pool)
        .await?;
    assert!(
        svc.clear(offboarding_id).await?,
        "resolved items let the clear through"
    );
    assert!(
        !svc.clear(offboarding_id).await?,
        "re-clear is a no-op"
    );

    let status: String =
        sqlx::query_scalar("SELECT status::text FROM lifecycle.offboardings WHERE id=$1")
            .bind(offboarding_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(status, "cleared", "clear stamped the derived state");
    Ok(())
}

/// A producer verb with NO ambient org scope fails closed and mutates nothing.
///
/// The module is tenant-agnostic (ADR-0029): it never guesses a company. The seams that
/// still key on one — the outbox record, the event payload's `company_id` (the downstream
/// consumers are still company-scoped and read it), the cross-module pesangon input reads —
/// take that company from the request scope the composing service binds. With no scope
/// bound, every producer verb refuses with `NoCompanyScope` before any statement runs:
/// nothing mutates, nothing stages. Probes one verb per producer service, then asserts zero
/// mutation and zero staged events.
#[tokio::test]
async fn producers_fail_closed_without_a_bound_org_scope() -> Result<(), Box<dyn std::error::Error>>
{
    use backbone_lifecycle::application::service::{FinalSettlementWriteService, FinalSettlementError};

    let pool = match connect().await {
        Some(p) => p,
        None => return Ok(()),
    };
    setup(&pool).await?;
    truncate_all(&pool).await?;

    // One row per workflow carrier, each parked one verb away from a state change (so an
    // unscoped success would be visible in the row).
    let promotion_id: Uuid = sqlx::query(
        r#"INSERT INTO lifecycle.promotions (employee_id, effective_date, status)
           VALUES ($1,NOW(),'approved') RETURNING id"#,
    )
    .bind(Uuid::new_v4())
    .fetch_one(&pool)
    .await?
    .get("id");

    let past_end = Utc::now().date_naive() - chrono::Duration::days(1);
    let onboarding_id: Uuid = sqlx::query(
        r#"INSERT INTO lifecycle.onboardings (employee_id, start_date, status, probation_end_date)
           VALUES ($1,NOW(),'completed',$2) RETURNING id"#,
    )
    .bind(Uuid::new_v4())
    .bind(past_end)
    .fetch_one(&pool)
    .await?
    .get("id");

    let offboarding_id: Uuid = sqlx::query(
        r#"INSERT INTO lifecycle.offboardings
               (employee_id, reason, notice_date, last_working_day, status)
           VALUES ($1,'resignation',NOW(),NOW(),'cleared') RETURNING id"#,
    )
    .bind(Uuid::new_v4())
    .fetch_one(&pool)
    .await?
    .get("id");

    // Bare pool, no scope bound: every producer verb is NoCompanyScope, never a guess.
    let err = PromotionWriteService::new(pool.clone())
        .effect(promotion_id)
        .await
        .expect_err("unscoped effect fails closed");
    assert!(
        matches!(
            err,
            backbone_lifecycle::application::service::PromotionEffectError::NoCompanyScope
        ),
        "unscoped effect is NoCompanyScope, got {err:?}"
    );

    let onb = OnboardingWriteService::new(pool.clone());
    let err = onb
        .complete(onboarding_id)
        .await
        .expect_err("unscoped complete fails closed");
    assert!(
        matches!(
            err,
            backbone_lifecycle::application::service::OnboardingCompleteError::NoCompanyScope
        ),
        "unscoped complete is NoCompanyScope, got {err:?}"
    );
    let err = onb
        .confirm(onboarding_id, false)
        .await
        .expect_err("unscoped confirm fails closed");
    assert!(
        matches!(
            err,
            backbone_lifecycle::application::service::OnboardingCompleteError::NoCompanyScope
        ),
        "unscoped confirm is NoCompanyScope, got {err:?}"
    );

    let err = OffboardingWriteService::with_pool(pool.clone())
        .close(offboarding_id)
        .await
        .expect_err("unscoped close fails closed");
    assert!(
        matches!(
            err,
            backbone_lifecycle::application::service::OffboardingCloseError::NoCompanyScope
        ),
        "unscoped close is NoCompanyScope, got {err:?}"
    );

    let err = FinalSettlementWriteService::with_pool(pool.clone())
        .draft_from_offboarding(offboarding_id)
        .await
        .expect_err("unscoped settlement draft fails closed");
    assert!(
        matches!(err, FinalSettlementError::NoCompanyScope),
        "unscoped settlement draft is NoCompanyScope, got {err:?}"
    );

    // Zero mutation, zero events: the rows are exactly as seeded.
    let promo_status: String =
        sqlx::query_scalar("SELECT status::text FROM lifecycle.promotions WHERE id=$1")
            .bind(promotion_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(promo_status, "approved", "unscoped effect mutated nothing");

    let confirmed_at: Option<DateTime<Utc>> =
        sqlx::query_scalar("SELECT confirmed_at FROM lifecycle.onboardings WHERE id=$1")
            .bind(onboarding_id)
            .fetch_one(&pool)
            .await?;
    assert!(confirmed_at.is_none(), "unscoped confirm stamped nothing");

    let off_status: String =
        sqlx::query_scalar("SELECT status::text FROM lifecycle.offboardings WHERE id=$1")
            .bind(offboarding_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(off_status, "cleared", "unscoped close mutated nothing");

    assert_eq!(
        outbox::pending_count(&pool, "lifecycle").await?,
        0,
        "no event staged by an unscoped call"
    );
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────────────────────
// PP 35/2021 settlement: the event and the row agree, settings apply, refusals hold
// ─────────────────────────────────────────────────────────────────────────────────────────────

/// Seed one leaver: an employment, a salary row, leave balances by type code, and a cleared
/// offboarding. Returns the offboarding id.
#[allow(clippy::too_many_arguments)]
async fn seed_leaver(
    pool: &PgPool,
    company_id: Uuid,
    employee_id: Uuid,
    join_date: &str,
    salary: i64,
    reason: &str,
    last_working_day: &str,
    leave: &[(&str, i64)],
) -> sqlx::Result<Uuid> {
    sqlx::query(
        r#"INSERT INTO employee.employments (company_id, employee_id, join_date, status)
           VALUES ($1,$2,$3::date,'active')"#,
    )
    .bind(company_id)
    .bind(employee_id)
    .bind(join_date)
    .execute(pool)
    .await?;
    sqlx::query(
        r#"INSERT INTO payroll.compensation_changes
               (company_id, employee_id, change_type, new_amount, effective_date)
           VALUES ($1,$2,'hire'::compensation_change_type,$3,$4::date)"#,
    )
    .bind(company_id)
    .bind(employee_id)
    .bind(Decimal::from(salary))
    .bind(join_date)
    .execute(pool)
    .await?;
    for (code, days) in leave {
        let type_id = seed_leave_type(pool, code).await?;
        sqlx::query(
            r#"INSERT INTO timeoff.timeoff_balances
                   (company_id, employee_id, timeoff_type_id, period, allocated, used)
               VALUES ($1,$2,$3,'2026',$4,0)"#,
        )
        .bind(company_id)
        .bind(employee_id)
        .bind(type_id)
        .bind(Decimal::from(*days))
        .execute(pool)
        .await?;
    }
    let id: Uuid = sqlx::query_scalar(
        r#"INSERT INTO lifecycle.offboardings
               (employee_id, reason, notice_date, last_working_day, status)
           VALUES ($1,$2::offboarding_reason,$3::date,$3::date,'cleared') RETURNING id"#,
    )
    .bind(employee_id)
    .bind(reason)
    .bind(last_working_day)
    .fetch_one(pool)
    .await?;
    Ok(id)
}

async fn set_setting(pool: &PgPool, key: &str, value: &str) -> sqlx::Result<()> {
    sqlx::query(
        r#"INSERT INTO platform.sysparams (group_name, key, value) VALUES ('lifecycle.offboarding',$1,$2)
           ON CONFLICT (group_name, key) DO UPDATE SET value = EXCLUDED.value"#,
    )
    .bind(key)
    .bind(value)
    .execute(pool)
    .await?;
    Ok(())
}

fn dec(s: &str) -> Decimal {
    Decimal::from_str_exact(s).unwrap()
}

/// A payload number (the producer serialises Decimals as JSON numbers) as a Decimal.
fn payload_dec(v: &serde_json::Value, field: &str) -> Decimal {
    let raw = &v[field];
    raw.as_str()
        .map(|s| s.to_string())
        .unwrap_or_else(|| raw.to_string())
        .parse::<Decimal>()
        .unwrap_or_else(|e| panic!("payload field {field} = {raw}: {e}"))
}

#[tokio::test]
async fn closed_payload_and_drafted_settlement_agree_under_company_settings(
) -> Result<(), Box<dyn std::error::Error>> {
    use backbone_lifecycle::application::service::FinalSettlementWriteService;

    let pool = match connect().await {
        Some(p) => p,
        None => return Ok(()),
    };
    setup(&pool).await?;
    truncate_all(&pool).await?;

    // A voluntary resignation after three completed years, 10M wage, 12 annual-leave days and
    // 30 sick-leave days (sick leave is not paid out). The company pays one month of uang pisah
    // and works a six-day week.
    set_setting(&pool, "uang_pisah.months", "1").await?;
    set_setting(&pool, "daily_wage.work_days_per_week", "6").await?;
    let company_id = Uuid::new_v4();
    let employee_id = Uuid::new_v4();
    let offboarding_id = seed_leaver(
        &pool, company_id, employee_id, "2023-04-10", 10_000_000, "resignation", "2026-10-07",
        &[("ANNUAL", 12), ("SICK", 30)],
    )
    .await?;

    // Close → the event; relay it through payroll's settlement consumer.
    let svc = OffboardingWriteService::with_pool(pool.clone());
    let event_id = scoped(&pool, company_id, svc.close(offboarding_id))
        .await??
        .expect("fresh close stages an event");
    let bus = IntegrationEventBus::new();
    bus.register_handler(std::sync::Arc::new(
        backbone_payroll::application::OffboardingSettlementHandler::new(pool.clone()),
    ))
    .await;
    assert_eq!(drain_lifecycle(&pool, bus).await?, 1);

    // Draft AFTER the consumer has written its `offboarding` compensation row: the wage read
    // must skip it (it is a settlement total, not a salary).
    let settlements = FinalSettlementWriteService::with_pool(pool.clone());
    let settlement_id =
        scoped(&pool, company_id, settlements.draft_from_offboarding(offboarding_id)).await??;

    // Hand-computed (PP 35/2021 Pasal 50; daily wage 10M / 25 = 400,000):
    //   uang pesangon 0 · UPMK 0 · uang pisah 1 × 10M = 10,000,000
    //   leave 12 annual days × 400,000 = 4,800,000 (the 30 sick days are not paid)
    //   last pay: 1-7 Oct 2026 on a six-day week = 6 days × 400,000 = 2,400,000 (via payroll)
    //   net payable = 10,000,000 + 4,800,000 = 14,800,000
    let payload: serde_json::Value =
        sqlx::query_scalar("SELECT payload FROM lifecycle.outbox_events WHERE id=$1")
            .bind(event_id)
            .fetch_one(&pool)
            .await?;
    let b = &payload["pesangon_breakdown"];
    let row = sqlx::query(
        r#"SELECT base_pay, unused_leave_payout, pesangon_amount, net_payable, uang_pesangon,
                  upmk, uang_pisah, monthly_wage, daily_wage, work_days_per_week,
                  unused_leave_days, tenure_years, legal_basis, statutory_effective_from,
                  last_pay_via_payroll
             FROM lifecycle.final_settlements WHERE id=$1"#,
    )
    .bind(settlement_id)
    .fetch_one(&pool)
    .await?;

    let expected = [
        ("uang_pesangon", "uang_pesangon", "0"),
        ("upmk", "upmk", "0"),
        ("uang_pisah", "uang_pisah", "10000000"),
        ("severance_total", "pesangon_amount", "10000000"),
        ("unused_leave_payout", "unused_leave_payout", "4800000"),
        ("last_pay", "base_pay", "2400000"),
        ("net_payable", "net_payable", "14800000"),
        ("monthly_wage", "monthly_wage", "10000000"),
        ("daily_wage", "daily_wage", "400000"),
        ("unused_leave_days", "unused_leave_days", "12"),
    ];
    for (payload_field, column, want) in expected {
        let from_row: Decimal = row.get(column);
        assert_eq!(from_row, dec(want), "row {column}");
        assert_eq!(payload_dec(b, payload_field), from_row, "payload {payload_field} = row {column}");
    }
    assert_eq!(payload_dec(b, "total"), dec("14800000"), "the earlier `total` name = net payable");
    assert_eq!(payload_dec(b, "upm"), Decimal::ZERO, "no UPM under PP 35/2021");
    assert_eq!(row.get::<i32, _>("work_days_per_week"), 6);
    assert_eq!(b["work_days_per_week"], serde_json::json!(6));
    assert_eq!(row.get::<String, _>("legal_basis"), "PP 35/2021 Pasal 50");
    assert_eq!(b["legal_basis"], serde_json::json!("PP 35/2021 Pasal 50"));
    assert_eq!(
        row.get::<chrono::NaiveDate, _>("statutory_effective_from").to_string(),
        "2021-02-02"
    );
    assert!(row.get::<bool, _>("last_pay_via_payroll"));
    let tenure: Decimal = row.get("tenure_years");
    assert!(tenure >= dec("3") && tenure < dec("4"), "three completed years, got {tenure}");
    assert_eq!(payload_dec(b, "tenure_years"), tenure);

    // Payroll recorded the carried total.
    let recorded: Decimal = sqlx::query_scalar(
        "SELECT new_amount FROM payroll.compensation_changes WHERE reference_id=$1",
    )
    .bind(offboarding_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(recorded, dec("14800000"), "payroll's settlement row is the net payable");
    Ok(())
}

#[tokio::test]
async fn unspecific_termination_refuses_close_and_draft() -> Result<(), Box<dyn std::error::Error>> {
    use backbone_lifecycle::application::service::{FinalSettlementError, FinalSettlementWriteService};
    use backbone_lifecycle::application::service::OffboardingCloseError;

    let pool = match connect().await {
        Some(p) => p,
        None => return Ok(()),
    };
    setup(&pool).await?;
    truncate_all(&pool).await?;
    let company_id = Uuid::new_v4();
    let employee_id = Uuid::new_v4();
    let offboarding_id = seed_leaver(
        &pool, company_id, employee_id, "2019-01-07", 10_000_000, "termination", "2026-10-07",
        &[("ANNUAL", 3)],
    )
    .await?;

    let err = scoped(&pool, company_id, OffboardingWriteService::with_pool(pool.clone()).close(offboarding_id))
        .await?
        .expect_err("termination names no PP 35/2021 case");
    assert!(matches!(err, OffboardingCloseError::Pesangon(_)), "{err:?}");
    assert_eq!(err.code(), "unspecific_offboarding_reason");
    assert_eq!(err.http_status(), 422);
    let status: String = sqlx::query_scalar("SELECT status::text FROM lifecycle.offboardings WHERE id=$1")
        .bind(offboarding_id)
        .fetch_one(&pool)
        .await?;
    assert_eq!(status, "cleared", "the refused close changed nothing");
    assert_eq!(outbox::pending_count(&pool, "lifecycle").await?, 0, "no event staged");

    let err = scoped(
        &pool,
        company_id,
        FinalSettlementWriteService::with_pool(pool.clone()).draft_from_offboarding(offboarding_id),
    )
    .await?
    .expect_err("the draft refuses the same way");
    assert!(matches!(err, FinalSettlementError::Pesangon(_)), "{err:?}");
    assert_eq!(err.code(), "unspecific_offboarding_reason");
    let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM lifecycle.final_settlements")
        .fetch_one(&pool)
        .await?;
    assert_eq!(rows, 0, "no settlement row");
    Ok(())
}

#[tokio::test]
async fn a_leaver_who_never_started_settles_at_zero() -> Result<(), Box<dyn std::error::Error>> {
    use backbone_lifecycle::application::service::FinalSettlementWriteService;

    let pool = match connect().await {
        Some(p) => p,
        None => return Ok(()),
    };
    setup(&pool).await?;
    truncate_all(&pool).await?;
    set_setting(&pool, "uang_pisah.months", "1").await?;
    let company_id = Uuid::new_v4();
    let employee_id = Uuid::new_v4();
    // The hire was due on 2 November; the offboarding's last day is 30 October.
    let offboarding_id = seed_leaver(
        &pool, company_id, employee_id, "2026-11-02", 8_500_000, "resignation", "2026-10-30",
        &[("ANNUAL", 12)],
    )
    .await?;
    let id = scoped(
        &pool,
        company_id,
        FinalSettlementWriteService::with_pool(pool.clone()).draft_from_offboarding(offboarding_id),
    )
    .await??;
    let row: (Decimal, Decimal, Decimal, Decimal, Decimal, Decimal) = sqlx::query_as(
        r#"SELECT base_pay, pesangon_amount, uang_pisah, unused_leave_payout, net_payable, tenure_years
             FROM lifecycle.final_settlements WHERE id=$1"#,
    )
    .bind(id)
    .fetch_one(&pool)
    .await?;
    let zero = Decimal::ZERO;
    assert_eq!(row, (zero, zero, zero, zero, zero, zero), "nothing is owed to a leaver who never started");
    Ok(())
}

#[tokio::test]
async fn statutory_set_resolves_as_of_the_last_working_day_and_fails_closed(
) -> Result<(), Box<dyn std::error::Error>> {
    use backbone_lifecycle::application::service::{FinalSettlementWriteService, PesangonError};
    use backbone_lifecycle::infrastructure::persistence::severance_params_repository::severance_params_as_of;
    use chrono::NaiveDate;

    let pool = match connect().await {
        Some(p) => p,
        None => return Ok(()),
    };
    setup(&pool).await?;
    truncate_all(&pool).await?;
    let day = |s: &str| NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap();
    let mut conn = pool.acquire().await?;

    // Before PP 35/2021 took effect there is no set: refused, not defaulted.
    let err = severance_params_as_of(&mut conn, "ID", day("2021-02-01"))
        .await
        .expect_err("no set before 2021-02-02");
    assert!(matches!(err, PesangonError::NoStatutoryParams { .. }), "{err:?}");
    assert_eq!(err.code(), "no_severance_params");
    // Another country has no set at all.
    assert!(severance_params_as_of(&mut conn, "MY", day("2026-10-01")).await.is_err());

    // The seeded set applies from its first day.
    let p = severance_params_as_of(&mut conn, "ID", day("2021-02-02")).await?;
    assert_eq!(p.effective_from(), day("2021-02-02"));
    assert_eq!(p.reasons["retirement"].pesangon_multiplier, dec("1.75"));

    // A newer COMPLETE case set (every row restated, retirement changed) takes over from its
    // effective date; the day before still reads the old one.
    sqlx::query(
        r#"INSERT INTO lifecycle.severance_reason_params
               (country_code, effective_from, reason_code, pesangon_multiplier, upmk_multiplier,
                uang_pisah_eligible, article)
           SELECT country_code, '2030-01-01', reason_code,
                  CASE WHEN reason_code = 'retirement' THEN 2.00 ELSE pesangon_multiplier END,
                  upmk_multiplier, uang_pisah_eligible, article
             FROM lifecycle.severance_reason_params WHERE effective_from = '2021-02-02'"#,
    )
    .execute(&pool)
    .await?;
    let old = severance_params_as_of(&mut conn, "ID", day("2029-12-31")).await?;
    assert_eq!(old.reasons["retirement"].pesangon_multiplier, dec("1.75"));
    let new = severance_params_as_of(&mut conn, "ID", day("2030-01-01")).await?;
    assert_eq!(new.reasons["retirement"].pesangon_multiplier, dec("2"));
    assert_eq!(new.effective_from(), day("2030-01-01"));
    // The scales had no newer set: they still resolve from 2021.
    assert_eq!(new.upmk_effective_from, day("2021-02-02"));

    // A lone correction row is an incomplete set: refused from its date on.
    sqlx::query(
        r#"INSERT INTO lifecycle.severance_reason_params
               (country_code, effective_from, reason_code, pesangon_multiplier, upmk_multiplier,
                uang_pisah_eligible, article)
           VALUES ('ID', '2031-01-01', 'death', 2.5, 1, false, 'a lone correction')"#,
    )
    .execute(&pool)
    .await?;
    let err = severance_params_as_of(&mut conn, "ID", day("2031-06-01"))
        .await
        .expect_err("an incomplete set refuses");
    assert!(matches!(err, PesangonError::IncompleteStatutoryParams { .. }), "{err:?}");
    drop(conn);

    // The draft verb surfaces the refusal for a last working day with no set in force.
    let company_id = Uuid::new_v4();
    let employee_id = Uuid::new_v4();
    let offboarding_id = seed_leaver(
        &pool, company_id, employee_id, "2015-01-05", 9_000_000, "retirement", "2020-12-31", &[],
    )
    .await?;
    let err = scoped(
        &pool,
        company_id,
        FinalSettlementWriteService::with_pool(pool.clone()).draft_from_offboarding(offboarding_id),
    )
    .await?
    .expect_err("no statutory set on 2020-12-31");
    assert_eq!(err.code(), "no_severance_params");
    assert_eq!(err.http_status(), 422);
    Ok(())
}

#[tokio::test]
async fn an_unusable_setting_refuses_rather_than_paying_the_default(
) -> Result<(), Box<dyn std::error::Error>> {
    use backbone_lifecycle::application::service::FinalSettlementWriteService;

    let pool = match connect().await {
        Some(p) => p,
        None => return Ok(()),
    };
    setup(&pool).await?;
    truncate_all(&pool).await?;
    set_setting(&pool, "daily_wage.work_days_per_week", "7").await?;
    let company_id = Uuid::new_v4();
    let employee_id = Uuid::new_v4();
    let offboarding_id = seed_leaver(
        &pool, company_id, employee_id, "2020-01-06", 10_000_000, "resignation", "2026-10-07",
        &[("ANNUAL", 2)],
    )
    .await?;
    let err = scoped(
        &pool,
        company_id,
        FinalSettlementWriteService::with_pool(pool.clone()).draft_from_offboarding(offboarding_id),
    )
    .await?
    .expect_err("a seven-day week is not a PP 36/2021 pattern");
    assert_eq!(err.code(), "invalid_offboarding_setting");
    assert!(err.to_string().contains("daily_wage.work_days_per_week"), "{err}");
    Ok(())
}

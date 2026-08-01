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

use backbone_messaging::{IntegrationEventBus, IntegrationEventEnvelope, IntegrationEventHandler};
use backbone_outbox::{inbox, outbox, relay, OutboxRecord};
use backbone_lifecycle::application::service::{
    OffboardingWriteService, OnboardingWriteService, PromotionWriteService,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::{PgPool, Row};
use uuid::Uuid;

/// Connect to the test DB, or `None` to skip.
async fn connect() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/backbone_hr".into());
    match PgPool::connect(&url).await {
        Ok(p) => Some(p),
        Err(e) => {
            eprintln!("skip career_lifecycle_flows: could not connect to `{url}` ({e}); set DATABASE_URL to run");
            None
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
/// missing-table error in the seed/assert phase. The framework `outbox::migrate` is itself
/// idempotent + concurrency-safe.
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
               company_id UUID NOT NULL,
               employee_id UUID NOT NULL,
               promotion_type promotion_type NOT NULL DEFAULT 'promotion',
               position_id_from UUID, position_id_to UUID,
               level_id_from UUID, level_id_to UUID,
               department_id_from UUID, department_id_to UUID,
               proposed_salary NUMERIC(18,2),
               effective_date DATE NOT NULL,
               status promotion_status NOT NULL DEFAULT 'draft',
               metadata JSONB NOT NULL DEFAULT '{}'::jsonb
           )"#,
        r#"CREATE TABLE IF NOT EXISTS lifecycle.onboardings (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               company_id UUID NOT NULL,
               employee_id UUID NOT NULL,
               start_date DATE NOT NULL,
               status onboarding_status NOT NULL DEFAULT 'pending',
               completed_at TIMESTAMPTZ,
               metadata JSONB NOT NULL DEFAULT '{}'::jsonb
           )"#,
        r#"CREATE TABLE IF NOT EXISTS lifecycle.onboarding_tasks (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               company_id UUID NOT NULL,
               onboarding_id UUID NOT NULL,
               title TEXT NOT NULL,
               category task_category,
               status task_status NOT NULL DEFAULT 'pending',
               metadata JSONB NOT NULL DEFAULT '{}'::jsonb
           )"#,
        r#"CREATE TABLE IF NOT EXISTS lifecycle.offboardings (
               id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
               company_id UUID NOT NULL,
               employee_id UUID NOT NULL,
               reason offboarding_reason NOT NULL DEFAULT 'resignation',
               notice_date DATE NOT NULL,
               last_working_day DATE NOT NULL,
               status offboarding_status NOT NULL DEFAULT 'initiated',
               metadata JSONB NOT NULL DEFAULT '{}'::jsonb
           )"#,
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

    // Outbox + inbox tables (framework DDL) in every schema the flows touch. `outbox::migrate` is
    // idempotent and concurrency-safe (CREATE ... IF NOT EXISTS + caught duplicates). `timeoff` is a
    // CONSUMER schema (its outbox_events is unused) — we migrate it so its `inbox_consumed` exists for
    // the offboarding-encash handler's `inbox::once`.
    outbox::migrate(pool, "lifecycle").await.expect("outbox migrate lifecycle");
    outbox::migrate(pool, "employee").await.expect("outbox migrate employee");
    outbox::migrate(pool, "payroll").await.expect("outbox migrate payroll");
    outbox::migrate(pool, "timeoff").await.expect("outbox migrate timeoff");

    Ok(())
}

/// Isolate a flow from any prior data in the shared shapes.
async fn truncate_all(pool: &PgPool) -> sqlx::Result<()> {
    for stmt in [
        "TRUNCATE lifecycle.promotions, lifecycle.onboardings, lifecycle.onboarding_tasks, lifecycle.offboardings",
        "TRUNCATE employee.employment_histories, employee.employments, employee.employees",
        "TRUNCATE payroll.compensation_changes",
        "TRUNCATE timeoff.timeoff_balances",
        "TRUNCATE lifecycle.outbox_events, employee.inbox_consumed, payroll.inbox_consumed, timeoff.inbox_consumed",
    ] {
        sqlx::query(stmt).execute(pool).await?;
    }
    Ok(())
}

/// Drain `lifecycle.outbox_events` through a bus carrying the registered handlers — exactly as the
/// composer's relay wiring does (envelope id = outbox row id = consumer dedup key).
async fn drain_lifecycle(pool: &PgPool, bus: IntegrationEventBus) -> Result<usize, backbone_outbox::OutboxError> {
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
async fn promotion_effective_flow_applies_both_targets_and_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
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
               (company_id, employee_id, promotion_type, position_id_from, position_id_to,
                proposed_salary, effective_date, status)
           VALUES ($1,$2,'promotion',$3,$4,$5,NOW(),'approved') RETURNING id"#,
    )
    .bind(company_id)
    .bind(employee_id)
    .bind(position_from)
    .bind(position_to)
    .bind(salary)
    .fetch_one(&pool)
    .await?
    .get("id");

    // ── 1. PRODUCER: effect() flips approved→effective + stages promotion.effective, in one tx. ──
    let svc = PromotionWriteService::new(pool.clone());
    let event_id = svc.effect(promotion_id).await?.expect("fresh effect stages an event");

    let promo_status: String =
        sqlx::query_scalar("SELECT status::text FROM lifecycle.promotions WHERE id=$1")
            .bind(promotion_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(promo_status, "effective", "promotion is effective after effect()");
    assert_eq!(outbox::pending_count(&pool, "lifecycle").await?, 1, "one event staged");

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
    assert_eq!(outbox::pending_count(&pool, "lifecycle").await?, 0, "outbox drained");

    // ── 3. ASSERT: employment_histories (employee target) + compensation_changes (payroll target). ─
    let eh_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM employee.employment_histories WHERE reference_id=$1")
            .bind(promotion_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(eh_count, 1, "exactly one employment_history row for this promotion");

    let eh = sqlx::query(
        "SELECT action::text AS action, position_id_from, position_id_to
         FROM employee.employment_histories WHERE reference_id=$1",
    )
    .bind(promotion_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(eh.get::<String, _>("action"), "promotion", "action mapped from promotion_type");
    assert_eq!(eh.get::<Option<Uuid>, _>("position_id_from"), Some(position_from));
    assert_eq!(eh.get::<Option<Uuid>, _>("position_id_to"), Some(position_to));

    let cc = sqlx::query(
        "SELECT change_type::text AS ct, new_amount, reference_id
         FROM payroll.compensation_changes WHERE reference_id=$1",
    )
    .bind(promotion_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(cc.get::<String, _>("ct"), "promotion", "compensation change_type='promotion'");
    assert_eq!(cc.get::<Decimal, _>("new_amount"), salary, "new_amount = proposed_salary");

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
    h1.handle(replay.clone()).await.expect("employee replay is Ok (no-op)");
    h2.handle(replay).await.expect("payroll replay is Ok (no-op)");

    let eh_after: i64 =
        sqlx::query_scalar("SELECT count(*) FROM employee.employment_histories WHERE reference_id=$1")
            .bind(promotion_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(eh_after, 1, "replay did not create a second employment_history");

    let cc_after: i64 =
        sqlx::query_scalar("SELECT count(*) FROM payroll.compensation_changes WHERE reference_id=$1")
            .bind(promotion_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(cc_after, 1, "replay did not create a second compensation_change");

    Ok(())
}

#[tokio::test]
async fn promotion_effective_is_idempotent_at_the_producer() -> Result<(), Box<dyn std::error::Error>> {
    let pool = match connect().await {
        Some(p) => p,
        None => return Ok(()),
    };
    setup(&pool).await?;
    truncate_all(&pool).await?;

    let promotion_id: Uuid = sqlx::query(
        r#"INSERT INTO lifecycle.promotions (company_id, employee_id, effective_date, status)
           VALUES ($1,$2,NOW(),'approved') RETURNING id"#,
    )
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .fetch_one(&pool)
    .await?
    .get("id");

    let svc = PromotionWriteService::new(pool.clone());
    let first = svc.effect(promotion_id).await?.expect("first effect stages an event");
    let second = svc.effect(promotion_id).await?;
    assert!(second.is_none(), "re-effect of an effective promotion stages no second event");
    assert_eq!(outbox::pending_count(&pool, "lifecycle").await?, 1, "still one event — id {first}");
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────────────────────
// Flow 2: onboarding.completed → employee.employments.status='active'
// ─────────────────────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn onboarding_completed_flow_activates_employment_and_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
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
        r#"INSERT INTO lifecycle.onboardings (company_id, employee_id, start_date, status)
           VALUES ($1,$2,NOW(),'in_progress') RETURNING id"#,
    )
    .bind(company_id)
    .bind(employee_id)
    .fetch_one(&pool)
    .await?
    .get("id");

    // ── 1. PRODUCER ───────────────────────────────────────────────────────────────────────────
    let svc = OnboardingWriteService::new(pool.clone());
    let event_id = svc.complete(onboarding_id).await?.expect("fresh complete stages an event");

    let ob = sqlx::query("SELECT status::text AS status, completed_at FROM lifecycle.onboardings WHERE id=$1")
        .bind(onboarding_id)
        .fetch_one(&pool)
        .await?;
    assert_eq!(ob.get::<String, _>("status"), "completed", "onboarding is completed");
    assert!(ob.get::<Option<DateTime<Utc>>, _>("completed_at").is_some(), "completed_at stamped");
    assert_eq!(outbox::pending_count(&pool, "lifecycle").await?, 1, "one event staged");

    // ── 2. RELAY → CONSUMER ───────────────────────────────────────────────────────────────────
    let bus = IntegrationEventBus::new();
    bus.register_handler(std::sync::Arc::new(
        backbone_employee::application::OnboardingCompletedHandler::new(pool.clone()),
    ))
    .await;
    let published = drain_lifecycle(&pool, bus).await?;
    assert_eq!(published, 1);
    assert_eq!(outbox::pending_count(&pool, "lifecycle").await?, 0, "outbox drained");

    // ── 3. ASSERT: employment flipped to active. ──────────────────────────────────────────────
    let emp_status: String =
        sqlx::query_scalar("SELECT status::text FROM employee.employments WHERE employee_id=$1")
            .bind(employee_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(emp_status, "active", "employment activated on onboarding completion");

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
    assert_eq!(emp_status_after, "active", "replay left the employment active (no-op)");
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

    let onboarding_id: Uuid = sqlx::query(
        r#"INSERT INTO lifecycle.onboardings (company_id, employee_id, start_date, status)
           VALUES ($1,$2,NOW(),'in_progress') RETURNING id"#,
    )
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .fetch_one(&pool)
    .await?
    .get("id");

    sqlx::query(
        r#"INSERT INTO lifecycle.onboarding_tasks (company_id, onboarding_id, title, status)
           VALUES ($1,$2,'collect docs','pending')"#,
    )
    .bind(Uuid::new_v4())
    .bind(onboarding_id)
    .execute(&pool)
    .await?;

    let svc = OnboardingWriteService::new(pool.clone());
    let res = svc.complete(onboarding_id).await;
    assert!(res.is_err(), "complete() rejects when a task is still pending");
    assert_eq!(outbox::pending_count(&pool, "lifecycle").await?, 0, "no event staged on rejection");
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────────────────────
// Flow 3: offboarding.closed → employee.employments.status='inactive' + payroll.compensation_changes
// ─────────────────────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn offboarding_closed_flow_deactivates_and_settles_and_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
    let pool = match connect().await {
        Some(p) => p,
        None => return Ok(()),
    };
    setup(&pool).await?;
    truncate_all(&pool).await?;

    let company_id = Uuid::new_v4();
    let employee_id = Uuid::new_v4();

    // ── Seed the three pesangon inputs ─────────────────────────────────────────────────────
    // join_date 2020-01-01 → last_working_day 2024-01-01 is exactly 1461 days (2020 is a leap
    // year), and 1461 / 365.25 = 4.000 tenure years. A clean, hand-computable tenure.
    sqlx::query(
        r#"INSERT INTO employee.employments (company_id, employee_id, join_date, status)
           VALUES ($1,$2,'2020-01-01','active')"#,
    )
    .bind(company_id)
    .bind(employee_id)
    .execute(&pool)
    .await?;

    // Current monthly salary = 22,000,000 (22M / 22 working days = 1,000,000/day — clean leave rate).
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
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await?;

    let offboarding_id: Uuid = sqlx::query(
        r#"INSERT INTO lifecycle.offboardings
               (company_id, employee_id, reason, notice_date, last_working_day, status)
           VALUES ($1,$2,'efficiency','2024-01-01','2024-01-01','cleared') RETURNING id"#,
    )
    .bind(company_id)
    .bind(employee_id)
    .fetch_one(&pool)
    .await?
    .get("id");

    // ── 1. PRODUCER ───────────────────────────────────────────────────────────────────────────
    // with_pool = pool-backed inputs + current-law pesangon config (the same wiring the lifecycle
    // module builder uses by default).
    let svc = OffboardingWriteService::with_pool(pool.clone());
    let event_id = svc.close(offboarding_id).await?.expect("fresh close stages an event");

    let ob_status: String =
        sqlx::query_scalar("SELECT status::text FROM lifecycle.offboardings WHERE id=$1")
            .bind(offboarding_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(ob_status, "closed", "offboarding is closed");
    assert_eq!(outbox::pending_count(&pool, "lifecycle").await?, 1, "one event staged");

    // The staged payload carries the full breakdown (self-auditing event). `->>` yields TEXT, so
    // parse the string back into a Decimal (NUMERIC decoding does not apply to a JSON text fetch).
    let carried_total_str: String =
        sqlx::query_scalar("SELECT payload->'pesangon_breakdown'->>'total' FROM lifecycle.outbox_events WHERE id=$1")
            .bind(event_id)
            .fetch_one(&pool)
            .await?;
    let carried_total = carried_total_str.parse::<Decimal>().expect("total parses");
    assert_eq!(carried_total, Decimal::new(207_400_000, 0), "payload carries the real pesangon total");

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
    assert_eq!(outbox::pending_count(&pool, "lifecycle").await?, 0, "outbox drained");

    // ── 3. ASSERT: employment inactive + REAL settlement row. ─────────────────────────────────
    let emp_status: String =
        sqlx::query_scalar("SELECT status::text FROM employee.employments WHERE employee_id=$1")
            .bind(employee_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(emp_status, "inactive", "employment deactivated on close");

    // Hand-computed 🇮🇩 pesangon for efficiency / 4.000yr tenure / 22M salary / 5 unused leave days:
    //   UPMK     = upmk_scale(4) × 22M         = 4 × 22M   =  88,000,000
    //   pesangon = min(1×4×22M, 8×22M=176M)    =           =  88,000,000
    //   UPM      = 0.15 × (88M + 88M)          =           =  26,400,000
    //   leave    = 5 × (22M / 22)              =           =   5,000,000
    //   total    = 88M + 88M + 26.4M + 5M      =           = 207,400,000
    let row: (String, Decimal) = sqlx::query_as(
        r#"SELECT change_type::text AS change_type, new_amount
             FROM payroll.compensation_changes WHERE reference_id=$1"#,
    )
    .bind(offboarding_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(row.0, "offboarding", "settlement row uses the 'offboarding' change type");
    assert_eq!(row.1, Decimal::new(207_400_000, 0), "settlement new_amount is the real pesangon total");

    let cc_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM payroll.compensation_changes WHERE reference_id=$1")
            .bind(offboarding_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(cc_count, 1, "exactly one settlement row for this offboarding");

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
    h1.handle(replay.clone()).await.expect("employee replay is Ok (no-op)");
    h2.handle(replay).await.expect("payroll replay is Ok (no-op)");

    let emp_after: String =
        sqlx::query_scalar("SELECT status::text FROM employee.employments WHERE employee_id=$1")
            .bind(employee_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(emp_after, "inactive", "replay left the employment inactive");

    let cc_after: i64 =
        sqlx::query_scalar("SELECT count(*) FROM payroll.compensation_changes WHERE reference_id=$1")
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
async fn offboarding_closed_also_zeroes_leave_balance_and_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
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
    .bind(Uuid::new_v4())
    .execute(&pool)
    .await?;

    let offboarding_id: Uuid = sqlx::query(
        r#"INSERT INTO lifecycle.offboardings
               (company_id, employee_id, reason, notice_date, last_working_day, status)
           VALUES ($1,$2,'efficiency','2024-01-01','2024-01-01','cleared') RETURNING id"#,
    )
    .bind(company_id)
    .bind(employee_id)
    .fetch_one(&pool)
    .await?
    .get("id");

    // ── 1. PRODUCER ───────────────────────────────────────────────────────────────────────────
    let svc = OffboardingWriteService::with_pool(pool.clone());
    let event_id = svc.close(offboarding_id).await?.expect("fresh close stages an event");
    assert_eq!(outbox::pending_count(&pool, "lifecycle").await?, 1, "one event staged");

    // ── 2. RELAY → CONSUMER (the timeoff encash target). ───────────────────────────────────────
    let bus = IntegrationEventBus::new();
    bus.register_handler(std::sync::Arc::new(
        backbone_timeoff::application::OffboardingEncashHandler::new(pool.clone()),
    ))
    .await;
    let published = drain_lifecycle(&pool, bus).await?;
    assert_eq!(published, 1);
    assert_eq!(outbox::pending_count(&pool, "lifecycle").await?, 0, "outbox drained");

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
    assert_eq!(balance_after.1, balance_after.0, "replay left the balance zeroed (no-op)");
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────────────────────
// Flow 2b: onboarding.completed → payroll initial CompensationChange (the deferred enroll consumer)
// ─────────────────────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn onboarding_completed_also_seeds_initial_compensation_and_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
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
        r#"INSERT INTO lifecycle.onboardings (company_id, employee_id, start_date, status)
           VALUES ($1,$2,NOW(),'in_progress') RETURNING id"#,
    )
    .bind(company_id)
    .bind(employee_id)
    .fetch_one(&pool)
    .await?
    .get("id");

    // ── 1. PRODUCER ───────────────────────────────────────────────────────────────────────────
    let svc = OnboardingWriteService::new(pool.clone());
    let event_id = svc.complete(onboarding_id).await?.expect("fresh complete stages an event");
    assert_eq!(outbox::pending_count(&pool, "lifecycle").await?, 1, "one event staged");

    // ── 2. RELAY → CONSUMER (the payroll enroll target). ──────────────────────────────────────
    let bus = IntegrationEventBus::new();
    bus.register_handler(std::sync::Arc::new(
        backbone_payroll::application::OnboardingEnrolledHandler::new(pool.clone()),
    ))
    .await;
    let published = drain_lifecycle(&pool, bus).await?;
    assert_eq!(published, 1);
    assert_eq!(outbox::pending_count(&pool, "lifecycle").await?, 0, "outbox drained");

    // ── 3. ASSERT: an initial compensation row seeded from base_salary. ────────────────────────
    let cc: (String, Decimal) = sqlx::query_as(
        r#"SELECT change_type::text AS change_type, new_amount
             FROM payroll.compensation_changes WHERE reference_id=$1"#,
    )
    .bind(onboarding_id)
    .fetch_one(&pool)
    .await?;
    assert_eq!(cc.0, "hire", "initial compensation uses the 'hire' change_type");
    assert_eq!(cc.1, base_salary, "new_amount = the joiner's base_salary");

    let cc_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM payroll.compensation_changes WHERE reference_id=$1")
            .bind(onboarding_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(cc_count, 1, "exactly one initial compensation row for this onboarding");

    assert!(
        inbox::was_consumed(&pool, "payroll", "onboarding.enroll", event_id).await?,
        "payroll inbox recorded the consumption"
    );

    // ── 4. IDEMPOTENCY: replay to the enroll handler — no second row. ──────────────────────────
    let replay = fetch_envelope(&pool, event_id).await?;
    let h = backbone_payroll::application::OnboardingEnrolledHandler::new(pool.clone());
    h.handle(replay).await.expect("replay is Ok (a no-op)");

    let cc_after: i64 =
        sqlx::query_scalar("SELECT count(*) FROM payroll.compensation_changes WHERE reference_id=$1")
            .bind(onboarding_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(cc_after, 1, "replay did not create a second initial compensation row");
    Ok(())
}

#[tokio::test]
async fn onboarding_enrolled_skips_when_no_starting_salary() -> Result<(), Box<dyn std::error::Error>> {
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
        r#"INSERT INTO lifecycle.onboardings (company_id, employee_id, start_date, status)
           VALUES ($1,$2,NOW(),'in_progress') RETURNING id"#,
    )
    .bind(company_id)
    .bind(employee_id)
    .fetch_one(&pool)
    .await?
    .get("id");

    let svc = OnboardingWriteService::new(pool.clone());
    let event_id = svc.complete(onboarding_id).await?.expect("fresh complete stages an event");

    let bus = IntegrationEventBus::new();
    bus.register_handler(std::sync::Arc::new(
        backbone_payroll::application::OnboardingEnrolledHandler::new(pool.clone()),
    ))
    .await;
    drain_lifecycle(&pool, bus).await?;

    // Claim recorded, but NO compensation row.
    let cc_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM payroll.compensation_changes WHERE reference_id=$1")
            .bind(onboarding_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(cc_count, 0, "no compensation row when base_salary is NULL (claim-but-skip)");

    assert!(
        inbox::was_consumed(&pool, "payroll", "onboarding.enroll", event_id).await?,
        "the event was still CLAIMED — a replay is a no-op even with no salary"
    );
    Ok(())
}

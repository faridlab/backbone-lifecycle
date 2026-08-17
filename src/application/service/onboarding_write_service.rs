//! Custom write-service — the lifecycle→employee onboarding handoff (ADR-005 compound events).
//!
//! This is the PRODUCER side of the `onboarding.completed` compound event. [`OnboardingWriteService::complete`]
//! is the one verb with a cross-module side effect, and it stages that side effect the
//! transactional-outbox way: in a SINGLE database transaction it (1) locks the `Onboarding`, (2) asserts
//! every mandatory `OnboardingTask` is resolved (none left `pending`/`blocked`), (3) flips `status` to
//! `completed` + stamps `completed_at=NOW`, and (4) stages a [`ONBOARDING_COMPLETED_EVENT_TYPE`] row into
//! `lifecycle.outbox_events` via the framework's [`backbone_outbox::outbox::stage`].
//!
//! That in-tx write is the load-bearing invariant: the completion transition and the event-emit
//! commit atomically. The relay (in backbone-hr-app) drains the row onto the integration bus; the
//! consumer applies it idempotently (inbox dedup on the event id):
//! - `employee.OnboardingCompletedHandler` — flips `employments.status` to `active`.
//!
//! Payroll enrollment (salary structure + BPJS) is a complex, multi-step target and is intentionally
//! NOT wired here — the event is still emitted, so a future `payroll.OnboardingEnrolledHandler` can
//! subscribe to `onboarding.completed` and enroll without changing the producer. See ADR-005 TODO.
//!
//! This is a user-owned custom file — it is NEVER regenerated.

use backbone_orm::company_scope;
use backbone_outbox::{outbox, OutboxRecord};
use chrono::Utc;
use sqlx::{PgPool, Row};
use uuid::Uuid;

/// The `event_type` stamped on every onboarding-completed outbox row. The employee consumer
/// subscribes to exactly this pattern (`"onboarding.completed"`).
pub const ONBOARDING_COMPLETED_EVENT_TYPE: &str = "onboarding.completed";

/// The `event_type` stamped on every probation-confirmed outbox row. The employee consumer
/// subscribes to exactly this pattern (`"lifecycle.probation_confirmed"`).
pub const PROBATION_CONFIRMED_EVENT_TYPE: &str = "lifecycle.probation_confirmed";

/// Input for [`OnboardingWriteService::create`]. Opening a journey starts it: the row
/// lands `in_progress` (the schema's `pending` stays reserved for imported records —
/// there is no separate start verb, and `complete` only runs on an in-flight journey).
#[derive(Debug, Clone, Default)]
pub struct NewOnboarding {
    pub employee_id: Uuid,
    pub start_date: chrono::NaiveDate,
    /// Planned probation end; confirmation is gated on it (see [`Self::confirm`]).
    pub probation_end_date: Option<chrono::NaiveDate>,
    pub template_id: Option<Uuid>,
}

/// Errors from the onboarding write-service.
#[derive(Debug, thiserror::Error)]
pub enum OnboardingCompleteError {
    /// No `Onboarding` exists for the given id in the caller's company.
    #[error("onboarding {0} not found")]
    NotFound(Uuid),
    /// The onboarding exists but is not `in_progress` (only an in-progress onboarding may be
    /// completed; a `completed` one is a no-op, anything else is a domain violation).
    #[error("onboarding {onboarding_id} is not in_progress (status: {status})")]
    NotInProgress { onboarding_id: Uuid, status: String },
    /// One or more mandatory tasks are still open (`pending` or `blocked`).
    #[error("onboarding {onboarding_id} has {open_count} open task(s) (pending/blocked); resolve before completing")]
    TasksOpen { onboarding_id: Uuid, open_count: i64 },
    /// The onboarding is not `completed` — probation is confirmed on a finished onboarding,
    /// not one still in flight.
    #[error("onboarding {onboarding_id} is not completed (status: {status}); complete it before confirming probation")]
    NotCompleted { onboarding_id: Uuid, status: String },
    /// No `probation_end_date` is planned, so there is nothing to confirm against.
    #[error("onboarding {onboarding_id} has no probation_end_date planned")]
    ProbationNotPlanned { onboarding_id: Uuid },
    /// The probation end date has not been reached and the caller did not force.
    #[error("onboarding {onboarding_id} probation ends {probation_end_date}; not reached yet (pass force to override)")]
    ProbationNotEnded { onboarding_id: Uuid, probation_end_date: chrono::NaiveDate },
    /// A database failure.
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    /// An outbox staging failure.
    #[error("outbox error: {0}")]
    Outbox(#[from] backbone_outbox::OutboxError),
}

impl OnboardingCompleteError {
    /// Stable machine code for the HTTP surface.
    pub fn code(&self) -> &'static str {
        match self {
            OnboardingCompleteError::NotFound(_) => "onboarding_not_found",
            OnboardingCompleteError::NotInProgress { .. } => "onboarding_not_in_progress",
            OnboardingCompleteError::TasksOpen { .. } => "onboarding_tasks_open",
            OnboardingCompleteError::NotCompleted { .. } => "onboarding_not_completed",
            OnboardingCompleteError::ProbationNotPlanned { .. } => "probation_not_planned",
            OnboardingCompleteError::ProbationNotEnded { .. } => "probation_not_ended",
            OnboardingCompleteError::Db(_) => "internal_error",
            OnboardingCompleteError::Outbox(_) => "internal_error",
        }
    }
    /// HTTP status for the HTTP surface.
    pub fn http_status(&self) -> u16 {
        match self {
            OnboardingCompleteError::NotFound(_) => 404,
            OnboardingCompleteError::NotInProgress { .. }
            | OnboardingCompleteError::TasksOpen { .. }
            | OnboardingCompleteError::NotCompleted { .. }
            | OnboardingCompleteError::ProbationNotPlanned { .. }
            | OnboardingCompleteError::ProbationNotEnded { .. } => 422,
            OnboardingCompleteError::Db(_) | OnboardingCompleteError::Outbox(_) => 500,
        }
    }
}

/// The lifecycle write-service that owns the onboarding in_progress→completed transition + the outbox emit.
///
/// Construct with [`OnboardingWriteService::new`]. This is a thin custom service — it does NOT replace
/// the CRUD `OnboardingService`; it adds the one compound-write verb that has a cross-module side
/// effect (the employment-status activation handoff).
pub struct OnboardingWriteService {
    pool: PgPool,
}

impl OnboardingWriteService {
    /// Create a new write-service bound to the given pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Record an onboarding journey in the `in_progress` state — opening it starts it.
    ///
    /// Scoped to the caller's company (the tenant comes from the auth context, never the
    /// body). Returns the new onboarding id. `probation_end_date` is the confirmation
    /// gate [`Self::confirm`] enforces later.
    pub async fn create(
        &self,
        company: Uuid,
        input: NewOnboarding,
    ) -> Result<Uuid, OnboardingCompleteError> {
        let mut tx = self.pool.begin().await?;
        company_scope::bind_company_on(&mut tx, company).await?;

        let id = Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO lifecycle.onboardings
                   (id, company_id, employee_id, start_date, status,
                    probation_end_date, template_id, metadata)
               VALUES ($1, $2, $3, $4, 'in_progress', $5, $6, $7::jsonb)"#,
        )
        .bind(id)
        .bind(company)
        .bind(input.employee_id)
        .bind(input.start_date)
        .bind(input.probation_end_date)
        .bind(input.template_id)
        .bind(
            r#"{"created_at":null,"updated_at":null,"deleted_at":null,
                "created_by":null,"updated_by":null,"deleted_by":null}"#,
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(id)
    }

    /// Mark the onboarding completed and stage an `onboarding.completed` outbox event — atomically.
    ///
    /// "All mandatory tasks done" is enforced as: no `onboarding_tasks` row for this onboarding is in
    /// a `pending` or `blocked` status (the schema carries no explicit `is_mandatory` flag, so a task
    /// is considered resolved when it is `done` OR `skipped`). An onboarding with zero tasks is
    /// completable (the joiner had nothing to do).
    ///
    /// # Returns
    ///
    /// - `Ok(Some(event_id))` on a fresh completion. `event_id` is the outbox row's id — the
    ///   end-to-end dedup key (it becomes the bus envelope id, which the consumer's inbox keys on).
    /// - `Ok(None)` if the onboarding was already `completed`. The producer is idempotent on the
    ///   onboarding's own state: re-calling `complete` on a completed onboarding stages NO second
    ///   event. (Consumer-side inbox dedup is the mandatory backstop regardless.)
    ///
    /// Only an `in_progress` onboarding may be completed; any other non-completed status is an
    /// [`OnboardingCompleteError::NotInProgress`].
    pub async fn complete(
        &self,
        company: Uuid,
        onboarding_id: Uuid,
    ) -> Result<Option<Uuid>, OnboardingCompleteError> {
        let mut tx = self.pool.begin().await?;
        // Bind the caller's company before any statement: the whole path runs
        // under the row-level fence, so a row from another tenant is invisible
        // (a cross-tenant id reads as NotFound, never as a mutable target).
        company_scope::bind_company_on(&mut tx, company).await?;

        // Lock the onboarding row for the duration of the state change + the outbox stage.
        let row = sqlx::query(
            r#"SELECT company_id, employee_id, status::text AS status
                 FROM lifecycle.onboardings
                WHERE id = $1 AND company_id = $2
                FOR UPDATE"#,
        )
        .bind(onboarding_id)
        .bind(company)
        .fetch_optional(&mut *tx)
        .await?;

        let row = match row {
            Some(r) => r,
            None => {
                tx.rollback().await?;
                return Err(OnboardingCompleteError::NotFound(onboarding_id));
            }
        };

        let company_id: Uuid = row.try_get("company_id")?;
        let employee_id: Uuid = row.try_get("employee_id")?;
        let status: String = row.try_get("status")?;

        if status == "completed" {
            // Producer-side idempotency: an already-completed onboarding does not emit a second event.
            tx.rollback().await?;
            return Ok(None);
        }
        if status != "in_progress" {
            tx.rollback().await?;
            return Err(OnboardingCompleteError::NotInProgress { onboarding_id, status });
        }

        // Assert every task is resolved. status::text — `task_status` is a Postgres enum.
        let open_count: i64 = sqlx::query_scalar(
            r#"SELECT count(*) FROM lifecycle.onboarding_tasks
                WHERE onboarding_id = $1
                  AND company_id = $2
                  AND status::text IN ('pending', 'blocked')"#,
        )
        .bind(onboarding_id)
        .bind(company)
        .fetch_one(&mut *tx)
        .await?;
        if open_count > 0 {
            tx.rollback().await?;
            return Err(OnboardingCompleteError::TasksOpen { onboarding_id, open_count });
        }

        // 1. Apply the state change.
        sqlx::query(
            r#"UPDATE lifecycle.onboardings
                  SET status = 'completed', completed_at = NOW()
                WHERE id = $1"#,
        )
        .bind(onboarding_id)
        .execute(&mut *tx)
        .await?;

        // 2. Assemble the payload. The employee consumer flips the employment to `active`; future
        //    payroll enrollment will key off the same employee_id.
        let payload = serde_json::json!({
            "onboarding_id": onboarding_id,
            "company_id": company_id,
            "employee_id": employee_id,
        });

        // 3. Stage the outbox event IN THE SAME TX as the state change.
        let event_id = Uuid::new_v4();
        let rec = OutboxRecord::new(
            ONBOARDING_COMPLETED_EVENT_TYPE,
            "Onboarding",
            onboarding_id.to_string(),
            company_id,
            payload,
            Utc::now(),
        )
        .with_id(event_id);
        outbox::stage(&mut *tx, "lifecycle", &rec).await?;

        tx.commit().await?;
        Ok(Some(event_id))
    }

    /// Confirm the joiner's probation and stage a `lifecycle.probation_confirmed` outbox
    /// event — atomically.
    ///
    /// The onboarding must already be `completed` (probation runs on a working joiner, not
    /// one still mid-journey), and it must carry a planned `probation_end_date`. The date
    /// gate: confirmation is allowed on/after that date, or earlier when `force` is set
    /// (an operator override — e.g. probation waived by policy). `confirmed_at` is stamped
    /// exactly once and doubles as the producer-side idempotency guard.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(event_id))` on a fresh confirmation.
    /// - `Ok(None)` if the onboarding was already confirmed — no second event is staged.
    ///   (Consumer-side inbox dedup is the mandatory backstop regardless.)
    pub async fn confirm(
        &self,
        company: Uuid,
        onboarding_id: Uuid,
        force: bool,
    ) -> Result<Option<Uuid>, OnboardingCompleteError> {
        let mut tx = self.pool.begin().await?;
        company_scope::bind_company_on(&mut tx, company).await?;

        let row = sqlx::query(
            r#"SELECT employee_id, status::text AS status,
                      probation_end_date, confirmed_at
                 FROM lifecycle.onboardings
                WHERE id = $1 AND company_id = $2
                FOR UPDATE"#,
        )
        .bind(onboarding_id)
        .bind(company)
        .fetch_optional(&mut *tx)
        .await?;

        let row = match row {
            Some(r) => r,
            None => {
                tx.rollback().await?;
                return Err(OnboardingCompleteError::NotFound(onboarding_id));
            }
        };

        let employee_id: Uuid = row.try_get("employee_id")?;
        let status: String = row.try_get("status")?;
        let probation_end_date: Option<chrono::NaiveDate> = row.try_get("probation_end_date")?;
        let confirmed_at: Option<chrono::DateTime<Utc>> = row.try_get("confirmed_at")?;

        if confirmed_at.is_some() {
            // Producer-side idempotency: an already-confirmed onboarding does not emit twice.
            tx.rollback().await?;
            return Ok(None);
        }
        if status != "completed" {
            tx.rollback().await?;
            return Err(OnboardingCompleteError::NotCompleted { onboarding_id, status });
        }
        let probation_end_date = match probation_end_date {
            Some(d) => d,
            None => {
                tx.rollback().await?;
                return Err(OnboardingCompleteError::ProbationNotPlanned { onboarding_id });
            }
        };
        let today = Utc::now().date_naive();
        if !force && probation_end_date > today {
            tx.rollback().await?;
            return Err(OnboardingCompleteError::ProbationNotEnded { onboarding_id, probation_end_date });
        }

        // 1. Apply the state change: stamp the confirmation exactly once.
        sqlx::query(
            r#"UPDATE lifecycle.onboardings
                  SET confirmed_at = NOW()
                WHERE id = $1"#,
        )
        .bind(onboarding_id)
        .execute(&mut *tx)
        .await?;

        // 2. Assemble the payload. The employee consumer appends an employment history row
        //    (action='confirmation') and CAS-flips employment_status probation→permanent.
        let payload = serde_json::json!({
            "onboarding_id": onboarding_id,
            "company_id": company,
            "employee_id": employee_id,
            "confirmation_date": today.to_string(),
        });

        // 3. Stage the outbox event IN THE SAME TX as the state change.
        let event_id = Uuid::new_v4();
        let rec = OutboxRecord::new(
            PROBATION_CONFIRMED_EVENT_TYPE,
            "Onboarding",
            onboarding_id.to_string(),
            company,
            payload,
            Utc::now(),
        )
        .with_id(event_id);
        outbox::stage(&mut *tx, "lifecycle", &rec).await?;

        tx.commit().await?;
        Ok(Some(event_id))
    }
}

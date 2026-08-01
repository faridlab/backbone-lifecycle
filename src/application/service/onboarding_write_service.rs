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

use backbone_outbox::{outbox, OutboxRecord};
use chrono::Utc;
use sqlx::{PgPool, Row};
use uuid::Uuid;

/// The `event_type` stamped on every onboarding-completed outbox row. The employee consumer
/// subscribes to exactly this pattern (`"onboarding.completed"`).
pub const ONBOARDING_COMPLETED_EVENT_TYPE: &str = "onboarding.completed";

/// Errors from the onboarding write-service.
#[derive(Debug, thiserror::Error)]
pub enum OnboardingCompleteError {
    /// No `Onboarding` exists for the given id.
    #[error("onboarding {0} not found")]
    NotFound(Uuid),
    /// The onboarding exists but is not `in_progress` (only an in-progress onboarding may be
    /// completed; a `completed` one is a no-op, anything else is a domain violation).
    #[error("onboarding {onboarding_id} is not in_progress (status: {status})")]
    NotInProgress { onboarding_id: Uuid, status: String },
    /// One or more mandatory tasks are still open (`pending` or `blocked`).
    #[error("onboarding {onboarding_id} has {open_count} open task(s) (pending/blocked); resolve before completing")]
    TasksOpen { onboarding_id: Uuid, open_count: i64 },
    /// A database failure.
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    /// An outbox staging failure.
    #[error("outbox error: {0}")]
    Outbox(#[from] backbone_outbox::OutboxError),
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
    pub async fn complete(&self, onboarding_id: Uuid) -> Result<Option<Uuid>, OnboardingCompleteError> {
        let mut tx = self.pool.begin().await?;

        // Lock the onboarding row for the duration of the state change + the outbox stage.
        let row = sqlx::query(
            r#"SELECT company_id, employee_id, status::text AS status
                 FROM lifecycle.onboardings
                WHERE id = $1
                FOR UPDATE"#,
        )
        .bind(onboarding_id)
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
                  AND status::text IN ('pending', 'blocked')"#,
        )
        .bind(onboarding_id)
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
}

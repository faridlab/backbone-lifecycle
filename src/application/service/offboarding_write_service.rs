//! Custom write-service — the lifecycle→employee/payroll offboarding handoff (ADR-005 compound events).
//!
//! This is the PRODUCER side of the `offboarding.closed` compound event, and it carries the REAL
//! 🇮🇩 pesangon (severance) breakdown in the payload. [`OffboardingWriteService::close`] is the one
//! verb with cross-module side effects, and it stages that side effect the transactional-outbox way:
//! in a SINGLE database transaction it (1) locks the `Offboarding`, (2) asserts `status=cleared`,
//! (3) flips `status` to `closed`, and (4) stages a [`OFFBOARDING_CLOSED_EVENT_TYPE`] row into
//! `lifecycle.outbox_events` via the framework's [`backbone_outbox::outbox::stage`].
//!
//! ## Producer-carried pesangon (the acyclic design)
//!
//! The full pesangon calc lives in [`crate::application::service::pesangon`] (pure, config-driven).
//! It needs three cross-module inputs: `join_date` (tenure), current monthly salary, and remaining
//! leave days. Lifecycle OWNS the close + the calc, so the producer gathers those inputs through the
//! [`OffboardingInputs`] port, runs the calc, and embeds the resulting [`PesangonBreakdown`] in the
//! event payload. Payroll's consumer then just writes the `CompensationChange` from the carried
//! breakdown — it does NOT recompute, so payroll never depends on lifecycle and the graph stays
//! acyclic. (See ADR-005.)
//!
//! That in-tx write is the load-bearing invariant: the close transition and the event-emit commit
//! atomically. The relay (in backbone-hr-app) drains the row onto the integration bus; the consumers
//! apply it idempotently (inbox dedup on the event id):
//! - `employee.OffboardingClosedHandler` — flips `employments.status` to `inactive`.
//! - `payroll.OffboardingSettlementHandler` — appends `compensation_changes` (change_type='offboarding',
//!   `new_amount` = the carried pesangon `total`, note carrying the full breakdown).
//!
//! This is a user-owned custom file — it is NEVER regenerated.

use crate::application::service::offboarding_ports::OffboardingInputs;
use crate::application::service::pesangon::{pesangon, PesangonConfig};
use crate::domain::entity::OffboardingReason;
use backbone_outbox::{outbox, OutboxRecord};
use chrono::Utc;
use rust_decimal::Decimal;
use sqlx::{PgPool, Row};
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

/// The `event_type` stamped on every offboarding-closed outbox row. Both consumers subscribe to
/// exactly this pattern (`"offboarding.closed"`).
pub const OFFBOARDING_CLOSED_EVENT_TYPE: &str = "offboarding.closed";

/// Days-per-year divisor for the tenure calculation (astronomical year length — matches the
/// 365.25-day basis used by payroll accruals, so a 4-year span with one leap day lands on exactly
/// 4.000 tenure years). Kept as a string and parsed into a `Decimal` at runtime to stay out of float.
const DAYS_PER_YEAR: &str = "365.25";

/// Errors from the offboarding write-service.
#[derive(Debug, thiserror::Error)]
pub enum OffboardingCloseError {
    /// No `Offboarding` exists for the given id.
    #[error("offboarding {0} not found")]
    NotFound(Uuid),
    /// The offboarding exists but is not `cleared` (only a cleared offboarding may be closed; a
    /// `closed` one is a no-op, anything else is a domain violation).
    #[error("offboarding {offboarding_id} is not cleared (status: {status})")]
    NotCleared { offboarding_id: Uuid, status: String },
    /// The employee has no `join_date` — tenure cannot be computed, so the pesangon cannot be
    /// computed. Fail closed: the offboarding is NOT closed.
    #[error("cannot compute pesangon: employee {employee_id} has no employment join_date")]
    MissingJoinDate { employee_id: Uuid },
    /// The employee has no salary row — the pesangon base is unknown. Fail closed.
    #[error("cannot compute pesangon: employee {employee_id} has no current salary")]
    MissingSalary { employee_id: Uuid },
    /// The offboarding reason could not be parsed back into [`OffboardingReason`]. Should never
    /// happen (the column is the enum) — a corrupt row fails loud rather than silently.
    #[error("invalid offboarding reason '{0}'")]
    BadReason(String),
    /// The pesangon calc rejected the reason (unknown to the config's `reason_rules`).
    #[error("pesangon calc: {0}")]
    Pesangon(#[from] crate::application::service::pesangon::PesangonError),
    /// A database failure.
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    /// An outbox staging failure.
    #[error("outbox error: {0}")]
    Outbox(#[from] backbone_outbox::OutboxError),
}

/// The lifecycle write-service that owns the offboarding cleared→closed transition + the outbox emit.
///
/// Construct with [`OffboardingWriteService::new`] (full: pool + inputs port + pesangon config) or
/// [`OffboardingWriteService::with_pool`] (defaults: pool-backed inputs + current-law config). This
/// is a thin custom service — it does NOT replace the CRUD `OffboardingService`; it adds the one
/// compound-write verb that has a cross-module side effect (the employment-deactivation +
/// final-settlement handoff) and computes the carried pesangon.
pub struct OffboardingWriteService {
    pool: PgPool,
    inputs: Arc<dyn OffboardingInputs>,
    cfg: PesangonConfig,
}

impl OffboardingWriteService {
    /// Create a new write-service bound to the given pool, inputs port, and pesangon config.
    pub fn new(pool: PgPool, inputs: Arc<dyn OffboardingInputs>, cfg: PesangonConfig) -> Self {
        Self { pool, inputs, cfg }
    }

    /// Convenience: pool-backed [`OffboardingInputs`] + current-law [`PesangonConfig::default`].
    /// Use this when the caller just has a pool (the integration test; a future HTTP handler that
    /// does not need to override rates or swap the input source).
    pub fn with_pool(pool: PgPool) -> Self {
        let inputs = Arc::new(
            crate::application::service::offboarding_ports::PoolOffboardingInputs::new(pool.clone()),
        );
        Self::new(pool, inputs, PesangonConfig::default())
    }

    /// Mark the offboarding closed, compute the 🇮🇩 pesangon, and stage an `offboarding.closed`
    /// outbox event carrying the breakdown — all atomically.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(event_id))` on a fresh close. `event_id` is the outbox row's id — the end-to-end
    ///   dedup key (it becomes the bus envelope id, which the consumers' inboxes key on).
    /// - `Ok(None)` if the offboarding was already `closed`. The producer is idempotent on the
    ///   offboarding's own state: re-calling `close` on a closed offboarding stages NO second event.
    ///   (Consumer-side inbox dedup is the mandatory backstop regardless.)
    ///
    /// Only a `cleared` offboarding may be closed; any other non-closed status is an
    /// [`OffboardingCloseError::NotCleared`]. Missing cross-module inputs (`join_date` / salary) fail
    /// closed — the offboarding is NOT closed and NO event is staged.
    pub async fn close(&self, offboarding_id: Uuid) -> Result<Option<Uuid>, OffboardingCloseError> {
        let mut tx = self.pool.begin().await?;

        // Lock the offboarding row for the duration of the state change + the outbox stage.
        let row = sqlx::query(
            r#"SELECT company_id, employee_id, reason::text AS reason, last_working_day, status::text AS status
                 FROM lifecycle.offboardings
                WHERE id = $1
                FOR UPDATE"#,
        )
        .bind(offboarding_id)
        .fetch_optional(&mut *tx)
        .await?;

        let row = match row {
            Some(r) => r,
            None => {
                tx.rollback().await?;
                return Err(OffboardingCloseError::NotFound(offboarding_id));
            }
        };

        let company_id: Uuid = row.try_get("company_id")?;
        let employee_id: Uuid = row.try_get("employee_id")?;
        let reason: String = row.try_get("reason")?;
        let last_working_day: chrono::NaiveDate = row.try_get("last_working_day")?;
        let status: String = row.try_get("status")?;

        if status == "closed" {
            // Producer-side idempotency: an already-closed offboarding does not emit a second event.
            tx.rollback().await?;
            return Ok(None);
        }
        if status != "cleared" {
            tx.rollback().await?;
            return Err(OffboardingCloseError::NotCleared { offboarding_id, status });
        }

        // ── Gather the three cross-module pesangon inputs. These are read-only cross-schema
        //    lookups, run before the state change so a missing prerequisite fails closed (the
        //    offboarding is NOT closed and NO event is staged). ──────────────────────────────
        let join_date = self
            .inputs
            .join_date(employee_id)
            .await?
            .ok_or(OffboardingCloseError::MissingJoinDate { employee_id })?;
        let monthly_salary = self
            .inputs
            .current_monthly_salary(employee_id)
            .await?
            .ok_or(OffboardingCloseError::MissingSalary { employee_id })?;
        let unused_leave_days = self.inputs.remaining_leave_days(employee_id).await?;

        // Tenure in years (Decimal) from day-level math: days_between / 365.25.
        let tenure_years = tenure_years(join_date, last_working_day);

        // Parse the reason back into the typed enum and run the pure calc.
        let reason_enum =
            OffboardingReason::from_str(&reason).map_err(|_| OffboardingCloseError::BadReason(reason.clone()))?;
        let breakdown =
            pesangon(reason_enum, tenure_years, monthly_salary, unused_leave_days, &self.cfg)?;

        // 1. Apply the state change.
        sqlx::query(
            r#"UPDATE lifecycle.offboardings
                  SET status = 'closed'
                WHERE id = $1"#,
        )
        .bind(offboarding_id)
        .execute(&mut *tx)
        .await?;

        // 2. Assemble the payload. Both consumers read off this same JSON: the employee consumer
        //    deactivates the employment; the payroll consumer appends a settlement row whose
        //    new_amount = breakdown.total. `reference_id=offboarding_id` is the idempotency link on
        //    both receiving tables. The full breakdown + the calc inputs are carried so the event is
        //    self-auditing (payroll never needs to recompute or call back into lifecycle).
        let payload = serde_json::json!({
            "offboarding_id": offboarding_id,
            "company_id": company_id,
            "employee_id": employee_id,
            "reason": reason,
            "last_working_day": last_working_day.to_string(),
            "pesangon_breakdown": breakdown,
            "tenure_years": tenure_years,
            "monthly_salary": monthly_salary,
            "unused_leave_days": unused_leave_days,
        });

        // 3. Stage the outbox event IN THE SAME TX as the state change.
        let event_id = Uuid::new_v4();
        let rec = OutboxRecord::new(
            OFFBOARDING_CLOSED_EVENT_TYPE,
            "Offboarding",
            offboarding_id.to_string(),
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

/// Tenure in fractional years between `join_date` and `as_of` (the offboarding's
/// `last_working_day`), using a 365.25-day year. Clamped to `>= 0` (a future-dated join_date
/// yields 0, not a negative tenure). Pure + total — mirrors the calc's own clamping convention.
fn tenure_years(join_date: chrono::NaiveDate, as_of: chrono::NaiveDate) -> Decimal {
    let days = (as_of - join_date).num_days();
    if days <= 0 {
        return Decimal::ZERO;
    }
    let divisor = Decimal::try_from(DAYS_PER_YEAR).unwrap_or_else(|_| Decimal::new(36525, 2));
    Decimal::from(days) / divisor
}

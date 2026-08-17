//! Custom write-service — the lifecycle→employee/payroll promotion handoff (ADR-005 compound events).
//!
//! This is the PRODUCER side of the `promotion.effective` compound event. [`PromotionWriteService::effect`]
//! is the one verb with cross-module side effects, and it stages that side effect the
//! transactional-outbox way: in a SINGLE database transaction it (1) locks the `Promotion`, (2) asserts
//! `status=approved` AND `effective_date` reached, (3) flips `status` to `effective`, and (4) stages a
//! [`PROMOTION_EFFECTIVE_EVENT_TYPE`] row into `lifecycle.outbox_events` via the framework's
//! [`backbone_outbox::outbox::stage`].
//!
//! That in-tx write is the load-bearing invariant: the promotion-effective transition and the
//! event-emit commit atomically, so there is never an "effective with no handoff started" window
//! (nor a handoff for a rolled-back transition). The relay (in backbone-hr-app) drains the row onto
//! the integration bus; the consumers apply it idempotently (inbox dedup on the event id, which the
//! relay preserves end-to-end as the bus envelope id):
//! - `employee.PromotionEffectiveHandler` — appends `employment_histories` (action='promotion').
//! - `payroll.PromotionSalaryHandler` — appends `compensation_changes` (change_type='promotion').
//!
//! This is a user-owned custom file — it is NEVER regenerated, so it is safe to edit freely.

use backbone_orm::company_scope;
use backbone_outbox::{outbox, OutboxRecord};
use chrono::Utc;
use rust_decimal::Decimal;
use sqlx::{PgPool, Row};
use uuid::Uuid;

/// The `event_type` stamped on every promotion-effective outbox row. Both consumers subscribe to
/// exactly this pattern (`"promotion.effective"`).
pub const PROMOTION_EFFECTIVE_EVENT_TYPE: &str = "promotion.effective";

/// Errors from the promotion write-service.
#[derive(Debug, thiserror::Error)]
pub enum PromotionEffectError {
    /// No `Promotion` exists for the given id.
    #[error("promotion {0} not found")]
    NotFound(Uuid),
    /// The promotion exists but is not `approved` (only an approved promotion may be effected; an
    /// already-`effective` one is a no-op, anything else is a domain violation).
    #[error("promotion {promotion_id} is not approved (status: {status})")]
    NotApproved { promotion_id: Uuid, status: String },
    /// The promotion is approved but its `effective_date` is still in the future.
    #[error("promotion {promotion_id} effective_date {effective_date} has not been reached yet")]
    NotYetEffective {
        promotion_id: Uuid,
        effective_date: chrono::NaiveDate,
    },
    /// The promotion exists but is not `pending` (only a pending promotion may be approved;
    /// an already-`approved` one is a no-op, anything else is a domain violation).
    #[error("promotion {promotion_id} is not pending (status: {status})")]
    NotPending { promotion_id: Uuid, status: String },
    /// A database failure.
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    /// An outbox staging failure.
    #[error("outbox error: {0}")]
    Outbox(#[from] backbone_outbox::OutboxError),
}

impl PromotionEffectError {
    /// Stable machine code for the HTTP surface.
    pub fn code(&self) -> &'static str {
        match self {
            PromotionEffectError::NotFound(_) => "promotion_not_found",
            PromotionEffectError::NotApproved { .. } => "promotion_not_approved",
            PromotionEffectError::NotYetEffective { .. } => "promotion_not_yet_effective",
            PromotionEffectError::NotPending { .. } => "promotion_not_pending",
            PromotionEffectError::Db(_) | PromotionEffectError::Outbox(_) => "internal_error",
        }
    }
    /// HTTP status for the HTTP surface.
    pub fn http_status(&self) -> u16 {
        match self {
            PromotionEffectError::NotFound(_) => 404,
            PromotionEffectError::NotApproved { .. }
            | PromotionEffectError::NotYetEffective { .. }
            | PromotionEffectError::NotPending { .. } => 422,
            PromotionEffectError::Db(_) | PromotionEffectError::Outbox(_) => 500,
        }
    }
}

/// Input for [`PromotionWriteService::create`]. Creation IS submission: the row lands
/// `pending` (awaiting approval) — `draft` stays reserved for imported/pre-created records.
#[derive(Debug, Clone, Default)]
pub struct NewPromotion {
    pub employee_id: Uuid,
    /// One of the `promotion_type` labels (promotion/transfer/demotion/lateral); empty = default.
    pub promotion_type: Option<String>,
    pub position_id_from: Option<Uuid>,
    pub position_id_to: Option<Uuid>,
    pub level_id_from: Option<Uuid>,
    pub level_id_to: Option<Uuid>,
    pub department_id_from: Option<Uuid>,
    pub department_id_to: Option<Uuid>,
    pub proposed_salary: Option<Decimal>,
    pub effective_date: chrono::NaiveDate,
    pub requested_by: Option<Uuid>,
    pub reason: Option<String>,
}

/// The lifecycle write-service that owns the promotion approved→effective transition + the outbox emit.
///
/// Construct with [`PromotionWriteService::new`]. This is a thin custom service — it does NOT replace
/// the CRUD `PromotionService`; it adds the one compound-write verb that has a cross-module side
/// effect (the role + salary handoff).
pub struct PromotionWriteService {
    pool: PgPool,
}

impl PromotionWriteService {
    /// Create a new write-service bound to the given pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Record a promotion request in the `pending` state — creation is submission.
    ///
    /// Scoped to the caller's company (the tenant comes from the auth context, never the
    /// body). Returns the new promotion id. `draft` remains a schema-level state for
    /// imported records; the guarded surface always enters at `pending`.
    pub async fn create(
        &self,
        company: Uuid,
        input: NewPromotion,
    ) -> Result<Uuid, PromotionEffectError> {
        let mut tx = self.pool.begin().await?;
        company_scope::bind_company_on(&mut tx, company).await?;

        let id = Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO lifecycle.promotions
                   (id, company_id, employee_id, promotion_type,
                    position_id_from, position_id_to, level_id_from, level_id_to,
                    department_id_from, department_id_to, proposed_salary,
                    effective_date, status, requested_by, reason, metadata)
               VALUES ($1, $2, $3, NULLIF($4, '')::promotion_type,
                       $5, $6, $7, $8, $9, $10, $11,
                       $12, 'pending', $13, $14, $15::jsonb)"#,
        )
        .bind(id)
        .bind(company)
        .bind(input.employee_id)
        .bind(input.promotion_type)
        .bind(input.position_id_from)
        .bind(input.position_id_to)
        .bind(input.level_id_from)
        .bind(input.level_id_to)
        .bind(input.department_id_from)
        .bind(input.department_id_to)
        .bind(input.proposed_salary)
        .bind(input.effective_date)
        .bind(input.requested_by)
        .bind(input.reason)
        .bind(
            r#"{"created_at":null,"updated_at":null,"deleted_at":null,
                "created_by":null,"updated_by":null,"deleted_by":null}"#,
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(id)
    }

    /// Approve a pending promotion — the gate in front of [`Self::effect`].
    ///
    /// A plain state change with no cross-module side effect (the handoff event fires at
    /// `effect`, once, atomically), so it stages no outbox row. `approved_by` stamps who
    /// approved (the operator's user id from the caller).
    ///
    /// # Returns
    ///
    /// - `Ok(true)` on a fresh approval.
    /// - `Ok(false)` if the promotion was already `approved` (idempotent no-op).
    /// - [`PromotionEffectError::NotPending`] for any other status (a `draft` must be
    ///   submitted, an `effective`/`rejected`/`cancelled` one cannot move this way).
    pub async fn approve(
        &self,
        company: Uuid,
        promotion_id: Uuid,
        approved_by: Option<Uuid>,
    ) -> Result<bool, PromotionEffectError> {
        let mut tx = self.pool.begin().await?;
        company_scope::bind_company_on(&mut tx, company).await?;

        let row = sqlx::query(
            r#"SELECT status::text AS status
                 FROM lifecycle.promotions
                WHERE id = $1 AND company_id = $2
                FOR UPDATE"#,
        )
        .bind(promotion_id)
        .bind(company)
        .fetch_optional(&mut *tx)
        .await?;
        let row = match row {
            Some(r) => r,
            None => {
                tx.rollback().await?;
                return Err(PromotionEffectError::NotFound(promotion_id));
            }
        };
        let status: String = row.try_get("status")?;

        if status == "approved" {
            tx.rollback().await?;
            return Ok(false);
        }
        if status != "pending" {
            tx.rollback().await?;
            return Err(PromotionEffectError::NotPending {
                promotion_id,
                status,
            });
        }

        // Belt-and-braces company predicate on the state change: the id was just read under
        // `FOR UPDATE` inside this scope, so the tenant is written into the statement itself.
        sqlx::query(
            r#"UPDATE lifecycle.promotions
                  SET status = 'approved', approved_by = $2
                WHERE id = $1 AND company_id = $3"#,
        )
        .bind(promotion_id)
        .bind(approved_by)
        .bind(company)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(true)
    }

    /// Mark the promotion effective and stage a `promotion.effective` outbox event — atomically.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(event_id))` on a fresh effect. `event_id` is the outbox row's id — the end-to-end
    ///   dedup key (it becomes the bus envelope id, which the consumers' inboxes key on).
    /// - `Ok(None)` if the promotion was already `effective`. The producer is idempotent on the
    ///   promotion's own state: re-calling `effect` on an effective promotion stages NO second event.
    ///   (Consumer-side inbox dedup is the mandatory backstop regardless — it catches relay
    ///   redelivery, which the producer cannot see.)
    ///
    /// Only an `approved` promotion whose `effective_date` has been reached may be effected; any other
    /// non-effective status is a [`PromotionEffectError::NotApproved`], and a future `effective_date`
    /// is a [`PromotionEffectError::NotYetEffective`]. The caller's `company` scopes the whole path —
    /// a promotion id from another tenant reads as [`PromotionEffectError::NotFound`].
    pub async fn effect(
        &self,
        company: Uuid,
        promotion_id: Uuid,
    ) -> Result<Option<Uuid>, PromotionEffectError> {
        let mut tx = self.pool.begin().await?;
        // Bind the caller's company before any statement: the whole path runs
        // under the row-level fence, so a row from another tenant is invisible
        // (a cross-tenant id reads as NotFound, never as a mutable target).
        company_scope::bind_company_on(&mut tx, company).await?;

        // Lock the promotion row for the duration of the state change + the outbox stage, so a
        // concurrent effect cannot race a second transition. `status::text` — the column is a Postgres
        // enum (`promotion_status`); sqlx will not decode an enum straight to a Rust `String`, so cast
        // here and compare below.
        let row = sqlx::query(
            r#"SELECT company_id, employee_id, promotion_type::text AS promotion_type,
                      position_id_from, position_id_to, level_id_from, level_id_to,
                      department_id_from, department_id_to, proposed_salary,
                      effective_date, status::text AS status
                 FROM lifecycle.promotions
                WHERE id = $1 AND company_id = $2
                FOR UPDATE"#,
        )
        .bind(promotion_id)
        .bind(company)
        .fetch_optional(&mut *tx)
        .await?;

        let row = match row {
            Some(r) => r,
            None => {
                tx.rollback().await?;
                return Err(PromotionEffectError::NotFound(promotion_id));
            }
        };

        let company_id: Uuid = row.try_get("company_id")?;
        let employee_id: Uuid = row.try_get("employee_id")?;
        let promotion_type: String = row.try_get("promotion_type")?;
        let position_id_from: Option<Uuid> = row.try_get("position_id_from")?;
        let position_id_to: Option<Uuid> = row.try_get("position_id_to")?;
        let level_id_from: Option<Uuid> = row.try_get("level_id_from")?;
        let level_id_to: Option<Uuid> = row.try_get("level_id_to")?;
        let department_id_from: Option<Uuid> = row.try_get("department_id_from")?;
        let department_id_to: Option<Uuid> = row.try_get("department_id_to")?;
        let proposed_salary: Option<Decimal> = row.try_get("proposed_salary")?;
        let effective_date: chrono::NaiveDate = row.try_get("effective_date")?;
        let status: String = row.try_get("status")?;

        if status == "effective" {
            // Producer-side idempotency: an already-effective promotion does not emit a second event.
            tx.rollback().await?;
            return Ok(None);
        }
        if status != "approved" {
            tx.rollback().await?;
            return Err(PromotionEffectError::NotApproved {
                promotion_id,
                status,
            });
        }
        // The move takes effect on/after its effective_date (server-local date comparison).
        let today = Utc::now().date_naive();
        if effective_date > today {
            tx.rollback().await?;
            return Err(PromotionEffectError::NotYetEffective {
                promotion_id,
                effective_date,
            });
        }

        // 1. Apply the state change (same belt-and-braces company predicate as `approve`).
        sqlx::query(
            r#"UPDATE lifecycle.promotions
                  SET status = 'effective'
                WHERE id = $1 AND company_id = $2"#,
        )
        .bind(promotion_id)
        .bind(company)
        .execute(&mut *tx)
        .await?;

        // 2. Assemble the payload. Both consumers read off this same JSON: the employee consumer
        //    appends employment_history (role/level/department from→to); the payroll consumer appends
        //    compensation_changes (proposed_salary). `reference_id=promotion_id` is the idempotency
        //    link on both receiving tables.
        let payload = serde_json::json!({
            "promotion_id": promotion_id,
            "company_id": company_id,
            "employee_id": employee_id,
            "promotion_type": promotion_type,
            "position_id_from": position_id_from,
            "position_id_to": position_id_to,
            "level_id_from": level_id_from,
            "level_id_to": level_id_to,
            "department_id_from": department_id_from,
            "department_id_to": department_id_to,
            // Decimal → string for JSON portability; the payroll consumer parses it back.
            "proposed_salary": proposed_salary.map(|d| d.to_string()),
            "effective_date": effective_date.to_string(),
        });

        // 3. Stage the outbox event IN THE SAME TX as the state change. The outbox row's `id` is the
        //    end-to-end dedup key (the relay preserves it as the bus envelope id, which the consumers'
        //    inboxes key on). `outbox::stage` is idempotent on the id (ON CONFLICT DO NOTHING).
        let event_id = Uuid::new_v4();
        let rec = OutboxRecord::new(
            PROMOTION_EFFECTIVE_EVENT_TYPE,
            "Promotion",
            promotion_id.to_string(),
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

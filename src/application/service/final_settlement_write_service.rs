//! Custom write-service — the final-settlement draft + GL confirmation.
//!
//! Two verbs, both scoped to the caller's company:
//!
//! - [`FinalSettlementWriteService::draft_from_offboarding`] assembles the leaver's
//!   final pay packet from the SAME cross-module inputs the close verb used
//!   ([`OffboardingInputs`] + the pure pesangon calc), so the settlement row and
//!   the `offboarding.closed` event payload can never disagree. One settlement
//!   per offboarding — enforced by a partial unique index, surfaced here as a
//!   409 carrying the existing row's id.
//! - [`FinalSettlementWriteService::confirm`] turns a draft into a balanced GL
//!   envelope and pushes it through the [`GlPostSink`] port (the shared
//!   backbone-gl-posting crate — the one seam every producer reaches accounting
//!   through; lifecycle never depends on backbone-accounting). The envelope is
//!   asserted balanced BEFORE it is sent, and the settlement row is only stamped
//!   (`status=confirmed` + the post/journal ids) AFTER accounting acks — a
//!   rejection leaves the draft untouched and retryable, never silently
//!   unposted, and never confirmed without a GL entry behind it. The
//!   idempotency key is `final_settlement:{company}:{id}`, stable per
//!   settlement, so a retry after a transport failure reuses accounting's dedup
//!   instead of double-posting.
//!
//! Posting shape (severance-type items only — final-period base pay flows
//! through payroll, not this envelope):
//!
//! ```text
//! Dr severance-expense account        pesangon (severance total)
//! Dr leave-encashment-expense account unused-leave payout
//! Cr employee-payable account         pesangon + leave payout
//! ```
//!
//! Tax withholding is deliberately NOT wired yet: a drafted deduction > 0 fails
//! closed with `tax_requires_account` until a withholding account joins the
//! seam. Single currency v1 (IDR), matching the family's posting convention.
//!
//! This is a user-owned custom file — it is NEVER regenerated.

use crate::application::service::offboarding_ports::OffboardingInputs;
use crate::application::service::pesangon::{money, pesangon, PesangonConfig};
use crate::domain::entity::OffboardingReason;
use backbone_gl_posting::{
    AccountingPostEnvelope, GlPostAck, GlPostLine, GlPostRejected, GlPostSink,
};
use backbone_orm::company_scope;
use chrono::{Datelike, Utc};
use rust_decimal::Decimal;
use sqlx::{PgPool, Row};
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

/// Currency stamped on the confirmation envelope. Single-currency v1, matching
/// the family's posting convention.
const ENVELOPE_CURRENCY: &str = "IDR";

/// The `source_type` discriminator accounting dedups this producer under.
const SOURCE_TYPE: &str = "final_settlement";

/// Errors from the final-settlement write-service.
#[derive(Debug, thiserror::Error)]
pub enum FinalSettlementError {
    /// No `Offboarding` exists for the given id in the caller's company.
    #[error("offboarding {0} not found")]
    OffboardingNotFound(Uuid),
    /// No `FinalSettlement` exists for the given id in the caller's company.
    #[error("final settlement {0} not found")]
    NotFound(Uuid),
    /// A settlement already exists for this offboarding (one per offboarding).
    #[error(
        "a final settlement already exists for offboarding {offboarding_id} ({settlement_id})"
    )]
    AlreadyDrafted {
        offboarding_id: Uuid,
        settlement_id: Uuid,
    },
    /// The leaver has no `join_date` — tenure cannot be computed. Fail closed: no draft.
    #[error("cannot compute settlement: employee {employee_id} has no employment join_date")]
    MissingJoinDate { employee_id: Uuid },
    /// The leaver has no salary row — the settlement base is unknown. Fail closed.
    #[error("cannot compute settlement: employee {employee_id} has no current salary")]
    MissingSalary { employee_id: Uuid },
    /// The offboarding reason could not be parsed back into the typed enum.
    #[error("invalid offboarding reason '{0}'")]
    BadReason(String),
    /// The pesangon calc rejected the reason.
    #[error("pesangon calc: {0}")]
    Pesangon(#[from] crate::application::service::pesangon::PesangonError),
    /// The settlement is not `draft` (only a draft may be confirmed; an already-confirmed
    /// one is a no-op via `Ok(None)`).
    #[error("final settlement {settlement_id} is not draft (status: {status})")]
    NotDraft { settlement_id: Uuid, status: String },
    /// Nothing would post — severance and leave payout are both zero.
    #[error("final settlement {0} has nothing to post (severance and leave payout are both zero)")]
    NothingToPost(Uuid),
    /// A tax deduction is drafted but the withholding account is not part of the seam yet.
    #[error("final settlement {0} carries a tax deduction ({1}); supply a withholding account before confirming")]
    TaxRequiresAccount(Uuid, Decimal),
    /// Accounting (through the [`GlPostSink`] port) rejected the envelope.
    #[error("GL post rejected ({code}): {message}")]
    GlRejected { code: String, message: String },
    /// The constructed envelope does not balance — a construction bug, never a data condition.
    #[error("internal error: settlement envelope does not balance (debits {0} != credits {1})")]
    Unbalanced(Decimal, Decimal),
    /// A database failure.
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
}

impl FinalSettlementError {
    /// Stable machine code for the HTTP surface.
    pub fn code(&self) -> &'static str {
        match self {
            FinalSettlementError::OffboardingNotFound(_) => "offboarding_not_found",
            FinalSettlementError::NotFound(_) => "final_settlement_not_found",
            FinalSettlementError::AlreadyDrafted { .. } => "settlement_already_drafted",
            FinalSettlementError::MissingJoinDate { .. } => "missing_join_date",
            FinalSettlementError::MissingSalary { .. } => "missing_salary",
            FinalSettlementError::BadReason(_) => "invalid_offboarding_reason",
            FinalSettlementError::Pesangon(_) => "pesangon_calc_error",
            FinalSettlementError::NotDraft { .. } => "settlement_not_draft",
            FinalSettlementError::NothingToPost(_) => "nothing_to_post",
            FinalSettlementError::TaxRequiresAccount(_, _) => "tax_requires_account",
            FinalSettlementError::GlRejected { .. } => "gl_post_rejected",
            FinalSettlementError::Unbalanced(_, _) => "envelope_unbalanced",
            FinalSettlementError::Db(_) => "internal_error",
        }
    }
    /// HTTP status for the HTTP surface.
    pub fn http_status(&self) -> u16 {
        match self {
            FinalSettlementError::OffboardingNotFound(_) | FinalSettlementError::NotFound(_) => 404,
            FinalSettlementError::AlreadyDrafted { .. } => 409,
            FinalSettlementError::MissingJoinDate { .. }
            | FinalSettlementError::MissingSalary { .. }
            | FinalSettlementError::BadReason(_)
            | FinalSettlementError::Pesangon(_) => 422,
            FinalSettlementError::NotDraft { .. }
            | FinalSettlementError::NothingToPost(_)
            | FinalSettlementError::TaxRequiresAccount(_, _)
            | FinalSettlementError::GlRejected { .. } => 422,
            FinalSettlementError::Unbalanced(_, _) | FinalSettlementError::Db(_) => 500,
        }
    }
}

/// The host-resolved accounts the confirmation envelope needs but the schema
/// does not own: which expense accounts absorb the severance and the leave
/// payout, and which payable account the leaver draws against. Accounting
/// supplies them at composition time — until then `confirm` callers must pass
/// them explicitly (the guarded route takes them on the body).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SettlementAccounts {
    /// Dr target for the pesangon total (severance expense).
    pub severance_expense_account_id: Uuid,
    /// Dr target for the unused-leave payout (leave-encashment expense).
    pub leave_encashment_expense_account_id: Uuid,
    /// Cr target — the leaver's payable.
    pub employee_payable_account_id: Uuid,
}

/// The lifecycle write-service that owns the final-settlement draft + GL confirmation.
///
/// Construct with [`FinalSettlementWriteService::new`] (full: pool + inputs port +
/// pesangon config + GL sink) or [`FinalSettlementWriteService::with_pool`]
/// (defaults: pool-backed inputs + current-law config + unwired GL sink).
pub struct FinalSettlementWriteService {
    pool: PgPool,
    inputs: Arc<dyn OffboardingInputs>,
    cfg: PesangonConfig,
    gl: Arc<dyn GlPostSink>,
}

impl FinalSettlementWriteService {
    /// Create a new write-service bound to the given pool, inputs port, config, and GL sink.
    pub fn new(
        pool: PgPool,
        inputs: Arc<dyn OffboardingInputs>,
        cfg: PesangonConfig,
        gl: Arc<dyn GlPostSink>,
    ) -> Self {
        Self {
            pool,
            inputs,
            cfg,
            gl,
        }
    }

    /// Convenience: pool-backed [`OffboardingInputs`] + current-law [`PesangonConfig`]
    /// defaults + the unwired GL sink (posting fails loudly with `gl_seam_unwired` until a
    /// real sink is supplied).
    pub fn with_pool(pool: PgPool) -> Self {
        let inputs = Arc::new(
            crate::application::service::offboarding_ports::PoolOffboardingInputs::new(
                pool.clone(),
            ),
        );
        Self::new(
            pool,
            inputs,
            PesangonConfig::default(),
            Arc::new(UnwiredGlSink),
        )
    }

    /// Replace the GL sink (the composition-time wiring point for accounting's adapter).
    pub fn with_gl_sink(mut self, gl: Arc<dyn GlPostSink>) -> Self {
        self.gl = gl;
        self
    }

    /// Draft the leaver's final settlement from a closed offboarding — idempotently.
    ///
    /// Assembles from the same inputs the close verb used (join date, current salary,
    /// remaining leave — through [`OffboardingInputs`]) plus the shared pesangon calc,
    /// so the row can never disagree with the `offboarding.closed` event payload:
    /// - `base_pay`: calendar-day proration of the final month:
    ///   `day_of(last_working_day) / days_in_month(last_working_day) × monthly_salary`
    /// - `pesangon_amount`: pesangon + UPMK + UPM (severance proper; leave is separate)
    /// - `unused_leave_payout`: the calc's leave payout
    /// - `net_payable`: the sum of the three
    /// - `period`: `YYYY-MM` of the last working day
    ///
    /// # Returns
    ///
    /// - `Ok(settlement_id)` on a fresh draft.
    /// - [`FinalSettlementError::AlreadyDrafted`] (409, carrying the existing id) when a
    ///   live settlement already exists for this offboarding — the partial unique index
    ///   makes the double-draft impossible even under concurrency; this surfaces it.
    pub async fn draft_from_offboarding(
        &self,
        company: Uuid,
        offboarding_id: Uuid,
    ) -> Result<Uuid, FinalSettlementError> {
        let mut tx = self.pool.begin().await?;
        // Bind the caller's company before any statement: the whole path runs
        // under the row-level fence, so a row from another tenant is invisible
        // (a cross-tenant id reads as NotFound, never as a source of truth).
        company_scope::bind_company_on(&mut tx, company).await?;

        let row = sqlx::query(
            r#"SELECT employee_id, reason::text AS reason, last_working_day
                 FROM lifecycle.offboardings
                WHERE id = $1 AND company_id = $2"#,
        )
        .bind(offboarding_id)
        .bind(company)
        .fetch_optional(&mut *tx)
        .await?;
        let row = match row {
            Some(r) => r,
            None => {
                tx.rollback().await?;
                return Err(FinalSettlementError::OffboardingNotFound(offboarding_id));
            }
        };
        let employee_id: Uuid = row.try_get("employee_id")?;
        let reason: String = row.try_get("reason")?;
        let last_working_day: chrono::NaiveDate = row.try_get("last_working_day")?;

        // Same cross-module inputs as the close verb, gathered before any write so a
        // missing prerequisite fails closed (no partial settlement row).
        let join_date = self
            .inputs
            .join_date(company, employee_id)
            .await?
            .ok_or(FinalSettlementError::MissingJoinDate { employee_id })?;
        let monthly_salary = self
            .inputs
            .current_monthly_salary(company, employee_id)
            .await?
            .ok_or(FinalSettlementError::MissingSalary { employee_id })?;
        let unused_leave_days = self
            .inputs
            .remaining_leave_days(company, employee_id)
            .await?;

        let tenure = crate::application::service::offboarding_write_service::tenure_years(
            join_date,
            last_working_day,
        );
        let reason_enum = OffboardingReason::from_str(&reason)
            .map_err(|_| FinalSettlementError::BadReason(reason.clone()))?;
        let breakdown = pesangon(
            reason_enum,
            tenure,
            monthly_salary,
            unused_leave_days,
            &self.cfg,
        )?;

        // Final-period base pay: calendar-day proration of the leaving month.
        let base_pay = money(
            monthly_salary * Decimal::from(last_working_day.day())
                / Decimal::from(last_working_day.num_days_in_month()),
        );
        // Severance proper (pesangon + UPMK + UPM); the leave payout is its own column.
        let pesangon_amount = money(breakdown.pesangon + breakdown.upmk + breakdown.upm);
        let unused_leave_payout = money(breakdown.unused_leave_payout);
        let net_payable = money(base_pay + pesangon_amount + unused_leave_payout);
        let period = last_working_day.format("%Y-%m").to_string();

        // One live settlement per offboarding. The partial unique index
        // (company_id, offboarding_id) WHERE not soft-deleted arbitrates under
        // concurrency; an empty RETURNING means a draft already exists.
        let id = Uuid::new_v4();
        let inserted: Option<Uuid> = sqlx::query_scalar(
            r#"INSERT INTO lifecycle.final_settlements
                   (id, company_id, employee_id, offboarding_id, period, base_pay,
                    unused_leave_payout, pesangon_amount, tax_deduction, net_payable,
                    status, metadata)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL, $9, 'draft', $10::jsonb)
               ON CONFLICT (company_id, offboarding_id)
                    WHERE (metadata->>'deleted_at') IS NULL
               DO NOTHING
               RETURNING id"#,
        )
        .bind(id)
        .bind(company)
        .bind(employee_id)
        .bind(offboarding_id)
        .bind(&period)
        .bind(base_pay)
        .bind(unused_leave_payout)
        .bind(pesangon_amount)
        .bind(net_payable)
        .bind(
            r#"{"created_at":null,"updated_at":null,"deleted_at":null,
                "created_by":null,"updated_by":null,"deleted_by":null}"#,
        )
        .fetch_optional(&mut *tx)
        .await?;

        let settlement_id = match inserted {
            Some(id) => id,
            None => {
                // Surface the winner, not just the collision.
                let existing: Option<Uuid> = sqlx::query_scalar(
                    r#"SELECT id FROM lifecycle.final_settlements
                        WHERE company_id = $1 AND offboarding_id = $2
                          AND (metadata->>'deleted_at') IS NULL"#,
                )
                .bind(company)
                .bind(offboarding_id)
                .fetch_optional(&mut *tx)
                .await?;
                tx.rollback().await?;
                return Err(FinalSettlementError::AlreadyDrafted {
                    offboarding_id,
                    settlement_id: existing.unwrap_or(id),
                });
            }
        };

        tx.commit().await?;
        Ok(settlement_id)
    }

    /// Confirm a draft settlement: build the balanced envelope, post it through the
    /// [`GlPostSink`] port, and stamp the row only after accounting acks.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(ack))` on a fresh confirmation (the ack carries the post + journal ids
    ///   and whether accounting's idempotency key short-circuited a duplicate).
    /// - `Ok(None)` if the settlement was already confirmed — producer-side idempotency;
    ///   no second envelope is ever sent.
    ///
    /// A sink rejection (including the unwired default's `gl_seam_unwired`) leaves the
    /// row `draft` and retryable: 422 to the caller, nothing stamped, nothing posted.
    pub async fn confirm(
        &self,
        company: Uuid,
        settlement_id: Uuid,
        accounts: SettlementAccounts,
    ) -> Result<Option<GlPostAck>, FinalSettlementError> {
        let mut tx = self.pool.begin().await?;
        company_scope::bind_company_on(&mut tx, company).await?;

        let row = sqlx::query(
            r#"SELECT employee_id, offboarding_id, period, base_pay,
                      unused_leave_payout, pesangon_amount, tax_deduction,
                      status::text AS status, accounting_post_id
                 FROM lifecycle.final_settlements
                WHERE id = $1 AND company_id = $2
                FOR UPDATE"#,
        )
        .bind(settlement_id)
        .bind(company)
        .fetch_optional(&mut *tx)
        .await?;
        let row = match row {
            Some(r) => r,
            None => {
                tx.rollback().await?;
                return Err(FinalSettlementError::NotFound(settlement_id));
            }
        };
        let employee_id: Uuid = row.try_get("employee_id")?;
        let offboarding_id: Uuid = row.try_get("offboarding_id")?;
        let period: String = row.try_get("period")?;
        let unused_leave_payout: Option<Decimal> = row.try_get("unused_leave_payout")?;
        let pesangon_amount: Option<Decimal> = row.try_get("pesangon_amount")?;
        let tax_deduction: Option<Decimal> = row.try_get("tax_deduction")?;
        let status: String = row.try_get("status")?;
        let accounting_post_id: Option<Uuid> = row.try_get("accounting_post_id")?;

        if accounting_post_id.is_some() {
            // Producer-side idempotency: an already-confirmed settlement does not post twice.
            tx.rollback().await?;
            return Ok(None);
        }
        if status != "draft" {
            tx.rollback().await?;
            return Err(FinalSettlementError::NotDraft {
                settlement_id,
                status,
            });
        }

        let severance = pesangon_amount.unwrap_or(Decimal::ZERO);
        let leave = unused_leave_payout.unwrap_or(Decimal::ZERO);
        if let Some(tax) = tax_deduction {
            if tax > Decimal::ZERO {
                tx.rollback().await?;
                return Err(FinalSettlementError::TaxRequiresAccount(settlement_id, tax));
            }
        }
        if severance.is_zero() && leave.is_zero() {
            tx.rollback().await?;
            return Err(FinalSettlementError::NothingToPost(settlement_id));
        }

        // Build the envelope: Dr the severance + leave-encashment expense accounts,
        // Cr the employee payable for the same total — party-tagged to the leaver.
        let mut lines = Vec::with_capacity(3);
        if !severance.is_zero() {
            lines.push(
                GlPostLine::debit(accounts.severance_expense_account_id, severance)
                    .with_party("employee", employee_id)
                    .with_description(format!("final settlement severance · period {period}")),
            );
        }
        if !leave.is_zero() {
            lines.push(
                GlPostLine::debit(accounts.leave_encashment_expense_account_id, leave)
                    .with_party("employee", employee_id)
                    .with_description(format!(
                        "final settlement leave encashment · period {period}"
                    )),
            );
        }
        lines.push(
            GlPostLine::credit(accounts.employee_payable_account_id, severance + leave)
                .with_party("employee", employee_id)
                .with_description(format!(
                    "final settlement payable · offboarding {offboarding_id}"
                )),
        );

        let envelope = AccountingPostEnvelope {
            // Stable per settlement: a retry after a transport failure reuses
            // accounting's dedup instead of double-posting.
            idempotency_key: format!("final_settlement:{company}:{settlement_id}"),
            company_id: company,
            branch_id: None,
            source_type: SOURCE_TYPE.to_string(),
            source_id: settlement_id,
            source_reference: Some(format!("final settlement · period {period}")),
            posting_date: Utc::now().date_naive(),
            currency: ENVELOPE_CURRENCY.to_string(),
            posting_type: "original".to_string(),
            reverses_post_id: None,
            description: Some(format!("final settlement for offboarding {offboarding_id}")),
            lines,
        };
        // Runtime guard, not a debug_assert: the balance must be checked in release builds too.
        // The credit is constructed as the exact sum of the debits, so this fires only on a
        // construction bug — but a post that accounting would reject is cheaper to catch here.
        if !envelope.is_balanced() {
            let (debits, credits) = envelope.totals();
            return Err(FinalSettlementError::Unbalanced(debits, credits));
        }

        // Post BEFORE stamping: the row only says "confirmed" once accounting says
        // the post exists. A rejection rolls everything back — the draft stays
        // retryable, never silently unposted, never posted-without-stamp.
        let ack = match self.gl.post(&envelope).await {
            Ok(ack) => ack,
            Err(rej) => {
                tx.rollback().await?;
                return Err(rejection_into(rej));
            }
        };

        // Belt-and-braces company predicate: the id was read under lock inside this scope;
        // writing the tenant into the statement keeps the invariant visible in the SQL itself.
        sqlx::query(
            r#"UPDATE lifecycle.final_settlements
                  SET status = 'confirmed',
                      accounting_post_id = $2,
                      journal_id = $3
                WHERE id = $1 AND company_id = $4"#,
        )
        .bind(settlement_id)
        .bind(ack.post_id)
        .bind(ack.journal_id)
        .bind(company)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Some(ack))
    }
}

/// The default sink: nothing is wired (accounting composes later). Posting fails
/// loudly with the stable `gl_seam_unwired` code — the settlement stays `draft`
/// and retryable, never silently unposted, and never confirmed without a GL
/// entry behind it.
pub struct UnwiredGlSink;

#[async_trait::async_trait]
impl GlPostSink for UnwiredGlSink {
    async fn post(&self, _envelope: &AccountingPostEnvelope) -> Result<GlPostAck, GlPostRejected> {
        Err(GlPostRejected {
            code: "gl_seam_unwired".to_string(),
            message: "the GL seam is not wired — supply a GlPostSink to confirm settlements"
                .to_string(),
        })
    }
}

/// Map a sink rejection onto the service's error surface.
fn rejection_into(rej: GlPostRejected) -> FinalSettlementError {
    FinalSettlementError::GlRejected {
        code: rej.code,
        message: rej.message,
    }
}

//! The employment-contract write path (#561): create-from-employment, the ONE
//! decision verb (renew / convert / end) filed into the approvals engine, the
//! dispatcher arms that apply the approved outcome, and the expiry reminder
//! tick. The record is the LOAD-BEARING truth: `end` opens the offboarding
//! whose pesangon rule (end_of_contract = no severance) is already built, and
//! the offboarding filer REFUSES an end_of_contract exit with no active PKWT
//! contract behind it (the council's seam-closure guard).
//!
//! Expiry is derived at read (`end_date < today AND status = 'active'`) — no
//! sweep, no expired status. The reminder's watermark (`reminder_sent_at`) is
//! the accrual pattern: once per contract per threshold window.

use chrono::{DateTime, Datelike, NaiveDate, Utc};
use sqlx::PgPool;
use std::sync::Arc;
use uuid::Uuid;

use super::contract_events::{ContractEvent, ContractEventSink, LoggingSink};

/// The statutory cap the cumulative column tracks (UU 11/2020).
pub const PKWT_CAP_MONTHS: i32 = 60;

#[derive(Debug, thiserror::Error)]
pub enum ContractError {
    #[error("db: {0}")]
    Db(#[from] sqlx::Error),
    #[error("not found: {0}")]
    NotFound(&'static str),
    #[error("invalid state: {0}")]
    InvalidState(String),
    #[error("invalid input: {0}")]
    Invalid(String),
    #[error("the contract approvals seam is not wired — supply a ContractApprovalsPort to file a decision")]
    Unwired,
    #[error("contract approvals transport: {0}")]
    Transport(String),
}

impl ContractError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Db(_) => "internal_error",
            Self::NotFound(_) => "not_found",
            Self::InvalidState(_) => "invalid_state",
            Self::Invalid(_) => "invalid_input",
            Self::Unwired => "contract_decision_unwired",
            Self::Transport(_) => "contract_decision_failed",
        }
    }

    pub fn http_status(&self) -> u16 {
        match self {
            Self::Db(_) => 500,
            Self::NotFound(_) => 404,
            _ => 422,
        }
    }
}

/// A new contract (create-from-employment or the backfill).
pub struct NewContract {
    pub employment_id: Uuid,
    pub employee_id: Uuid,
    /// "pkwtt" | "pkwt"
    pub contract_type: String,
    pub contract_no: Option<String>,
    pub start_date: NaiveDate,
    /// Required for pkwt; NULL (indefinite) for pkwtt.
    pub end_date: Option<NaiveDate>,
    pub document_file_id: Option<Uuid>,
    pub template_id: Option<Uuid>,
    pub created_by: Option<Uuid>,
}

/// The decision outcomes. ONE verb files all three; approvals policy
/// discriminates on the outcome field (an HR manager may renew, only a
/// director ends).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContractDecision {
    Renew,
    Convert,
    End,
}

impl ContractDecision {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "renew" => Some(Self::Renew),
            "convert" => Some(Self::Convert),
            "end" => Some(Self::End),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Renew => "renew",
            Self::Convert => "convert",
            Self::End => "end",
        }
    }
}

/// The decision filing the approvals port carries into the engine.
#[derive(Debug, Clone)]
pub struct ContractDecisionFiling {
    pub contract_id: Uuid,
    pub employment_id: Uuid,
    pub employee_id: Uuid,
    pub contract_type: String,
    pub end_date: Option<NaiveDate>,
    pub outcome: &'static str,
    /// The renewed term, when the outcome is renew.
    pub new_end_date: Option<NaiveDate>,
}

/// The approvals seam (the promotion-port posture): unwired, the decision
/// verb fails closed.
#[async_trait::async_trait]
pub trait ContractApprovalsPort: Send + Sync {
    async fn file_decision(&self, filing: &ContractDecisionFiling) -> Result<Uuid, String>;
}

struct UnwiredApprovals;

#[async_trait::async_trait]
impl ContractApprovalsPort for UnwiredApprovals {
    async fn file_decision(&self, _filing: &ContractDecisionFiling) -> Result<Uuid, String> {
        Err("unwired".into())
    }
}

pub struct ContractWriteService {
    pool: PgPool,
    approvals: std::sync::RwLock<Arc<dyn ContractApprovalsPort>>,
    events: std::sync::RwLock<Arc<dyn ContractEventSink>>,
}

impl ContractWriteService {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            approvals: std::sync::RwLock::new(Arc::new(UnwiredApprovals)),
            events: std::sync::RwLock::new(Arc::new(LoggingSink)),
        }
    }

    pub fn set_approvals(&self, port: Arc<dyn ContractApprovalsPort>) {
        *self.approvals.write().expect("contract approvals lock poisoned") = port;
    }

    pub fn set_event_sink(&self, sink: Arc<dyn ContractEventSink>) {
        *self.events.write().expect("contract events lock poisoned") = sink;
    }

    fn events(&self) -> Arc<dyn ContractEventSink> {
        self.events.read().expect("contract events lock poisoned").clone()
    }

    async fn bind_ambient(tx: &mut sqlx::PgConnection) -> Result<(), sqlx::Error> {
        if let Some(scope) = backbone_orm::org_scope::current_org_scope() {
            backbone_orm::org_scope::bind_org_scope_on(tx, &scope).await?;
        }
        Ok(())
    }

    /// Create a contract (the one-active-per-employment index arbitrates a
    /// race; a second active row is a caught conflict, not a double truth).
    pub async fn create(&self, n: NewContract) -> Result<Uuid, ContractError> {
        let is_pkwt = match n.contract_type.as_str() {
            "pkwt" => true,
            "pkwtt" => false,
            other => {
                return Err(ContractError::Invalid(format!(
                    "contract_type must be pkwt or pkwtt, got '{other}'"
                )))
            }
        };
        if is_pkwt && n.end_date.is_none() {
            return Err(ContractError::Invalid("a pkwt contract needs an end_date".into()));
        }
        if let Some(end) = n.end_date {
            if end <= n.start_date {
                return Err(ContractError::Invalid("end_date must be after start_date".into()));
            }
        }
        let id = Uuid::new_v4();
        let mut tx = self.pool.begin().await?;
        Self::bind_ambient(&mut tx).await?;
        // The chain's cumulative ledger starts with this row's own term.
        let cumulative = n
            .end_date
            .map(|end| months_between(n.start_date, end))
            .filter(|_| is_pkwt)
            .unwrap_or(0);
        let ins = sqlx::query(
            r#"INSERT INTO lifecycle.contracts
                   (id, employment_id, employee_id, contract_type, contract_no,
                    start_date, end_date, status, document_file_id, template_id,
                    cumulative_pkwt_months, created_by)
               VALUES ($1, $2, $3, $4::contract_type, $5, $6, $7, 'active', $8, $9, $10, $11)"#,
        )
        .bind(id)
        .bind(n.employment_id)
        .bind(n.employee_id)
        .bind(&n.contract_type)
        .bind(&n.contract_no)
        .bind(n.start_date)
        .bind(n.end_date)
        .bind(n.document_file_id)
        .bind(n.template_id)
        .bind(cumulative)
        .bind(n.created_by);
        if let Err(err) = ins.execute(&mut *tx).await {
            if err.as_database_error().map(|d| d.is_unique_violation()).unwrap_or(false) {
                return Err(ContractError::InvalidState(
                    "this employment already has an active contract".into(),
                ));
            }
            return Err(err.into());
        }
        tx.commit().await?;
        Ok(id)
    }

    /// File the ONE decision (renew / convert / end) into the approvals
    /// engine; the outcome applies ONLY on the approved verdict (the
    /// dispatcher arms). While open, the row reads decision_pending.
    pub async fn file_decision(
        &self,
        contract_id: Uuid,
        outcome: ContractDecision,
        new_end_date: Option<NaiveDate>,
    ) -> Result<Uuid, ContractError> {
        let port = self.approvals.read().expect("contract approvals lock poisoned").clone();
        let mut tx = self.pool.begin().await?;
        Self::bind_ambient(&mut tx).await?;
        use sqlx::Row;
        let row = sqlx::query(
            r#"SELECT employment_id, employee_id, contract_type::text, end_date
                 FROM lifecycle.contracts
                WHERE id = $1 AND status = 'active' AND (metadata->>'deleted_at') IS NULL
                FOR UPDATE"#,
        )
        .bind(contract_id)
        .fetch_optional(&mut *tx)
        .await?;
        let row = match row {
            Some(r) => r,
            None => {
                tx.rollback().await?;
                return Err(ContractError::NotFound("active contract"));
            }
        };
        let employment_id: Uuid = row.try_get("employment_id")?;
        let employee_id: Uuid = row.try_get("employee_id")?;
        let contract_type: String = row.try_get("contract_type")?;
        let end_date: Option<NaiveDate> = row.try_get("end_date")?;

        // Outcome typing: renew keeps the kind (pkwt only); convert goes
        // pkwt → pkwtt; end needs a fixed term to end.
        match outcome {
            ContractDecision::Renew => {
                if contract_type != "pkwt" {
                    return Err(ContractError::Invalid("only a pkwt contract renews (a pkwtt is indefinite)".into()));
                }
                if new_end_date.is_none() {
                    return Err(ContractError::Invalid("a renewal needs the new end_date".into()));
                }
            }
            ContractDecision::Convert => {
                if contract_type != "pkwt" {
                    return Err(ContractError::Invalid("only a pkwt contract converts to pkwtt".into()));
                }
            }
            ContractDecision::End => {
                if end_date.is_none() {
                    return Err(ContractError::Invalid("only a fixed-term contract ends at term".into()));
                }
            }
        }

        let request_id = port
            .file_decision(&ContractDecisionFiling {
                contract_id,
                employment_id,
                employee_id,
                contract_type,
                end_date,
                outcome: outcome.as_str(),
                new_end_date,
            })
            .await
            .map_err(ContractError::Transport)?;

        sqlx::query(
            "UPDATE lifecycle.contracts SET status = 'decision_pending' WHERE id = $1",
        )
        .bind(contract_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(request_id)
    }

    /// The dispatcher's APPROVED arm: apply the outcome. Renew/convert
    /// supersede the row and mint the successor (chain + cumulative); end
    /// marks the row ended and opens the offboarding (reason
    /// end_of_contract, last working day = the contract's end).
    pub async fn apply_decision(
        &self,
        contract_id: Uuid,
        outcome: ContractDecision,
        new_end_date: Option<NaiveDate>,
    ) -> Result<Option<Uuid>, ContractError> {
        let mut tx = self.pool.begin().await?;
        Self::bind_ambient(&mut tx).await?;
        use sqlx::Row;
        let row = sqlx::query(
            r#"SELECT employment_id, employee_id, contract_type::text, end_date,
                      start_date, cumulative_pkwt_months, status::text
                 FROM lifecycle.contracts WHERE id = $1 FOR UPDATE"#,
        )
        .bind(contract_id)
        .fetch_optional(&mut *tx)
        .await?;
        let row = match row {
            Some(r) => r,
            None => return Err(ContractError::NotFound("contract")),
        };
        let status: String = row.try_get("status")?;
        if status == "superseded" || status == "ended" {
            tx.rollback().await?;
            return Ok(None); // idempotent replay
        }
        if status != "decision_pending" {
            return Err(ContractError::InvalidState(format!(
                "the contract is {status}, not decision_pending"
            )));
        }
        let employment_id: Uuid = row.try_get("employment_id")?;
        let employee_id: Uuid = row.try_get("employee_id")?;
        let start_date: NaiveDate = row.try_get("start_date")?;
        let prior_cumulative: i32 = row.try_get("cumulative_pkwt_months")?;
        let end_date: Option<NaiveDate> = row.try_get("end_date")?;

        let mut offboarding_id = None;
        match outcome {
            ContractDecision::Renew | ContractDecision::Convert => {
                let new_type = if outcome == ContractDecision::Convert { "pkwtt" } else { "pkwt" };
                let new_end: Option<chrono::NaiveDate> = if outcome == ContractDecision::Renew {
                    Some(new_end_date.ok_or_else(|| ContractError::Invalid("renewal lost its end_date".into()))?)
                } else {
                    None
                };
                let today = Utc::now().date_naive();
                let cumulative = if new_type == "pkwt" {
                    prior_cumulative + months_between(today, new_end.expect("checked above"))
                } else {
                    0 // conversion resets: the chain is now indefinite
                };
                if new_type == "pkwt" && cumulative > PKWT_CAP_MONTHS {
                    // Recorded, not refused: the cap is stamped so HR sees it
                    // (the conversion decision is the compliant exit).
                    tracing::warn!(
                        target: "contracts",
                        contract_id = %contract_id,
                        cumulative,
                        "pkwt renewal pushes past the 5-year statutory cap"
                    );
                }
                sqlx::query(
                    r#"INSERT INTO lifecycle.contracts
                           (id, employment_id, employee_id, contract_type, start_date,
                            end_date, status, previous_contract_id, cumulative_pkwt_months)
                       VALUES ($1, $2, $3, $4::contract_type, $5, $6, 'active', $7, $8)"#,
                )
                .bind(Uuid::new_v4())
                .bind(employment_id)
                .bind(employee_id)
                .bind(new_type)
                .bind(today)
                .bind(new_end)
                .bind(contract_id)
                .bind(cumulative)
                .execute(&mut *tx)
                .await?;
                sqlx::query(
                    "UPDATE lifecycle.contracts SET status = 'superseded' WHERE id = $1",
                )
                .bind(contract_id)
                .execute(&mut *tx)
                .await?;
                // The employment projection rides the record-change lane at
                // the HOST (the dispatcher composes both sides); the module
                // owns only the contract rows.
            }
            ContractDecision::End => {
                let last_day = end_date
                    .ok_or_else(|| ContractError::Invalid("end lost its end_date".into()))?;
                sqlx::query(
                    "UPDATE lifecycle.contracts SET status = 'ended' WHERE id = $1",
                )
                .bind(contract_id)
                .execute(&mut *tx)
                .await?;
                // The offboarding the pesangon calc already knows: reason
                // end_of_contract = no severance. Notice = the decision date;
                // the last working day = the contract's end.
                let oid = Uuid::new_v4();
                let today = Utc::now().date_naive();
                let notice = if last_day < today { last_day } else { today };
                sqlx::query(
                    r#"INSERT INTO lifecycle.offboardings
                           (id, employee_id, reason, notice_date, last_working_day,
                            status, metadata)
                       VALUES ($1, $2, 'end_of_contract', $3, $4, 'in_progress', $5::jsonb)"#,
                )
                .bind(oid)
                .bind(employee_id)
                .bind(notice)
                .bind(last_day)
                .bind(
                    r#"{"created_at":null,"updated_at":null,"deleted_at":null,
                        "created_by":null,"updated_by":null,"deleted_by":null}"#,
                )
                .execute(&mut *tx)
                .await?;
                offboarding_id = Some(oid);
            }
        }
        tx.commit().await?;
        self.events().publish(ContractEvent::Decided {
            contract_id,
            employee_id,
            outcome: outcome.as_str(),
        });
        Ok(offboarding_id)
    }

    /// The dispatcher's REFUSED arm: the decision dies, the contract stays
    /// active (the row was never touched by the filing).
    pub async fn refuse_decision(&self, contract_id: Uuid) -> Result<(), ContractError> {
        let mut tx = self.pool.begin().await?;
        Self::bind_ambient(&mut tx).await?;
        let moved = sqlx::query(
            "UPDATE lifecycle.contracts SET status = 'active' \
              WHERE id = $1 AND status = 'decision_pending'",
        )
        .bind(contract_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        tx.commit().await?;
        if moved != 1 {
            return Err(ContractError::InvalidState(
                "the contract is not decision_pending".into(),
            ));
        }
        Ok(())
    }

    /// The reminder tick: active PKWT contracts inside the window, reminded
    /// once (the watermark). Emits `lifecycle.contract.expiring` per row.
    pub async fn remind_due(&self, now: DateTime<Utc>, days: i32) -> Result<Vec<Uuid>, ContractError> {
        let mut tx = self.pool.begin().await?;
        Self::bind_ambient(&mut tx).await?;
        let rows: Vec<(Uuid, Uuid, NaiveDate)> = sqlx::query_as(
            r#"SELECT id, employee_id, end_date FROM lifecycle.contracts
                WHERE status = 'active' AND contract_type = 'pkwt'
                  AND end_date IS NOT NULL
                  AND end_date <= ($1::date + make_interval(days => $2))
                  AND reminder_sent_at IS NULL
                  AND (metadata->>'deleted_at') IS NULL
                ORDER BY end_date
                LIMIT 100
                FOR UPDATE SKIP LOCKED"#,
        )
        .bind(now.date_naive())
        .bind(days)
        .fetch_all(&mut *tx)
        .await?;
        for (id, employee_id, end_date) in &rows {
            sqlx::query("UPDATE lifecycle.contracts SET reminder_sent_at = $2 WHERE id = $1")
                .bind(id)
                .bind(now)
                .execute(&mut *tx)
                .await?;
            self.events().publish(ContractEvent::Expiring {
                contract_id: *id,
                employee_id: *employee_id,
                end_date: *end_date,
            });
        }
        tx.commit().await?;
        Ok(rows.iter().map(|(id, _, _)| *id).collect())
    }
}

/// Whole months between two dates (the cap's ledger grain).
fn months_between(from: NaiveDate, to: NaiveDate) -> i32 {
    let mut months = (to.year() - from.year()) * 12 + (to.month() as i32 - from.month() as i32);
    if to.day() < from.day() {
        months -= 1;
    }
    months.max(0)
}

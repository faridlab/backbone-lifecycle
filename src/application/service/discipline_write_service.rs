//! The discipline write path (#562): one SP record per warning letter, valid
//! on service (the council's re-key — acknowledgement is metadata, never a
//! validity gate), with an optional approvals port for the employee's contest.
//!
//! Lifecycle: `issued → acknowledged | acknowledged_pending ⇄ contested`,
//! `cancelled` terminal (HR reason, or an upheld contest). There is NO
//! `expired` status — expiry is the one shared derived predicate
//! [`ACTIVE_PREDICATE`], used by the escalation snapshots, the self lane and
//! any future offboarding validation alike, so no consumer drifts onto a
//! hand-rolled variant.
//!
//! The `prior_active_sp*` columns are ISSUE-TIME SNAPSHOTS: what HR saw when
//! the letter went out (evidence for a PHI/post-hoc review), never live
//! escalation inputs — issuance never refuses on the ladder (gross violations
//! legitimately jump levels).

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use std::sync::Arc;
use uuid::Uuid;

use super::discipline_events::{DisciplineEvent, DisciplineEventSink, LoggingSink};

/// The one shared "active" predicate: issued-and-unexpired (contested and
/// cancelled excluded). Every consumer computes activity through THIS
/// fragment — a per-callsite variant is how the drift bug happens.
pub const ACTIVE_PREDICATE: &str =
    "(status IN ('issued', 'acknowledged', 'acknowledged_pending') AND valid_until >= now() \
      AND (metadata->>'deleted_at') IS NULL)";

#[derive(Debug, thiserror::Error)]
pub enum DisciplineError {
    #[error("db: {0}")]
    Db(#[from] sqlx::Error),
    #[error("not found: {0}")]
    NotFound(&'static str),
    #[error("invalid state: {0}")]
    InvalidState(&'static str),
    #[error("invalid input: {0}")]
    Invalid(String),
    #[error("the discipline approvals seam is not wired — supply a DisciplineApprovalsPort to contest a record")]
    UnwiredContest,
    #[error("discipline approvals transport: {0}")]
    ContestTransport(String),
}

impl DisciplineError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Db(_) => "internal_error",
            Self::NotFound(_) => "not_found",
            Self::InvalidState(_) => "invalid_state",
            Self::Invalid(_) => "invalid_input",
            Self::UnwiredContest => "discipline_contest_unwired",
            Self::ContestTransport(_) => "discipline_contest_failed",
        }
    }

    pub fn http_status(&self) -> u16 {
        match self {
            Self::Db(_) => 500,
            Self::NotFound(_) => 404,
            Self::InvalidState(_) | Self::Invalid(_) | Self::UnwiredContest | Self::ContestTransport(_) => 422,
        }
    }
}

/// A new warning letter. `served_disposition` is mandatory — the employer's
/// act that makes the record valid (council amendment).
pub struct NewDisciplineRecord {
    pub employee_id: Uuid,
    /// "sp1" | "sp2" | "sp3"
    pub level: String,
    pub offense: String,
    pub description: String,
    pub valid_until: DateTime<Utc>,
    /// "delivered" | "witnessed_refusal"
    pub served_disposition: String,
    pub document_file_id: Option<Uuid>,
    pub issued_by: Option<Uuid>,
}

/// The contest filing the approvals port carries into the engine.
#[derive(Debug, Clone)]
pub struct ContestFiling {
    pub record_id: Uuid,
    pub employee_id: Uuid,
    pub level: String,
    pub offense: String,
    pub reason: String,
}

/// The approvals seam for a contest (the promotion-port posture): the default
/// is unwired and `contest` fails closed with a stable code.
#[async_trait::async_trait]
pub trait DisciplineApprovalsPort: Send + Sync {
    /// File the contest; returns the engine's approval request id.
    async fn file_contest(&self, filing: &ContestFiling) -> Result<Uuid, String>;
}

struct UnwiredApprovals;

#[async_trait::async_trait]
impl DisciplineApprovalsPort for UnwiredApprovals {
    async fn file_contest(&self, _filing: &ContestFiling) -> Result<Uuid, String> {
        Err("unwired".into())
    }
}

pub struct DisciplineWriteService {
    pool: PgPool,
    approvals: std::sync::RwLock<Arc<dyn DisciplineApprovalsPort>>,
    events: std::sync::RwLock<Arc<dyn DisciplineEventSink>>,
}

impl DisciplineWriteService {
    /// The database this verb runs on: the composer's request pool when the
    /// tenant router installed one, else the composed pool.
    fn rpool(&self) -> sqlx::PgPool {
        crate::request_pool::current().unwrap_or_else(|| self.pool.clone())
    }

    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            approvals: std::sync::RwLock::new(Arc::new(UnwiredApprovals)),
            events: std::sync::RwLock::new(Arc::new(LoggingSink)),
        }
    }

    /// Wire the approvals port (contest lane). Unwired, `contest` fails closed.
    pub fn set_approvals(&self, port: Arc<dyn DisciplineApprovalsPort>) {
        *self.approvals.write().expect("discipline approvals lock poisoned") = port;
    }

    /// Wire the event sink (the issued notification rides this).
    pub fn set_event_sink(&self, sink: Arc<dyn DisciplineEventSink>) {
        *self.events.write().expect("discipline events lock poisoned") = sink;
    }

    fn events(&self) -> Arc<dyn DisciplineEventSink> {
        self.events.read().expect("discipline events lock poisoned").clone()
    }

    async fn bind_ambient(tx: &mut sqlx::PgConnection) -> Result<(), sqlx::Error> {
        if let Some(scope) = backbone_orm::org_scope::current_org_scope() {
            backbone_orm::org_scope::bind_org_scope_on(tx, &scope).await?;
        }
        Ok(())
    }

    /// Issue a warning letter. Never refuses on the ladder — the prior active
    /// counts are stamped as issue-time evidence.
    pub async fn issue(&self, n: NewDisciplineRecord) -> Result<Uuid, DisciplineError> {
        if !matches!(n.level.as_str(), "sp1" | "sp2" | "sp3") {
            return Err(DisciplineError::Invalid("level must be sp1, sp2 or sp3".into()));
        }
        if !matches!(n.served_disposition.as_str(), "delivered" | "witnessed_refusal") {
            return Err(DisciplineError::Invalid(
                "served_disposition must be delivered or witnessed_refusal".into(),
            ));
        }
        if n.offense.trim().is_empty() || n.description.trim().is_empty() {
            return Err(DisciplineError::Invalid("offense and description must not be empty".into()));
        }
        if n.valid_until <= Utc::now() {
            return Err(DisciplineError::Invalid("valid_until must be in the future".into()));
        }

        let id = Uuid::new_v4();
        let mut tx = self.rpool().begin().await?;
        Self::bind_ambient(&mut tx).await?;
        // The issue-time ladder snapshot (evidence, not enforcement).
        let (prior_sp1, prior_sp2): (i32, i32) = sqlx::query(
            &format!(
                r#"SELECT COUNT(*) FILTER (WHERE level = 'sp1')::int,
                          COUNT(*) FILTER (WHERE level = 'sp2')::int
                     FROM lifecycle.discipline_records
                    WHERE employee_id = $1 AND {ACTIVE_PREDICATE}"#
            ),
        )
        .bind(n.employee_id)
        .fetch_one(&mut *tx)
        .await
        .map(|r| {
            use sqlx::Row;
            (r.get::<i32, _>(0), r.get::<i32, _>(1))
        })?;

        sqlx::query(
            r#"INSERT INTO lifecycle.discipline_records
                   (id, employee_id, level, offense, description, issued_at,
                    valid_until, served_disposition, status,
                    document_file_id, prior_active_sp1, prior_active_sp2, issued_by)
               VALUES ($1, $2, $3::discipline_level, $4, $5, now(), $6,
                       $7::discipline_served_disposition, 'issued', $8, $9, $10, $11)"#,
        )
        .bind(id)
        .bind(n.employee_id)
        .bind(&n.level)
        .bind(&n.offense)
        .bind(&n.description)
        .bind(n.valid_until)
        .bind(&n.served_disposition)
        .bind(n.document_file_id)
        .bind(prior_sp1)
        .bind(prior_sp2)
        .bind(n.issued_by)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        self.events().publish(DisciplineEvent::Issued {
            record_id: id,
            employee_id: n.employee_id,
            level: n.level.clone(),
        });
        Ok(id)
    }

    /// The employee acknowledges (self lane). Acknowledgement is metadata,
    /// never a validity gate — this only stamps the channel and the moment.
    pub async fn acknowledge(&self, record_id: Uuid, employee_id: Uuid) -> Result<(), DisciplineError> {
        let mut tx = self.rpool().begin().await?;
        Self::bind_ambient(&mut tx).await?;
        let moved = sqlx::query(
            r#"UPDATE lifecycle.discipline_records
                  SET status = 'acknowledged', acknowledged_at = now(), acknowledged_via = 'in_app'
                WHERE id = $1 AND employee_id = $2
                  AND status IN ('issued', 'acknowledged_pending')
                  AND (metadata->>'deleted_at') IS NULL"#,
        )
        .bind(record_id)
        .bind(employee_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        tx.commit().await?;
        if moved != 1 {
            return Err(DisciplineError::InvalidState(
                "only your own issued record can be acknowledged",
            ));
        }
        Ok(())
    }

    /// The employee contests (self lane): the record moves to `contested` and
    /// the filing goes to the approvals engine; an upheld contest cancels the
    /// record, a refused one returns it to `acknowledged_pending`.
    pub async fn contest(&self, record_id: Uuid, employee_id: Uuid, reason: String) -> Result<Uuid, DisciplineError> {
        if reason.trim().is_empty() {
            return Err(DisciplineError::Invalid("a contest needs a reason".into()));
        }
        let port = self.approvals.read().expect("discipline approvals lock poisoned").clone();

        let mut tx = self.rpool().begin().await?;
        Self::bind_ambient(&mut tx).await?;
        use sqlx::Row;
        let row = sqlx::query(
            r#"SELECT level::text, offense FROM lifecycle.discipline_records
                WHERE id = $1 AND employee_id = $2
                  AND status IN ('issued', 'acknowledged_pending')
                  AND (metadata->>'deleted_at') IS NULL
                FOR UPDATE"#,
        )
        .bind(record_id)
        .bind(employee_id)
        .fetch_optional(&mut *tx)
        .await?;
        let row = match row {
            Some(r) => r,
            None => {
                tx.rollback().await?;
                return Err(DisciplineError::InvalidState(
                    "only your own issued record can be contested",
                ));
            }
        };
        let level: String = row.try_get("level")?;
        let offense: String = row.try_get("offense")?;

        // File first (outside the tx — the engine owns its own transaction):
        // a refused filing leaves the record untouched.
        let request_id = port
            .file_contest(&ContestFiling {
                record_id,
                employee_id,
                level,
                offense,
                reason,
            })
            .await
            .map_err(DisciplineError::ContestTransport)?;

        let moved = sqlx::query(
            r#"UPDATE lifecycle.discipline_records SET status = 'contested'
                WHERE id = $1 AND status IN ('issued', 'acknowledged_pending')"#,
        )
        .bind(record_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        tx.commit().await?;
        if moved != 1 {
            return Err(DisciplineError::InvalidState("the record changed state under the contest"));
        }
        Ok(request_id)
    }

    /// The contest was UPHELD (the dispatcher's approved arm): the record is
    /// cancelled — it was wrongly issued.
    pub async fn contest_upheld(&self, record_id: Uuid) -> Result<(), DisciplineError> {
        self.move_status(record_id, "contested", "cancelled", Some("contest upheld by review")).await
    }

    /// The contest was REFUSED (the dispatcher's rejected arm): the record
    /// returns to `acknowledged_pending`.
    pub async fn contest_refused(&self, record_id: Uuid) -> Result<(), DisciplineError> {
        self.move_status(record_id, "contested", "acknowledged_pending", None).await
    }

    /// HR cancels a record (reason mandatory — the audited-verb posture).
    pub async fn cancel(&self, record_id: Uuid, reason: String) -> Result<(), DisciplineError> {
        if reason.trim().is_empty() {
            return Err(DisciplineError::Invalid("cancelling a record needs a reason".into()));
        }
        self.move_status(record_id, "any", "cancelled", Some(&reason)).await
    }

    async fn move_status(
        &self,
        record_id: Uuid,
        from: &str,
        to: &str,
        reason: Option<&str>,
    ) -> Result<(), DisciplineError> {
        let mut tx = self.rpool().begin().await?;
        Self::bind_ambient(&mut tx).await?;
        let exists: Option<String> = sqlx::query_scalar(
            "SELECT status::text FROM lifecycle.discipline_records WHERE id = $1",
        )
        .bind(record_id)
        .fetch_optional(&mut *tx)
        .await?;
        match exists.as_deref() {
            None => return Err(DisciplineError::NotFound("discipline record")),
            Some("cancelled") if to != "cancelled" => {
                return Err(DisciplineError::InvalidState("the record is cancelled (terminal)"))
            }
            Some(current) if from != "any" && current != from => {
                let msg = format!("the record is {current}, not {from}");
                return Err(DisciplineError::InvalidState(msg.leak()))
            }
            _ => {}
        }
        sqlx::query(&format!(
            r#"UPDATE lifecycle.discipline_records
                  SET status = '{to}', cancel_reason = $2
                WHERE id = $1 AND status <> 'cancelled'"#
        ))
        .bind(record_id)
        .bind(reason)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }
}

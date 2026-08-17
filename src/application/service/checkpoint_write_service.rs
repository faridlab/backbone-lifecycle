//! Checkpoint write-services (hand-authored, user-owned): create verbs for
//! onboarding tasks and clearance items that can put a mail activity on the
//! responsible person's plate.
//!
//! The generic CRUD surface can already insert rows; these verbs exist for the
//! one thing it cannot do — the notification side effect. Like the interview
//! schedule verb elsewhere in the family, the seam is fail-closed: a create
//! that explicitly asks to notify while no [`ActivitySink`] adapter is wired
//! refuses BEFORE any row is written (422 `activity_seam_unwired`), and a
//! create with no `notify_user_id` stays silent on purpose. A wired adapter is
//! called after commit — it owns its own durability, so a failure there leaves
//! the checkpoint recorded (the true state) and is surfaced for retry.
//!
//! Every verb takes the caller's `company` and binds it onto the transaction
//! before any statement runs, so the whole path is correct under the strict
//! company fence (row-level security).

use crate::application::service::activity_port::{ActivityCommand, ActivityRejected, ActivitySink};
use backbone_orm::company_scope;
use chrono::NaiveDate;
use sqlx::PgPool;
use std::sync::Arc;
use uuid::Uuid;

/// Errors from the checkpoint write-services.
#[derive(Debug, thiserror::Error)]
pub enum CheckpointError {
    /// No onboarding exists for the given id in the caller's company.
    #[error("onboarding {0} not found")]
    OnboardingNotFound(Uuid),
    /// No offboarding exists for the given id in the caller's company.
    #[error("offboarding {0} not found")]
    OffboardingNotFound(Uuid),
    /// The caller asked to notify but no activity adapter is wired.
    #[error("the activity seam is not wired — supply an ActivitySink to notify users")]
    ActivitySeamUnwired,
    /// The wired adapter failed after the checkpoint was already recorded.
    #[error("activity scheduling failed (checkpoint is recorded): {0}")]
    ActivityFailed(#[from] ActivityRejected),
    /// A database failure.
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
}

impl CheckpointError {
    /// Stable machine code for the HTTP surface.
    pub fn code(&self) -> &'static str {
        match self {
            CheckpointError::OnboardingNotFound(_) => "onboarding_not_found",
            CheckpointError::OffboardingNotFound(_) => "offboarding_not_found",
            CheckpointError::ActivitySeamUnwired => "activity_seam_unwired",
            CheckpointError::ActivityFailed(_) => "activity_scheduling_failed",
            CheckpointError::Db(_) => "internal_error",
        }
    }
    /// HTTP status for the HTTP surface.
    pub fn http_status(&self) -> u16 {
        match self {
            CheckpointError::OnboardingNotFound(_) | CheckpointError::OffboardingNotFound(_) => 404,
            CheckpointError::ActivitySeamUnwired | CheckpointError::ActivityFailed(_) => 422,
            CheckpointError::Db(_) => 500,
        }
    }
}

/// Input for [`OnboardingTaskWriteService::create_task`].
#[derive(Debug, Clone)]
pub struct NewOnboardingTask {
    pub onboarding_id: Uuid,
    pub title: String,
    /// Task category label; must parse into the `task_category` enum when set.
    pub category: Option<String>,
    /// The employee the task belongs to (stored on the row).
    pub owner_employee_id: Option<Uuid>,
    pub due_date: Option<NaiveDate>,
    /// The resolved USER to put an activity on. `None` = record silently.
    pub notify_user_id: Option<Uuid>,
}

/// Input for [`ClearanceItemWriteService::create_clearance_item`].
#[derive(Debug, Clone)]
pub struct NewClearanceItem {
    pub offboarding_id: Uuid,
    pub title: String,
    /// The employee responsible for clearing (stored on the row).
    pub clearer_employee_id: Option<Uuid>,
    /// The resolved USER to put an activity on. `None` = record silently.
    pub notify_user_id: Option<Uuid>,
}

/// The canonical audit-metadata JSON every hand-written insert stamps.
const AUDIT_METADATA: &str =
    r#"{"created_at":null,"updated_at":null,"deleted_at":null,"created_by":null,"updated_by":null,"deleted_by":null}"#;

/// Creates onboarding tasks (optionally notifying the owner).
pub struct OnboardingTaskWriteService {
    pool: PgPool,
    activities: Arc<dyn ActivitySink>,
}

impl OnboardingTaskWriteService {
    pub fn new(pool: PgPool, activities: Arc<dyn ActivitySink>) -> Self {
        Self { pool, activities }
    }

    /// Record one onboarding task and optionally schedule the owner's activity.
    ///
    /// Fails closed before any write when `notify_user_id` is set but the seam
    /// is unwired. Returns the new task id.
    pub async fn create_task(&self, company: Uuid, input: NewOnboardingTask) -> Result<Uuid, CheckpointError> {
        if input.notify_user_id.is_some() && !self.activities.is_wired() {
            return Err(CheckpointError::ActivitySeamUnwired);
        }

        let mut tx = self.pool.begin().await?;
        company_scope::bind_company_on(&mut tx, company).await?;

        // The parent onboarding must exist in this company (fenced check).
        let parent: Option<Uuid> = sqlx::query_scalar(
            "SELECT id FROM lifecycle.onboardings WHERE id = $1 AND company_id = $2",
        )
        .bind(input.onboarding_id)
        .bind(company)
        .fetch_optional(&mut *tx)
        .await?;
        if parent.is_none() {
            tx.rollback().await?;
            return Err(CheckpointError::OnboardingNotFound(input.onboarding_id));
        }

        let id = Uuid::new_v4();
        // Captured before the bind moves it — the post-commit activity uses the same text.
        let summary = format!("onboarding task: {}", input.title);
        sqlx::query(
            r#"INSERT INTO lifecycle.onboarding_tasks
                   (id, company_id, onboarding_id, title, category, owner_employee_id,
                    due_date, status, metadata)
               VALUES ($1, $2, $3, $4, NULLIF($5, '')::task_category, $6, $7, 'pending', $8::jsonb)"#,
        )
        .bind(id)
        .bind(company)
        .bind(input.onboarding_id)
        .bind(input.title)
        .bind(input.category)
        .bind(input.owner_employee_id)
        .bind(input.due_date)
        .bind(AUDIT_METADATA)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        // Notify after commit: the adapter owns its own durability. A failure
        // leaves the task recorded (true state) and is surfaced for retry.
        if let Some(user_id) = input.notify_user_id {
            self.activities
                .schedule(ActivityCommand {
                    res_model: "onboarding_task",
                    res_id: id,
                    summary,
                    note: None,
                    deadline: input.due_date,
                    user_id,
                })
                .await?;
        }
        Ok(id)
    }
}

/// Creates clearance items (optionally notifying the responsible party).
pub struct ClearanceItemWriteService {
    pool: PgPool,
    activities: Arc<dyn ActivitySink>,
}

impl ClearanceItemWriteService {
    pub fn new(pool: PgPool, activities: Arc<dyn ActivitySink>) -> Self {
        Self { pool, activities }
    }

    /// Record one clearance item and optionally schedule the clearer's activity.
    ///
    /// Fails closed before any write when `notify_user_id` is set but the seam
    /// is unwired. Returns the new item id.
    pub async fn create_clearance_item(
        &self,
        company: Uuid,
        input: NewClearanceItem,
    ) -> Result<Uuid, CheckpointError> {
        if input.notify_user_id.is_some() && !self.activities.is_wired() {
            return Err(CheckpointError::ActivitySeamUnwired);
        }

        let mut tx = self.pool.begin().await?;
        company_scope::bind_company_on(&mut tx, company).await?;

        let parent: Option<Uuid> = sqlx::query_scalar(
            "SELECT id FROM lifecycle.offboardings WHERE id = $1 AND company_id = $2",
        )
        .bind(input.offboarding_id)
        .bind(company)
        .fetch_optional(&mut *tx)
        .await?;
        if parent.is_none() {
            tx.rollback().await?;
            return Err(CheckpointError::OffboardingNotFound(input.offboarding_id));
        }

        let id = Uuid::new_v4();
        // Captured before the bind moves it — the post-commit activity uses the same text.
        let summary = format!("clearance item: {}", input.title);
        sqlx::query(
            r#"INSERT INTO lifecycle.clearance_items
                   (id, company_id, offboarding_id, title, clearer_employee_id, status, metadata)
               VALUES ($1, $2, $3, $4, $5, 'pending', $6::jsonb)"#,
        )
        .bind(id)
        .bind(company)
        .bind(input.offboarding_id)
        .bind(input.title)
        .bind(input.clearer_employee_id)
        .bind(AUDIT_METADATA)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        if let Some(user_id) = input.notify_user_id {
            self.activities
                .schedule(ActivityCommand {
                    res_model: "clearance_item",
                    res_id: id,
                    summary,
                    note: None,
                    deadline: None,
                    user_id,
                })
                .await?;
        }
        Ok(id)
    }
}

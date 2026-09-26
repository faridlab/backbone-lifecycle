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
//! Tenancy: none, by design (ADR-0029). The module carries no scoping column;
//! a composing service that decorates these tables org-scoped has its
//! middleware bind an org request scope, which every verb propagates onto its
//! transaction. Unfenced deployments have no ambient scope and skip the bind.

use crate::application::service::activity_port::{ActivityCommand, ActivityRejected, ActivitySink};
use chrono::NaiveDate;
use sqlx::PgPool;
use std::sync::Arc;
use uuid::Uuid;

/// Errors from the checkpoint write-services.
#[derive(Debug, thiserror::Error)]
pub enum CheckpointError {
    /// No onboarding exists for the given id in the caller's scope.
    #[error("onboarding {0} not found")]
    OnboardingNotFound(Uuid),
    /// No offboarding exists for the given id in the caller's scope.
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
    /// No onboarding task exists for the given id in the caller's scope.
    #[error("onboarding task {0} not found")]
    TaskNotFound(Uuid),
    /// A resolution verb named something other than done/skipped/blocked.
    #[error("invalid task resolution: {0}")]
    InvalidTaskResolution(String),
}

impl CheckpointError {
    /// Stable machine code for the HTTP surface.
    pub fn code(&self) -> &'static str {
        match self {
            CheckpointError::OnboardingNotFound(_) => "onboarding_not_found",
            CheckpointError::OffboardingNotFound(_) => "offboarding_not_found",
            CheckpointError::ActivitySeamUnwired => "activity_seam_unwired",
            CheckpointError::ActivityFailed(_) => "activity_scheduling_failed",
            CheckpointError::TaskNotFound(_) => "task_not_found",
            CheckpointError::InvalidTaskResolution(_) => "invalid_task_resolution",
            CheckpointError::Db(_) => "internal_error",
        }
    }
    /// HTTP status for the HTTP surface.
    pub fn http_status(&self) -> u16 {
        match self {
            CheckpointError::OnboardingNotFound(_) | CheckpointError::OffboardingNotFound(_) => 404,
            CheckpointError::TaskNotFound(_) => 404,
            CheckpointError::ActivitySeamUnwired
            | CheckpointError::ActivityFailed(_)
            | CheckpointError::InvalidTaskResolution(_) => 422,
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
    events: std::sync::RwLock<std::sync::Arc<dyn super::lifecycle_events::LifecycleEventSink>>,
}

impl OnboardingTaskWriteService {
    /// The database this verb runs on: the composer's request pool when the
    /// tenant router installed one, else the composed pool.
    fn rpool(&self) -> sqlx::PgPool {
        crate::request_pool::current().unwrap_or_else(|| self.pool.clone())
    }

    pub fn new(pool: PgPool, activities: Arc<dyn ActivitySink>) -> Self {
        Self {
            pool,
            activities,
            events: std::sync::RwLock::new(std::sync::Arc::new(
                super::lifecycle_events::LoggingSink,
            )),
        }
    }

    /// Wire the lifecycle event sink (the task-assigned notification rides it).
    pub fn set_event_sink(&self, sink: std::sync::Arc<dyn super::lifecycle_events::LifecycleEventSink>) {
        *self.events.write().expect("lifecycle events lock poisoned") = sink;
    }

    /// Record one onboarding task and optionally schedule the owner's activity.
    ///
    /// Fails closed before any write when `notify_user_id` is set but the seam
    /// is unwired. Returns the new task id.
    pub async fn create_task(&self, input: NewOnboardingTask) -> Result<Uuid, CheckpointError> {
        if input.notify_user_id.is_some() && !self.activities.is_wired() {
            return Err(CheckpointError::ActivitySeamUnwired);
        }

        let mut tx = self.rpool().begin().await?;
        // Propagate the ambient request scope, when one is bound, onto this transaction:
        // rows a deployment's fence decorates are invisible to an unscoped connection.
        // Unfenced deployments have no ambient scope and skip this entirely.
        if let Some(scope) = backbone_orm::org_scope::current_org_scope() {
            backbone_orm::org_scope::bind_org_scope_on(&mut *tx, &scope).await?;
        }

        // The parent onboarding must exist within the bound scope (a fenced
        // deployment makes rows from other units invisible, so a foreign id
        // reads as a missing parent).
        let parent: Option<(Uuid, Uuid)> = sqlx::query(
            "SELECT id, employee_id FROM lifecycle.onboardings WHERE id = $1",
        )
        .bind(input.onboarding_id)
        .fetch_optional(&mut *tx)
        .await?
        .map(|r| {
            use sqlx::Row;
            (r.get::<Uuid, _>("id"), r.get::<Uuid, _>("employee_id"))
        });
        let Some((_, joiner_employee_id)) = parent else {
            tx.rollback().await?;
            return Err(CheckpointError::OnboardingNotFound(input.onboarding_id));
        };

        let id = Uuid::new_v4();
        // Captured before the bind moves it — the post-commit activity uses the same text.
        let summary = format!("onboarding task: {}", input.title);
        sqlx::query(
            r#"INSERT INTO lifecycle.onboarding_tasks
                   (id, onboarding_id, title, category, owner_employee_id,
                    due_date, status, metadata)
               VALUES ($1, $2, $3, NULLIF($4, '')::task_category, $5, $6, 'pending', $7::jsonb)"#,
        )
        .bind(id)
        .bind(input.onboarding_id)
        .bind(input.title.clone())
        .bind(input.category)
        .bind(input.owner_employee_id)
        .bind(input.due_date)
        .bind(AUDIT_METADATA)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        // The assigned owner learns the task is on their plate (#558's
        // task-assigned arm) — fired whenever the row names an owner.
        if let Some(owner_employee_id) = input.owner_employee_id {
            self.events
                .read()
                .expect("lifecycle events lock poisoned")
                .clone()
                .publish(super::lifecycle_events::LifecycleEvent::OnboardingTaskAssigned {
                    task_id: id,
                    onboarding_id: input.onboarding_id,
                    employee_id: joiner_employee_id,
                    owner_employee_id,
                    title: input.title.clone(),
                });
        }

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
    /// Resolve one onboarding task — the verb that records WHO finished it.
    ///
    /// `resolution` is `done` (the step happened), `skipped` (deliberately
    /// not applicable) or `blocked` (cannot proceed; flags the checklist).
    /// Only a `pending` or `blocked` task may move — a terminal task answers
    /// `Ok(false)` (idempotent on the row's own state). The actor and the
    /// moment land in the row's metadata (`resolved_by` / `resolved_at` /
    /// `resolution`), the audit columns the table already carries.
    pub async fn resolve_task(
        &self,
        task_id: Uuid,
        resolution: &'static str,
        actor: Option<Uuid>,
    ) -> Result<bool, CheckpointError> {
        if !matches!(resolution, "done" | "skipped" | "blocked") {
            return Err(CheckpointError::InvalidTaskResolution(resolution.to_string()));
        }
        let mut tx = self.rpool().begin().await?;
        if let Some(scope) = backbone_orm::org_scope::current_org_scope() {
            backbone_orm::org_scope::bind_org_scope_on(&mut *tx, &scope).await?;
        }
        let status: Option<String> = sqlx::query_scalar(
            "SELECT status::text FROM lifecycle.onboarding_tasks WHERE id = $1 FOR UPDATE",
        )
        .bind(task_id)
        .fetch_optional(&mut *tx)
        .await?;
        let Some(status) = status else {
            tx.rollback().await?;
            return Err(CheckpointError::TaskNotFound(task_id));
        };
        if status == "done" || status == "skipped" {
            // Terminal: nothing to record, and saying so is not an error.
            tx.rollback().await?;
            return Ok(false);
        }
        sqlx::query(
            r#"UPDATE lifecycle.onboarding_tasks
                  SET status = $2::lifecycle.task_status,
                      metadata = metadata
                          || jsonb_build_object(
                                 'resolution', to_jsonb($2::text),
                                 'resolved_at', to_jsonb(now()),
                                 'resolved_by', to_jsonb($3))
                WHERE id = $1"#,
        )
        .bind(task_id)
        .bind(resolution)
        .bind(actor)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(true)
    }

    /// How many of an onboarding's tasks are still open (`pending` or
    /// `blocked`). Zero means the checklist is complete — the caller's
    /// auto-complete policy may then fire the onboarding's own verb.
    pub async fn open_task_count(&self, onboarding_id: Uuid) -> Result<i64, CheckpointError> {
        let count = sqlx::query_scalar(
            r#"SELECT count(*) FROM lifecycle.onboarding_tasks
                WHERE onboarding_id = $1 AND status IN ('pending', 'blocked')"#,
        )
        .bind(onboarding_id)
        .fetch_optional(&self.rpool())
        .await?
        .unwrap_or(0);
        Ok(count)
    }
}

/// Creates clearance items (optionally notifying the responsible party).
pub struct ClearanceItemWriteService {
    pool: PgPool,
    activities: Arc<dyn ActivitySink>,
}

impl ClearanceItemWriteService {
    /// The database this verb runs on: the composer's request pool when the
    /// tenant router installed one, else the composed pool.
    fn rpool(&self) -> sqlx::PgPool {
        crate::request_pool::current().unwrap_or_else(|| self.pool.clone())
    }

    pub fn new(pool: PgPool, activities: Arc<dyn ActivitySink>) -> Self {
        Self { pool, activities }
    }

    /// Record one clearance item and optionally schedule the clearer's activity.
    ///
    /// Fails closed before any write when `notify_user_id` is set but the seam
    /// is unwired. Returns the new item id.
    pub async fn create_clearance_item(
        &self,
        input: NewClearanceItem,
    ) -> Result<Uuid, CheckpointError> {
        if input.notify_user_id.is_some() && !self.activities.is_wired() {
            return Err(CheckpointError::ActivitySeamUnwired);
        }

        let mut tx = self.rpool().begin().await?;
        if let Some(scope) = backbone_orm::org_scope::current_org_scope() {
            backbone_orm::org_scope::bind_org_scope_on(&mut *tx, &scope).await?;
        }

        let parent: Option<Uuid> = sqlx::query_scalar(
            "SELECT id FROM lifecycle.offboardings WHERE id = $1",
        )
        .bind(input.offboarding_id)
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
                   (id, offboarding_id, title, clearer_employee_id, status, metadata)
               VALUES ($1, $2, $3, $4, 'pending', $5::jsonb)"#,
        )
        .bind(id)
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

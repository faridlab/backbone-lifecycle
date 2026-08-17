//! Outbound activity-scheduling port (hand-authored, user-owned).
//!
//! Lifecycle wants to put a mail activity on a checkpoint owner's plate when an
//! onboarding task or a clearance item is assigned — but the activity table
//! lives in the mail module's schema, and a domain module must not grow a
//! Cargo edge into another domain module. So the dependency points the other
//! way: this module declares the PORT, the host app supplies the ADAPTER (an
//! `ActivitySink` implemented over the mail module's activity write service)
//! at composition time.
//!
//! The default is [`UnwiredActivitySink`]: verbs that only *record* a
//! checkpoint still work, and a verb that explicitly asks for a notification
//! fails closed with the stable `activity_seam_unwired` code rather than
//! silently skipping the notification — the caller asked for a side effect,
//! so "nothing happened" must be loud, not quiet.

use chrono::NaiveDate;
use uuid::Uuid;

/// One scheduled-activity request, in the vocabulary of the generic activity
/// model (resource + summary + optional deadline and assignee).
#[derive(Debug, Clone)]
pub struct ActivityCommand {
    /// The business object the activity is about, e.g. `"onboarding_task"`.
    pub res_model: &'static str,
    /// Id of that object.
    pub res_id: Uuid,
    /// One-line human summary shown in the activity list.
    pub summary: String,
    /// Longer body, if any.
    pub note: Option<String>,
    /// Due date for the activity (usually the checkpoint's own due date).
    pub deadline: Option<NaiveDate>,
    /// The user the activity is assigned to. Activities belong to USERS, not
    /// employees — resolving "owner employee → their login user" is a
    /// host-app concern (identity lives outside this module), so the caller
    /// passes the resolved id in.
    pub user_id: Uuid,
}

/// Acknowledgement: the activity exists, keyed by its own id.
#[derive(Debug, Clone, Copy)]
pub struct ActivityAck {
    pub activity_id: Uuid,
}

/// Why a scheduled activity was rejected by the adapter.
#[derive(Debug, thiserror::Error)]
#[error("{code}: {message}")]
pub struct ActivityRejected {
    /// Stable machine code (`activity_seam_unwired` for the default sink).
    pub code: String,
    pub message: String,
}

/// The port. Implemented by the host app over its real activity service;
/// [`UnwiredActivitySink`] is the fail-closed default.
#[async_trait::async_trait]
pub trait ActivitySink: Send + Sync {
    async fn schedule(&self, cmd: ActivityCommand) -> Result<ActivityAck, ActivityRejected>;

    /// Whether a real adapter sits behind this port. Lets a verb that was
    /// explicitly asked to notify refuse BEFORE mutating any row (fail-closed
    /// pre-check) instead of discovering the missing seam afterwards.
    fn is_wired(&self) -> bool {
        true
    }
}

/// The default sink: nothing is wired. Any explicit notification request
/// fails loudly with `activity_seam_unwired`.
pub struct UnwiredActivitySink;

#[async_trait::async_trait]
impl ActivitySink for UnwiredActivitySink {
    fn is_wired(&self) -> bool {
        false
    }

    async fn schedule(&self, _cmd: ActivityCommand) -> Result<ActivityAck, ActivityRejected> {
        Err(ActivityRejected {
            code: "activity_seam_unwired".to_string(),
            message: "the activity seam is not wired — supply an ActivitySink to notify users"
                .to_string(),
        })
    }
}

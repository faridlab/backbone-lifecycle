//! The lifecycle event port (#558's remaining arms): what the module tells the
//! world when an exit starts or a task lands on a plate, and the seam the
//! composing service implements to deliver it (the notification producer's
//! arms).

use chrono::NaiveDate;
use uuid::Uuid;

/// What the module announces.
#[derive(Debug, Clone)]
pub enum LifecycleEvent {
    /// An offboarding was opened for an employee (any reason — the employee
    /// learns their exit process has started).
    OffboardingOpened {
        offboarding_id: Uuid,
        employee_id: Uuid,
        last_working_day: NaiveDate,
    },
    /// An onboarding task was assigned to a specific owner.
    OnboardingTaskAssigned {
        task_id: Uuid,
        onboarding_id: Uuid,
        /// The joiner the checklist belongs to.
        employee_id: Uuid,
        /// The task's owner (the notified party).
        owner_employee_id: Uuid,
        title: String,
    },
}

/// The event sink port. The default logs.
pub trait LifecycleEventSink: Send + Sync {
    fn publish(&self, event: LifecycleEvent);
}

/// The default sink: logs, delivers nothing.
pub struct LoggingSink;

impl LifecycleEventSink for LoggingSink {
    fn publish(&self, event: LifecycleEvent) {
        match event {
            LifecycleEvent::OffboardingOpened { offboarding_id, .. } => {
                tracing::info!(
                    target: "lifecycle",
                    offboarding_id = %offboarding_id,
                    "offboarding opened (no event sink wired)"
                );
            }
            LifecycleEvent::OnboardingTaskAssigned { task_id, .. } => {
                tracing::info!(
                    target: "lifecycle",
                    task_id = %task_id,
                    "onboarding task assigned (no event sink wired)"
                );
            }
        }
    }
}

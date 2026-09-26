//! The discipline event port: what the module tells the world when a warning
//! letter goes out, and the seam a composing service implements to make it
//! durable (the #558 producer's discipline arm consumes it).

use uuid::Uuid;

/// What the module announces.
#[derive(Debug, Clone)]
pub enum DisciplineEvent {
    /// A warning letter was issued and served.
    Issued {
        record_id: Uuid,
        employee_id: Uuid,
        /// "sp1" | "sp2" | "sp3"
        level: String,
    },
}

/// The event sink port. The default logs.
pub trait DisciplineEventSink: Send + Sync {
    fn publish(&self, event: DisciplineEvent);
}

/// The default sink: logs, delivers nothing.
pub struct LoggingSink;

impl DisciplineEventSink for LoggingSink {
    fn publish(&self, event: DisciplineEvent) {
        match event {
            DisciplineEvent::Issued { record_id, employee_id, level } => {
                tracing::info!(
                    target: "discipline",
                    record_id = %record_id,
                    employee_id = %employee_id,
                    level = %level,
                    "warning letter issued (no event sink wired)"
                );
            }
        }
    }
}

//! The contract event port: what the module tells the world (a fixed-term
//! contract nearing its end; a decision applied), and the seam the composing
//! service implements to deliver them (the #558 producer's arms).

use chrono::NaiveDate;
use uuid::Uuid;

/// What the module announces.
#[derive(Debug, Clone)]
pub enum ContractEvent {
    /// An active PKWT contract entered the reminder window.
    Expiring {
        contract_id: Uuid,
        employee_id: Uuid,
        end_date: NaiveDate,
    },
    /// A decision was applied (renew / convert / end).
    Decided {
        contract_id: Uuid,
        employee_id: Uuid,
        outcome: &'static str,
    },
}

/// The event sink port. The default logs.
pub trait ContractEventSink: Send + Sync {
    fn publish(&self, event: ContractEvent);
}

/// The default sink: logs, delivers nothing.
pub struct LoggingSink;

impl ContractEventSink for LoggingSink {
    fn publish(&self, event: ContractEvent) {
        match event {
            ContractEvent::Expiring { contract_id, end_date, .. } => {
                tracing::info!(
                    target: "contracts",
                    contract_id = %contract_id,
                    end_date = %end_date,
                    "contract entering expiry window (no event sink wired)"
                );
            }
            ContractEvent::Decided { contract_id, outcome, .. } => {
                tracing::info!(
                    target: "contracts",
                    contract_id = %contract_id,
                    outcome,
                    "contract decision applied (no event sink wired)"
                );
            }
        }
    }
}

//! The one place a leaver's settlement is assembled: gather the inputs, read
//! the statutory set and the company settings, run the pure calculation.
//!
//! Both the close verb (which carries the result on the `offboarding.closed`
//! event) and the settlement draft verb (which stores it on the settlement
//! row) call [`compute_for_offboarding`], so the two can only disagree if an
//! input itself changed between the two calls.
//!
//! This is a user-owned custom file — it is NEVER regenerated.

use crate::application::service::offboarding_ports::OffboardingInputs;
use crate::application::service::pesangon::{
    compute_settlement, severance_basis, PesangonError, SettlementBreakdown, SettlementInputs,
    SEVERANCE_COUNTRY,
};
use crate::domain::entity::OffboardingReason;
use crate::infrastructure::persistence::severance_params_repository::{
    severance_params_as_of, severance_settings,
};
use chrono::NaiveDate;
use sqlx::PgConnection;
use std::str::FromStr;
use uuid::Uuid;

/// Why the settlement inputs could not be assembled.
#[derive(Debug, thiserror::Error)]
pub enum SettlementComputeError {
    #[error("cannot compute the settlement: employee {employee_id} has no employment join_date")]
    MissingJoinDate { employee_id: Uuid },
    #[error("cannot compute the settlement: employee {employee_id} has no current salary")]
    MissingSalary { employee_id: Uuid },
    #[error("invalid offboarding reason '{0}'")]
    BadReason(String),
    #[error(transparent)]
    Pesangon(#[from] PesangonError),
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
}

/// Assemble and compute the settlement for one leaver.
///
/// `conn` is the calling verb's own connection (the statutory tables and the
/// settings are read on it); the cross-module inputs go through `inputs`,
/// which carries the company for the still-company-keyed sibling reads.
/// The reason is checked first, so an unspecific reason refuses before any
/// other read.
pub async fn compute_for_offboarding(
    inputs: &dyn OffboardingInputs,
    conn: &mut PgConnection,
    company_id: Uuid,
    employee_id: Uuid,
    reason: &str,
    last_working_day: NaiveDate,
) -> Result<SettlementBreakdown, SettlementComputeError> {
    let reason_enum = OffboardingReason::from_str(reason)
        .map_err(|_| SettlementComputeError::BadReason(reason.to_string()))?;
    severance_basis(reason_enum)?;

    let join_date = inputs
        .join_date(company_id, employee_id)
        .await?
        .ok_or(SettlementComputeError::MissingJoinDate { employee_id })?;
    let monthly_wage = inputs
        .current_monthly_salary(company_id, employee_id)
        .await?
        .ok_or(SettlementComputeError::MissingSalary { employee_id })?;
    let settings = severance_settings(conn).await?;
    let unused_leave_days = inputs
        .unused_leave_days(company_id, employee_id, &settings.leave_type_codes, last_working_day)
        .await?;
    let params = severance_params_as_of(conn, SEVERANCE_COUNTRY, last_working_day).await?;

    Ok(compute_settlement(
        &SettlementInputs {
            reason: reason_enum,
            join_date,
            last_working_day,
            monthly_wage,
            unused_leave_days,
        },
        &params,
        &settings,
    )?)
}

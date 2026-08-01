//! Read ports for the offboarding→pesangon settlement (ADR-005).
//!
//! The 🇮🇩 pesangon calc ([`crate::application::service::pesangon::pesangon`]) needs three
//! cross-module inputs at close-time: the employee's `join_date` (for tenure), their current
//! gross monthly salary (payroll), and their remaining leave days (timeoff). Lifecycle OWNS the
//! close transition and the pesangon computation, but it does NOT own those tables — so it reads
//! them through this port trait.
//!
//! ## Why a port, and why pool-backed by default
//!
//! The dependency graph must stay acyclic: lifecycle may READ employee/payroll/timeoff, but none of
//! them read lifecycle. Adding a Cargo edge from lifecycle to those crates would couple lifecycle to
//! their internal service APIs; instead lifecycle defines this trait seam and ships a default
//! [`PoolOffboardingInputs`] that does scalar SQL reads against the three tables (the same read
//! pattern as `backbone_employee::EmployeeQueryService::statutory_inputs`, just behind a trait so it
//! is injectable/mockable at composition time). The composer and the integration test use the
//! pool-backed default; a future deployment can swap in a module-instance-backed impl without
//! touching callers.
//!
//! This is a user-owned custom file — it is NEVER regenerated.

use async_trait::async_trait;
use chrono::NaiveDate;
use rust_decimal::Decimal;
use sqlx::PgPool;
use uuid::Uuid;

/// The three cross-module inputs the pesangon calc consumes at offboarding close.
///
/// Each method is fallible at the DB layer. The producer interprets `None` from
/// [`Self::join_date`] / [`Self::current_monthly_salary`] as a missing-prerequisite hard error
/// (fail closed — an offboarding must NOT close without a real pesangon), and treats the leave
/// balance as zero when no rows exist (no leave to pay out is a normal state, not an error).
#[async_trait]
pub trait OffboardingInputs: Send + Sync {
    /// The employee's `join_date` from `employee.employments` — the earliest non-deleted row,
    /// i.e. the start of service (the tenure base).
    async fn join_date(&self, employee_id: Uuid) -> Result<Option<NaiveDate>, sqlx::Error>;

    /// The employee's current gross monthly salary — the latest non-null
    /// `payroll.compensation_changes.new_amount` ordered by `effective_date` descending (the
    /// running salary set by the most recent hire/promotion/transfer row).
    async fn current_monthly_salary(&self, employee_id: Uuid)
        -> Result<Option<Decimal>, sqlx::Error>;

    /// Remaining leave days across all the employee's non-deleted `timeoff.timeoff_balances` rows
    /// — `SUM(allocated - used)`. Returns `0` when the employee has no balance rows.
    async fn remaining_leave_days(&self, employee_id: Uuid) -> Result<Decimal, sqlx::Error>;
}

/// Default pool-backed [`OffboardingInputs`] — scalar SQL reads against the employee / payroll /
/// timeoff tables. Constructed from the shared pool the composer/test already holds.
pub struct PoolOffboardingInputs {
    pool: PgPool,
}

impl PoolOffboardingInputs {
    /// Create a new pool-backed inputs reader.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl OffboardingInputs for PoolOffboardingInputs {
    async fn join_date(&self, employee_id: Uuid) -> Result<Option<NaiveDate>, sqlx::Error> {
        let row: Option<(NaiveDate,)> = sqlx::query_as(
            r#"SELECT join_date
                 FROM employee.employments
                WHERE employee_id = $1
                  AND (metadata->>'deleted_at') IS NULL
                ORDER BY join_date ASC
                LIMIT 1"#,
        )
        .bind(employee_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(d,)| d))
    }

    async fn current_monthly_salary(
        &self,
        employee_id: Uuid,
    ) -> Result<Option<Decimal>, sqlx::Error> {
        let row: Option<(Decimal,)> = sqlx::query_as(
            r#"SELECT new_amount
                 FROM payroll.compensation_changes
                WHERE employee_id = $1
                  AND new_amount IS NOT NULL
                  AND (metadata->>'deleted_at') IS NULL
                ORDER BY effective_date DESC NULLS LAST,
                         (metadata->>'created_at') DESC NULLS LAST
                LIMIT 1"#,
        )
        .bind(employee_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(amount,)| amount))
    }

    async fn remaining_leave_days(&self, employee_id: Uuid) -> Result<Decimal, sqlx::Error> {
        // COALESCE turns "no balance rows" into 0 (no leave to pay out) rather than NULL.
        let row: Option<(Decimal,)> = sqlx::query_as(
            r#"SELECT COALESCE(SUM(allocated - used), 0)
                 FROM timeoff.timeoff_balances
                WHERE employee_id = $1
                  AND (metadata->>'deleted_at') IS NULL"#,
        )
        .bind(employee_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(d,)| d).unwrap_or(Decimal::ZERO))
    }
}

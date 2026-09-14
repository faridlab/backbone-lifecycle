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
/// Every method takes the caller's `company` and reads inside an organization
/// request scope keyed on that company's unit — the source tables carry their own
/// row-level security fences, so an unbound read returns zero rows regardless of
/// the WHERE clause.
///
/// Each method is fallible at the DB layer. The producer interprets `None` from
/// [`Self::join_date`] / [`Self::current_monthly_salary`] as a missing-prerequisite hard error
/// (fail closed — an offboarding must NOT close without a real pesangon), and treats the leave
/// balance as zero when no rows exist (no leave to pay out is a normal state, not an error).
#[async_trait]
pub trait OffboardingInputs: Send + Sync {
    /// The employee's `join_date` from `employee.employments` — the earliest non-deleted row,
    /// i.e. the start of service (the tenure base).
    async fn join_date(&self, company: Uuid, employee_id: Uuid)
        -> Result<Option<NaiveDate>, sqlx::Error>;

    /// The employee's current gross monthly salary — the latest non-null
    /// `payroll.compensation_changes.new_amount` ordered by `effective_date` descending (the
    /// running salary set by the most recent hire/promotion/transfer row).
    async fn current_monthly_salary(&self, company: Uuid, employee_id: Uuid)
        -> Result<Option<Decimal>, sqlx::Error>;

    /// Remaining leave days across all the employee's non-deleted `timeoff.timeoff_balances` rows
    /// — `SUM(allocated - used)`. Returns `0` when the employee has no balance rows.
    async fn remaining_leave_days(&self, company: Uuid, employee_id: Uuid)
        -> Result<Decimal, sqlx::Error>;
}

/// Default pool-backed [`OffboardingInputs`] — scalar SQL reads against the employee / payroll /
/// timeoff tables, each wrapped in an organization request scope. Constructed from the shared pool
/// the composer/test already holds.
pub struct PoolOffboardingInputs {
    pool: PgPool,
}

impl PoolOffboardingInputs {
    /// Create a new pool-backed inputs reader.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

impl PoolOffboardingInputs {
    /// Run one scalar read under an organization request scope.
    ///
    /// The three source tables were re-keyed onto the organization unit axis, so the reads fence on
    /// `org_unit_id` against the session's entitlement union rather than naming a company. The scope
    /// rides a request-dedicated connection: a fenced read must never borrow an outer transaction's
    /// connection, or it answers under whatever scope that one carries.
    ///
    /// The caller still names a company. When an ambient scope is already open the read joins it —
    /// the composing service has already resolved the session's entitlements, and a named company
    /// must never widen them. Otherwise the company's own unit is the scope, which is what a direct
    /// call (a test, a job) means.
    async fn scoped_scalar<T>(
        &self,
        company: Uuid,
        query: sqlx::query::QueryScalar<
            '_,
            sqlx::Postgres,
            T,
            sqlx::postgres::PgArguments,
        >,
    ) -> Result<Option<T>, sqlx::Error>
    where
        T: Send + Unpin + for<'r> sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
    {
        use backbone_orm::org_scope::{self, OrgScope};

        let scope = org_scope::current_org_scope()
            .unwrap_or_else(|| OrgScope::for_company_unit(company));
        let mut tx = self.pool.begin().await?;
        org_scope::bind_org_scope_on(&mut tx, &scope).await?;
        let out = query.fetch_optional(&mut *tx).await?;
        tx.commit().await?;
        Ok(out)
    }

    /// The organization units a read may answer under — the scope's entitlement union.
    fn scope_units(company: Uuid) -> Vec<Uuid> {
        backbone_orm::org_scope::current_org_scope()
            .map(|s| s.scope_unit_ids().to_vec())
            .unwrap_or_else(|| vec![company])
    }
}

#[async_trait]
impl OffboardingInputs for PoolOffboardingInputs {
    async fn join_date(
        &self,
        company: Uuid,
        employee_id: Uuid,
    ) -> Result<Option<NaiveDate>, sqlx::Error> {
        self.scoped_scalar(
            company,
            sqlx::query_scalar(
                r#"SELECT join_date
                     FROM employee.employments
                    WHERE employee_id = $1
                      AND org_unit_id = ANY($2)
                      AND (metadata->>'deleted_at') IS NULL
                    ORDER BY join_date ASC
                    LIMIT 1"#,
            )
            .bind(employee_id)
            .bind(Self::scope_units(company)),
        )
        .await
    }

    async fn current_monthly_salary(
        &self,
        company: Uuid,
        employee_id: Uuid,
    ) -> Result<Option<Decimal>, sqlx::Error> {
        self.scoped_scalar(
            company,
            sqlx::query_scalar(
                r#"SELECT new_amount
                     FROM payroll.compensation_changes
                    WHERE employee_id = $1
                      AND org_unit_id = ANY($2)
                      AND new_amount IS NOT NULL
                      AND (metadata->>'deleted_at') IS NULL
                    ORDER BY effective_date DESC NULLS LAST,
                             (metadata->>'created_at') DESC NULLS LAST
                    LIMIT 1"#,
            )
            .bind(employee_id)
            .bind(Self::scope_units(company)),
        )
        .await
    }

    async fn remaining_leave_days(
        &self,
        company: Uuid,
        employee_id: Uuid,
    ) -> Result<Decimal, sqlx::Error> {
        // COALESCE turns "no balance rows" into 0 (no leave to pay out) rather than NULL.
        self.scoped_scalar(
            company,
            sqlx::query_scalar(
                r#"SELECT COALESCE(SUM(allocated - used), 0)
                     FROM timeoff.timeoff_balances
                    WHERE employee_id = $1
                      AND org_unit_id = ANY($2)
                      AND (metadata->>'deleted_at') IS NULL"#,
            )
            .bind(employee_id)
            .bind(Self::scope_units(company)),
        )
        .await
        .map(|d| d.unwrap_or(Decimal::ZERO))
    }
}

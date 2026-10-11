//! The module's extension (hand-written; ADR-0031): what the module adds beside its
//! generated services. The generated module and builder carry `ModuleExt` and
//! `ModuleBuilderExt` and dereference to them, so their fields read as `module.field`.

#[allow(unused_imports)]
use super::*;

/// State the module adds; read through the generated module's `Deref`.
pub struct ModuleExt {
    /// The three career-lifecycle write-services (ADR-005 producers): each flips a workflow status
    /// and stages the matching compound event in one tx. Public so the composer / tests can drive
    /// them (`module.promotion_write_service.effect(id)`, etc.).
    pub promotion_write_service: Arc<application::service::PromotionWriteService>,
    pub onboarding_write_service: Arc<application::service::OnboardingWriteService>,
    pub offboarding_write_service: Arc<application::service::OffboardingWriteService>,
    /// Checkpoint create verbs (onboarding tasks / clearance items) with the optional
    /// notify side effect through the activity port. Public so the composer / tests can
    /// drive them directly.
    pub onboarding_task_write_service: Arc<application::service::OnboardingTaskWriteService>,
    pub clearance_item_write_service: Arc<application::service::ClearanceItemWriteService>,
    /// Final-settlement draft + GL confirmation. Drafts from the same pesangon inputs as
    /// the close verb; confirm posts through the GL port and stamps only after the ack.
    pub final_settlement_write_service: Arc<application::service::FinalSettlementWriteService>,
    /// Discipline records (SP1-SP3): issue/acknowledge/contest/cancel with the
    /// derived-expiry predicate. The contest approvals port and the issued
    /// event sink are unwired by default (fail-closed / log-only).
    pub discipline_write_service: Arc<application::service::DisciplineWriteService>,
    /// Employment contracts: create, the decision verb (through the
    /// approvals port), the dispatcher arms, the reminder tick.
    pub contract_write_service: Arc<application::service::ContractWriteService>,
}

/// State the builder adds.
pub struct ModuleBuilderExt {
    // ADR-005 offboarding producer: optional override for the settlement input port. When unset,
    // build() falls back to a pool-backed OffboardingInputs — so the composer's
    // `with_database(pool).build()` keeps working unchanged. The statutory severance numbers are
    // not configurable here: they are effective-dated rows in the lifecycle.severance_* tables.
    offboarding_inputs: Option<Arc<dyn application::service::OffboardingInputs>>,
    // Outbound seams, unwired by default: the activity port (checkpoint notifies) and the GL
    // posting port (settlement confirmation). The host app supplies the real adapters at
    // composition time; the unwired defaults fail loudly (422) on any explicit side-effect
    // request rather than silently skipping it.
    activity_sink: Option<Arc<dyn application::service::ActivitySink>>,
    gl_sink: Option<Arc<dyn backbone_gl_posting::GlPostSink>>,
}

impl Default for ModuleBuilderExt {
    fn default() -> Self {
        Self {
            offboarding_inputs: None,
            activity_sink: None,
            gl_sink: None,
        }
    }
}

impl ModuleBuilderExt {
    /// Build the extension's state from what the generated build made.
    #[allow(unused_variables, clippy::redundant_clone)]
    pub(crate) fn build(self, parts: &ModuleParts<'_>) -> anyhow::Result<ModuleExt> {
        let db_pool = parts.db_pool.clone();
        // ADR-005 producers: bound to the same pool as the repos. Each verb opens its own tx around
        // the state change + the outbox stage; no shared mutable state, so an Arc is purely for cheap reuse.
        let promotion_write_service = Arc::new(application::service::PromotionWriteService::new(db_pool.clone()));
        let onboarding_write_service = Arc::new(application::service::OnboardingWriteService::new(db_pool.clone()));
        // The offboarding producer also computes the settlement, so it needs the cross-module
        // inputs port. Fall back to pool-backed inputs when the builder didn't override it.
        let offboarding_inputs = self.offboarding_inputs.unwrap_or_else(|| {
            Arc::new(application::service::PoolOffboardingInputs::new(db_pool.clone()))
        });
        let offboarding_write_service = Arc::new(application::service::OffboardingWriteService::new(
            db_pool.clone(),
            offboarding_inputs.clone(),
        ));
        // The settlement drafts through the SAME computation the close verb uses, so the row
        // agrees with the offboarding.closed event payload. Seams default to the
        // unwired sinks (loud 422 on explicit side-effect requests) until the host wires them.
        let gl_sink = self.gl_sink.unwrap_or_else(|| {
            Arc::new(application::service::UnwiredGlSink)
        });
        let final_settlement_write_service =
            Arc::new(application::service::FinalSettlementWriteService::new(
                db_pool.clone(),
                offboarding_inputs,
                gl_sink,
            ));
        // Checkpoint create verbs share the activity port (unwired by default).
        let activity_sink = self.activity_sink.unwrap_or_else(|| {
            Arc::new(application::service::UnwiredActivitySink)
        });
        let onboarding_task_write_service =
            Arc::new(application::service::OnboardingTaskWriteService::new(
                db_pool.clone(),
                activity_sink.clone(),
            ));
        let clearance_item_write_service =
            Arc::new(application::service::ClearanceItemWriteService::new(
                db_pool.clone(),
                activity_sink,
            ));
        let discipline_write_service =
            Arc::new(application::service::DisciplineWriteService::new(db_pool.clone()));
        let contract_write_service =
            Arc::new(application::service::ContractWriteService::new(db_pool.clone()));
        Ok(ModuleExt {
            promotion_write_service,
            onboarding_write_service,
            offboarding_write_service,
            onboarding_task_write_service,
            clearance_item_write_service,
            final_settlement_write_service,
            discipline_write_service,
            contract_write_service,
        })
    }
}

impl crate::ThisModule {
    /// The guarded surface: read-only CRUD for every entity + the validated write verbs
    /// (workflow transitions, checkpoint creates, settlement draft/confirm), each gated on the
    /// org context the composing service's org auth layer inserts (the acting identity comes off
    /// the signed token; the database scope is the one the host bound). This is the production
    /// mount — generic mutation never reaches it.
    pub fn guarded_routes(&self) -> Router {
        presentation::http::create_guarded_lifecycle_routes(self)
    }
}

impl crate::ThisModuleBuilder {
    /// Override the cross-module read port the offboarding producer uses to gather the settlement
    /// inputs (join_date / current salary / unused annual leave). Defaults to a pool-backed impl that
    /// reads `employee.employments` / `payroll.compensation_changes` / `timeoff.timeoff_balances`
    /// directly — only override if you need a different read source.
    pub fn with_offboarding_inputs(mut self, inputs: Arc<dyn application::service::OffboardingInputs>) -> Self {
        self.offboarding_inputs = Some(inputs);
        self
    }
    /// Wire the activity port (checkpoint notifies). Unwired by default: a create that
    /// explicitly asks to notify fails closed with 422 `activity_seam_unwired` instead of
    /// silently skipping the notification.
    pub fn with_activity_sink(mut self, sink: Arc<dyn application::service::ActivitySink>) -> Self {
        self.activity_sink = Some(sink);
        self
    }
    /// Wire the GL posting port (settlement confirmation). Unwired by default: confirm fails
    /// loudly with 422 `gl_seam_unwired` and the settlement stays draft and retryable.
    pub fn with_gl_sink(mut self, sink: Arc<dyn backbone_gl_posting::GlPostSink>) -> Self {
        self.gl_sink = Some(sink);
        self
    }
}

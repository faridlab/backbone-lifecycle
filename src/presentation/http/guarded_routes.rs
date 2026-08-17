//! Guarded route composition — the RECOMMENDED way to mount the lifecycle
//! module (hand-authored, user-owned).
//!
//! The generated CRUD surface writes rows with no domain validation: a
//! generic update could set an onboarding `completed` without staging the
//! employment-activation event, or flip a settlement `confirmed` with no GL
//! entry behind it. This composition closes that bypass:
//!
//! - every entity stays READABLE through the generated GET endpoints;
//! - side-effect-free checkpoint data (onboarding tasks, clearance items,
//!   exit interviews) also keeps the generic write endpoints;
//! - every workflow carrier (onboarding, promotion, offboarding, final
//!   settlement) has NO generic write surface at all — creation and every
//!   transition go through write-service verbs, so no path can set a
//!   lifecycle state directly and sidestep a verb's side effects (the
//!   outbox emits, the vacancy-clearance derivation, the GL post).
//!
//! Every write handler extracts the caller's company from the
//! [`CompanyContext`] the `company_auth` middleware inserts — the tenant
//! comes from the signed token, never the request body — and passes it down
//! so each verb runs inside a company-scoped transaction (row-level
//! security does the actual fencing).

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
    Json, Router,
};
use backbone_auth::company::CompanyContext;
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::application::service::{
    ClearanceItemWriteService, FinalSettlementError, FinalSettlementWriteService,
    NewClearanceItem, NewOffboarding, NewOnboarding, NewOnboardingTask, NewPromotion,
    OffboardingWriteService, OnboardingTaskWriteService, OnboardingWriteService,
    PromotionWriteService, SettlementAccounts,
};
use crate::LifecycleModule;

#[derive(Debug, Serialize)]
struct ErrorBody {
    error: &'static str,
    message: String,
}
#[derive(Debug, Serialize)]
struct IdResponse {
    id: Uuid,
}
#[derive(Debug, Serialize)]
struct OkResponse {
    ok: bool,
}

fn status_of(code: u16) -> StatusCode {
    StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
}

fn err_response(code: &'static str, status: u16, message: String) -> axum::response::Response {
    (status_of(status), Json(ErrorBody { error: code, message })).into_response()
}

// ── Onboardings ─────────────────────────────────────────────────────────────────

#[derive(Debug, Default, Deserialize)]
struct CreateOnboardingBody {
    #[serde(default)]
    employee_id: Option<Uuid>,
    #[serde(default)]
    start_date: Option<NaiveDate>,
    #[serde(default)]
    probation_end_date: Option<NaiveDate>,
    #[serde(default)]
    template_id: Option<Uuid>,
}

async fn create_onboarding(
    State(svc): State<Arc<OnboardingWriteService>>,
    tenant: CompanyContext,
    b: Option<Json<CreateOnboardingBody>>,
) -> axum::response::Response {
    let b = b.map(|Json(b)| b).unwrap_or_default();
    let (employee_id, start_date) = match (b.employee_id, b.start_date) {
        (Some(e), Some(s)) => (e, s),
        _ => {
            return err_response(
                "bad_request",
                400,
                "employeeId and startDate are required".to_string(),
            )
        }
    };
    match svc
        .create(
            tenant.company_id,
            NewOnboarding {
                employee_id,
                start_date,
                probation_end_date: b.probation_end_date,
                template_id: b.template_id,
            },
        )
        .await
    {
        Ok(id) => (StatusCode::CREATED, Json(IdResponse { id })).into_response(),
        Err(e) => err_response(e.code(), e.http_status(), e.to_string()),
    }
}

async fn complete_onboarding(
    State(svc): State<Arc<OnboardingWriteService>>,
    tenant: CompanyContext,
    Path(onboarding_id): Path<Uuid>,
) -> axum::response::Response {
    match svc.complete(tenant.company_id, onboarding_id).await {
        Ok(Some(event_id)) => {
            (StatusCode::OK, Json(IdResponse { id: event_id })).into_response()
        }
        // Idempotent no-op: already completed; no second event.
        Ok(None) => (StatusCode::OK, Json(OkResponse { ok: false })).into_response(),
        Err(e) => err_response(e.code(), e.http_status(), e.to_string()),
    }
}

#[derive(Debug, Default, Deserialize)]
struct ConfirmBody {
    /// Operator override: confirm probation before the planned end date.
    #[serde(default)]
    force: bool,
}

async fn confirm_onboarding(
    State(svc): State<Arc<OnboardingWriteService>>,
    tenant: CompanyContext,
    Path(onboarding_id): Path<Uuid>,
    b: Option<Json<ConfirmBody>>,
) -> axum::response::Response {
    let force = b.map(|Json(b)| b.force).unwrap_or_default();
    match svc.confirm(tenant.company_id, onboarding_id, force).await {
        Ok(Some(event_id)) => {
            (StatusCode::OK, Json(IdResponse { id: event_id })).into_response()
        }
        // Idempotent no-op: already confirmed; no second event.
        Ok(None) => (StatusCode::OK, Json(OkResponse { ok: false })).into_response(),
        Err(e) => err_response(e.code(), e.http_status(), e.to_string()),
    }
}

// ── Promotions ──────────────────────────────────────────────────────────────────

#[derive(Debug, Default, Deserialize)]
struct CreatePromotionBody {
    #[serde(default)]
    employee_id: Option<Uuid>,
    #[serde(default)]
    promotion_type: Option<String>,
    #[serde(default)]
    position_id_from: Option<Uuid>,
    #[serde(default)]
    position_id_to: Option<Uuid>,
    #[serde(default)]
    level_id_from: Option<Uuid>,
    #[serde(default)]
    level_id_to: Option<Uuid>,
    #[serde(default)]
    department_id_from: Option<Uuid>,
    #[serde(default)]
    department_id_to: Option<Uuid>,
    #[serde(default)]
    proposed_salary: Option<rust_decimal::Decimal>,
    #[serde(default)]
    effective_date: Option<NaiveDate>,
    #[serde(default)]
    requested_by: Option<Uuid>,
    #[serde(default)]
    reason: Option<String>,
}

async fn create_promotion(
    State(svc): State<Arc<PromotionWriteService>>,
    tenant: CompanyContext,
    b: Option<Json<CreatePromotionBody>>,
) -> axum::response::Response {
    let b = b.map(|Json(b)| b).unwrap_or_default();
    let (employee_id, effective_date) = match (b.employee_id, b.effective_date) {
        (Some(e), Some(d)) => (e, d),
        _ => {
            return err_response(
                "bad_request",
                400,
                "employeeId and effectiveDate are required".to_string(),
            )
        }
    };
    match svc
        .create(
            tenant.company_id,
            NewPromotion {
                employee_id,
                promotion_type: b.promotion_type,
                position_id_from: b.position_id_from,
                position_id_to: b.position_id_to,
                level_id_from: b.level_id_from,
                level_id_to: b.level_id_to,
                department_id_from: b.department_id_from,
                department_id_to: b.department_id_to,
                proposed_salary: b.proposed_salary,
                effective_date,
                requested_by: b.requested_by,
                reason: b.reason,
            },
        )
        .await
    {
        Ok(id) => (StatusCode::CREATED, Json(IdResponse { id })).into_response(),
        Err(e) => err_response(e.code(), e.http_status(), e.to_string()),
    }
}

#[derive(Debug, Default, Deserialize)]
struct ApproveBody {
    /// The approving operator's user id, stamped on the row.
    #[serde(default)]
    approved_by: Option<Uuid>,
}

async fn approve_promotion(
    State(svc): State<Arc<PromotionWriteService>>,
    tenant: CompanyContext,
    Path(promotion_id): Path<Uuid>,
    b: Option<Json<ApproveBody>>,
) -> axum::response::Response {
    let approved_by = b.map(|Json(b)| b.approved_by).unwrap_or_default();
    match svc.approve(tenant.company_id, promotion_id, approved_by).await {
        Ok(moved) => (StatusCode::OK, Json(OkResponse { ok: moved })).into_response(),
        Err(e) => err_response(e.code(), e.http_status(), e.to_string()),
    }
}

async fn effect_promotion(
    State(svc): State<Arc<PromotionWriteService>>,
    tenant: CompanyContext,
    Path(promotion_id): Path<Uuid>,
) -> axum::response::Response {
    match svc.effect(tenant.company_id, promotion_id).await {
        Ok(Some(event_id)) => {
            (StatusCode::OK, Json(IdResponse { id: event_id })).into_response()
        }
        // Idempotent no-op: already effective; no second event.
        Ok(None) => (StatusCode::OK, Json(OkResponse { ok: false })).into_response(),
        Err(e) => err_response(e.code(), e.http_status(), e.to_string()),
    }
}

// ── Offboardings ────────────────────────────────────────────────────────────────

#[derive(Debug, Default, Deserialize)]
struct CreateOffboardingBody {
    #[serde(default)]
    employee_id: Option<Uuid>,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default)]
    notice_date: Option<NaiveDate>,
    #[serde(default)]
    last_working_day: Option<NaiveDate>,
}

async fn create_offboarding(
    State(svc): State<Arc<OffboardingWriteService>>,
    tenant: CompanyContext,
    b: Option<Json<CreateOffboardingBody>>,
) -> axum::response::Response {
    let b = b.map(|Json(b)| b).unwrap_or_default();
    let (employee_id, notice_date, last_working_day) =
        match (b.employee_id, b.notice_date, b.last_working_day) {
            (Some(e), Some(n), Some(l)) => (e, n, l),
            _ => {
                return err_response(
                    "bad_request",
                    400,
                    "employeeId, noticeDate and lastWorkingDay are required".to_string(),
                )
            }
        };
    match svc
        .create(
            tenant.company_id,
            NewOffboarding {
                employee_id,
                reason: b.reason,
                notice_date,
                last_working_day,
            },
        )
        .await
    {
        Ok(id) => (StatusCode::CREATED, Json(IdResponse { id })).into_response(),
        Err(e) => err_response(e.code(), e.http_status(), e.to_string()),
    }
}

async fn clear_offboarding(
    State(svc): State<Arc<OffboardingWriteService>>,
    tenant: CompanyContext,
    Path(offboarding_id): Path<Uuid>,
) -> axum::response::Response {
    match svc.clear(tenant.company_id, offboarding_id).await {
        Ok(moved) => (StatusCode::OK, Json(OkResponse { ok: moved })).into_response(),
        Err(e) => err_response(e.code(), e.http_status(), e.to_string()),
    }
}

async fn close_offboarding(
    State(svc): State<Arc<OffboardingWriteService>>,
    tenant: CompanyContext,
    Path(offboarding_id): Path<Uuid>,
) -> axum::response::Response {
    match svc.close(tenant.company_id, offboarding_id).await {
        Ok(Some(event_id)) => {
            (StatusCode::OK, Json(IdResponse { id: event_id })).into_response()
        }
        // Idempotent no-op: already closed; no second event.
        Ok(None) => (StatusCode::OK, Json(OkResponse { ok: false })).into_response(),
        Err(e) => err_response(e.code(), e.http_status(), e.to_string()),
    }
}

// ── Checkpoints (onboarding tasks / clearance items) ────────────────────────────

#[derive(Debug, Default, Deserialize)]
struct CreateTaskBody {
    #[serde(default)]
    onboarding_id: Option<Uuid>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    category: Option<String>,
    #[serde(default)]
    owner_employee_id: Option<Uuid>,
    #[serde(default)]
    due_date: Option<NaiveDate>,
    /// The owner's login user, when the caller wants an activity on their
    /// plate. Omit to record silently.
    #[serde(default)]
    notify_user_id: Option<Uuid>,
}

async fn create_onboarding_task(
    State(svc): State<Arc<OnboardingTaskWriteService>>,
    tenant: CompanyContext,
    b: Option<Json<CreateTaskBody>>,
) -> axum::response::Response {
    let b = b.map(|Json(b)| b).unwrap_or_default();
    let (onboarding_id, title) = match (b.onboarding_id, b.title) {
        (Some(o), Some(t)) if !t.trim().is_empty() => (o, t),
        _ => {
            return err_response(
                "bad_request",
                400,
                "onboardingId and title are required".to_string(),
            )
        }
    };
    match svc
        .create_task(
            tenant.company_id,
            NewOnboardingTask {
                onboarding_id,
                title,
                category: b.category,
                owner_employee_id: b.owner_employee_id,
                due_date: b.due_date,
                notify_user_id: b.notify_user_id,
            },
        )
        .await
    {
        Ok(id) => (StatusCode::CREATED, Json(IdResponse { id })).into_response(),
        Err(e) => err_response(e.code(), e.http_status(), e.to_string()),
    }
}

#[derive(Debug, Default, Deserialize)]
struct CreateClearanceItemBody {
    #[serde(default)]
    offboarding_id: Option<Uuid>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    clearer_employee_id: Option<Uuid>,
    /// The responsible party's login user, when the caller wants an activity
    /// on their plate. Omit to record silently.
    #[serde(default)]
    notify_user_id: Option<Uuid>,
}

async fn create_clearance_item(
    State(svc): State<Arc<ClearanceItemWriteService>>,
    tenant: CompanyContext,
    b: Option<Json<CreateClearanceItemBody>>,
) -> axum::response::Response {
    let b = b.map(|Json(b)| b).unwrap_or_default();
    let (offboarding_id, title) = match (b.offboarding_id, b.title) {
        (Some(o), Some(t)) if !t.trim().is_empty() => (o, t),
        _ => {
            return err_response(
                "bad_request",
                400,
                "offboardingId and title are required".to_string(),
            )
        }
    };
    match svc
        .create_clearance_item(
            tenant.company_id,
            NewClearanceItem {
                offboarding_id,
                title,
                clearer_employee_id: b.clearer_employee_id,
                notify_user_id: b.notify_user_id,
            },
        )
        .await
    {
        Ok(id) => (StatusCode::CREATED, Json(IdResponse { id })).into_response(),
        Err(e) => err_response(e.code(), e.http_status(), e.to_string()),
    }
}

// ── Final settlements ───────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct DraftSettlementBody {
    offboarding_id: Uuid,
}

async fn draft_final_settlement(
    State(svc): State<Arc<FinalSettlementWriteService>>,
    tenant: CompanyContext,
    Json(b): Json<DraftSettlementBody>,
) -> axum::response::Response {
    match svc
        .draft_from_offboarding(tenant.company_id, b.offboarding_id)
        .await
    {
        Ok(id) => (StatusCode::CREATED, Json(IdResponse { id })).into_response(),
        Err(e) => err_response(e.code(), e.http_status(), e.to_string()),
    }
}

async fn confirm_final_settlement(
    State(svc): State<Arc<FinalSettlementWriteService>>,
    tenant: CompanyContext,
    Path(settlement_id): Path<Uuid>,
    Json(b): Json<SettlementAccounts>,
) -> axum::response::Response {
    match svc.confirm(tenant.company_id, settlement_id, b).await {
        Ok(Some(ack)) => (StatusCode::OK, Json(ack)).into_response(),
        // Idempotent no-op: already confirmed; no second envelope.
        Ok(None) => (StatusCode::OK, Json(OkResponse { ok: false })).into_response(),
        Err(e) => err_response(e.code(), e.http_status(), e.to_string()),
    }
}

// ── Composition ─────────────────────────────────────────────────────────────────

/// The verb routes (state-machine writes). Combined with read-only CRUD for
/// every entity; generic writes stay unmounted for the workflow carriers.
/// Each service gets its own state-typed router; merging normalizes them.
fn create_lifecycle_verb_routes(
    onboardings: Arc<OnboardingWriteService>,
    promotions: Arc<PromotionWriteService>,
    offboardings: Arc<OffboardingWriteService>,
    tasks: Arc<OnboardingTaskWriteService>,
    clearance: Arc<ClearanceItemWriteService>,
    settlements: Arc<FinalSettlementWriteService>,
) -> Router {
    let onboardings = Router::new()
        .route("/onboardings", post(create_onboarding))
        .route("/onboardings/:id/complete", post(complete_onboarding))
        .route("/onboardings/:id/confirm", post(confirm_onboarding))
        .with_state(onboardings);

    let promotions = Router::new()
        .route("/promotions", post(create_promotion))
        .route("/promotions/:id/approve", post(approve_promotion))
        .route("/promotions/:id/effect", post(effect_promotion))
        .with_state(promotions);

    let offboardings = Router::new()
        .route("/offboardings", post(create_offboarding))
        .route("/offboardings/:id/clear", post(clear_offboarding))
        .route("/offboardings/:id/close", post(close_offboarding))
        .with_state(offboardings);

    let checkpoints = Router::new()
        .route("/onboarding_tasks", post(create_onboarding_task))
        .with_state(tasks)
        .merge(Router::new().route("/clearance_items", post(create_clearance_item)).with_state(clearance));

    let settlements = Router::new()
        .route("/final_settlements/draft", post(draft_final_settlement))
        .route("/final_settlements/:id/confirm", post(confirm_final_settlement))
        .with_state(settlements);

    Router::new()
        .merge(onboardings)
        .merge(promotions)
        .merge(offboardings)
        .merge(checkpoints)
        .merge(settlements)
}

/// Mount the lifecycle module with write paths locked to validated verbs.
///
/// = read-only CRUD for every entity
/// + generic writes for side-effect-free checkpoint data (onboarding tasks,
///   clearance items, exit interviews)
/// + the state-machine verbs (onboardings / promotions / offboardings /
///   checkpoints / settlements) — the four workflow carriers have NO generic
///   write surface at all, so no path can set a lifecycle state directly and
///   sidestep a verb's side effects (the outbox emits, the vacancy-clearance
///   derivation, the GL post).
///
/// Prefer this over `LifecycleModule::all_crud_routes()` for any real deployment.
pub fn create_guarded_lifecycle_routes(m: &LifecycleModule) -> Router {
    use crate::presentation::http::{
        create_clearance_item_write_routes, create_exit_interview_write_routes,
        create_onboarding_task_write_routes,
    };

    Router::new()
        // Safe base: GET-only for all seven entities.
        .merge(m.readonly_routes())
        // Checkpoint data keeps generic writes (no cross-entity invariants to bypass;
        // the notify side effect lives only in the verb, which is additive).
        .merge(create_onboarding_task_write_routes(m.onboarding_task_service.clone()))
        .merge(create_clearance_item_write_routes(m.clearance_item_service.clone()))
        .merge(create_exit_interview_write_routes(m.exit_interview_service.clone()))
        // The workflow carriers: verbs only.
        .merge(create_lifecycle_verb_routes(
            m.onboarding_write_service.clone(),
            m.promotion_write_service.clone(),
            m.offboarding_write_service.clone(),
            m.onboarding_task_write_service.clone(),
            m.clearance_item_write_service.clone(),
            m.final_settlement_write_service.clone(),
        ))
}

// Keep the error type referenced even if a handler path is compiled out.
#[allow(dead_code)]
fn _error_types_referenced(_: FinalSettlementError) {}

//! The approvals seam for promotions: the port the create verb files
//! through and the approve verb reads back (the same TR2 posture leave,
//! overtime and corrections use). The default is unwired — an unwired
//! deployment keeps the bespoke approve verb exactly as it was.

use async_trait::async_trait;
use chrono::NaiveDate;
use uuid::Uuid;

/// What a promotion filing carries: enough of the move for an approver to
/// render a verdict row without another read.
#[derive(Debug, Clone)]
pub struct PromotionFiling {
    pub promotion_id: Uuid,
    pub employee_id: Uuid,
    pub promotion_type: String,
    pub effective_date: NaiveDate,
    pub position_id_to: Option<Uuid>,
    pub level_id_to: Option<Uuid>,
    pub department_id_to: Option<Uuid>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromotionVerdict {
    Pending,
    Approved,
    Rejected,
}

#[derive(Debug, thiserror::Error)]
pub enum PromotionSeamError {
    #[error("promotions approvals seam is not wired")]
    Unwired,
    #[error("no approval request {0} is known to the engine")]
    UnknownApprovalRequest(Uuid),
    #[error("approvals transport: {0}")]
    Transport(String),
}

#[async_trait]
pub trait PromotionFilingPort: Send + Sync {
    async fn file(&self, filing: &PromotionFiling) -> Result<Uuid, PromotionSeamError>;
    async fn status(&self, approval_request_id: Uuid) -> Result<PromotionVerdict, PromotionSeamError>;
}

/// The unwired default.
pub struct UnwiredPromotionApprovals;

#[async_trait]
impl PromotionFilingPort for UnwiredPromotionApprovals {
    async fn file(&self, _filing: &PromotionFiling) -> Result<Uuid, PromotionSeamError> {
        Err(PromotionSeamError::Unwired)
    }
    async fn status(&self, id: Uuid) -> Result<PromotionVerdict, PromotionSeamError> {
        Err(PromotionSeamError::UnknownApprovalRequest(id))
    }
}

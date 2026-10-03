use serde::{Deserialize, Serialize};
use sqlx::Type;
use std::str::FromStr;
#[cfg(feature = "openapi")]
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Type)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "offboarding_reason", rename_all = "snake_case")]
pub enum OffboardingReason {
    Resignation,
    MergerConsolidationSplit,
    Takeover,
    TakeoverChangedTerms,
    EfficiencyLosses,
    EfficiencyPreventLosses,
    ClosureLosses,
    ClosureNotLosses,
    ClosureForceMajeure,
    ForceMajeureNoClosure,
    PkpuLosses,
    PkpuNotLosses,
    Bankruptcy,
    EmployerViolationRequest,
    WorkerRequestRejected,
    AbsenceWithoutNotice,
    ViolationAfterWarnings,
    UrgentViolation,
    DetainedCompanyLoss,
    DetainedNoCompanyLoss,
    ConvictedCompanyLoss,
    ConvictedNoCompanyLoss,
    ProlongedIllness,
    Retirement,
    Death,
    EndOfContract,
    Termination,
    MergerAcquisition,
    Efficiency,
    ForceMajeure,
    Misconduct,
}

impl std::fmt::Display for OffboardingReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Resignation => write!(f, "resignation"),
            Self::MergerConsolidationSplit => write!(f, "merger_consolidation_split"),
            Self::Takeover => write!(f, "takeover"),
            Self::TakeoverChangedTerms => write!(f, "takeover_changed_terms"),
            Self::EfficiencyLosses => write!(f, "efficiency_losses"),
            Self::EfficiencyPreventLosses => write!(f, "efficiency_prevent_losses"),
            Self::ClosureLosses => write!(f, "closure_losses"),
            Self::ClosureNotLosses => write!(f, "closure_not_losses"),
            Self::ClosureForceMajeure => write!(f, "closure_force_majeure"),
            Self::ForceMajeureNoClosure => write!(f, "force_majeure_no_closure"),
            Self::PkpuLosses => write!(f, "pkpu_losses"),
            Self::PkpuNotLosses => write!(f, "pkpu_not_losses"),
            Self::Bankruptcy => write!(f, "bankruptcy"),
            Self::EmployerViolationRequest => write!(f, "employer_violation_request"),
            Self::WorkerRequestRejected => write!(f, "worker_request_rejected"),
            Self::AbsenceWithoutNotice => write!(f, "absence_without_notice"),
            Self::ViolationAfterWarnings => write!(f, "violation_after_warnings"),
            Self::UrgentViolation => write!(f, "urgent_violation"),
            Self::DetainedCompanyLoss => write!(f, "detained_company_loss"),
            Self::DetainedNoCompanyLoss => write!(f, "detained_no_company_loss"),
            Self::ConvictedCompanyLoss => write!(f, "convicted_company_loss"),
            Self::ConvictedNoCompanyLoss => write!(f, "convicted_no_company_loss"),
            Self::ProlongedIllness => write!(f, "prolonged_illness"),
            Self::Retirement => write!(f, "retirement"),
            Self::Death => write!(f, "death"),
            Self::EndOfContract => write!(f, "end_of_contract"),
            Self::Termination => write!(f, "termination"),
            Self::MergerAcquisition => write!(f, "merger_acquisition"),
            Self::Efficiency => write!(f, "efficiency"),
            Self::ForceMajeure => write!(f, "force_majeure"),
            Self::Misconduct => write!(f, "misconduct"),
        }
    }
}

impl FromStr for OffboardingReason {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "resignation" => Ok(Self::Resignation),
            "merger_consolidation_split" => Ok(Self::MergerConsolidationSplit),
            "takeover" => Ok(Self::Takeover),
            "takeover_changed_terms" => Ok(Self::TakeoverChangedTerms),
            "efficiency_losses" => Ok(Self::EfficiencyLosses),
            "efficiency_prevent_losses" => Ok(Self::EfficiencyPreventLosses),
            "closure_losses" => Ok(Self::ClosureLosses),
            "closure_not_losses" => Ok(Self::ClosureNotLosses),
            "closure_force_majeure" => Ok(Self::ClosureForceMajeure),
            "force_majeure_no_closure" => Ok(Self::ForceMajeureNoClosure),
            "pkpu_losses" => Ok(Self::PkpuLosses),
            "pkpu_not_losses" => Ok(Self::PkpuNotLosses),
            "bankruptcy" => Ok(Self::Bankruptcy),
            "employer_violation_request" => Ok(Self::EmployerViolationRequest),
            "worker_request_rejected" => Ok(Self::WorkerRequestRejected),
            "absence_without_notice" => Ok(Self::AbsenceWithoutNotice),
            "violation_after_warnings" => Ok(Self::ViolationAfterWarnings),
            "urgent_violation" => Ok(Self::UrgentViolation),
            "detained_company_loss" => Ok(Self::DetainedCompanyLoss),
            "detained_no_company_loss" => Ok(Self::DetainedNoCompanyLoss),
            "convicted_company_loss" => Ok(Self::ConvictedCompanyLoss),
            "convicted_no_company_loss" => Ok(Self::ConvictedNoCompanyLoss),
            "prolonged_illness" => Ok(Self::ProlongedIllness),
            "retirement" => Ok(Self::Retirement),
            "death" => Ok(Self::Death),
            "end_of_contract" => Ok(Self::EndOfContract),
            "termination" => Ok(Self::Termination),
            "merger_acquisition" => Ok(Self::MergerAcquisition),
            "efficiency" => Ok(Self::Efficiency),
            "force_majeure" => Ok(Self::ForceMajeure),
            "misconduct" => Ok(Self::Misconduct),
            _ => Err(format!("Unknown OffboardingReason variant: {}", s)),
        }
    }
}

impl Default for OffboardingReason {
    fn default() -> Self {
        Self::Resignation
    }
}

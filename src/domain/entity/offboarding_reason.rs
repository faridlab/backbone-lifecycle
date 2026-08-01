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
    Termination,
    EndOfContract,
    Retirement,
    Death,
    MergerAcquisition,
    Efficiency,
    ForceMajeure,
    Misconduct,
}

impl std::fmt::Display for OffboardingReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Resignation => write!(f, "resignation"),
            Self::Termination => write!(f, "termination"),
            Self::EndOfContract => write!(f, "end_of_contract"),
            Self::Retirement => write!(f, "retirement"),
            Self::Death => write!(f, "death"),
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
            "termination" => Ok(Self::Termination),
            "end_of_contract" => Ok(Self::EndOfContract),
            "retirement" => Ok(Self::Retirement),
            "death" => Ok(Self::Death),
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

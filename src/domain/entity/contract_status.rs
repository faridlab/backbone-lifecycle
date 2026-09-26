use serde::{Deserialize, Serialize};
use sqlx::Type;
use std::str::FromStr;
#[cfg(feature = "openapi")]
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Type)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "contract_status", rename_all = "snake_case")]
pub enum ContractStatus {
    Active,
    DecisionPending,
    Superseded,
    Ended,
}

impl std::fmt::Display for ContractStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::DecisionPending => write!(f, "decision_pending"),
            Self::Superseded => write!(f, "superseded"),
            Self::Ended => write!(f, "ended"),
        }
    }
}

impl FromStr for ContractStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "active" => Ok(Self::Active),
            "decision_pending" => Ok(Self::DecisionPending),
            "superseded" => Ok(Self::Superseded),
            "ended" => Ok(Self::Ended),
            _ => Err(format!("Unknown ContractStatus variant: {}", s)),
        }
    }
}

impl Default for ContractStatus {
    fn default() -> Self {
        Self::Active
    }
}

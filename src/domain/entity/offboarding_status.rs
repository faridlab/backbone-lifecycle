use serde::{Deserialize, Serialize};
use sqlx::Type;
use std::str::FromStr;
#[cfg(feature = "openapi")]
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Type)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "offboarding_status", rename_all = "snake_case")]
pub enum OffboardingStatus {
    Initiated,
    InProgress,
    Cleared,
    Closed,
}

impl std::fmt::Display for OffboardingStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Initiated => write!(f, "initiated"),
            Self::InProgress => write!(f, "in_progress"),
            Self::Cleared => write!(f, "cleared"),
            Self::Closed => write!(f, "closed"),
        }
    }
}

impl FromStr for OffboardingStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "initiated" => Ok(Self::Initiated),
            "in_progress" => Ok(Self::InProgress),
            "cleared" => Ok(Self::Cleared),
            "closed" => Ok(Self::Closed),
            _ => Err(format!("Unknown OffboardingStatus variant: {}", s)),
        }
    }
}

impl Default for OffboardingStatus {
    fn default() -> Self {
        Self::Initiated
    }
}

use serde::{Deserialize, Serialize};
use sqlx::Type;
use std::str::FromStr;
#[cfg(feature = "openapi")]
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Type)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "onboarding_status", rename_all = "snake_case")]
pub enum OnboardingStatus {
    Pending,
    InProgress,
    Completed,
    Abandoned,
}

impl std::fmt::Display for OnboardingStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::InProgress => write!(f, "in_progress"),
            Self::Completed => write!(f, "completed"),
            Self::Abandoned => write!(f, "abandoned"),
        }
    }
}

impl FromStr for OnboardingStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "pending" => Ok(Self::Pending),
            "in_progress" => Ok(Self::InProgress),
            "completed" => Ok(Self::Completed),
            "abandoned" => Ok(Self::Abandoned),
            _ => Err(format!("Unknown OnboardingStatus variant: {}", s)),
        }
    }
}

impl Default for OnboardingStatus {
    fn default() -> Self {
        Self::Pending
    }
}

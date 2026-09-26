use serde::{Deserialize, Serialize};
use sqlx::Type;
use std::str::FromStr;
#[cfg(feature = "openapi")]
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Type)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "discipline_status", rename_all = "snake_case")]
pub enum DisciplineStatus {
    Issued,
    Acknowledged,
    AcknowledgedPending,
    Contested,
    Cancelled,
}

impl std::fmt::Display for DisciplineStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Issued => write!(f, "issued"),
            Self::Acknowledged => write!(f, "acknowledged"),
            Self::AcknowledgedPending => write!(f, "acknowledged_pending"),
            Self::Contested => write!(f, "contested"),
            Self::Cancelled => write!(f, "cancelled"),
        }
    }
}

impl FromStr for DisciplineStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "issued" => Ok(Self::Issued),
            "acknowledged" => Ok(Self::Acknowledged),
            "acknowledged_pending" => Ok(Self::AcknowledgedPending),
            "contested" => Ok(Self::Contested),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(format!("Unknown DisciplineStatus variant: {}", s)),
        }
    }
}

impl Default for DisciplineStatus {
    fn default() -> Self {
        Self::Issued
    }
}

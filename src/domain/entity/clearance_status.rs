use serde::{Deserialize, Serialize};
use sqlx::Type;
use std::str::FromStr;
#[cfg(feature = "openapi")]
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Type)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "clearance_status", rename_all = "snake_case")]
pub enum ClearanceStatus {
    Pending,
    Cleared,
    Blocked,
}

impl std::fmt::Display for ClearanceStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Cleared => write!(f, "cleared"),
            Self::Blocked => write!(f, "blocked"),
        }
    }
}

impl FromStr for ClearanceStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "pending" => Ok(Self::Pending),
            "cleared" => Ok(Self::Cleared),
            "blocked" => Ok(Self::Blocked),
            _ => Err(format!("Unknown ClearanceStatus variant: {}", s)),
        }
    }
}

impl Default for ClearanceStatus {
    fn default() -> Self {
        Self::Pending
    }
}

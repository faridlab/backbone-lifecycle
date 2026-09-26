use serde::{Deserialize, Serialize};
use sqlx::Type;
use std::str::FromStr;
#[cfg(feature = "openapi")]
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Type)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "discipline_served_disposition", rename_all = "snake_case")]
pub enum DisciplineServedDisposition {
    Delivered,
    WitnessedRefusal,
}

impl std::fmt::Display for DisciplineServedDisposition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Delivered => write!(f, "delivered"),
            Self::WitnessedRefusal => write!(f, "witnessed_refusal"),
        }
    }
}

impl FromStr for DisciplineServedDisposition {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "delivered" => Ok(Self::Delivered),
            "witnessed_refusal" => Ok(Self::WitnessedRefusal),
            _ => Err(format!("Unknown DisciplineServedDisposition variant: {}", s)),
        }
    }
}

impl Default for DisciplineServedDisposition {
    fn default() -> Self {
        Self::Delivered
    }
}

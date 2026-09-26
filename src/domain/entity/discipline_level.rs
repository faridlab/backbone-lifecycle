use serde::{Deserialize, Serialize};
use sqlx::Type;
use std::str::FromStr;
#[cfg(feature = "openapi")]
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Type)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "discipline_level", rename_all = "snake_case")]
pub enum DisciplineLevel {
    Sp1,
    Sp2,
    Sp3,
}

impl std::fmt::Display for DisciplineLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sp1 => write!(f, "sp1"),
            Self::Sp2 => write!(f, "sp2"),
            Self::Sp3 => write!(f, "sp3"),
        }
    }
}

impl FromStr for DisciplineLevel {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "sp1" => Ok(Self::Sp1),
            "sp2" => Ok(Self::Sp2),
            "sp3" => Ok(Self::Sp3),
            _ => Err(format!("Unknown DisciplineLevel variant: {}", s)),
        }
    }
}

impl Default for DisciplineLevel {
    fn default() -> Self {
        Self::Sp1
    }
}

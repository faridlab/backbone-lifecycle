use serde::{Deserialize, Serialize};
use sqlx::Type;
use std::str::FromStr;
#[cfg(feature = "openapi")]
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Type)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "contract_type", rename_all = "snake_case")]
pub enum ContractType {
    Pkwtt,
    Pkwt,
}

impl std::fmt::Display for ContractType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pkwtt => write!(f, "pkwtt"),
            Self::Pkwt => write!(f, "pkwt"),
        }
    }
}

impl FromStr for ContractType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "pkwtt" => Ok(Self::Pkwtt),
            "pkwt" => Ok(Self::Pkwt),
            _ => Err(format!("Unknown ContractType variant: {}", s)),
        }
    }
}

impl Default for ContractType {
    fn default() -> Self {
        Self::Pkwtt
    }
}

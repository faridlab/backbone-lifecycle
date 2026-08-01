use serde::{Deserialize, Serialize};
use sqlx::Type;
use std::str::FromStr;
#[cfg(feature = "openapi")]
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Type)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "promotion_type", rename_all = "snake_case")]
pub enum PromotionType {
    Promotion,
    Transfer,
    Demotion,
    Lateral,
}

impl std::fmt::Display for PromotionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Promotion => write!(f, "promotion"),
            Self::Transfer => write!(f, "transfer"),
            Self::Demotion => write!(f, "demotion"),
            Self::Lateral => write!(f, "lateral"),
        }
    }
}

impl FromStr for PromotionType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "promotion" => Ok(Self::Promotion),
            "transfer" => Ok(Self::Transfer),
            "demotion" => Ok(Self::Demotion),
            "lateral" => Ok(Self::Lateral),
            _ => Err(format!("Unknown PromotionType variant: {}", s)),
        }
    }
}

impl Default for PromotionType {
    fn default() -> Self {
        Self::Promotion
    }
}

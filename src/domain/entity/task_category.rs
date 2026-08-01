use serde::{Deserialize, Serialize};
use sqlx::Type;
use std::str::FromStr;
#[cfg(feature = "openapi")]
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Type)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "task_category", rename_all = "snake_case")]
pub enum TaskCategory {
    Document,
    Equipment,
    AccountAccess,
    PolicyAck,
    Induction,
}

impl std::fmt::Display for TaskCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Document => write!(f, "document"),
            Self::Equipment => write!(f, "equipment"),
            Self::AccountAccess => write!(f, "account_access"),
            Self::PolicyAck => write!(f, "policy_ack"),
            Self::Induction => write!(f, "induction"),
        }
    }
}

impl FromStr for TaskCategory {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "document" => Ok(Self::Document),
            "equipment" => Ok(Self::Equipment),
            "account_access" => Ok(Self::AccountAccess),
            "policy_ack" => Ok(Self::PolicyAck),
            "induction" => Ok(Self::Induction),
            _ => Err(format!("Unknown TaskCategory variant: {}", s)),
        }
    }
}

impl Default for TaskCategory {
    fn default() -> Self {
        Self::Document
    }
}

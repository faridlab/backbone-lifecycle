use serde::{Deserialize, Serialize};
use sqlx::Type;
use std::str::FromStr;
#[cfg(feature = "openapi")]
use utoipa::ToSchema;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Type)]
#[cfg_attr(feature = "openapi", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "task_status", rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Done,
    Skipped,
    Blocked,
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "pending"),
            Self::Done => write!(f, "done"),
            Self::Skipped => write!(f, "skipped"),
            Self::Blocked => write!(f, "blocked"),
        }
    }
}

impl FromStr for TaskStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "pending" => Ok(Self::Pending),
            "done" => Ok(Self::Done),
            "skipped" => Ok(Self::Skipped),
            "blocked" => Ok(Self::Blocked),
            _ => Err(format!("Unknown TaskStatus variant: {}", s)),
        }
    }
}

impl Default for TaskStatus {
    fn default() -> Self {
        Self::Pending
    }
}

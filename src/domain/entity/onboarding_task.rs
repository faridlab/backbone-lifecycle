use chrono::{DateTime, Utc, NaiveDate};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

use super::TaskCategory;
use super::TaskStatus;
use super::AuditMetadata;

/// Strongly-typed ID for OnboardingTask
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OnboardingTaskId(pub Uuid);

impl OnboardingTaskId {
    pub fn new(id: Uuid) -> Self { Self(id) }
    pub fn generate() -> Self { Self(Uuid::new_v4()) }
    pub fn into_inner(self) -> Uuid { self.0 }
}

impl std::fmt::Display for OnboardingTaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for OnboardingTaskId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

impl From<Uuid> for OnboardingTaskId {
    fn from(id: Uuid) -> Self { Self(id) }
}

impl From<OnboardingTaskId> for Uuid {
    fn from(id: OnboardingTaskId) -> Self { id.0 }
}

impl AsRef<Uuid> for OnboardingTaskId {
    fn as_ref(&self) -> &Uuid { &self.0 }
}

impl std::ops::Deref for OnboardingTaskId {
    type Target = Uuid;
    fn deref(&self) -> &Self::Target { &self.0 }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct OnboardingTask {
    pub id: Uuid,
    pub company_id: Uuid,
    pub onboarding_id: Uuid,
    pub title: String,
    pub category: Option<TaskCategory>,
    pub owner_employee_id: Option<Uuid>,
    pub due_date: Option<NaiveDate>,
    pub status: TaskStatus,
    #[serde(default)]
    #[sqlx(json)]
    pub metadata: AuditMetadata,
}

impl OnboardingTask {
    /// Create a builder for OnboardingTask
    pub fn builder() -> OnboardingTaskBuilder {
        OnboardingTaskBuilder::default()
    }

    /// Create a new OnboardingTask with required fields
    pub fn new(company_id: Uuid, onboarding_id: Uuid, title: String, status: TaskStatus) -> Self {
        Self {
            id: Uuid::new_v4(),
            company_id,
            onboarding_id,
            title,
            category: None,
            owner_employee_id: None,
            due_date: None,
            status,
            metadata: AuditMetadata::default(),
        }
    }

    /// Get the entity's unique identifier
    pub fn id(&self) -> &Uuid {
        &self.id
    }

    /// Get a strongly-typed ID for this entity
    pub fn typed_id(&self) -> OnboardingTaskId {
        OnboardingTaskId(self.id)
    }

    /// Get when this entity was created
    pub fn created_at(&self) -> Option<&DateTime<Utc>> {
        self.metadata.created_at.as_ref()
    }

    /// Get when this entity was last updated
    pub fn updated_at(&self) -> Option<&DateTime<Utc>> {
        self.metadata.updated_at.as_ref()
    }

    /// Check if this entity is soft deleted
    pub fn is_deleted(&self) -> bool {
        self.metadata.deleted_at.is_some()
    }

    /// Check if this entity is active (not deleted)
    pub fn is_active(&self) -> bool {
        self.metadata.deleted_at.is_none()
    }

    /// Get when this entity was deleted
    pub fn deleted_at(&self) -> Option<&DateTime<Utc>> {
        self.metadata.deleted_at.as_ref()
    }

    /// Get who created this entity
    pub fn created_by(&self) -> Option<&Uuid> {
        self.metadata.created_by.as_ref()
    }

    /// Get who last updated this entity
    pub fn updated_by(&self) -> Option<&Uuid> {
        self.metadata.updated_by.as_ref()
    }

    /// Get who deleted this entity
    pub fn deleted_by(&self) -> Option<&Uuid> {
        self.metadata.deleted_by.as_ref()
    }

    /// Get the current status
    pub fn status(&self) -> &TaskStatus {
        &self.status
    }


    // ==========================================================
    // Fluent Setters (with_* for optional fields)
    // ==========================================================

    /// Set the category field (chainable)
    pub fn with_category(mut self, value: TaskCategory) -> Self {
        self.category = Some(value);
        self
    }

    /// Set the owner_employee_id field (chainable)
    pub fn with_owner_employee_id(mut self, value: Uuid) -> Self {
        self.owner_employee_id = Some(value);
        self
    }

    /// Set the due_date field (chainable)
    pub fn with_due_date(mut self, value: NaiveDate) -> Self {
        self.due_date = Some(value);
        self
    }

    // ==========================================================
    // Partial Update
    // ==========================================================

    /// Apply partial updates from a map of field name to JSON value
    pub fn apply_patch(&mut self, fields: std::collections::HashMap<String, serde_json::Value>) {
        for (key, value) in fields {
            match key.as_str() {
                "company_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.company_id = v; }
                }
                "onboarding_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.onboarding_id = v; }
                }
                "title" => {
                    if let Ok(v) = serde_json::from_value(value) { self.title = v; }
                }
                "category" => {
                    if let Ok(v) = serde_json::from_value(value) { self.category = v; }
                }
                "owner_employee_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.owner_employee_id = v; }
                }
                "due_date" => {
                    if let Ok(v) = serde_json::from_value(value) { self.due_date = v; }
                }
                "status" => {
                    if let Ok(v) = serde_json::from_value(value) { self.status = v; }
                }
                _ => {} // ignore unknown fields
            }
        }
    }

    // <<< CUSTOM METHODS START >>>
    // <<< CUSTOM METHODS END >>>
}

impl super::Entity for OnboardingTask {
    type Id = Uuid;

    fn entity_id(&self) -> &Self::Id {
        &self.id
    }

    fn entity_type() -> &'static str {
        "OnboardingTask"
    }
}

impl backbone_core::PersistentEntity for OnboardingTask {
    fn entity_id(&self) -> String {
        self.id.to_string()
    }
    fn set_entity_id(&mut self, id: String) {
        if let Ok(uuid) = uuid::Uuid::parse_str(&id) {
            self.id = uuid;
        }
    }
    fn created_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.metadata.created_at
    }
    fn set_created_at(&mut self, ts: chrono::DateTime<chrono::Utc>) {
        self.metadata.created_at = Some(ts);
    }
    fn updated_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.metadata.updated_at
    }
    fn set_updated_at(&mut self, ts: chrono::DateTime<chrono::Utc>) {
        self.metadata.updated_at = Some(ts);
    }
    fn deleted_at(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.metadata.deleted_at
    }
    fn set_deleted_at(&mut self, ts: Option<chrono::DateTime<chrono::Utc>>) {
        self.metadata.deleted_at = ts;
    }
}

impl backbone_orm::EntityRepoMeta for OnboardingTask {
    fn column_types() -> std::collections::HashMap<String, String> {
        let mut m = std::collections::HashMap::new();
        m.insert("id".to_string(), "uuid".to_string());
        m.insert("company_id".to_string(), "uuid".to_string());
        m.insert("onboarding_id".to_string(), "uuid".to_string());
        m.insert("owner_employee_id".to_string(), "uuid".to_string());
        m.insert("category".to_string(), "task_category".to_string());
        m.insert("status".to_string(), "task_status".to_string());
        m
    }
    fn search_fields() -> &'static [&'static str] {
        &["title"]
    }
    fn company_field() -> Option<&'static str> {
        Some("company_id")
    }
}

/// Builder for OnboardingTask entity
///
/// Provides a fluent API for constructing OnboardingTask instances.
/// System fields (id, metadata, timestamps) are auto-initialized.
#[derive(Debug, Clone, Default)]
pub struct OnboardingTaskBuilder {
    company_id: Option<Uuid>,
    onboarding_id: Option<Uuid>,
    title: Option<String>,
    category: Option<TaskCategory>,
    owner_employee_id: Option<Uuid>,
    due_date: Option<NaiveDate>,
    status: Option<TaskStatus>,
}

impl OnboardingTaskBuilder {
    /// Set the company_id field (required)
    pub fn company_id(mut self, value: Uuid) -> Self {
        self.company_id = Some(value);
        self
    }

    /// Set the onboarding_id field (required)
    pub fn onboarding_id(mut self, value: Uuid) -> Self {
        self.onboarding_id = Some(value);
        self
    }

    /// Set the title field (required)
    pub fn title(mut self, value: String) -> Self {
        self.title = Some(value);
        self
    }

    /// Set the category field (optional)
    pub fn category(mut self, value: TaskCategory) -> Self {
        self.category = Some(value);
        self
    }

    /// Set the owner_employee_id field (optional)
    pub fn owner_employee_id(mut self, value: Uuid) -> Self {
        self.owner_employee_id = Some(value);
        self
    }

    /// Set the due_date field (optional)
    pub fn due_date(mut self, value: NaiveDate) -> Self {
        self.due_date = Some(value);
        self
    }

    /// Set the status field (default: `TaskStatus::default()`)
    pub fn status(mut self, value: TaskStatus) -> Self {
        self.status = Some(value);
        self
    }

    /// Build the OnboardingTask entity
    ///
    /// Returns Err if any required field without a default is missing.
    pub fn build(self) -> Result<OnboardingTask, String> {
        let company_id = self.company_id.ok_or_else(|| "company_id is required".to_string())?;
        let onboarding_id = self.onboarding_id.ok_or_else(|| "onboarding_id is required".to_string())?;
        let title = self.title.ok_or_else(|| "title is required".to_string())?;

        Ok(OnboardingTask {
            id: Uuid::new_v4(),
            company_id,
            onboarding_id,
            title,
            category: self.category,
            owner_employee_id: self.owner_employee_id,
            due_date: self.due_date,
            status: self.status.unwrap_or(TaskStatus::default()),
            metadata: AuditMetadata::default(),
        })
    }
}

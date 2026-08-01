use chrono::{DateTime, Utc, NaiveDate};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

use super::OnboardingStatus;
use super::AuditMetadata;

/// Strongly-typed ID for Onboarding
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OnboardingId(pub Uuid);

impl OnboardingId {
    pub fn new(id: Uuid) -> Self { Self(id) }
    pub fn generate() -> Self { Self(Uuid::new_v4()) }
    pub fn into_inner(self) -> Uuid { self.0 }
}

impl std::fmt::Display for OnboardingId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for OnboardingId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

impl From<Uuid> for OnboardingId {
    fn from(id: Uuid) -> Self { Self(id) }
}

impl From<OnboardingId> for Uuid {
    fn from(id: OnboardingId) -> Self { id.0 }
}

impl AsRef<Uuid> for OnboardingId {
    fn as_ref(&self) -> &Uuid { &self.0 }
}

impl std::ops::Deref for OnboardingId {
    type Target = Uuid;
    fn deref(&self) -> &Self::Target { &self.0 }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Onboarding {
    pub id: Uuid,
    pub company_id: Uuid,
    pub employee_id: Uuid,
    pub start_date: NaiveDate,
    pub status: OnboardingStatus,
    pub completed_at: Option<DateTime<Utc>>,
    pub template_id: Option<Uuid>,
    #[serde(default)]
    #[sqlx(json)]
    pub metadata: AuditMetadata,
}

impl Onboarding {
    /// Create a builder for Onboarding
    pub fn builder() -> OnboardingBuilder {
        OnboardingBuilder::default()
    }

    /// Create a new Onboarding with required fields
    pub fn new(company_id: Uuid, employee_id: Uuid, start_date: NaiveDate, status: OnboardingStatus) -> Self {
        Self {
            id: Uuid::new_v4(),
            company_id,
            employee_id,
            start_date,
            status,
            completed_at: None,
            template_id: None,
            metadata: AuditMetadata::default(),
        }
    }

    /// Get the entity's unique identifier
    pub fn id(&self) -> &Uuid {
        &self.id
    }

    /// Get a strongly-typed ID for this entity
    pub fn typed_id(&self) -> OnboardingId {
        OnboardingId(self.id)
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
    pub fn status(&self) -> &OnboardingStatus {
        &self.status
    }


    // ==========================================================
    // Fluent Setters (with_* for optional fields)
    // ==========================================================

    /// Set the completed_at field (chainable)
    pub fn with_completed_at(mut self, value: DateTime<Utc>) -> Self {
        self.completed_at = Some(value);
        self
    }

    /// Set the template_id field (chainable)
    pub fn with_template_id(mut self, value: Uuid) -> Self {
        self.template_id = Some(value);
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
                "employee_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.employee_id = v; }
                }
                "start_date" => {
                    if let Ok(v) = serde_json::from_value(value) { self.start_date = v; }
                }
                "status" => {
                    if let Ok(v) = serde_json::from_value(value) { self.status = v; }
                }
                "completed_at" => {
                    if let Ok(v) = serde_json::from_value(value) { self.completed_at = v; }
                }
                "template_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.template_id = v; }
                }
                _ => {} // ignore unknown fields
            }
        }
    }

    // <<< CUSTOM METHODS START >>>
    // <<< CUSTOM METHODS END >>>
}

impl super::Entity for Onboarding {
    type Id = Uuid;

    fn entity_id(&self) -> &Self::Id {
        &self.id
    }

    fn entity_type() -> &'static str {
        "Onboarding"
    }
}

impl backbone_core::PersistentEntity for Onboarding {
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

impl backbone_orm::EntityRepoMeta for Onboarding {
    fn column_types() -> std::collections::HashMap<String, String> {
        let mut m = std::collections::HashMap::new();
        m.insert("id".to_string(), "uuid".to_string());
        m.insert("company_id".to_string(), "uuid".to_string());
        m.insert("employee_id".to_string(), "uuid".to_string());
        m.insert("template_id".to_string(), "uuid".to_string());
        m.insert("status".to_string(), "onboarding_status".to_string());
        m
    }
    fn search_fields() -> &'static [&'static str] {
        &[]
    }
    fn company_field() -> Option<&'static str> {
        Some("company_id")
    }
}

/// Builder for Onboarding entity
///
/// Provides a fluent API for constructing Onboarding instances.
/// System fields (id, metadata, timestamps) are auto-initialized.
#[derive(Debug, Clone, Default)]
pub struct OnboardingBuilder {
    company_id: Option<Uuid>,
    employee_id: Option<Uuid>,
    start_date: Option<NaiveDate>,
    status: Option<OnboardingStatus>,
    completed_at: Option<DateTime<Utc>>,
    template_id: Option<Uuid>,
}

impl OnboardingBuilder {
    /// Set the company_id field (required)
    pub fn company_id(mut self, value: Uuid) -> Self {
        self.company_id = Some(value);
        self
    }

    /// Set the employee_id field (required)
    pub fn employee_id(mut self, value: Uuid) -> Self {
        self.employee_id = Some(value);
        self
    }

    /// Set the start_date field (required)
    pub fn start_date(mut self, value: NaiveDate) -> Self {
        self.start_date = Some(value);
        self
    }

    /// Set the status field (default: `OnboardingStatus::default()`)
    pub fn status(mut self, value: OnboardingStatus) -> Self {
        self.status = Some(value);
        self
    }

    /// Set the completed_at field (optional)
    pub fn completed_at(mut self, value: DateTime<Utc>) -> Self {
        self.completed_at = Some(value);
        self
    }

    /// Set the template_id field (optional)
    pub fn template_id(mut self, value: Uuid) -> Self {
        self.template_id = Some(value);
        self
    }

    /// Build the Onboarding entity
    ///
    /// Returns Err if any required field without a default is missing.
    pub fn build(self) -> Result<Onboarding, String> {
        let company_id = self.company_id.ok_or_else(|| "company_id is required".to_string())?;
        let employee_id = self.employee_id.ok_or_else(|| "employee_id is required".to_string())?;
        let start_date = self.start_date.ok_or_else(|| "start_date is required".to_string())?;

        Ok(Onboarding {
            id: Uuid::new_v4(),
            company_id,
            employee_id,
            start_date,
            status: self.status.unwrap_or(OnboardingStatus::default()),
            completed_at: self.completed_at,
            template_id: self.template_id,
            metadata: AuditMetadata::default(),
        })
    }
}

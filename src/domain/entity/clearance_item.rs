use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

use super::ClearanceStatus;
use super::AuditMetadata;

/// Strongly-typed ID for ClearanceItem
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ClearanceItemId(pub Uuid);

impl ClearanceItemId {
    pub fn new(id: Uuid) -> Self { Self(id) }
    pub fn generate() -> Self { Self(Uuid::new_v4()) }
    pub fn into_inner(self) -> Uuid { self.0 }
}

impl std::fmt::Display for ClearanceItemId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for ClearanceItemId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

impl From<Uuid> for ClearanceItemId {
    fn from(id: Uuid) -> Self { Self(id) }
}

impl From<ClearanceItemId> for Uuid {
    fn from(id: ClearanceItemId) -> Self { id.0 }
}

impl AsRef<Uuid> for ClearanceItemId {
    fn as_ref(&self) -> &Uuid { &self.0 }
}

impl std::ops::Deref for ClearanceItemId {
    type Target = Uuid;
    fn deref(&self) -> &Self::Target { &self.0 }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ClearanceItem {
    pub id: Uuid,
    pub company_id: Uuid,
    pub offboarding_id: Uuid,
    pub title: String,
    pub clearer_employee_id: Option<Uuid>,
    pub status: ClearanceStatus,
    #[serde(default)]
    #[sqlx(json)]
    pub metadata: AuditMetadata,
}

impl ClearanceItem {
    /// Create a builder for ClearanceItem
    pub fn builder() -> ClearanceItemBuilder {
        ClearanceItemBuilder::default()
    }

    /// Create a new ClearanceItem with required fields
    pub fn new(company_id: Uuid, offboarding_id: Uuid, title: String, status: ClearanceStatus) -> Self {
        Self {
            id: Uuid::new_v4(),
            company_id,
            offboarding_id,
            title,
            clearer_employee_id: None,
            status,
            metadata: AuditMetadata::default(),
        }
    }

    /// Get the entity's unique identifier
    pub fn id(&self) -> &Uuid {
        &self.id
    }

    /// Get a strongly-typed ID for this entity
    pub fn typed_id(&self) -> ClearanceItemId {
        ClearanceItemId(self.id)
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
    pub fn status(&self) -> &ClearanceStatus {
        &self.status
    }


    // ==========================================================
    // Fluent Setters (with_* for optional fields)
    // ==========================================================

    /// Set the clearer_employee_id field (chainable)
    pub fn with_clearer_employee_id(mut self, value: Uuid) -> Self {
        self.clearer_employee_id = Some(value);
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
                "offboarding_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.offboarding_id = v; }
                }
                "title" => {
                    if let Ok(v) = serde_json::from_value(value) { self.title = v; }
                }
                "clearer_employee_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.clearer_employee_id = v; }
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

impl super::Entity for ClearanceItem {
    type Id = Uuid;

    fn entity_id(&self) -> &Self::Id {
        &self.id
    }

    fn entity_type() -> &'static str {
        "ClearanceItem"
    }
}

impl backbone_core::PersistentEntity for ClearanceItem {
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

impl backbone_orm::EntityRepoMeta for ClearanceItem {
    fn column_types() -> std::collections::HashMap<String, String> {
        let mut m = std::collections::HashMap::new();
        m.insert("id".to_string(), "uuid".to_string());
        m.insert("company_id".to_string(), "uuid".to_string());
        m.insert("offboarding_id".to_string(), "uuid".to_string());
        m.insert("clearer_employee_id".to_string(), "uuid".to_string());
        m.insert("status".to_string(), "clearance_status".to_string());
        m
    }
    fn search_fields() -> &'static [&'static str] {
        &["title"]
    }
    fn company_field() -> Option<&'static str> {
        Some("company_id")
    }
}

/// Builder for ClearanceItem entity
///
/// Provides a fluent API for constructing ClearanceItem instances.
/// System fields (id, metadata, timestamps) are auto-initialized.
#[derive(Debug, Clone, Default)]
pub struct ClearanceItemBuilder {
    company_id: Option<Uuid>,
    offboarding_id: Option<Uuid>,
    title: Option<String>,
    clearer_employee_id: Option<Uuid>,
    status: Option<ClearanceStatus>,
}

impl ClearanceItemBuilder {
    /// Set the company_id field (required)
    pub fn company_id(mut self, value: Uuid) -> Self {
        self.company_id = Some(value);
        self
    }

    /// Set the offboarding_id field (required)
    pub fn offboarding_id(mut self, value: Uuid) -> Self {
        self.offboarding_id = Some(value);
        self
    }

    /// Set the title field (required)
    pub fn title(mut self, value: String) -> Self {
        self.title = Some(value);
        self
    }

    /// Set the clearer_employee_id field (optional)
    pub fn clearer_employee_id(mut self, value: Uuid) -> Self {
        self.clearer_employee_id = Some(value);
        self
    }

    /// Set the status field (default: `ClearanceStatus::default()`)
    pub fn status(mut self, value: ClearanceStatus) -> Self {
        self.status = Some(value);
        self
    }

    /// Build the ClearanceItem entity
    ///
    /// Returns Err if any required field without a default is missing.
    pub fn build(self) -> Result<ClearanceItem, String> {
        let company_id = self.company_id.ok_or_else(|| "company_id is required".to_string())?;
        let offboarding_id = self.offboarding_id.ok_or_else(|| "offboarding_id is required".to_string())?;
        let title = self.title.ok_or_else(|| "title is required".to_string())?;

        Ok(ClearanceItem {
            id: Uuid::new_v4(),
            company_id,
            offboarding_id,
            title,
            clearer_employee_id: self.clearer_employee_id,
            status: self.status.unwrap_or(ClearanceStatus::default()),
            metadata: AuditMetadata::default(),
        })
    }
}

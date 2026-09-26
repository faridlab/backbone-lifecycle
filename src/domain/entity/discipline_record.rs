use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

use super::DisciplineLevel;
use super::DisciplineServedDisposition;
use super::DisciplineStatus;
use super::AuditMetadata;

/// Strongly-typed ID for DisciplineRecord
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DisciplineRecordId(pub Uuid);

impl DisciplineRecordId {
    pub fn new(id: Uuid) -> Self { Self(id) }
    pub fn generate() -> Self { Self(Uuid::new_v4()) }
    pub fn into_inner(self) -> Uuid { self.0 }
}

impl std::fmt::Display for DisciplineRecordId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for DisciplineRecordId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

impl From<Uuid> for DisciplineRecordId {
    fn from(id: Uuid) -> Self { Self(id) }
}

impl From<DisciplineRecordId> for Uuid {
    fn from(id: DisciplineRecordId) -> Self { id.0 }
}

impl AsRef<Uuid> for DisciplineRecordId {
    fn as_ref(&self) -> &Uuid { &self.0 }
}

impl std::ops::Deref for DisciplineRecordId {
    type Target = Uuid;
    fn deref(&self) -> &Self::Target { &self.0 }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct DisciplineRecord {
    pub id: Uuid,
    pub employee_id: Uuid,
    pub level: DisciplineLevel,
    pub offense: String,
    pub description: String,
    pub issued_at: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub served_disposition: DisciplineServedDisposition,
    pub status: DisciplineStatus,
    pub acknowledged_at: Option<DateTime<Utc>>,
    pub acknowledged_via: Option<String>,
    pub document_file_id: Option<Uuid>,
    pub cancel_reason: Option<String>,
    pub prior_active_sp1: i32,
    pub prior_active_sp2: i32,
    pub issued_by: Option<Uuid>,
    #[serde(default)]
    #[sqlx(json)]
    pub metadata: AuditMetadata,
}

impl DisciplineRecord {
    /// Create a builder for DisciplineRecord
    pub fn builder() -> DisciplineRecordBuilder {
        <DisciplineRecordBuilder as Default>::default()
    }

    /// Create a new DisciplineRecord with required fields
    pub fn new(employee_id: Uuid, level: DisciplineLevel, offense: String, description: String, issued_at: DateTime<Utc>, valid_until: DateTime<Utc>, served_disposition: DisciplineServedDisposition, status: DisciplineStatus, prior_active_sp1: i32, prior_active_sp2: i32) -> Self {
        Self {
            id: Uuid::new_v4(),
            employee_id,
            level,
            offense,
            description,
            issued_at,
            valid_until,
            served_disposition,
            status,
            acknowledged_at: None,
            acknowledged_via: None,
            document_file_id: None,
            cancel_reason: None,
            prior_active_sp1,
            prior_active_sp2,
            issued_by: None,
            metadata: AuditMetadata::default(),
        }
    }

    /// Get the entity's unique identifier
    pub fn id(&self) -> &Uuid {
        &self.id
    }

    /// Get a strongly-typed ID for this entity
    pub fn typed_id(&self) -> DisciplineRecordId {
        DisciplineRecordId(self.id)
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
    pub fn status(&self) -> &DisciplineStatus {
        &self.status
    }


    // ==========================================================
    // Fluent Setters (with_* for optional fields)
    // ==========================================================

    /// Set the acknowledged_at field (chainable)
    pub fn with_acknowledged_at(mut self, value: DateTime<Utc>) -> Self {
        self.acknowledged_at = Some(value);
        self
    }

    /// Set the acknowledged_via field (chainable)
    pub fn with_acknowledged_via(mut self, value: String) -> Self {
        self.acknowledged_via = Some(value);
        self
    }

    /// Set the document_file_id field (chainable)
    pub fn with_document_file_id(mut self, value: Uuid) -> Self {
        self.document_file_id = Some(value);
        self
    }

    /// Set the cancel_reason field (chainable)
    pub fn with_cancel_reason(mut self, value: String) -> Self {
        self.cancel_reason = Some(value);
        self
    }

    /// Set the issued_by field (chainable)
    pub fn with_issued_by(mut self, value: Uuid) -> Self {
        self.issued_by = Some(value);
        self
    }

    // ==========================================================
    // Partial Update
    // ==========================================================

    /// Apply partial updates from a map of field name to JSON value
    pub fn apply_patch(&mut self, fields: std::collections::HashMap<String, serde_json::Value>) {
        for (key, value) in fields {
            match key.as_str() {
                "employee_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.employee_id = v; }
                }
                "level" => {
                    if let Ok(v) = serde_json::from_value(value) { self.level = v; }
                }
                "offense" => {
                    if let Ok(v) = serde_json::from_value(value) { self.offense = v; }
                }
                "description" => {
                    if let Ok(v) = serde_json::from_value(value) { self.description = v; }
                }
                "issued_at" => {
                    if let Ok(v) = serde_json::from_value(value) { self.issued_at = v; }
                }
                "valid_until" => {
                    if let Ok(v) = serde_json::from_value(value) { self.valid_until = v; }
                }
                "served_disposition" => {
                    if let Ok(v) = serde_json::from_value(value) { self.served_disposition = v; }
                }
                "status" => {
                    if let Ok(v) = serde_json::from_value(value) { self.status = v; }
                }
                "acknowledged_at" => {
                    if let Ok(v) = serde_json::from_value(value) { self.acknowledged_at = v; }
                }
                "acknowledged_via" => {
                    if let Ok(v) = serde_json::from_value(value) { self.acknowledged_via = v; }
                }
                "document_file_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.document_file_id = v; }
                }
                "cancel_reason" => {
                    if let Ok(v) = serde_json::from_value(value) { self.cancel_reason = v; }
                }
                "prior_active_sp1" => {
                    if let Ok(v) = serde_json::from_value(value) { self.prior_active_sp1 = v; }
                }
                "prior_active_sp2" => {
                    if let Ok(v) = serde_json::from_value(value) { self.prior_active_sp2 = v; }
                }
                "issued_by" => {
                    if let Ok(v) = serde_json::from_value(value) { self.issued_by = v; }
                }
                _ => {} // ignore unknown fields
            }
        }
    }

    // <<< CUSTOM METHODS START >>>
    // <<< CUSTOM METHODS END >>>
}

impl super::Entity for DisciplineRecord {
    type Id = Uuid;

    fn entity_id(&self) -> &Self::Id {
        &self.id
    }

    fn entity_type() -> &'static str {
        "DisciplineRecord"
    }
}

impl backbone_core::PersistentEntity for DisciplineRecord {
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

impl backbone_orm::EntityRepoMeta for DisciplineRecord {
    fn column_types() -> std::collections::HashMap<String, String> {
        let mut m = std::collections::HashMap::new();
        m.insert("id".to_string(), "uuid".to_string());
        m.insert("employee_id".to_string(), "uuid".to_string());
        m.insert("document_file_id".to_string(), "uuid".to_string());
        m.insert("level".to_string(), "discipline_level".to_string());
        m.insert("served_disposition".to_string(), "discipline_served_disposition".to_string());
        m.insert("status".to_string(), "discipline_status".to_string());
        m
    }
    fn search_fields() -> &'static [&'static str] {
        &["offense", "description"]
    }
}

/// Builder for DisciplineRecord entity
///
/// Provides a fluent API for constructing DisciplineRecord instances.
/// System fields (id, metadata, timestamps) are auto-initialized.
#[derive(Debug, Clone, Default)]
pub struct DisciplineRecordBuilder {
    employee_id: Option<Uuid>,
    level: Option<DisciplineLevel>,
    offense: Option<String>,
    description: Option<String>,
    issued_at: Option<DateTime<Utc>>,
    valid_until: Option<DateTime<Utc>>,
    served_disposition: Option<DisciplineServedDisposition>,
    status: Option<DisciplineStatus>,
    acknowledged_at: Option<DateTime<Utc>>,
    acknowledged_via: Option<String>,
    document_file_id: Option<Uuid>,
    cancel_reason: Option<String>,
    prior_active_sp1: Option<i32>,
    prior_active_sp2: Option<i32>,
    issued_by: Option<Uuid>,
}

impl DisciplineRecordBuilder {
    /// Set the employee_id field (required)
    pub fn employee_id(mut self, value: Uuid) -> Self {
        self.employee_id = Some(value);
        self
    }

    /// Set the level field (required)
    pub fn level(mut self, value: DisciplineLevel) -> Self {
        self.level = Some(value);
        self
    }

    /// Set the offense field (required)
    pub fn offense(mut self, value: String) -> Self {
        self.offense = Some(value);
        self
    }

    /// Set the description field (required)
    pub fn description(mut self, value: String) -> Self {
        self.description = Some(value);
        self
    }

    /// Set the issued_at field (required)
    pub fn issued_at(mut self, value: DateTime<Utc>) -> Self {
        self.issued_at = Some(value);
        self
    }

    /// Set the valid_until field (required)
    pub fn valid_until(mut self, value: DateTime<Utc>) -> Self {
        self.valid_until = Some(value);
        self
    }

    /// Set the served_disposition field (required)
    pub fn served_disposition(mut self, value: DisciplineServedDisposition) -> Self {
        self.served_disposition = Some(value);
        self
    }

    /// Set the status field (default: `DisciplineStatus::default()`)
    pub fn status(mut self, value: DisciplineStatus) -> Self {
        self.status = Some(value);
        self
    }

    /// Set the acknowledged_at field (optional)
    pub fn acknowledged_at(mut self, value: DateTime<Utc>) -> Self {
        self.acknowledged_at = Some(value);
        self
    }

    /// Set the acknowledged_via field (optional)
    pub fn acknowledged_via(mut self, value: String) -> Self {
        self.acknowledged_via = Some(value);
        self
    }

    /// Set the document_file_id field (optional)
    pub fn document_file_id(mut self, value: Uuid) -> Self {
        self.document_file_id = Some(value);
        self
    }

    /// Set the cancel_reason field (optional)
    pub fn cancel_reason(mut self, value: String) -> Self {
        self.cancel_reason = Some(value);
        self
    }

    /// Set the prior_active_sp1 field (default: `0`)
    pub fn prior_active_sp1(mut self, value: i32) -> Self {
        self.prior_active_sp1 = Some(value);
        self
    }

    /// Set the prior_active_sp2 field (default: `0`)
    pub fn prior_active_sp2(mut self, value: i32) -> Self {
        self.prior_active_sp2 = Some(value);
        self
    }

    /// Set the issued_by field (optional)
    pub fn issued_by(mut self, value: Uuid) -> Self {
        self.issued_by = Some(value);
        self
    }

    /// Build the DisciplineRecord entity
    ///
    /// Returns Err if any required field without a default is missing.
    pub fn build(self) -> Result<DisciplineRecord, String> {
        let employee_id = self.employee_id.ok_or_else(|| "employee_id is required".to_string())?;
        let level = self.level.ok_or_else(|| "level is required".to_string())?;
        let offense = self.offense.ok_or_else(|| "offense is required".to_string())?;
        let description = self.description.ok_or_else(|| "description is required".to_string())?;
        let issued_at = self.issued_at.ok_or_else(|| "issued_at is required".to_string())?;
        let valid_until = self.valid_until.ok_or_else(|| "valid_until is required".to_string())?;
        let served_disposition = self.served_disposition.ok_or_else(|| "served_disposition is required".to_string())?;

        Ok(DisciplineRecord {
            id: Uuid::new_v4(),
            employee_id,
            level,
            offense,
            description,
            issued_at,
            valid_until,
            served_disposition,
            status: self.status.unwrap_or_default(),
            acknowledged_at: self.acknowledged_at,
            acknowledged_via: self.acknowledged_via,
            document_file_id: self.document_file_id,
            cancel_reason: self.cancel_reason,
            prior_active_sp1: self.prior_active_sp1.unwrap_or(0),
            prior_active_sp2: self.prior_active_sp2.unwrap_or(0),
            issued_by: self.issued_by,
            metadata: AuditMetadata::default(),
        })
    }
}

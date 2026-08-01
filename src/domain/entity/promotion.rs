use chrono::{DateTime, Utc, NaiveDate};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;
use rust_decimal::Decimal;

use super::PromotionType;
use super::PromotionStatus;
use super::AuditMetadata;

/// Strongly-typed ID for Promotion
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PromotionId(pub Uuid);

impl PromotionId {
    pub fn new(id: Uuid) -> Self { Self(id) }
    pub fn generate() -> Self { Self(Uuid::new_v4()) }
    pub fn into_inner(self) -> Uuid { self.0 }
}

impl std::fmt::Display for PromotionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for PromotionId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

impl From<Uuid> for PromotionId {
    fn from(id: Uuid) -> Self { Self(id) }
}

impl From<PromotionId> for Uuid {
    fn from(id: PromotionId) -> Self { id.0 }
}

impl AsRef<Uuid> for PromotionId {
    fn as_ref(&self) -> &Uuid { &self.0 }
}

impl std::ops::Deref for PromotionId {
    type Target = Uuid;
    fn deref(&self) -> &Self::Target { &self.0 }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Promotion {
    pub id: Uuid,
    pub company_id: Uuid,
    pub employee_id: Uuid,
    pub promotion_type: PromotionType,
    pub position_id_from: Option<Uuid>,
    pub position_id_to: Option<Uuid>,
    pub level_id_from: Option<Uuid>,
    pub level_id_to: Option<Uuid>,
    pub department_id_from: Option<Uuid>,
    pub department_id_to: Option<Uuid>,
    pub proposed_salary: Option<Decimal>,
    pub effective_date: NaiveDate,
    pub status: PromotionStatus,
    pub requested_by: Option<Uuid>,
    pub approved_by: Option<Uuid>,
    pub appraisal_id: Option<Uuid>,
    pub reason: Option<String>,
    #[serde(default)]
    #[sqlx(json)]
    pub metadata: AuditMetadata,
}

impl Promotion {
    /// Create a builder for Promotion
    pub fn builder() -> PromotionBuilder {
        PromotionBuilder::default()
    }

    /// Create a new Promotion with required fields
    pub fn new(company_id: Uuid, employee_id: Uuid, promotion_type: PromotionType, effective_date: NaiveDate, status: PromotionStatus) -> Self {
        Self {
            id: Uuid::new_v4(),
            company_id,
            employee_id,
            promotion_type,
            position_id_from: None,
            position_id_to: None,
            level_id_from: None,
            level_id_to: None,
            department_id_from: None,
            department_id_to: None,
            proposed_salary: None,
            effective_date,
            status,
            requested_by: None,
            approved_by: None,
            appraisal_id: None,
            reason: None,
            metadata: AuditMetadata::default(),
        }
    }

    /// Get the entity's unique identifier
    pub fn id(&self) -> &Uuid {
        &self.id
    }

    /// Get a strongly-typed ID for this entity
    pub fn typed_id(&self) -> PromotionId {
        PromotionId(self.id)
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
    pub fn status(&self) -> &PromotionStatus {
        &self.status
    }


    // ==========================================================
    // Fluent Setters (with_* for optional fields)
    // ==========================================================

    /// Set the position_id_from field (chainable)
    pub fn with_position_id_from(mut self, value: Uuid) -> Self {
        self.position_id_from = Some(value);
        self
    }

    /// Set the position_id_to field (chainable)
    pub fn with_position_id_to(mut self, value: Uuid) -> Self {
        self.position_id_to = Some(value);
        self
    }

    /// Set the level_id_from field (chainable)
    pub fn with_level_id_from(mut self, value: Uuid) -> Self {
        self.level_id_from = Some(value);
        self
    }

    /// Set the level_id_to field (chainable)
    pub fn with_level_id_to(mut self, value: Uuid) -> Self {
        self.level_id_to = Some(value);
        self
    }

    /// Set the department_id_from field (chainable)
    pub fn with_department_id_from(mut self, value: Uuid) -> Self {
        self.department_id_from = Some(value);
        self
    }

    /// Set the department_id_to field (chainable)
    pub fn with_department_id_to(mut self, value: Uuid) -> Self {
        self.department_id_to = Some(value);
        self
    }

    /// Set the proposed_salary field (chainable)
    pub fn with_proposed_salary(mut self, value: Decimal) -> Self {
        self.proposed_salary = Some(value);
        self
    }

    /// Set the requested_by field (chainable)
    pub fn with_requested_by(mut self, value: Uuid) -> Self {
        self.requested_by = Some(value);
        self
    }

    /// Set the approved_by field (chainable)
    pub fn with_approved_by(mut self, value: Uuid) -> Self {
        self.approved_by = Some(value);
        self
    }

    /// Set the appraisal_id field (chainable)
    pub fn with_appraisal_id(mut self, value: Uuid) -> Self {
        self.appraisal_id = Some(value);
        self
    }

    /// Set the reason field (chainable)
    pub fn with_reason(mut self, value: String) -> Self {
        self.reason = Some(value);
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
                "promotion_type" => {
                    if let Ok(v) = serde_json::from_value(value) { self.promotion_type = v; }
                }
                "position_id_from" => {
                    if let Ok(v) = serde_json::from_value(value) { self.position_id_from = v; }
                }
                "position_id_to" => {
                    if let Ok(v) = serde_json::from_value(value) { self.position_id_to = v; }
                }
                "level_id_from" => {
                    if let Ok(v) = serde_json::from_value(value) { self.level_id_from = v; }
                }
                "level_id_to" => {
                    if let Ok(v) = serde_json::from_value(value) { self.level_id_to = v; }
                }
                "department_id_from" => {
                    if let Ok(v) = serde_json::from_value(value) { self.department_id_from = v; }
                }
                "department_id_to" => {
                    if let Ok(v) = serde_json::from_value(value) { self.department_id_to = v; }
                }
                "proposed_salary" => {
                    if let Ok(v) = serde_json::from_value(value) { self.proposed_salary = v; }
                }
                "effective_date" => {
                    if let Ok(v) = serde_json::from_value(value) { self.effective_date = v; }
                }
                "status" => {
                    if let Ok(v) = serde_json::from_value(value) { self.status = v; }
                }
                "requested_by" => {
                    if let Ok(v) = serde_json::from_value(value) { self.requested_by = v; }
                }
                "approved_by" => {
                    if let Ok(v) = serde_json::from_value(value) { self.approved_by = v; }
                }
                "appraisal_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.appraisal_id = v; }
                }
                "reason" => {
                    if let Ok(v) = serde_json::from_value(value) { self.reason = v; }
                }
                _ => {} // ignore unknown fields
            }
        }
    }

    // <<< CUSTOM METHODS START >>>
    // <<< CUSTOM METHODS END >>>
}

impl super::Entity for Promotion {
    type Id = Uuid;

    fn entity_id(&self) -> &Self::Id {
        &self.id
    }

    fn entity_type() -> &'static str {
        "Promotion"
    }
}

impl backbone_core::PersistentEntity for Promotion {
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

impl backbone_orm::EntityRepoMeta for Promotion {
    fn column_types() -> std::collections::HashMap<String, String> {
        let mut m = std::collections::HashMap::new();
        m.insert("id".to_string(), "uuid".to_string());
        m.insert("company_id".to_string(), "uuid".to_string());
        m.insert("employee_id".to_string(), "uuid".to_string());
        m.insert("appraisal_id".to_string(), "uuid".to_string());
        m.insert("promotion_type".to_string(), "promotion_type".to_string());
        m.insert("status".to_string(), "promotion_status".to_string());
        m
    }
    fn search_fields() -> &'static [&'static str] {
        &[]
    }
    fn company_field() -> Option<&'static str> {
        Some("company_id")
    }
}

/// Builder for Promotion entity
///
/// Provides a fluent API for constructing Promotion instances.
/// System fields (id, metadata, timestamps) are auto-initialized.
#[derive(Debug, Clone, Default)]
pub struct PromotionBuilder {
    company_id: Option<Uuid>,
    employee_id: Option<Uuid>,
    promotion_type: Option<PromotionType>,
    position_id_from: Option<Uuid>,
    position_id_to: Option<Uuid>,
    level_id_from: Option<Uuid>,
    level_id_to: Option<Uuid>,
    department_id_from: Option<Uuid>,
    department_id_to: Option<Uuid>,
    proposed_salary: Option<Decimal>,
    effective_date: Option<NaiveDate>,
    status: Option<PromotionStatus>,
    requested_by: Option<Uuid>,
    approved_by: Option<Uuid>,
    appraisal_id: Option<Uuid>,
    reason: Option<String>,
}

impl PromotionBuilder {
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

    /// Set the promotion_type field (default: `PromotionType::default()`)
    pub fn promotion_type(mut self, value: PromotionType) -> Self {
        self.promotion_type = Some(value);
        self
    }

    /// Set the position_id_from field (optional)
    pub fn position_id_from(mut self, value: Uuid) -> Self {
        self.position_id_from = Some(value);
        self
    }

    /// Set the position_id_to field (optional)
    pub fn position_id_to(mut self, value: Uuid) -> Self {
        self.position_id_to = Some(value);
        self
    }

    /// Set the level_id_from field (optional)
    pub fn level_id_from(mut self, value: Uuid) -> Self {
        self.level_id_from = Some(value);
        self
    }

    /// Set the level_id_to field (optional)
    pub fn level_id_to(mut self, value: Uuid) -> Self {
        self.level_id_to = Some(value);
        self
    }

    /// Set the department_id_from field (optional)
    pub fn department_id_from(mut self, value: Uuid) -> Self {
        self.department_id_from = Some(value);
        self
    }

    /// Set the department_id_to field (optional)
    pub fn department_id_to(mut self, value: Uuid) -> Self {
        self.department_id_to = Some(value);
        self
    }

    /// Set the proposed_salary field (optional)
    pub fn proposed_salary(mut self, value: Decimal) -> Self {
        self.proposed_salary = Some(value);
        self
    }

    /// Set the effective_date field (required)
    pub fn effective_date(mut self, value: NaiveDate) -> Self {
        self.effective_date = Some(value);
        self
    }

    /// Set the status field (default: `PromotionStatus::default()`)
    pub fn status(mut self, value: PromotionStatus) -> Self {
        self.status = Some(value);
        self
    }

    /// Set the requested_by field (optional)
    pub fn requested_by(mut self, value: Uuid) -> Self {
        self.requested_by = Some(value);
        self
    }

    /// Set the approved_by field (optional)
    pub fn approved_by(mut self, value: Uuid) -> Self {
        self.approved_by = Some(value);
        self
    }

    /// Set the appraisal_id field (optional)
    pub fn appraisal_id(mut self, value: Uuid) -> Self {
        self.appraisal_id = Some(value);
        self
    }

    /// Set the reason field (optional)
    pub fn reason(mut self, value: String) -> Self {
        self.reason = Some(value);
        self
    }

    /// Build the Promotion entity
    ///
    /// Returns Err if any required field without a default is missing.
    pub fn build(self) -> Result<Promotion, String> {
        let company_id = self.company_id.ok_or_else(|| "company_id is required".to_string())?;
        let employee_id = self.employee_id.ok_or_else(|| "employee_id is required".to_string())?;
        let effective_date = self.effective_date.ok_or_else(|| "effective_date is required".to_string())?;

        Ok(Promotion {
            id: Uuid::new_v4(),
            company_id,
            employee_id,
            promotion_type: self.promotion_type.unwrap_or(PromotionType::default()),
            position_id_from: self.position_id_from,
            position_id_to: self.position_id_to,
            level_id_from: self.level_id_from,
            level_id_to: self.level_id_to,
            department_id_from: self.department_id_from,
            department_id_to: self.department_id_to,
            proposed_salary: self.proposed_salary,
            effective_date,
            status: self.status.unwrap_or(PromotionStatus::default()),
            requested_by: self.requested_by,
            approved_by: self.approved_by,
            appraisal_id: self.appraisal_id,
            reason: self.reason,
            metadata: AuditMetadata::default(),
        })
    }
}

use chrono::{DateTime, Utc, NaiveDate};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

use super::ContractType;
use super::ContractStatus;
use super::AuditMetadata;

/// Strongly-typed ID for Contract
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ContractId(pub Uuid);

impl ContractId {
    pub fn new(id: Uuid) -> Self { Self(id) }
    pub fn generate() -> Self { Self(Uuid::new_v4()) }
    pub fn into_inner(self) -> Uuid { self.0 }
}

impl std::fmt::Display for ContractId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for ContractId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

impl From<Uuid> for ContractId {
    fn from(id: Uuid) -> Self { Self(id) }
}

impl From<ContractId> for Uuid {
    fn from(id: ContractId) -> Self { id.0 }
}

impl AsRef<Uuid> for ContractId {
    fn as_ref(&self) -> &Uuid { &self.0 }
}

impl std::ops::Deref for ContractId {
    type Target = Uuid;
    fn deref(&self) -> &Self::Target { &self.0 }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Contract {
    pub id: Uuid,
    pub employment_id: Uuid,
    pub employee_id: Uuid,
    pub contract_type: ContractType,
    pub contract_no: Option<String>,
    pub start_date: NaiveDate,
    pub end_date: Option<NaiveDate>,
    pub status: ContractStatus,
    pub document_file_id: Option<Uuid>,
    pub template_id: Option<Uuid>,
    pub previous_contract_id: Option<Uuid>,
    pub cumulative_pkwt_months: i32,
    pub reminder_sent_at: Option<DateTime<Utc>>,
    pub created_by: Option<Uuid>,
    #[serde(default)]
    #[sqlx(json)]
    pub metadata: AuditMetadata,
}

impl Contract {
    /// Create a builder for Contract
    pub fn builder() -> ContractBuilder {
        <ContractBuilder as Default>::default()
    }

    /// Create a new Contract with required fields
    pub fn new(employment_id: Uuid, employee_id: Uuid, contract_type: ContractType, start_date: NaiveDate, status: ContractStatus, cumulative_pkwt_months: i32) -> Self {
        Self {
            id: Uuid::new_v4(),
            employment_id,
            employee_id,
            contract_type,
            contract_no: None,
            start_date,
            end_date: None,
            status,
            document_file_id: None,
            template_id: None,
            previous_contract_id: None,
            cumulative_pkwt_months,
            reminder_sent_at: None,
            created_by: None,
            metadata: AuditMetadata::default(),
        }
    }

    /// Get the entity's unique identifier
    pub fn id(&self) -> &Uuid {
        &self.id
    }

    /// Get a strongly-typed ID for this entity
    pub fn typed_id(&self) -> ContractId {
        ContractId(self.id)
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
    pub fn status(&self) -> &ContractStatus {
        &self.status
    }


    // ==========================================================
    // Fluent Setters (with_* for optional fields)
    // ==========================================================

    /// Set the contract_no field (chainable)
    pub fn with_contract_no(mut self, value: String) -> Self {
        self.contract_no = Some(value);
        self
    }

    /// Set the end_date field (chainable)
    pub fn with_end_date(mut self, value: NaiveDate) -> Self {
        self.end_date = Some(value);
        self
    }

    /// Set the document_file_id field (chainable)
    pub fn with_document_file_id(mut self, value: Uuid) -> Self {
        self.document_file_id = Some(value);
        self
    }

    /// Set the template_id field (chainable)
    pub fn with_template_id(mut self, value: Uuid) -> Self {
        self.template_id = Some(value);
        self
    }

    /// Set the previous_contract_id field (chainable)
    pub fn with_previous_contract_id(mut self, value: Uuid) -> Self {
        self.previous_contract_id = Some(value);
        self
    }

    /// Set the reminder_sent_at field (chainable)
    pub fn with_reminder_sent_at(mut self, value: DateTime<Utc>) -> Self {
        self.reminder_sent_at = Some(value);
        self
    }

    // ==========================================================
    // Partial Update
    // ==========================================================

    /// Apply partial updates from a map of field name to JSON value
    pub fn apply_patch(&mut self, fields: std::collections::HashMap<String, serde_json::Value>) {
        for (key, value) in fields {
            match key.as_str() {
                "employment_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.employment_id = v; }
                }
                "employee_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.employee_id = v; }
                }
                "contract_type" => {
                    if let Ok(v) = serde_json::from_value(value) { self.contract_type = v; }
                }
                "contract_no" => {
                    if let Ok(v) = serde_json::from_value(value) { self.contract_no = v; }
                }
                "start_date" => {
                    if let Ok(v) = serde_json::from_value(value) { self.start_date = v; }
                }
                "end_date" => {
                    if let Ok(v) = serde_json::from_value(value) { self.end_date = v; }
                }
                "status" => {
                    if let Ok(v) = serde_json::from_value(value) { self.status = v; }
                }
                "document_file_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.document_file_id = v; }
                }
                "template_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.template_id = v; }
                }
                "previous_contract_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.previous_contract_id = v; }
                }
                "cumulative_pkwt_months" => {
                    if let Ok(v) = serde_json::from_value(value) { self.cumulative_pkwt_months = v; }
                }
                "reminder_sent_at" => {
                    if let Ok(v) = serde_json::from_value(value) { self.reminder_sent_at = v; }
                }
                _ => {} // ignore unknown fields
            }
        }
    }

    // <<< CUSTOM METHODS START >>>
    // <<< CUSTOM METHODS END >>>
}

impl super::Entity for Contract {
    type Id = Uuid;

    fn entity_id(&self) -> &Self::Id {
        &self.id
    }

    fn entity_type() -> &'static str {
        "Contract"
    }
}

impl backbone_core::PersistentEntity for Contract {
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

impl backbone_orm::EntityRepoMeta for Contract {
    fn column_types() -> std::collections::HashMap<String, String> {
        let mut m = std::collections::HashMap::new();
        m.insert("id".to_string(), "uuid".to_string());
        m.insert("employment_id".to_string(), "uuid".to_string());
        m.insert("employee_id".to_string(), "uuid".to_string());
        m.insert("document_file_id".to_string(), "uuid".to_string());
        m.insert("template_id".to_string(), "uuid".to_string());
        m.insert("previous_contract_id".to_string(), "uuid".to_string());
        m.insert("contract_type".to_string(), "contract_type".to_string());
        m.insert("status".to_string(), "contract_status".to_string());
        m
    }
    fn search_fields() -> &'static [&'static str] {
        &[]
    }
}

/// Builder for Contract entity
///
/// Provides a fluent API for constructing Contract instances.
/// System fields (id, metadata, timestamps) are auto-initialized.
#[derive(Debug, Clone, Default)]
pub struct ContractBuilder {
    employment_id: Option<Uuid>,
    employee_id: Option<Uuid>,
    contract_type: Option<ContractType>,
    contract_no: Option<String>,
    start_date: Option<NaiveDate>,
    end_date: Option<NaiveDate>,
    status: Option<ContractStatus>,
    document_file_id: Option<Uuid>,
    template_id: Option<Uuid>,
    previous_contract_id: Option<Uuid>,
    cumulative_pkwt_months: Option<i32>,
    reminder_sent_at: Option<DateTime<Utc>>,
}

impl ContractBuilder {
    /// Set the employment_id field (required)
    pub fn employment_id(mut self, value: Uuid) -> Self {
        self.employment_id = Some(value);
        self
    }

    /// Set the employee_id field (required)
    pub fn employee_id(mut self, value: Uuid) -> Self {
        self.employee_id = Some(value);
        self
    }

    /// Set the contract_type field (required)
    pub fn contract_type(mut self, value: ContractType) -> Self {
        self.contract_type = Some(value);
        self
    }

    /// Set the contract_no field (optional)
    pub fn contract_no(mut self, value: String) -> Self {
        self.contract_no = Some(value);
        self
    }

    /// Set the start_date field (required)
    pub fn start_date(mut self, value: NaiveDate) -> Self {
        self.start_date = Some(value);
        self
    }

    /// Set the end_date field (optional)
    pub fn end_date(mut self, value: NaiveDate) -> Self {
        self.end_date = Some(value);
        self
    }

    /// Set the status field (default: `ContractStatus::default()`)
    pub fn status(mut self, value: ContractStatus) -> Self {
        self.status = Some(value);
        self
    }

    /// Set the document_file_id field (optional)
    pub fn document_file_id(mut self, value: Uuid) -> Self {
        self.document_file_id = Some(value);
        self
    }

    /// Set the template_id field (optional)
    pub fn template_id(mut self, value: Uuid) -> Self {
        self.template_id = Some(value);
        self
    }

    /// Set the previous_contract_id field (optional)
    pub fn previous_contract_id(mut self, value: Uuid) -> Self {
        self.previous_contract_id = Some(value);
        self
    }

    /// Set the cumulative_pkwt_months field (default: `0`)
    pub fn cumulative_pkwt_months(mut self, value: i32) -> Self {
        self.cumulative_pkwt_months = Some(value);
        self
    }

    /// Set the reminder_sent_at field (optional)
    pub fn reminder_sent_at(mut self, value: DateTime<Utc>) -> Self {
        self.reminder_sent_at = Some(value);
        self
    }

    /// Build the Contract entity
    ///
    /// Returns Err if any required field without a default is missing.
    pub fn build(self) -> Result<Contract, String> {
        let employment_id = self.employment_id.ok_or_else(|| "employment_id is required".to_string())?;
        let employee_id = self.employee_id.ok_or_else(|| "employee_id is required".to_string())?;
        let contract_type = self.contract_type.ok_or_else(|| "contract_type is required".to_string())?;
        let start_date = self.start_date.ok_or_else(|| "start_date is required".to_string())?;

        Ok(Contract {
            id: Uuid::new_v4(),
            employment_id,
            employee_id,
            contract_type,
            contract_no: self.contract_no,
            start_date,
            end_date: self.end_date,
            status: self.status.unwrap_or_default(),
            document_file_id: self.document_file_id,
            template_id: self.template_id,
            previous_contract_id: self.previous_contract_id,
            cumulative_pkwt_months: self.cumulative_pkwt_months.unwrap_or(0),
            reminder_sent_at: self.reminder_sent_at,
            created_by: None,
            metadata: AuditMetadata::default(),
        })
    }
}

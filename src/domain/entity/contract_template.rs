use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

use super::ContractType;
use super::AuditMetadata;

/// Strongly-typed ID for ContractTemplate
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ContractTemplateId(pub Uuid);

impl ContractTemplateId {
    pub fn new(id: Uuid) -> Self { Self(id) }
    pub fn generate() -> Self { Self(Uuid::new_v4()) }
    pub fn into_inner(self) -> Uuid { self.0 }
}

impl std::fmt::Display for ContractTemplateId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for ContractTemplateId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

impl From<Uuid> for ContractTemplateId {
    fn from(id: Uuid) -> Self { Self(id) }
}

impl From<ContractTemplateId> for Uuid {
    fn from(id: ContractTemplateId) -> Self { id.0 }
}

impl AsRef<Uuid> for ContractTemplateId {
    fn as_ref(&self) -> &Uuid { &self.0 }
}

impl std::ops::Deref for ContractTemplateId {
    type Target = Uuid;
    fn deref(&self) -> &Self::Target { &self.0 }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct ContractTemplate {
    pub id: Uuid,
    pub name: String,
    pub contract_type: ContractType,
    pub body_template: String,
    #[serde(default)]
    #[sqlx(json)]
    pub metadata: AuditMetadata,
}

impl ContractTemplate {
    /// Create a builder for ContractTemplate
    pub fn builder() -> ContractTemplateBuilder {
        <ContractTemplateBuilder as Default>::default()
    }

    /// Create a new ContractTemplate with required fields
    pub fn new(name: String, contract_type: ContractType, body_template: String) -> Self {
        Self {
            id: Uuid::new_v4(),
            name,
            contract_type,
            body_template,
            metadata: AuditMetadata::default(),
        }
    }

    /// Get the entity's unique identifier
    pub fn id(&self) -> &Uuid {
        &self.id
    }

    /// Get a strongly-typed ID for this entity
    pub fn typed_id(&self) -> ContractTemplateId {
        ContractTemplateId(self.id)
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


    // ==========================================================
    // Partial Update
    // ==========================================================

    /// Apply partial updates from a map of field name to JSON value
    pub fn apply_patch(&mut self, fields: std::collections::HashMap<String, serde_json::Value>) {
        for (key, value) in fields {
            match key.as_str() {
                "name" => {
                    if let Ok(v) = serde_json::from_value(value) { self.name = v; }
                }
                "contract_type" => {
                    if let Ok(v) = serde_json::from_value(value) { self.contract_type = v; }
                }
                "body_template" => {
                    if let Ok(v) = serde_json::from_value(value) { self.body_template = v; }
                }
                _ => {} // ignore unknown fields
            }
        }
    }

    // <<< CUSTOM METHODS START >>>
    // <<< CUSTOM METHODS END >>>
}

impl super::Entity for ContractTemplate {
    type Id = Uuid;

    fn entity_id(&self) -> &Self::Id {
        &self.id
    }

    fn entity_type() -> &'static str {
        "ContractTemplate"
    }
}

impl backbone_core::PersistentEntity for ContractTemplate {
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

impl backbone_orm::EntityRepoMeta for ContractTemplate {
    fn column_types() -> std::collections::HashMap<String, String> {
        let mut m = std::collections::HashMap::new();
        m.insert("id".to_string(), "uuid".to_string());
        m.insert("contract_type".to_string(), "contract_type".to_string());
        m
    }
    fn search_fields() -> &'static [&'static str] {
        &["name", "body_template"]
    }
}

/// Builder for ContractTemplate entity
///
/// Provides a fluent API for constructing ContractTemplate instances.
/// System fields (id, metadata, timestamps) are auto-initialized.
#[derive(Debug, Clone, Default)]
pub struct ContractTemplateBuilder {
    name: Option<String>,
    contract_type: Option<ContractType>,
    body_template: Option<String>,
}

impl ContractTemplateBuilder {
    /// Set the name field (required)
    pub fn name(mut self, value: String) -> Self {
        self.name = Some(value);
        self
    }

    /// Set the contract_type field (required)
    pub fn contract_type(mut self, value: ContractType) -> Self {
        self.contract_type = Some(value);
        self
    }

    /// Set the body_template field (required)
    pub fn body_template(mut self, value: String) -> Self {
        self.body_template = Some(value);
        self
    }

    /// Build the ContractTemplate entity
    ///
    /// Returns Err if any required field without a default is missing.
    pub fn build(self) -> Result<ContractTemplate, String> {
        let name = self.name.ok_or_else(|| "name is required".to_string())?;
        let contract_type = self.contract_type.ok_or_else(|| "contract_type is required".to_string())?;
        let body_template = self.body_template.ok_or_else(|| "body_template is required".to_string())?;

        Ok(ContractTemplate {
            id: Uuid::new_v4(),
            name,
            contract_type,
            body_template,
            metadata: AuditMetadata::default(),
        })
    }
}

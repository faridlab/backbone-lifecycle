use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;
use super::AuditMetadata;

/// Strongly-typed ID for OnboardingTemplateTask
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OnboardingTemplateTaskId(pub Uuid);

impl OnboardingTemplateTaskId {
    pub fn new(id: Uuid) -> Self { Self(id) }
    pub fn generate() -> Self { Self(Uuid::new_v4()) }
    pub fn into_inner(self) -> Uuid { self.0 }
}

impl std::fmt::Display for OnboardingTemplateTaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for OnboardingTemplateTaskId {
    type Err = uuid::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(s)?))
    }
}

impl From<Uuid> for OnboardingTemplateTaskId {
    fn from(id: Uuid) -> Self { Self(id) }
}

impl From<OnboardingTemplateTaskId> for Uuid {
    fn from(id: OnboardingTemplateTaskId) -> Self { id.0 }
}

impl AsRef<Uuid> for OnboardingTemplateTaskId {
    fn as_ref(&self) -> &Uuid { &self.0 }
}

impl std::ops::Deref for OnboardingTemplateTaskId {
    type Target = Uuid;
    fn deref(&self) -> &Self::Target { &self.0 }
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct OnboardingTemplateTask {
    pub id: Uuid,
    pub template_id: Uuid,
    pub title: String,
    pub category: Option<String>,
    pub owner_role: Option<String>,
    pub due_day_offset: i32,
    pub ordinal: i32,
    #[serde(default)]
    #[sqlx(json)]
    pub metadata: AuditMetadata,
}

impl OnboardingTemplateTask {
    /// Create a builder for OnboardingTemplateTask
    pub fn builder() -> OnboardingTemplateTaskBuilder {
        <OnboardingTemplateTaskBuilder as Default>::default()
    }

    /// Create a new OnboardingTemplateTask with required fields
    pub fn new(template_id: Uuid, title: String, due_day_offset: i32, ordinal: i32) -> Self {
        Self {
            id: Uuid::new_v4(),
            template_id,
            title,
            category: None,
            owner_role: None,
            due_day_offset,
            ordinal,
            metadata: AuditMetadata::default(),
        }
    }

    /// Get the entity's unique identifier
    pub fn id(&self) -> &Uuid {
        &self.id
    }

    /// Get a strongly-typed ID for this entity
    pub fn typed_id(&self) -> OnboardingTemplateTaskId {
        OnboardingTemplateTaskId(self.id)
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
    // Fluent Setters (with_* for optional fields)
    // ==========================================================

    /// Set the category field (chainable)
    pub fn with_category(mut self, value: String) -> Self {
        self.category = Some(value);
        self
    }

    /// Set the owner_role field (chainable)
    pub fn with_owner_role(mut self, value: String) -> Self {
        self.owner_role = Some(value);
        self
    }

    // ==========================================================
    // Partial Update
    // ==========================================================

    /// Apply partial updates from a map of field name to JSON value
    pub fn apply_patch(&mut self, fields: std::collections::HashMap<String, serde_json::Value>) {
        for (key, value) in fields {
            match key.as_str() {
                "template_id" => {
                    if let Ok(v) = serde_json::from_value(value) { self.template_id = v; }
                }
                "title" => {
                    if let Ok(v) = serde_json::from_value(value) { self.title = v; }
                }
                "category" => {
                    if let Ok(v) = serde_json::from_value(value) { self.category = v; }
                }
                "owner_role" => {
                    if let Ok(v) = serde_json::from_value(value) { self.owner_role = v; }
                }
                "due_day_offset" => {
                    if let Ok(v) = serde_json::from_value(value) { self.due_day_offset = v; }
                }
                "ordinal" => {
                    if let Ok(v) = serde_json::from_value(value) { self.ordinal = v; }
                }
                _ => {} // ignore unknown fields
            }
        }
    }

    // <<< CUSTOM METHODS START >>>
    // <<< CUSTOM METHODS END >>>
}

impl super::Entity for OnboardingTemplateTask {
    type Id = Uuid;

    fn entity_id(&self) -> &Self::Id {
        &self.id
    }

    fn entity_type() -> &'static str {
        "OnboardingTemplateTask"
    }
}

impl backbone_core::PersistentEntity for OnboardingTemplateTask {
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

impl backbone_orm::EntityRepoMeta for OnboardingTemplateTask {
    fn column_types() -> std::collections::HashMap<String, String> {
        let mut m = std::collections::HashMap::new();
        m.insert("id".to_string(), "uuid".to_string());
        m.insert("template_id".to_string(), "uuid".to_string());
        m
    }
    fn search_fields() -> &'static [&'static str] {
        &["title"]
    }
}

/// Builder for OnboardingTemplateTask entity
///
/// Provides a fluent API for constructing OnboardingTemplateTask instances.
/// System fields (id, metadata, timestamps) are auto-initialized.
#[derive(Debug, Clone, Default)]
pub struct OnboardingTemplateTaskBuilder {
    template_id: Option<Uuid>,
    title: Option<String>,
    category: Option<String>,
    owner_role: Option<String>,
    due_day_offset: Option<i32>,
    ordinal: Option<i32>,
}

impl OnboardingTemplateTaskBuilder {
    /// Set the template_id field (required)
    pub fn template_id(mut self, value: Uuid) -> Self {
        self.template_id = Some(value);
        self
    }

    /// Set the title field (required)
    pub fn title(mut self, value: String) -> Self {
        self.title = Some(value);
        self
    }

    /// Set the category field (optional)
    pub fn category(mut self, value: String) -> Self {
        self.category = Some(value);
        self
    }

    /// Set the owner_role field (optional)
    pub fn owner_role(mut self, value: String) -> Self {
        self.owner_role = Some(value);
        self
    }

    /// Set the due_day_offset field (default: `0`)
    pub fn due_day_offset(mut self, value: i32) -> Self {
        self.due_day_offset = Some(value);
        self
    }

    /// Set the ordinal field (default: `0`)
    pub fn ordinal(mut self, value: i32) -> Self {
        self.ordinal = Some(value);
        self
    }

    /// Build the OnboardingTemplateTask entity
    ///
    /// Returns Err if any required field without a default is missing.
    pub fn build(self) -> Result<OnboardingTemplateTask, String> {
        let template_id = self.template_id.ok_or_else(|| "template_id is required".to_string())?;
        let title = self.title.ok_or_else(|| "title is required".to_string())?;

        Ok(OnboardingTemplateTask {
            id: Uuid::new_v4(),
            template_id,
            title,
            category: self.category,
            owner_role: self.owner_role,
            due_day_offset: self.due_day_offset.unwrap_or(0),
            ordinal: self.ordinal.unwrap_or(0),
            metadata: AuditMetadata::default(),
        })
    }
}

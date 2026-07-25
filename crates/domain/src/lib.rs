#![forbid(unsafe_code)]

use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const MAX_NAMESPACE_NAME_BYTES: usize = 50;
pub const MAX_ENTITY_PATH_BYTES: usize = 260;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NamespaceName(String);

impl NamespaceName {
    pub fn new(value: impl Into<String>) -> Result<Self, IdentifierError> {
        let value = value.into();
        validate_identifier("namespace", &value, MAX_NAMESPACE_NAME_BYTES)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NamespaceName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EntityPath(String);

impl EntityPath {
    pub fn new(value: impl Into<String>) -> Result<Self, IdentifierError> {
        let value = value.into();
        validate_identifier("entity path", &value, MAX_ENTITY_PATH_BYTES)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EntityPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PlacementGroupId(String);

impl PlacementGroupId {
    pub fn new(value: impl Into<String>) -> Result<Self, IdentifierError> {
        let value = value.into();
        validate_identifier("placement group", &value, 128)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum IdentifierError {
    #[error("{kind} cannot be empty")]
    Empty { kind: &'static str },
    #[error("{kind} exceeds its {maximum}-byte limit")]
    TooLong { kind: &'static str, maximum: usize },
    #[error("{kind} contains a control character")]
    ControlCharacter { kind: &'static str },
}

fn validate_identifier(
    kind: &'static str,
    value: &str,
    maximum: usize,
) -> Result<(), IdentifierError> {
    if value.is_empty() {
        return Err(IdentifierError::Empty { kind });
    }
    if value.len() > maximum {
        return Err(IdentifierError::TooLong { kind, maximum });
    }
    if value.chars().any(char::is_control) {
        return Err(IdentifierError::ControlCharacter { kind });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_rejects_empty_names() {
        assert_eq!(
            NamespaceName::new(""),
            Err(IdentifierError::Empty { kind: "namespace" })
        );
    }

    #[test]
    fn entity_path_accepts_subscription_paths() {
        let path = EntityPath::new("orders/subscriptions/accounting");
        assert_eq!(
            path.as_ref().map(EntityPath::as_str),
            Ok("orders/subscriptions/accounting")
        );
    }
}

use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const MAX_NAMESPACE_NAME_BYTES: usize = 50;
pub const MAX_ENTITY_PATH_BYTES: usize = 260;
pub const MAX_PLACEMENT_GROUP_ID_BYTES: usize = 128;
/// The Service Bus session identifier limit.
pub const MAX_SESSION_ID_BYTES: usize = 128;

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

/// Names the ordered subset of a queue a session receiver owns.
///
/// Ordering within a session is the only FIFO guarantee the broker makes, and a
/// session identifier is part of the key of every message in it, so the same
/// control-character rule applies here as to the entity scope.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionId(String);

impl SessionId {
    pub fn new(value: impl Into<String>) -> Result<Self, IdentifierError> {
        let value = value.into();
        validate_identifier("session id", &value, MAX_SESSION_ID_BYTES)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SessionId {
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
        validate_identifier("placement group", &value, MAX_PLACEMENT_GROUP_ID_BYTES)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum IdentifierError {
    #[error("{kind} cannot be empty")]
    Empty { kind: &'static str },
    #[error("{kind} exceeds its {maximum}-byte limit")]
    TooLong { kind: &'static str, maximum: usize },
    #[error("{kind} contains a control character")]
    ControlCharacter { kind: &'static str },
}

/// Rejecting control characters is what lets the storage key encoding use a
/// zero byte to terminate the namespace and entity path segments. Without it,
/// a crafted name could forge the key of another entity.
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

    #[test]
    fn identifiers_reject_the_key_separator_byte() {
        assert_eq!(
            NamespaceName::new("tenant\0forged"),
            Err(IdentifierError::ControlCharacter { kind: "namespace" })
        );
        assert_eq!(
            EntityPath::new("orders\0forged"),
            Err(IdentifierError::ControlCharacter {
                kind: "entity path"
            })
        );
        assert_eq!(
            SessionId::new("cart-1\0forged"),
            Err(IdentifierError::ControlCharacter { kind: "session id" })
        );
    }

    #[test]
    fn session_ids_are_bounded_and_non_empty() {
        assert_eq!(
            SessionId::new(""),
            Err(IdentifierError::Empty { kind: "session id" })
        );
        assert_eq!(
            SessionId::new("s".repeat(MAX_SESSION_ID_BYTES + 1)),
            Err(IdentifierError::TooLong {
                kind: "session id",
                maximum: MAX_SESSION_ID_BYTES
            })
        );
        assert_eq!(
            SessionId::new("s".repeat(MAX_SESSION_ID_BYTES))
                .as_ref()
                .map(SessionId::as_str),
            Ok("s".repeat(MAX_SESSION_ID_BYTES).as_str())
        );
    }
}

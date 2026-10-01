use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const MAX_NAMESPACE_NAME_BYTES: usize = 50;
pub const MAX_ENTITY_PATH_BYTES: usize = 260;
pub const MAX_PLACEMENT_GROUP_ID_BYTES: usize = 128;
/// The Service Bus session identifier limit.
pub const MAX_SESSION_ID_BYTES: usize = 128;
/// Native conservative ASCII single-segment subscription name limit.
pub const MAX_SUBSCRIPTION_NAME_BYTES: usize = 50;
/// Suffix naming an entity's dead-letter queue, per the Service Bus path model.
pub const DEAD_LETTER_QUEUE_SUFFIX: &str = "/$deadletterqueue";
pub const SUBSCRIPTION_PATH_SEGMENT: &str = "/subscriptions/";

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

    /// The dead-letter queue that shadows this entity.
    ///
    /// Fails only when the suffixed path would exceed the entity length limit,
    /// which is why creating a queue validates this up front rather than
    /// discovering it at the first dead-lettering.
    pub fn dead_letter_queue(&self) -> Result<Self, IdentifierError> {
        Self::new(format!("{}{DEAD_LETTER_QUEUE_SUFFIX}", self.0))
    }

    /// Whether this path names a dead-letter queue. Such paths are reserved:
    /// they exist as shadows of their parent, never created or sent to
    /// directly.
    pub fn is_dead_letter_queue(&self) -> bool {
        self.0
            .to_ascii_lowercase()
            .ends_with(DEAD_LETTER_QUEUE_SUFFIX)
    }

    /// The canonical path of one subscription under this topic.
    pub fn subscription(&self, name: &SubscriptionName) -> Result<Self, IdentifierError> {
        Self::new(format!(
            "{}{SUBSCRIPTION_PATH_SEGMENT}{}",
            self.0,
            name.as_str()
        ))
    }

    /// Reserves the entire subscription branch, including malformed or nested tails.
    pub fn is_subscription_path(&self) -> bool {
        self.0
            .as_bytes()
            .windows(SUBSCRIPTION_PATH_SEGMENT.len())
            .any(|part| part.eq_ignore_ascii_case(SUBSCRIPTION_PATH_SEGMENT.as_bytes()))
    }
}

impl fmt::Display for EntityPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A single ASCII subscription segment with alphanumeric first and last bytes.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct SubscriptionName(String);

impl SubscriptionName {
    pub fn new(value: impl Into<String>) -> Result<Self, IdentifierError> {
        let value = value.into();
        Self::validate(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn validate(value: &str) -> Result<(), IdentifierError> {
        let kind = "subscription name";
        validate_identifier(kind, value, MAX_SUBSCRIPTION_NAME_BYTES)?;
        let bytes = value.as_bytes();
        if !bytes.first().is_some_and(u8::is_ascii_alphanumeric)
            || !bytes.last().is_some_and(u8::is_ascii_alphanumeric)
            || bytes
                .iter()
                .any(|&byte| !byte.is_ascii_alphanumeric() && !matches!(byte, b'.' | b'-' | b'_'))
        {
            return Err(IdentifierError::InvalidCharacter { kind });
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for SubscriptionName {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for SubscriptionName {
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
    #[error("{kind} contains a forbidden character")]
    InvalidCharacter { kind: &'static str },
    #[error("{kind} exceeds its {maximum}-UTF-16-unit limit")]
    TooLongUtf16 { kind: &'static str, maximum: usize },
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

    #[test]
    fn subscription_names_accept_only_bounded_single_ascii_segments() {
        let kind = "subscription name";
        assert_eq!(
            SubscriptionName::new(""),
            Err(IdentifierError::Empty { kind })
        );
        assert_eq!(
            SubscriptionName::new("a".repeat(MAX_SUBSCRIPTION_NAME_BYTES + 1)),
            Err(IdentifierError::TooLong {
                kind,
                maximum: MAX_SUBSCRIPTION_NAME_BYTES,
            })
        );
        let maximum_name = "a".repeat(MAX_SUBSCRIPTION_NAME_BYTES);
        for name in ["a", "0", "Orders.v2-West_1", maximum_name.as_str()] {
            let name = SubscriptionName::new(name).expect("valid subscription name");
            assert_eq!(name.to_string(), name.as_str());
        }
        for name in [".", "..", "-x", "x_", "a/b", "a b", "a:b", "a\u{e9}b"] {
            assert_eq!(
                SubscriptionName::new(name),
                Err(IdentifierError::InvalidCharacter { kind }),
                "{name:?}",
            );
        }
        assert_eq!(
            SubscriptionName::new("a\0b"),
            Err(IdentifierError::ControlCharacter { kind })
        );
    }

    #[test]
    fn subscription_names_validate_during_transparent_deserialization() {
        let name = SubscriptionName::new("accounting.v2").expect("valid subscription name");
        let envelope = crate::codec::encode(&name).expect("name encodes");
        assert_eq!(
            envelope,
            crate::codec::encode(&name.as_str()).expect("transparent string encodes")
        );
        assert_eq!(
            crate::codec::decode::<SubscriptionName>(&envelope),
            Ok(name)
        );
        for invalid in ["", "a/b", "..", "_accounting", "accounting-"] {
            let envelope = crate::codec::encode(&invalid).expect("string encodes");
            assert_eq!(
                crate::codec::decode::<SubscriptionName>(&envelope),
                Err(crate::CodecError::Decode),
                "{invalid:?}",
            );
        }
    }

    #[test]
    fn subscription_paths_compose_canonically_and_reserve_case_insensitive_branches() {
        let name = SubscriptionName::new("accounting").expect("valid subscription name");
        let topic = EntityPath::new("orders").expect("valid topic path");
        assert_eq!(
            topic
                .subscription(&name)
                .expect("valid subscription path")
                .as_str(),
            "orders/subscriptions/accounting"
        );
        for path in [
            "orders/subscriptions/accounting",
            "orders/SuBsCrIpTiOnS/",
            "orders/subscriptions/a/subscriptions/b",
            "orders/subscriptions//bad",
            "orders/subscriptions/..",
        ] {
            assert!(
                EntityPath::new(path)
                    .expect("ordinary path remains valid")
                    .is_subscription_path()
            );
        }
        for path in ["orders", "orders/subscription/a", "orders-subscriptions-a"] {
            assert!(
                !EntityPath::new(path)
                    .expect("valid path")
                    .is_subscription_path()
            );
        }
    }

    #[test]
    fn composed_subscription_path_obeys_the_existing_entity_byte_limit() {
        let name = SubscriptionName::new("a").expect("valid subscription name");
        let parent_bytes =
            MAX_ENTITY_PATH_BYTES - SUBSCRIPTION_PATH_SEGMENT.len() - name.as_str().len();
        let topic = EntityPath::new("t".repeat(parent_bytes)).expect("valid topic path");
        assert_eq!(
            topic
                .subscription(&name)
                .expect("boundary path fits")
                .as_str()
                .len(),
            MAX_ENTITY_PATH_BYTES
        );
        let too_long = EntityPath::new("t".repeat(parent_bytes + 1)).expect("parent still fits");
        assert_eq!(
            too_long.subscription(&name),
            Err(IdentifierError::TooLong {
                kind: "entity path",
                maximum: MAX_ENTITY_PATH_BYTES,
            })
        );
    }
}

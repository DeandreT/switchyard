use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const MAX_NAMESPACE_NAME_BYTES: usize = 50;
/// Maximum bytes in a caller-controlled queue or topic path.
pub const MAX_ENTITY_PATH_BYTES: usize = 260;
pub const MAX_PLACEMENT_GROUP_ID_BYTES: usize = 128;
/// The Service Bus session identifier limit.
pub const MAX_SESSION_ID_BYTES: usize = 128;
/// The Service Bus subscription-name limit.
pub const MAX_SUBSCRIPTION_NAME_CHARACTERS: usize = 50;
/// Suffix naming an entity's dead-letter queue, per the Service Bus path model.
pub const DEAD_LETTER_QUEUE_SUFFIX: &str = "/$deadletterqueue";

const SUBSCRIPTION_PATH_SEGMENT: &str = "/subscriptions/";
const SUBSCRIPTION_COLLECTION_SUFFIX: &str = "/subscriptions";
const MANAGEMENT_SUFFIX: &str = "/$management";
const MAX_INTERNAL_ENTITY_PATH_BYTES: usize = MAX_ENTITY_PATH_BYTES
    + SUBSCRIPTION_PATH_SEGMENT.len()
    + MAX_SUBSCRIPTION_NAME_CHARACTERS
    + DEAD_LETTER_QUEUE_SUFFIX.len();

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

    /// Rehydrates a broker-owned composite path whose individually validated
    /// topic and subscription components may exceed the top-level name bound
    /// once their well-known path segments are joined.
    pub(crate) fn from_internal(value: impl Into<String>) -> Result<Self, IdentifierError> {
        let value = value.into();
        validate_identifier("entity path", &value, MAX_INTERNAL_ENTITY_PATH_BYTES)?;
        Ok(Self(value))
    }

    /// The dead-letter queue that shadows this entity.
    ///
    /// Top-level names and subscription names are validated separately, so the
    /// broker-owned composite address has room for this well-known suffix.
    pub fn dead_letter_queue(&self) -> Result<Self, IdentifierError> {
        Self::from_internal(format!("{}{DEAD_LETTER_QUEUE_SUFFIX}", self.0))
    }

    /// Whether this path names a dead-letter queue. Such paths are reserved:
    /// they exist as shadows of their parent, never created or sent to
    /// directly.
    pub fn is_dead_letter_queue(&self) -> bool {
        self.0
            .to_ascii_lowercase()
            .ends_with(DEAD_LETTER_QUEUE_SUFFIX)
    }

    /// Whether this path occupies the reserved subscription-address shape.
    pub fn is_subscription(&self) -> bool {
        let lowercase = self.0.to_ascii_lowercase();
        lowercase.contains(SUBSCRIPTION_PATH_SEGMENT)
            || lowercase.ends_with(SUBSCRIPTION_COLLECTION_SUFFIX)
    }

    /// Whether this path collides with the entity-local AMQP management node.
    pub fn is_management(&self) -> bool {
        self.0.to_ascii_lowercase().ends_with(MANAGEMENT_SUFFIX)
    }

    /// The canonical storage and protocol path of a subscription below this
    /// topic. The well-known segment is folded while the caller-controlled
    /// topic and subscription names retain their case.
    pub fn subscription(&self, name: &SubscriptionName) -> Result<Self, IdentifierError> {
        Self::from_internal(format!(
            "{}{SUBSCRIPTION_PATH_SEGMENT}{}",
            self.0,
            name.as_str()
        ))
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

/// One durable subscription below a topic.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SubscriptionName(String);

impl SubscriptionName {
    pub fn new(value: impl Into<String>) -> Result<Self, IdentifierError> {
        let value = value.into();
        if value.is_empty() {
            return Err(IdentifierError::Empty {
                kind: "subscription name",
            });
        }
        if value.chars().count() > MAX_SUBSCRIPTION_NAME_CHARACTERS {
            return Err(IdentifierError::TooManyCharacters {
                kind: "subscription name",
                maximum: MAX_SUBSCRIPTION_NAME_CHARACTERS,
            });
        }
        let mut characters = value.chars();
        let first = characters
            .next()
            .expect("an empty subscription name was rejected above");
        let last = value
            .chars()
            .next_back()
            .expect("an empty subscription name was rejected above");
        if !first.is_ascii_alphanumeric()
            || !last.is_ascii_alphanumeric()
            || !value
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || ".-_".contains(character))
        {
            return Err(IdentifierError::RestrictedCharacter {
                kind: "subscription name",
            });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SubscriptionName {
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
    #[error("{kind} exceeds its {maximum}-character limit")]
    TooManyCharacters { kind: &'static str, maximum: usize },
    #[error("{kind} contains a control character")]
    ControlCharacter { kind: &'static str },
    #[error("{kind} contains a restricted character")]
    RestrictedCharacter { kind: &'static str },
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
    fn broker_owned_collection_and_management_shapes_are_recognized() {
        assert!(
            EntityPath::new("orders/Subscriptions")
                .expect("syntactically valid path")
                .is_subscription()
        );
        assert!(
            EntityPath::new("orders/$Management")
                .expect("syntactically valid path")
                .is_management()
        );
        assert!(
            !EntityPath::new("orders/subscriptions-archive")
                .expect("ordinary path")
                .is_subscription()
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
    fn subscription_names_follow_service_bus_limits() {
        let longest = "s".repeat(MAX_SUBSCRIPTION_NAME_CHARACTERS);
        assert_eq!(
            SubscriptionName::new(longest.clone())
                .as_ref()
                .map(SubscriptionName::as_str),
            Ok(longest.as_str())
        );
        assert_eq!(
            SubscriptionName::new(format!("{longest}s")),
            Err(IdentifierError::TooManyCharacters {
                kind: "subscription name",
                maximum: MAX_SUBSCRIPTION_NAME_CHARACTERS,
            })
        );
        for name in [
            "",
            "bad/name",
            "bad?name",
            "bad\\name",
            "bad\0name",
            "has space",
            "café",
            ".leading",
            "trailing-",
            "$management",
            "$deadletterqueue",
        ] {
            assert!(SubscriptionName::new(name).is_err(), "{name:?}");
        }
        for name in ["Accounting", "north-america", "priority.high", "team_2"] {
            assert!(SubscriptionName::new(name).is_ok(), "{name:?}");
        }
    }

    #[test]
    fn subscription_paths_use_one_canonical_well_known_segment() {
        let topic = EntityPath::new("billing").expect("valid topic");
        let name = SubscriptionName::new("Accounting").expect("valid subscription");
        let subscription = topic.subscription(&name).expect("valid path");
        assert_eq!(subscription.as_str(), "billing/subscriptions/Accounting");
        assert!(subscription.is_subscription());
    }

    #[test]
    fn broker_owned_composite_paths_apply_component_limits_separately() {
        let topic = EntityPath::new("t".repeat(MAX_ENTITY_PATH_BYTES)).expect("maximum topic");
        let subscription = topic
            .subscription(
                &SubscriptionName::new("s".repeat(MAX_SUBSCRIPTION_NAME_CHARACTERS))
                    .expect("maximum subscription"),
            )
            .expect("the maximum composite subscription path is valid");
        assert!(subscription.as_str().len() > MAX_ENTITY_PATH_BYTES);
        let dead_letters = subscription
            .dead_letter_queue()
            .expect("the maximum subscription DLQ path is valid");
        assert_eq!(dead_letters.as_str().len(), MAX_INTERNAL_ENTITY_PATH_BYTES);
        assert_eq!(
            EntityPath::new("x".repeat(MAX_ENTITY_PATH_BYTES + 1)),
            Err(IdentifierError::TooLong {
                kind: "entity path",
                maximum: MAX_ENTITY_PATH_BYTES,
            })
        );
    }
}

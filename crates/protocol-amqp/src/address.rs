//! Turning what a client attaches to into what the broker names.
//!
//! A Service Bus client carries the namespace in the hostname it opens the
//! connection with and the entity in the link's source or target address. The
//! broker names both explicitly, so the edge resolves one into the other before
//! any command is proposed.

use domain::{EntityPath, NamespaceName, SessionId, SubscriptionName};

use crate::ProtocolError;

/// Suffix that names an entity's dead-letter queue rather than the entity.
pub const DEAD_LETTER_SUFFIX: &str = "/$deadletterqueue";

/// Path segment separating a topic from one of its subscriptions.
pub const SUBSCRIPTION_SEGMENT: &str = "/subscriptions/";

/// What a link address resolved to.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Attachment {
    /// A plain entity address; committed metadata distinguishes queues and topics.
    Queue(EntityPath),
    DeadLetter(EntityPath),
    Subscription {
        topic: EntityPath,
        subscription: SubscriptionName,
    },
    SubscriptionDeadLetter {
        topic: EntityPath,
        subscription: SubscriptionName,
    },
}

impl Attachment {
    /// The canonical storage path, preserving topic and subscription name case.
    pub fn canonical_entity(&self) -> Result<EntityPath, ProtocolError> {
        match self {
            Self::Queue(entity) => Ok(entity.clone()),
            Self::DeadLetter(parent) => parent.dead_letter_queue(),
            Self::Subscription {
                topic,
                subscription,
            } => topic.subscription(subscription),
            Self::SubscriptionDeadLetter {
                topic,
                subscription,
            } => topic
                .subscription(subscription)
                .and_then(|entity| entity.dead_letter_queue()),
        }
        .map_err(|source| ProtocolError::InvalidAddress {
            address: self.address(),
            detail: source.to_string(),
        })
    }

    fn address(&self) -> String {
        match self {
            Self::Queue(entity) => entity.to_string(),
            Self::DeadLetter(parent) => format!("{parent}{DEAD_LETTER_SUFFIX}"),
            Self::Subscription {
                topic,
                subscription,
            } => {
                format!("{topic}{SUBSCRIPTION_SEGMENT}{subscription}")
            }
            Self::SubscriptionDeadLetter {
                topic,
                subscription,
            } => {
                format!("{topic}{SUBSCRIPTION_SEGMENT}{subscription}{DEAD_LETTER_SUFFIX}")
            }
        }
    }
}

/// Resolves the namespace a connection is for from the hostname it opened with.
///
/// `tenant.switchyard.example` and a bare `tenant` both name `tenant`, so a
/// deployment can put namespaces in DNS without the broker caring whether it
/// did.
pub fn namespace_from_hostname(hostname: &str) -> Result<NamespaceName, ProtocolError> {
    let label = hostname.split('.').next().unwrap_or_default();
    if label.is_empty() {
        return Err(ProtocolError::MissingNamespace);
    }
    NamespaceName::new(label).map_err(|source| ProtocolError::InvalidAddress {
        address: hostname.to_owned(),
        detail: source.to_string(),
    })
}

/// Resolves a link's source or target address to the entity it attaches to.
///
/// Matching is case-insensitive on the well-known suffixes, because the Service
/// Bus SDKs do not agree on their casing, but the entity path itself is passed
/// through as written: it is part of a storage key, and folding its case would
/// merge two entities a client considers distinct.
pub fn parse_attachment(address: &str) -> Result<Attachment, ProtocolError> {
    let trimmed = address.trim_start_matches('/');
    if trimmed.is_empty() {
        return Err(ProtocolError::InvalidAddress {
            address: address.to_owned(),
            detail: String::from("address names no entity"),
        });
    }
    let (base, dead_letter) = match strip_control_suffix(trimmed, DEAD_LETTER_SUFFIX) {
        Some(base) => (base, true),
        None => (trimmed, false),
    };
    if base.is_empty()
        || base.split('/').any(str::is_empty)
        || base.split('/').any(|part| {
            part.eq_ignore_ascii_case("$management")
                || part.eq_ignore_ascii_case("$deadletterqueue")
                || part.eq_ignore_ascii_case("$transfer")
        })
    {
        return Err(invalid_address(
            address,
            "address contains an unsupported or empty path segment",
        ));
    }

    // Only the separator before the leaf is a control segment. A topic can
    // itself end in the literal name "Subscriptions".
    if let Some((parent, leaf)) = base.rsplit_once('/')
        && let Some((topic, control)) = parent.rsplit_once('/')
        && control.eq_ignore_ascii_case("subscriptions")
    {
        let topic = entity(address, topic)?;
        if topic.is_subscription_path() {
            return Err(invalid_address(
                address,
                "nested subscription paths are not supported",
            ));
        }
        let subscription = SubscriptionName::new(leaf)
            .map_err(|source| invalid_address(address, &source.to_string()))?;
        let attachment = if dead_letter {
            Attachment::SubscriptionDeadLetter {
                topic,
                subscription,
            }
        } else {
            Attachment::Subscription {
                topic,
                subscription,
            }
        };
        attachment.canonical_entity()?;
        return Ok(attachment);
    }
    let entity = entity(address, base)?;
    if entity.is_subscription_path() {
        return Err(invalid_address(
            address,
            "address must name exactly one subscription",
        ));
    }
    let attachment = if dead_letter {
        Attachment::DeadLetter(entity)
    } else {
        Attachment::Queue(entity)
    };
    attachment.canonical_entity()?;
    Ok(attachment)
}

pub(crate) fn strip_control_suffix<'a>(address: &'a str, suffix: &str) -> Option<&'a str> {
    let split = address.len().checked_sub(suffix.len())?;
    address
        .get(split..)?
        .eq_ignore_ascii_case(suffix)
        .then(|| &address[..split])
}

fn invalid_address(address: &str, detail: &str) -> ProtocolError {
    ProtocolError::InvalidAddress {
        address: address.to_owned(),
        detail: detail.to_owned(),
    }
}

/// Reads a session identifier a client asked for, rejecting one the broker
/// could not key on.
pub fn parse_session_id(value: &str) -> Result<SessionId, ProtocolError> {
    SessionId::new(value).map_err(|source| ProtocolError::InvalidSessionId {
        session_id: value.to_owned(),
        detail: source.to_string(),
    })
}

fn entity(address: &str, path: &str) -> Result<EntityPath, ProtocolError> {
    EntityPath::new(path).map_err(|source| ProtocolError::InvalidAddress {
        address: address.to_owned(),
        detail: source.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn queue(address: &str) -> Attachment {
        parse_attachment(address).expect("a valid address")
    }

    #[test]
    fn a_namespace_comes_from_the_first_label_of_the_hostname() {
        assert_eq!(
            namespace_from_hostname("tenant.switchyard.example")
                .expect("a valid hostname")
                .as_str(),
            "tenant"
        );
        assert_eq!(
            namespace_from_hostname("tenant")
                .expect("a bare hostname is a namespace")
                .as_str(),
            "tenant"
        );
        assert_eq!(
            namespace_from_hostname(""),
            Err(ProtocolError::MissingNamespace)
        );
        assert_eq!(
            namespace_from_hostname(".switchyard.example"),
            Err(ProtocolError::MissingNamespace)
        );
    }

    #[test]
    fn a_plain_address_is_a_queue() {
        assert_eq!(
            queue("orders"),
            Attachment::Queue(EntityPath::new("orders").expect("valid"))
        );
        // A leading slash is how some SDKs write the same address.
        assert_eq!(queue("/orders"), queue("orders"));
    }

    #[test]
    fn a_dead_letter_address_is_not_a_queue_of_that_name() {
        assert_eq!(
            queue("orders/$deadletterqueue"),
            Attachment::DeadLetter(EntityPath::new("orders").expect("valid"))
        );
        // The SDKs disagree on casing of the well-known suffix.
        assert_eq!(
            queue("orders/$DeadLetterQueue"),
            Attachment::DeadLetter(EntityPath::new("orders").expect("valid"))
        );
    }

    #[test]
    fn a_subscription_address_carries_its_topic() {
        assert_eq!(
            queue("billing/Subscriptions/accounting"),
            Attachment::Subscription {
                topic: EntityPath::new("billing").expect("valid"),
                subscription: SubscriptionName::new("accounting").expect("valid"),
            }
        );
    }

    #[test]
    fn subscription_controls_are_canonical_without_folding_user_names() {
        for address in [
            "Topic/Subscriptions/Member/$DeadLetterQueue",
            "/Topic/sUbScRiPtIoNs/Member/$deadletterqueue",
        ] {
            assert_eq!(
                queue(address),
                Attachment::SubscriptionDeadLetter {
                    topic: EntityPath::new("Topic").expect("valid"),
                    subscription: SubscriptionName::new("Member").expect("valid"),
                }
            );
            assert_eq!(
                queue(address)
                    .canonical_entity()
                    .expect("canonical")
                    .as_str(),
                "Topic/subscriptions/Member/$deadletterqueue"
            );
        }
        assert_eq!(
            queue("a/Subscriptions/Subscriptions/Subscriptions")
                .canonical_entity()
                .expect("literal names")
                .as_str(),
            "a/Subscriptions/subscriptions/Subscriptions"
        );
        assert_eq!(
            queue("a/Subscriptions"),
            Attachment::Queue(EntityPath::new("a/Subscriptions").expect("literal primary name"))
        );
    }

    #[test]
    fn malformed_nested_and_repeated_endpoint_paths_are_refused() {
        for address in [
            "topic/subscriptions/",
            "topic/subscriptions/member/extra",
            "topic/subscriptions/member/subscriptions/nested",
            "topic/subscriptions/bad name",
            "topic/subscriptions/.member",
            "topic/subscriptions/member/$deadletterqueue/$deadletterqueue",
            "topic/$deadletterqueue/subscriptions/member",
            "topic/$management",
            "topic//$deadletterqueue",
        ] {
            assert!(parse_attachment(address).is_err(), "{address:?}");
        }
    }

    #[test]
    fn an_entity_path_keeps_the_case_it_was_written_with() {
        // The path is part of a storage key: folding case would merge entities
        // the client considers distinct.
        assert_eq!(
            queue("Orders"),
            Attachment::Queue(EntityPath::new("Orders").expect("valid"))
        );
        assert_ne!(queue("Orders"), queue("orders"));
    }

    #[test]
    fn an_address_that_names_nothing_is_refused() {
        for address in ["", "/", "billing/subscriptions/"] {
            assert!(
                parse_attachment(address).is_err(),
                "{address:?} should not resolve"
            );
        }
    }

    #[test]
    fn a_session_id_the_broker_cannot_key_on_is_refused() {
        assert!(parse_session_id("cart-1").is_ok());
        assert!(matches!(
            parse_session_id("cart\u{0}1"),
            Err(ProtocolError::InvalidSessionId { .. })
        ));
    }
}

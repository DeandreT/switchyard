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
    Queue(EntityPath),
    /// The shadow of a queue, before the link role decides whether it may be
    /// received from or must be refused as a send target.
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
    let lowercase = trimmed.to_ascii_lowercase();

    // Matched against the folded copy, but sliced out of the original, so the
    // entity keeps the case the client wrote.
    let (path, folded, dead_letter) = match lowercase.strip_suffix(DEAD_LETTER_SUFFIX) {
        Some(folded) => (&trimmed[..folded.len()], folded, true),
        None => (trimmed, lowercase.as_str(), false),
    };
    if let Some(position) = folded.find(SUBSCRIPTION_SEGMENT) {
        let topic = entity(address, &path[..position])?;
        let subscription = subscription(address, &path[position + SUBSCRIPTION_SEGMENT.len()..])?;
        return Ok(if dead_letter {
            Attachment::SubscriptionDeadLetter {
                topic,
                subscription,
            }
        } else {
            Attachment::Subscription {
                topic,
                subscription,
            }
        });
    }
    let entity = entity(address, path)?;
    if dead_letter && entity.is_dead_letter_queue() {
        return Err(ProtocolError::InvalidAddress {
            address: address.to_owned(),
            detail: String::from("a dead-letter queue cannot have its own dead-letter queue"),
        });
    }
    Ok(if dead_letter {
        Attachment::DeadLetter(entity)
    } else {
        Attachment::Queue(entity)
    })
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

fn subscription(address: &str, name: &str) -> Result<SubscriptionName, ProtocolError> {
    SubscriptionName::new(name).map_err(|source| ProtocolError::InvalidAddress {
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
    fn a_subscription_dead_letter_address_keeps_both_owners() {
        assert_eq!(
            queue("billing/Subscriptions/accounting/$DeadLetterQueue"),
            Attachment::SubscriptionDeadLetter {
                topic: EntityPath::new("billing").expect("valid"),
                subscription: SubscriptionName::new("accounting").expect("valid"),
            }
        );
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
        for address in [
            "",
            "/",
            "billing/subscriptions/",
            "billing/subscriptions/a/b",
            "orders/$deadletterqueue/$deadletterqueue",
        ] {
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

//! Turning what a client attaches to into what the broker names.
//!
//! A Service Bus client carries the namespace in the hostname it opens the
//! connection with and the entity in the link's source or target address. The
//! broker names both explicitly, so the edge resolves one into the other before
//! any command is proposed.

use std::borrow::Cow;

use auth::ResourceScope;
use domain::{EntityPath, NamespaceName, SessionId, SubscriptionName};
use percent_encoding::percent_decode_str;

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
/// Absolute URIs must first pass `normalize_link_address` with a trusted authority.
///
/// Service Bus entity identity is case-insensitive. Caller-controlled entity
/// and subscription components are resolved through canonical domain
/// identifiers, while well-known path segments are matched without regard to
/// case, so every spelling reaches one canonical storage identity.
pub fn parse_attachment(address: &str) -> Result<Attachment, ProtocolError> {
    if address.contains("://") || address.starts_with("//") {
        return Err(invalid_address(
            address,
            "absolute link address needs a trusted namespace authority",
        ));
    }
    let trimmed = address.trim_start_matches('/');
    if trimmed.is_empty() {
        return Err(ProtocolError::InvalidAddress {
            address: address.to_owned(),
            detail: String::from("address names no entity"),
        });
    }
    let lowercase = trimmed.to_ascii_lowercase();

    // Match against the folded copy but retain the original slices until each
    // caller-controlled component is validated and canonicalized below.
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

/// Resolves an absolute AMQP link URI only within a configured service authority.
///
/// Relative addresses are unchanged. An absolute URI requires the exact trusted
/// host from listener configuration, never the peer's Open hostname. Ports,
/// credentials, queries, fragments, controls, and dot-segment rewrites are
/// refused. URI escapes follow the same decoding rules as SAS resource scopes.
pub fn normalize_link_address<'a>(
    address: &'a str,
    expected_host: Option<&str>,
) -> Result<Cow<'a, str>, ProtocolError> {
    if address.starts_with("//") || address.chars().any(char::is_control) {
        return Err(invalid_address(
            address,
            "ambiguous or invalid link address",
        ));
    }
    let Some((_, authority_and_path)) = address.split_once("://") else {
        return Ok(Cow::Borrowed(address));
    };
    let expected_host = expected_host.ok_or_else(|| {
        invalid_address(
            address,
            "absolute link address needs a trusted namespace authority",
        )
    })?;
    let raw_authority = authority_and_path.split('/').next().unwrap_or_default();
    if !raw_authority.eq_ignore_ascii_case(expected_host) {
        return Err(invalid_address(
            address,
            "absolute link address needs the exact configured authority",
        ));
    }
    // URL parsers normalize dot segments. Refuse those spellings before parsing
    // rather than changing the entity that the peer actually supplied.
    if let Some((_, raw_path)) = authority_and_path.split_once('/') {
        for part in raw_path.split('/') {
            let decoded = percent_decode_str(part)
                .decode_utf8()
                .map_err(|_| invalid_address(address, "invalid URI path"))?;
            if matches!(decoded.as_ref(), "." | "..") {
                return Err(invalid_address(
                    address,
                    "dot segments are not entity identities",
                ));
            }
        }
    }
    let scope = ResourceScope::parse(address)
        .map_err(|_| invalid_address(address, "invalid absolute AMQP address"))?;
    if !scope.host().eq_ignore_ascii_case(expected_host) {
        return Err(invalid_address(
            address,
            "absolute link address names another service authority",
        ));
    }
    Ok(Cow::Owned(scope.path().collect::<Vec<_>>().join("/")))
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
            namespace_from_hostname("Tenant.Switchyard.Example")
                .expect("hostname identity is case-insensitive")
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
            queue("BiLlInG/Subscriptions/AcCoUnTiNg"),
            Attachment::Subscription {
                topic: EntityPath::new("billing").expect("valid"),
                subscription: SubscriptionName::new("accounting").expect("valid"),
            }
        );
    }

    #[test]
    fn a_subscription_dead_letter_address_keeps_both_owners() {
        assert_eq!(
            queue("BiLlInG/Subscriptions/AcCoUnTiNg/$DeadLetterQueue"),
            Attachment::SubscriptionDeadLetter {
                topic: EntityPath::new("billing").expect("valid"),
                subscription: SubscriptionName::new("accounting").expect("valid"),
            }
        );
    }

    #[test]
    fn entity_paths_resolve_to_one_case_insensitive_identity() {
        assert_eq!(
            queue("Orders"),
            Attachment::Queue(EntityPath::new("orders").expect("valid"))
        );
        assert_eq!(queue("Orders"), queue("orders"));
        assert_eq!(
            queue("Orders/$DeadLetterQueue"),
            queue("orders/$deadletterqueue")
        );
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

    #[test]
    fn absolute_addresses_need_the_configured_authority() {
        for path in [
            "Orders",
            "Orders/$DeadLetterQueue",
            "Events/Subscriptions/Audit",
            "Events/Subscriptions/Audit/$DeadLetterQueue",
        ] {
            let uri = format!("amqps://TENANT.example/{path}");
            let normalized = normalize_link_address(&uri, Some("tenant.example")).unwrap();
            assert_eq!(
                parse_attachment(&normalized).unwrap(),
                parse_attachment(path).unwrap()
            );
            assert!(parse_attachment(&uri).is_err());
            assert!(normalize_link_address(&uri, None).is_err());
        }
        assert_eq!(normalize_link_address("/Orders", None).unwrap(), "/Orders");
        assert_eq!(
            normalize_link_address(
                "amqps://tenant.example/%6frders/$management",
                Some("tenant.example")
            )
            .unwrap(),
            "orders/$management"
        );
    }

    #[test]
    fn foreign_ambiguous_and_rewritten_addresses_are_refused() {
        for address in [
            "amqps://foreign.example/orders",
            "amqps://tenant.example.evil/orders",
            "amqps://user@tenant.example/orders",
            "amqps://tenant.example:5671/orders",
            "amqps://@tenant.example/orders",
            "amqps://tenant.example:/orders",
            "amqps://tenant.example/orders?query",
            "amqps://tenant.example/orders#fragment",
            "amqp://tenant.example/orders",
            "https://tenant.example/orders",
            "//tenant.example/orders",
            "amqps://tenant.example/a/../orders",
            "amqps://tenant.example/a/%2e%2e/orders",
            "amqps://tenant.example/a/./orders",
            "amqps://tenant.example/orders%2fother",
            "amqps://tenant.example/%ZZ",
            "amqps://tenant.example/ord\ners",
        ] {
            assert!(
                normalize_link_address(address, Some("tenant.example")).is_err(),
                "{address:?}"
            );
        }
    }
}

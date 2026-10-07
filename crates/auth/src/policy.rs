use std::{
    collections::HashMap,
    fmt,
    ops::{BitOr, BitOrAssign},
    sync::Arc,
};

use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

use crate::Permission;

mod atom_https;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PermissionSet(u8);

impl PermissionSet {
    pub const NONE: Self = Self(0);
    pub const SEND: Self = Self(1 << 0);
    pub const LISTEN: Self = Self(1 << 1);
    pub const MANAGE: Self = Self(1 << 2);

    pub const fn allows(self, permission: Permission) -> bool {
        let manage = self.0 & Self::MANAGE.0 != 0;
        match permission {
            Permission::Send => manage || self.0 & Self::SEND.0 != 0,
            Permission::Listen => manage || self.0 & Self::LISTEN.0 != 0,
            Permission::Manage => manage,
            Permission::Audit | Permission::Cluster => false,
        }
    }
}

impl BitOr for PermissionSet {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl BitOrAssign for PermissionSet {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

/// A namespace or entity audience, split on resource boundaries.
///
/// The path is stored as decoded segments so authorization is a hierarchy
/// comparison, never a string prefix comparison.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ResourceScope {
    host: String,
    path: Vec<String>,
}

impl ResourceScope {
    pub fn parse(audience: &str) -> Result<Self, ResourceScopeError> {
        let url = Url::parse(audience).map_err(|_| ResourceScopeError::InvalidUri)?;
        if url.scheme() != "amqps"
            || !url.username().is_empty()
            || url.password().is_some()
            || url.port().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(ResourceScopeError::InvalidUri);
        }
        let host = url
            .host_str()
            .filter(|host| !host.is_empty())
            .ok_or(ResourceScopeError::InvalidUri)?
            .to_ascii_lowercase();

        let path = match url.path() {
            "" => "",
            path => path
                .strip_prefix('/')
                .ok_or(ResourceScopeError::InvalidPath)?,
        };
        let path = path.strip_suffix('/').unwrap_or(path);
        let path = if path.is_empty() {
            Vec::new()
        } else {
            path.split('/')
                .map(decode_path_segment)
                .collect::<Result<Vec<_>, _>>()?
        };
        Ok(Self { host, path })
    }

    pub fn namespace(host: impl AsRef<str>) -> Result<Self, ResourceScopeError> {
        let scope = Self::parse(&format!("amqps://{}", host.as_ref()))?;
        if !scope.path.is_empty() {
            return Err(ResourceScopeError::InvalidUri);
        }
        Ok(scope)
    }

    pub fn entity(
        host: impl AsRef<str>,
        entity_path: impl AsRef<str>,
    ) -> Result<Self, ResourceScopeError> {
        let mut scope = Self::namespace(host)?;
        scope.path = entity_path
            .as_ref()
            .split('/')
            .map(validate_literal_path_segment)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(scope)
    }

    /// Applies the positional control aliases used by AMQP entity endpoints.
    /// Native entity names retain their literal spelling unless explicitly converted.
    pub fn into_amqp_scope(mut self) -> Self {
        normalize_control_segments(&mut self.path);
        self
    }

    pub fn contains(&self, requested: &Self) -> bool {
        self.host == requested.host
            && self.path.len() <= requested.path.len()
            && self
                .path
                .iter()
                .zip(&requested.path)
                .all(|(granted, requested)| granted == requested)
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn path(&self) -> impl Iterator<Item = &str> {
        self.path.iter().map(String::as_str)
    }
}

fn normalize_control_segments(path: &mut [String]) {
    let mut end = path.len();
    if end > 1 && path[end - 1].eq_ignore_ascii_case("$management") {
        path[end - 1] = "$management".to_owned();
        end -= 1;
    }
    if end > 1 && path[end - 1].eq_ignore_ascii_case("$deadletterqueue") {
        path[end - 1] = "$deadletterqueue".to_owned();
        end -= 1;
    }
    if end >= 3
        && path[end - 2].eq_ignore_ascii_case("subscriptions")
        && is_subscription_leaf(&path[end - 1])
    {
        path[end - 2] = "subscriptions".to_owned();
    }
}

// Match routed SubscriptionName's ASCII subset without depending on domain storage types.
fn is_subscription_leaf(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() <= 50
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && bytes
            .iter()
            .all(|&byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
}

fn decode_path_segment(segment: &str) -> Result<String, ResourceScopeError> {
    validate_percent_encoding(segment)?;
    let decoded = percent_decode_str(segment)
        .decode_utf8()
        .map_err(|_| ResourceScopeError::InvalidPath)?
        .into_owned();
    if decoded.is_empty()
        || decoded == "."
        || decoded == ".."
        || decoded.contains('/')
        || decoded.chars().any(char::is_control)
    {
        return Err(ResourceScopeError::InvalidPath);
    }
    Ok(decoded)
}

fn validate_literal_path_segment(segment: &str) -> Result<String, ResourceScopeError> {
    if segment.is_empty()
        || segment == "."
        || segment == ".."
        || segment.chars().any(char::is_control)
    {
        return Err(ResourceScopeError::InvalidPath);
    }
    Ok(segment.to_owned())
}

pub(crate) fn validate_percent_encoding(value: &str) -> Result<(), ResourceScopeError> {
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit()
            {
                return Err(ResourceScopeError::InvalidPercentEncoding);
            }
            index += 3;
        } else {
            index += 1;
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ResourceScopeError {
    #[error("the resource scope is not an absolute amqps URI")]
    InvalidUri,
    #[error("the resource scope has malformed percent encoding")]
    InvalidPercentEncoding,
    #[error("the resource scope contains an unusable path segment")]
    InvalidPath,
}

#[derive(Clone)]
pub struct SharedAccessKey(Arc<str>);

impl SharedAccessKey {
    pub fn new(value: impl Into<String>) -> Result<Self, PolicyError> {
        let value = value.into();
        if value.is_empty() {
            return Err(PolicyError::EmptyKey);
        }
        Ok(Self(value.into()))
    }

    pub(crate) fn expose(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl fmt::Debug for SharedAccessKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
    }
}

#[derive(Clone, Debug)]
pub struct SharedAccessRule {
    name: String,
    scope: ResourceScope,
    primary_key: SharedAccessKey,
    secondary_key: Option<SharedAccessKey>,
    permissions: PermissionSet,
}

impl SharedAccessRule {
    pub fn new(
        name: impl Into<String>,
        scope: ResourceScope,
        primary_key: SharedAccessKey,
        secondary_key: Option<SharedAccessKey>,
        permissions: PermissionSet,
    ) -> Result<Self, PolicyError> {
        let name = name.into();
        if name.is_empty() {
            return Err(PolicyError::EmptyRuleName);
        }
        if permissions == PermissionSet::NONE {
            return Err(PolicyError::NoPermissions);
        }
        Ok(Self {
            name,
            scope,
            primary_key,
            secondary_key,
            permissions,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn scope(&self) -> &ResourceScope {
        &self.scope
    }

    pub fn permissions(&self) -> PermissionSet {
        self.permissions
    }

    pub(crate) fn keys(&self) -> impl Iterator<Item = &SharedAccessKey> {
        std::iter::once(&self.primary_key).chain(self.secondary_key.as_ref())
    }
}

#[derive(Clone, Debug, Default)]
pub struct SharedAccessPolicy {
    rules: Arc<HashMap<String, SharedAccessRule>>,
}

impl SharedAccessPolicy {
    pub fn new(rules: impl IntoIterator<Item = SharedAccessRule>) -> Result<Self, PolicyError> {
        let mut by_name = HashMap::new();
        for rule in rules {
            let name = rule.name.clone();
            if by_name.insert(name.clone(), rule).is_some() {
                return Err(PolicyError::DuplicateRule(name));
            }
        }
        Ok(Self {
            rules: Arc::new(by_name),
        })
    }

    pub(crate) fn rule(&self, name: &str) -> Option<&SharedAccessRule> {
        self.rules.get(name)
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum PolicyError {
    #[error("a shared-access rule name cannot be empty")]
    EmptyRuleName,
    #[error("a shared-access key cannot be empty")]
    EmptyKey,
    #[error("a shared-access rule must grant at least one permission")]
    NoPermissions,
    #[error("shared-access rule {0:?} is configured more than once")]
    DuplicateRule(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_hosts_cannot_smuggle_an_entity_path() {
        for host in ["tenant.example/orders", "tenant.example/%6frders"] {
            assert_eq!(
                ResourceScope::namespace(host),
                Err(ResourceScopeError::InvalidUri)
            );
            assert_eq!(
                ResourceScope::entity(host, "other"),
                Err(ResourceScopeError::InvalidUri)
            );
        }
        assert!(ResourceScope::namespace("tenant.example").is_ok());
        assert!(ResourceScope::parse("amqps://tenant.example/orders").is_ok());
    }

    #[test]
    fn manage_includes_data_plane_rights() {
        assert!(PermissionSet::MANAGE.allows(Permission::Manage));
        assert!(PermissionSet::MANAGE.allows(Permission::Send));
        assert!(PermissionSet::MANAGE.allows(Permission::Listen));
        assert!(!PermissionSet::MANAGE.allows(Permission::Audit));
    }

    #[test]
    fn scope_comparison_uses_path_segments() {
        let orders = ResourceScope::parse("amqps://tenant.servicebus.windows.net/orders").unwrap();
        let dead_letters =
            ResourceScope::parse("amqps://tenant.servicebus.windows.net/orders/$deadletterqueue")
                .unwrap();
        let archive =
            ResourceScope::parse("amqps://tenant.servicebus.windows.net/orders-archive").unwrap();

        assert!(orders.contains(&dead_letters));
        assert!(!orders.contains(&archive));
    }

    #[test]
    fn a_namespace_scope_contains_its_entities_but_not_another_host() {
        let namespace = ResourceScope::namespace("tenant.servicebus.windows.net").unwrap();
        let orders = ResourceScope::entity("tenant.servicebus.windows.net", "orders").unwrap();
        let foreign = ResourceScope::entity("other.servicebus.windows.net", "orders").unwrap();

        assert!(namespace.contains(&orders));
        assert!(!namespace.contains(&foreign));
    }

    #[test]
    fn explicit_amqp_conversion_normalizes_only_positional_transport_controls() {
        let host = "tenant.servicebus.windows.net";
        for (sdk_path, canonical) in [
            (
                "Orders/SubScriptions/Accounting",
                "Orders/subscriptions/Accounting",
            ),
            (
                "Orders/Subscriptions/Accounting/$DeadLetterQueue",
                "Orders/subscriptions/Accounting/$deadletterqueue",
            ),
            (
                "Orders/Subscriptions/Accounting/$Management",
                "Orders/subscriptions/Accounting/$management",
            ),
            (
                "Orders/Subscriptions/Accounting/$DeadLetterQueue/$MANAGEMENT",
                "Orders/subscriptions/Accounting/$deadletterqueue/$management",
            ),
            (
                "Orders/$DeadLetterQueue/$Management",
                "Orders/$deadletterqueue/$management",
            ),
            (
                "Subscriptions/Subscriptions/Subscriptions/$DeadLetterQueue",
                "Subscriptions/subscriptions/Subscriptions/$deadletterqueue",
            ),
            (
                "Orders/Subscriptions/Subscriptions/Accounting",
                "Orders/Subscriptions/subscriptions/Accounting",
            ),
        ] {
            let expected = ResourceScope::entity(host, canonical)
                .expect("canonical scope")
                .into_amqp_scope();
            assert_eq!(
                ResourceScope::entity(host, sdk_path)
                    .expect("literal scope")
                    .into_amqp_scope(),
                expected
            );
            assert_eq!(
                ResourceScope::parse(&format!("amqps://{host}/{sdk_path}"))
                    .expect("URI scope")
                    .into_amqp_scope(),
                expected
            );
            assert_eq!(expected.path().collect::<Vec<_>>().join("/"), canonical);
        }
    }

    #[test]
    fn user_names_and_bare_control_words_keep_their_original_spelling() {
        let host = "tenant.servicebus.windows.net";
        for path in [
            "Subscriptions",
            "Orders/Subscriptions",
            "$Management",
            "$DeadLetterQueue",
            "Orders/Subscriptions/_invalid",
            "Orders/Subscriptions/a/b",
        ] {
            let scope = ResourceScope::entity(host, path)
                .expect("ordinary resource path")
                .into_amqp_scope();
            assert_eq!(scope.path().collect::<Vec<_>>().join("/"), path);
        }
        for leaf in [".", "..", "-x", "x_", "a\u{e9}b", "a:b", &"a".repeat(51)] {
            let path = format!("Orders/Subscriptions/{leaf}");
            assert!(!is_subscription_leaf(leaf));
            if !matches!(leaf, "." | "..") {
                let scope = ResourceScope::entity(host, &path)
                    .expect("ordinary resource path")
                    .into_amqp_scope();
                assert_eq!(scope.path().collect::<Vec<_>>().join("/"), path);
            }
        }
        for (left, right) in [
            (
                "Orders/subscriptions/Accounting",
                "orders/subscriptions/Accounting",
            ),
            (
                "Orders/subscriptions/Accounting",
                "Orders/subscriptions/accounting",
            ),
            (
                "Subscriptions/subscriptions/Subscriptions/$deadletterqueue",
                "subscriptions/subscriptions/Subscriptions/$deadletterqueue",
            ),
            (
                "Subscriptions/subscriptions/Subscriptions/$deadletterqueue",
                "Subscriptions/subscriptions/subscriptions/$deadletterqueue",
            ),
        ] {
            let left = ResourceScope::entity(host, left)
                .expect("left scope")
                .into_amqp_scope();
            let right = ResourceScope::entity(host, right)
                .expect("right scope")
                .into_amqp_scope();
            assert_ne!(left, right);
            assert!(!left.contains(&right));
            assert!(!right.contains(&left));
        }
    }

    #[test]
    fn namespace_topic_subscription_and_endpoint_grants_have_exact_boundaries() {
        let host = "tenant.servicebus.windows.net";
        let paths = [
            "Orders",
            "Orders/subscriptions/Accounting",
            "Orders/subscriptions/Accounting/$deadletterqueue",
            "Orders/subscriptions/Accounting/$management",
            "Orders/subscriptions/Accounting/$deadletterqueue/$management",
        ];
        let scopes = paths.map(|path| {
            ResourceScope::entity(host, path)
                .expect("scope")
                .into_amqp_scope()
        });
        let namespace = ResourceScope::namespace(host).expect("namespace");
        assert!(scopes.iter().all(|scope| namespace.contains(scope)));
        assert!(scopes.iter().all(|scope| scopes[0].contains(scope)));
        assert!(scopes[1..].iter().all(|scope| scopes[1].contains(scope)));
        assert!(scopes[2].contains(&scopes[4]));
        for (granted, requested) in [(1, 0), (2, 1), (2, 3), (3, 1), (3, 2), (4, 2), (4, 3)] {
            assert!(!scopes[granted].contains(&scopes[requested]));
        }
        for sibling in [
            "Orders-archive/subscriptions/Accounting",
            "Orders/subscriptions/Accounting-old",
            "Orders/subscriptions/Billing",
        ] {
            assert!(!scopes[1].contains(&ResourceScope::entity(host, sibling).expect("sibling")));
        }
        let foreign =
            ResourceScope::entity("other.servicebus.windows.net", paths[1]).expect("foreign scope");
        assert!(!namespace.contains(&foreign));
        assert!(!scopes[1].contains(&foreign));
    }

    #[test]
    fn decoded_controls_share_hash_identity_but_encoded_slashes_are_rejected() {
        let encoded = ResourceScope::parse("amqps://tenant.servicebus.windows.net/Orders/%53ubscriptions/Accounting/%24DeadLetterQueue").expect("decoded scope").into_amqp_scope();
        let canonical = ResourceScope::entity(
            "tenant.servicebus.windows.net",
            "Orders/subscriptions/Accounting/$deadletterqueue",
        )
        .expect("canonical scope")
        .into_amqp_scope();
        assert_eq!(encoded, canonical);
        let mut set = std::collections::HashSet::new();
        set.insert(encoded);
        set.insert(canonical);
        assert_eq!(set.len(), 1);
        for invalid in [
            "amqps://tenant.servicebus.windows.net/Orders/Subscriptions/Accounting%2Fsibling",
            "amqps://tenant.servicebus.windows.net/Orders/Subscriptions/%00Accounting",
            "amqps://tenant.servicebus.windows.net/Orders/%53ubscriptions/Accounting%GG",
        ] {
            assert!(ResourceScope::parse(invalid).is_err());
        }
    }

    #[test]
    fn default_scopes_keep_native_literal_control_names_distinct() {
        let host = "tenant.servicebus.windows.net";
        for (literal, alias) in [
            ("Orders/$Management", "Orders/$management"),
            ("Orders/$DeadLetterQueue", "Orders/$deadletterqueue"),
            (
                "Orders/Subscriptions/Accounting",
                "Orders/subscriptions/Accounting",
            ),
        ] {
            let left = ResourceScope::entity(host, literal).expect("literal scope");
            let right = ResourceScope::entity(host, alias).expect("alias scope");
            assert_ne!(left, right);
            assert!(!left.contains(&right));
            assert!(!right.contains(&left));
            assert_eq!(
                ResourceScope::parse(&format!("amqps://{host}/{literal}"))
                    .expect("literal URI scope"),
                left
            );
            assert_eq!(
                left.into_amqp_scope(),
                right.into_amqp_scope(),
                "only explicit transport conversion aliases controls"
            );
        }
    }

    #[test]
    fn keys_are_never_printed() {
        let key = SharedAccessKey::new("the-secret").unwrap();
        assert_eq!(format!("{key:?}"), "<redacted>");
    }
}

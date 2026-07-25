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
        Self::parse(&format!("amqps://{}", host.as_ref()))
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
    fn keys_are_never_printed() {
        let key = SharedAccessKey::new("the-secret").unwrap();
        assert_eq!(format!("{key:?}"), "<redacted>");
    }
}

//! Bounded, local workload policy; authenticated capabilities require `workload-tls`.
//!
//! Parsing a policy or certificate does not authenticate a caller. Only the TLS
//! endpoint's completed, verified handshake can produce a peer capability.

mod certificate;
#[cfg(feature = "workload-tls")]
mod tls;

use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
};

use serde::Deserialize;
use sha2::{Digest, Sha256};
use thiserror::Error;
use url::Url;

use crate::{PermissionSet, ResourceScope};
use certificate::{Interval, certificate, pem_certificates};

#[cfg(feature = "workload-tls")]
pub use tls::{AdmittedWorkloadStream, PreparedWorkloadTls, VerifiedWorkloadPeer};

const MAX_CONFIG_BYTES: usize = 256 * 1024;
const MAX_CONFIG_NODES: usize = 8192;
const MAX_ROOTS: usize = 8;
const MAX_BINDINGS: usize = 64;
const MAX_REVOKED: usize = 256;
const MAX_PRINCIPAL_BYTES: usize = 512;
const MAX_SCOPE_BYTES: usize = 2048;
pub(super) const MAX_CERT_BYTES: usize = 16 * 1024;
pub(super) const MAX_CHAIN_BYTES: usize = 64 * 1024;
#[cfg(feature = "workload-tls")]
pub(super) const MAX_CHAIN_CERTS: usize = 8;

/// Static, public-safe categories. Inputs and TLS/backend diagnostics are not echoed.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum WorkloadError {
    #[error("workload input exceeds the local profile bounds")]
    TooLarge,
    #[error("workload policy is invalid")]
    InvalidPolicy,
    #[error("workload certificate is outside the local profile")]
    InvalidCertificate,
    #[error("workload identity is not locally authorized")]
    IdentityDenied,
    #[error("workload credential or lease is not currently valid")]
    NotValid,
    #[error("workload authorization clock is unavailable")]
    ClockUnavailable,
    #[error("workload TLS configuration is invalid")]
    InvalidTlsConfiguration,
    #[error("workload TLS handshake failed")]
    HandshakeFailed,
    #[error("workload TLS admission deadline expired")]
    DeadlineExpired,
    #[error("workload capability belongs to a different endpoint")]
    WrongEndpoint,
    #[error("workload resource or permission is not authorized")]
    PermissionDenied,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum ClientAuth {
    #[default]
    Required,
    Optional,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    version: u64,
    trust_domain: String,
    roots_pem: Vec<String>,
    bindings: Vec<BindingConfiguration>,
    #[serde(default)]
    revoked_leaf_sha256: Vec<String>,
    #[serde(default)]
    client_auth: ClientAuth,
    #[serde(default = "default_connection_seconds")]
    max_connection_seconds: u64,
}

const fn default_connection_seconds() -> u64 {
    3600
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BindingConfiguration {
    principal: String,
    scope: String,
    permissions: Vec<String>,
}

#[derive(Clone)]
#[cfg_attr(not(feature = "workload-tls"), allow(dead_code))]
struct Binding {
    scope: ResourceScope,
    permissions: PermissionSet,
}

#[cfg_attr(not(feature = "workload-tls"), allow(dead_code))]
struct Policy {
    trust_domain: String,
    roots: Vec<Vec<u8>>,
    root_interval: Interval,
    bindings: HashMap<String, Binding>,
    revoked: HashSet<[u8; 32]>,
    client_auth: ClientAuth,
    max_connection_seconds: u64,
    namespace: ResourceScope,
}

/// An immutable version-1 policy, independent of networking and wall-clock lookup.
/// Loading does not prove a TLS handshake, mint a grant, or activate a listener.
#[derive(Clone)]
pub struct WorkloadPolicy(Arc<Policy>);

impl fmt::Debug for WorkloadPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkloadPolicy")
            .field("roots", &self.0.roots.len())
            .field("bindings", &self.0.bindings.len())
            .field("revoked", &self.0.revoked.len())
            .finish_non_exhaustive()
    }
}

impl WorkloadPolicy {
    /// Loads bounded JSON for one server-selected namespace root. Dates are
    /// structurally checked here; actual current time is checked during TLS
    /// preparation and every admission. Policy changes require a new endpoint.
    pub fn from_json(json: &str, namespace: ResourceScope) -> Result<Self, WorkloadError> {
        if json.len() > MAX_CONFIG_BYTES {
            return Err(WorkloadError::TooLarge);
        }
        if namespace.path().next().is_some() || namespace.host().len() > MAX_SCOPE_BYTES - 8 {
            return Err(WorkloadError::InvalidPolicy);
        }
        let value = crate::jwt::bounded_json(json.as_bytes(), MAX_CONFIG_NODES)
            .map_err(|_| WorkloadError::InvalidPolicy)?;
        if !value.is_object()
            || value
                .get("bindings")
                .and_then(serde_json::Value::as_array)
                .is_none_or(|bindings| bindings.iter().any(|binding| !binding.is_object()))
        {
            return Err(WorkloadError::InvalidPolicy);
        }
        let config: Configuration =
            serde_json::from_value(value).map_err(|_| WorkloadError::InvalidPolicy)?;
        if config.version != 1
            || !valid_domain(&config.trust_domain)
            || config.roots_pem.is_empty()
            || config.roots_pem.len() > MAX_ROOTS
            || config.bindings.len() > MAX_BINDINGS
            || config.revoked_leaf_sha256.len() > MAX_REVOKED
            || !(1..=3600).contains(&config.max_connection_seconds)
        {
            return Err(WorkloadError::InvalidPolicy);
        }
        let mut roots = Vec::new();
        let mut root_interval = Interval {
            from: 0,
            until: u64::MAX,
        };
        let mut root_hashes = HashSet::new();
        for pem in config.roots_pem {
            let mut certificates = pem_certificates(pem.as_bytes(), 1)?;
            let der = certificates
                .pop()
                .ok_or(WorkloadError::InvalidCertificate)?;
            let cert = certificate(&der)?;
            certificate::require_ca(&cert)?;
            root_interval = root_interval.intersection(certificate::interval(&cert)?)?;
            if !root_hashes.insert(fingerprint(&der)) {
                return Err(WorkloadError::InvalidPolicy);
            }
            roots.push(der);
        }
        let mut bindings = HashMap::new();
        for binding in config.bindings {
            if !valid_principal(&binding.principal, &config.trust_domain)
                || binding.scope.len() > MAX_SCOPE_BYTES
                || binding.permissions.is_empty()
                || binding.permissions.len() > 3
            {
                return Err(WorkloadError::InvalidPolicy);
            }
            let scope =
                ResourceScope::parse(&binding.scope).map_err(|_| WorkloadError::InvalidPolicy)?;
            if !namespace.contains(&scope) {
                return Err(WorkloadError::InvalidPolicy);
            }
            let mut permissions = PermissionSet::NONE;
            let mut seen = HashSet::new();
            for right in binding.permissions {
                if !seen.insert(right.clone()) {
                    return Err(WorkloadError::InvalidPolicy);
                }
                permissions |= match right.as_str() {
                    "send" => PermissionSet::SEND,
                    "listen" => PermissionSet::LISTEN,
                    "manage" => PermissionSet::MANAGE,
                    _ => return Err(WorkloadError::InvalidPolicy),
                };
            }
            if bindings
                .insert(binding.principal, Binding { scope, permissions })
                .is_some()
            {
                return Err(WorkloadError::InvalidPolicy);
            }
        }
        let mut revoked = HashSet::new();
        for hash in config.revoked_leaf_sha256 {
            let hash = decode_fingerprint(&hash)?;
            if !revoked.insert(hash) {
                return Err(WorkloadError::InvalidPolicy);
            }
        }
        Ok(Self(Arc::new(Policy {
            trust_domain: config.trust_domain,
            roots,
            root_interval,
            bindings,
            revoked,
            client_auth: config.client_auth,
            max_connection_seconds: config.max_connection_seconds,
            namespace,
        })))
    }

    /// Optional means absence yields no workload authority. An integrating
    /// listener must require an independently configured alternative credential.
    pub fn requires_client_certificate(&self) -> bool {
        self.0.client_auth == ClientAuth::Required
    }

    pub fn namespace(&self) -> &ResourceScope {
        &self.0.namespace
    }
}

fn valid_domain(domain: &str) -> bool {
    !domain.is_empty()
        && domain.len() <= 253
        && domain.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .as_bytes()
                    .first()
                    .is_some_and(u8::is_ascii_alphanumeric)
                && label
                    .as_bytes()
                    .last()
                    .is_some_and(u8::is_ascii_alphanumeric)
                && label
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        })
}

fn valid_principal(principal: &str, domain: &str) -> bool {
    if principal.len() > MAX_PRINCIPAL_BYTES {
        return false;
    }
    let Some(rest) = principal.strip_prefix("spiffe://") else {
        return false;
    };
    let Some((authority, path)) = rest.split_once('/') else {
        return false;
    };
    if authority != domain
        || !valid_domain(authority)
        || !path.split('/').all(|segment| {
            !segment.is_empty()
                && segment != "."
                && segment != ".."
                && segment
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
        })
    {
        return false;
    }
    Url::parse(principal).is_ok_and(|url| {
        url.as_str() == principal
            && url.host_str() == Some(domain)
            && url.query().is_none()
            && url.fragment().is_none()
            && url.username().is_empty()
            && url.password().is_none()
            && url.port().is_none()
    })
}

fn fingerprint(der: &[u8]) -> [u8; 32] {
    Sha256::digest(der).into()
}

fn decode_fingerprint(value: &str) -> Result<[u8; 32], WorkloadError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(WorkloadError::InvalidPolicy);
    }
    let mut bytes = [0; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| WorkloadError::InvalidPolicy)?;
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests;

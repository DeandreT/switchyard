use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::Arc,
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use jsonwebtoken::{Algorithm, DecodingKey, crypto::rust_crypto::DEFAULT_PROVIDER};
use serde::Deserialize;
use thiserror::Error;
use url::Url;

use crate::{AccessGrant, PermissionSet, ResourceScope};

mod json;

const MAX_CONFIG_BYTES: usize = 64 * 1024;
const MAX_KEYS: usize = 8;
const MAX_BINDINGS: usize = 64;
const MAX_TOKEN_BYTES: usize = 8 * 1024;
const MAX_HEADER_BYTES: usize = 2 * 1024;
const MAX_CLAIMS_BYTES: usize = 6 * 1024;
const MAX_DEPTH: usize = 8;
const MAX_CONFIG_NODES: usize = 2048;
const MAX_HEADER_NODES: usize = 64;
const MAX_CLAIMS_NODES: usize = 256;
const MAX_KID_BYTES: usize = 128;
const MAX_SUBJECT_BYTES: usize = 512;
const MAX_RESOURCE_BYTES: usize = 2 * 1024;
const MAX_AUDIENCES: usize = 8;
const MAX_LIFETIME_SECONDS: u64 = 3600;

/// An immutable, offline access-token policy with locally pinned RSA keys and rights.
///
/// This profile accepts only RS256 and `typ: at+jwt`. It performs no discovery,
/// network key refresh, role-claim interpretation, or cloud identity authorization.
#[derive(Clone)]
pub struct JwtPolicy(Arc<PreparedPolicy>);

impl fmt::Debug for JwtPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("JwtPolicy")
            .field("keys", &self.0.keys.len())
            .field("bindings", &self.0.bindings.len())
            .finish_non_exhaustive()
    }
}

struct PreparedPolicy {
    issuer: String,
    audience: String,
    keys: HashMap<String, PreparedKey>,
    bindings: HashMap<String, Binding>,
}

struct PreparedKey {
    key: DecodingKey,
    signature_bytes: usize,
}

struct Binding {
    scope: ResourceScope,
    permissions: PermissionSet,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    version: u64,
    issuer: String,
    audience: String,
    keys: Vec<PublicKey>,
    bindings: Vec<Principal>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PublicKey {
    kid: String,
    kty: String,
    alg: String,
    #[serde(rename = "use")]
    purpose: String,
    n: String,
    e: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Principal {
    subject: String,
    scope: String,
    permissions: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    alg: String,
    kid: String,
    typ: String,
}

#[derive(Deserialize)]
struct Claims {
    iss: String,
    sub: String,
    aud: Audience,
    iat: u64,
    exp: u64,
    nbf: Option<u64>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Audience {
    Single(String),
    Multiple(Vec<String>),
}

/// Static rejection categories deliberately omit token, claim and key contents.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum JwtError {
    #[error("JWT input exceeds the offline profile bounds")]
    TooLarge,
    #[error("JWT JSON is malformed")]
    MalformedJson,
    #[error("JWT JSON contains a duplicate member")]
    DuplicateMember,
    #[error("JWT JSON exceeds the nesting limit")]
    TooDeep,
    #[error("JWT policy configuration is invalid")]
    InvalidConfiguration,
    #[error("JWT policy contains duplicate identities")]
    DuplicateIdentity,
    #[error("JWT public key is outside the pinned RSA profile")]
    InvalidKey,
    #[error("JWT compact encoding is invalid")]
    InvalidEncoding,
    #[error("JWT protected header is outside the offline profile")]
    InvalidHeader,
    #[error("JWT key identifier is not configured")]
    UnknownKey,
    #[error("JWT signature verification failed")]
    InvalidSignature,
    #[error("JWT claims are outside the offline profile")]
    InvalidClaims,
    #[error("JWT issuer does not match the configured issuer")]
    IssuerMismatch,
    #[error("JWT audience does not match the configured resource audience")]
    AudienceMismatch,
    #[error("JWT is not yet valid")]
    NotYetValid,
    #[error("JWT has expired")]
    Expired,
    #[error("JWT subject has no local binding")]
    UnknownSubject,
    #[error("JWT local binding does not authorize the requested CBS scope")]
    ScopeMismatch,
}

impl JwtPolicy {
    /// Loads version 1 JSON: issuer, resource audience, public RSA keys and bindings.
    /// Rights come only from each unique subject's local scope/permissions binding.
    pub fn from_json(configuration: &str) -> Result<Self, JwtError> {
        if configuration.len() > MAX_CONFIG_BYTES {
            return Err(JwtError::TooLarge);
        }
        let value = bounded_json(configuration.as_bytes(), MAX_CONFIG_NODES)?;
        if !value.is_object()
            || ["keys", "bindings"].iter().any(|collection| {
                value
                    .get(collection)
                    .and_then(serde_json::Value::as_array)
                    .is_none_or(|entries| entries.iter().any(|entry| !entry.is_object()))
            })
        {
            return Err(JwtError::InvalidConfiguration);
        }
        let configuration: Configuration =
            serde_json::from_value(value).map_err(|_| JwtError::InvalidConfiguration)?;
        if configuration.version != 1
            || !valid_text(&configuration.issuer, MAX_RESOURCE_BYTES)
            || !valid_text(&configuration.audience, MAX_RESOURCE_BYTES)
            || configuration.keys.is_empty()
            || configuration.keys.len() > MAX_KEYS
            || configuration.bindings.is_empty()
            || configuration.bindings.len() > MAX_BINDINGS
        {
            return Err(JwtError::InvalidConfiguration);
        }
        let issuer =
            Url::parse(&configuration.issuer).map_err(|_| JwtError::InvalidConfiguration)?;
        if issuer.scheme() != "https"
            || issuer.host_str().is_none()
            || !issuer.username().is_empty()
            || issuer.password().is_some()
            || issuer.query().is_some()
            || issuer.fragment().is_some()
        {
            return Err(JwtError::InvalidConfiguration);
        }
        let mut keys = HashMap::new();
        for public in configuration.keys {
            if !valid_kid(&public.kid)
                || public.kty != "RSA"
                || public.alg != "RS256"
                || public.purpose != "sig"
                || public.e != "AQAB"
            {
                return Err(JwtError::InvalidKey);
            }
            let modulus = decode_segment(&public.n, 512).map_err(|_| JwtError::InvalidKey)?;
            if modulus.first().is_none_or(|byte| *byte == 0)
                || modulus.last().is_none_or(|byte| byte & 1 == 0)
            {
                return Err(JwtError::InvalidKey);
            }
            let bits = modulus.len() * 8 - modulus[0].leading_zeros() as usize;
            if !(2048..=4096).contains(&bits) {
                return Err(JwtError::InvalidKey);
            }
            let key = DecodingKey::from_rsa_raw_components(&modulus, &[1, 0, 1]);
            let prepared = PreparedKey {
                key,
                signature_bytes: modulus.len(),
            };
            if keys.insert(public.kid, prepared).is_some() {
                return Err(JwtError::DuplicateIdentity);
            }
        }
        let mut bindings = HashMap::new();
        for principal in configuration.bindings {
            if !valid_text(&principal.subject, MAX_SUBJECT_BYTES)
                || !valid_text(&principal.scope, MAX_RESOURCE_BYTES)
                || principal.permissions.is_empty()
                || principal.permissions.len() > 3
            {
                return Err(JwtError::InvalidConfiguration);
            }
            let scope = ResourceScope::parse(&principal.scope)
                .map_err(|_| JwtError::InvalidConfiguration)?
                .into_amqp_scope();
            let mut permissions = PermissionSet::NONE;
            let mut seen = HashSet::new();
            for permission in principal.permissions {
                if !seen.insert(permission.clone()) {
                    return Err(JwtError::InvalidConfiguration);
                }
                permissions |= match permission.as_str() {
                    "send" => PermissionSet::SEND,
                    "listen" => PermissionSet::LISTEN,
                    "manage" => PermissionSet::MANAGE,
                    _ => return Err(JwtError::InvalidConfiguration),
                };
            }
            if bindings
                .insert(principal.subject, Binding { scope, permissions })
                .is_some()
            {
                return Err(JwtError::DuplicateIdentity);
            }
        }
        Ok(Self(Arc::new(PreparedPolicy {
            issuer: configuration.issuer,
            audience: configuration.audience,
            keys,
            bindings,
        })))
    }

    /// Validates against a caller-supplied epoch with zero clock skew.
    /// The resource audience in `aud` is distinct from the requested CBS URI.
    pub fn validate(
        &self,
        token: &str,
        requested: &ResourceScope,
        now_epoch_seconds: u64,
    ) -> Result<AccessGrant, JwtError> {
        if token.len() > MAX_TOKEN_BYTES || !scope_is_bounded(requested) {
            return Err(JwtError::TooLarge);
        }
        let mut segments = token.split('.');
        let header_segment = segments.next().ok_or(JwtError::InvalidEncoding)?;
        let claims_segment = segments.next().ok_or(JwtError::InvalidEncoding)?;
        let signature_segment = segments.next().ok_or(JwtError::InvalidEncoding)?;
        if segments.next().is_some() {
            return Err(JwtError::InvalidEncoding);
        }
        let header_bytes = decode_segment(header_segment, MAX_HEADER_BYTES)?;
        let claims_bytes = decode_segment(claims_segment, MAX_CLAIMS_BYTES)?;
        let header_value = bounded_json(&header_bytes, MAX_HEADER_NODES)?;
        if !header_value.is_object() {
            return Err(JwtError::InvalidHeader);
        }
        let header: Header =
            serde_json::from_value(header_value).map_err(|_| JwtError::InvalidHeader)?;
        if header.alg != "RS256" || header.typ != "at+jwt" || !valid_kid(&header.kid) {
            return Err(JwtError::InvalidHeader);
        }
        let claims_value = bounded_json(&claims_bytes, MAX_CLAIMS_NODES)?;
        let key = self.0.keys.get(&header.kid).ok_or(JwtError::UnknownKey)?;
        let signature = decode_segment(signature_segment, key.signature_bytes)?;
        if signature.len() != key.signature_bytes {
            return Err(JwtError::InvalidSignature);
        }
        let signing_length = header_segment.len() + 1 + claims_segment.len();
        // Select this maintained backend explicitly, not a process-global provider.
        let verifier = (DEFAULT_PROVIDER.verifier_factory)(&Algorithm::RS256, &key.key)
            .map_err(|_| JwtError::InvalidKey)?;
        verifier
            .verify(&token.as_bytes()[..signing_length], &signature)
            .map_err(|_| JwtError::InvalidSignature)?;
        if !claims_value.is_object()
            || claims_value
                .get("nbf")
                .is_some_and(serde_json::Value::is_null)
        {
            return Err(JwtError::InvalidClaims);
        }
        let claims: Claims =
            serde_json::from_value(claims_value).map_err(|_| JwtError::InvalidClaims)?;
        if !valid_text(&claims.iss, MAX_RESOURCE_BYTES)
            || !valid_text(&claims.sub, MAX_SUBJECT_BYTES)
            || claims
                .exp
                .checked_sub(claims.iat)
                .is_none_or(|duration| duration == 0 || duration > MAX_LIFETIME_SECONDS)
            || claims.nbf.is_some_and(|nbf| nbf >= claims.exp)
        {
            return Err(JwtError::InvalidClaims);
        }
        if claims.iss != self.0.issuer {
            return Err(JwtError::IssuerMismatch);
        }
        if !claims.aud.matches(&self.0.audience)? {
            return Err(JwtError::AudienceMismatch);
        }
        if now_epoch_seconds < claims.iat || claims.nbf.is_some_and(|nbf| now_epoch_seconds < nbf) {
            return Err(JwtError::NotYetValid);
        }
        if now_epoch_seconds >= claims.exp {
            return Err(JwtError::Expired);
        }
        let binding = self
            .0
            .bindings
            .get(&claims.sub)
            .ok_or(JwtError::UnknownSubject)?;
        if !binding.scope.contains(&requested.clone().into_amqp_scope()) {
            return Err(JwtError::ScopeMismatch);
        }
        Ok(AccessGrant::verified_jwt(
            claims.sub,
            self.0.issuer.clone(),
            binding.scope.clone(),
            claims.exp,
            binding.permissions,
        ))
    }
}

impl Audience {
    fn matches(&self, configured: &str) -> Result<bool, JwtError> {
        let audiences = match self {
            Self::Single(audience) => std::slice::from_ref(audience),
            Self::Multiple(audiences) => audiences.as_slice(),
        };
        if audiences.is_empty() || audiences.len() > MAX_AUDIENCES {
            return Err(JwtError::InvalidClaims);
        }
        let mut seen = HashSet::new();
        for audience in audiences {
            if !valid_text(audience, MAX_RESOURCE_BYTES) || !seen.insert(audience) {
                return Err(JwtError::InvalidClaims);
            }
        }
        Ok(audiences.iter().any(|audience| audience == configured))
    }
}

fn valid_text(value: &str, maximum: usize) -> bool {
    !value.is_empty() && value.len() <= maximum && !value.chars().any(char::is_control)
}

fn valid_kid(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_KID_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn scope_is_bounded(scope: &ResourceScope) -> bool {
    let Some(mut bytes) = 8usize.checked_add(scope.host().len()) else {
        return false;
    };
    for segment in scope.path() {
        let Some(next) = bytes
            .checked_add(1)
            .and_then(|bytes| bytes.checked_add(segment.len()))
        else {
            return false;
        };
        bytes = next;
        if bytes > MAX_RESOURCE_BYTES {
            return false;
        }
    }
    bytes <= MAX_RESOURCE_BYTES
}

fn bounded_json(bytes: &[u8], maximum_nodes: usize) -> Result<serde_json::Value, JwtError> {
    json::parse(bytes, MAX_DEPTH, maximum_nodes).map_err(|failure| match failure {
        json::Failure::Malformed => JwtError::MalformedJson,
        json::Failure::Duplicate => JwtError::DuplicateMember,
        json::Failure::Depth => JwtError::TooDeep,
        json::Failure::Nodes => JwtError::TooLarge,
    })
}

fn decode_segment(segment: &str, maximum_bytes: usize) -> Result<Vec<u8>, JwtError> {
    if segment.is_empty()
        || segment.len() % 4 == 1
        || !segment
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(JwtError::InvalidEncoding);
    }
    let decoded_bytes = segment.len() / 4 * 3
        + match segment.len() % 4 {
            2 => 1,
            3 => 2,
            _ => 0,
        };
    if decoded_bytes > maximum_bytes {
        return Err(JwtError::TooLarge);
    }
    let decoded = URL_SAFE_NO_PAD
        .decode(segment)
        .map_err(|_| JwtError::InvalidEncoding)?;
    if URL_SAFE_NO_PAD.encode(&decoded) != segment {
        return Err(JwtError::InvalidEncoding);
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests;

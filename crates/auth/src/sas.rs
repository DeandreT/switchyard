use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use percent_encoding::percent_decode_str;
use sha2::Sha256;
use thiserror::Error;

use crate::{
    Permission, PermissionSet, ResourceScope, ResourceScopeError, SharedAccessPolicy,
    policy::validate_percent_encoding,
};

const TOKEN_PREFIX: &str = "SharedAccessSignature ";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccessGrant {
    subject: String,
    scope: ResourceScope,
    expires_at_epoch_seconds: u64,
    permissions: PermissionSet,
}

impl AccessGrant {
    pub fn subject(&self) -> &str {
        &self.subject
    }

    pub fn scope(&self) -> &ResourceScope {
        &self.scope
    }

    pub fn expires_at_epoch_seconds(&self) -> u64 {
        self.expires_at_epoch_seconds
    }

    pub fn permissions(&self) -> PermissionSet {
        self.permissions
    }

    pub fn allows(
        &self,
        requested: &ResourceScope,
        permission: Permission,
        now_epoch_seconds: u64,
    ) -> bool {
        now_epoch_seconds < self.expires_at_epoch_seconds
            && self.scope.contains(requested)
            && self.permissions.allows(permission)
    }
}

impl SharedAccessPolicy {
    pub fn authenticate_plain(
        &self,
        key_name: &str,
        presented_key: &str,
    ) -> Result<AccessGrant, SasError> {
        let rule = self.rule(key_name).ok_or(SasError::InvalidCredential)?;
        let valid = rule
            .keys()
            .map(|key| credential_matches(key.expose(), presented_key.as_bytes()))
            .fold(false, |either, matches| either | matches);
        if !valid {
            return Err(SasError::InvalidCredential);
        }
        Ok(AccessGrant {
            subject: key_name.to_owned(),
            scope: rule.scope().clone(),
            expires_at_epoch_seconds: u64::MAX,
            permissions: rule.permissions(),
        })
    }

    /// Validates one Service Bus shared-access token for the CBS audience.
    ///
    /// The HMAC input deliberately retains the token's encoded `sr` field.
    /// Re-encoding the decoded URI can change its bytes and invalidate a
    /// signature that Service Bus clients generated correctly.
    pub fn validate_sas(
        &self,
        token: &str,
        requested_audience: &str,
        now_epoch_seconds: u64,
    ) -> Result<AccessGrant, SasError> {
        let token = ParsedToken::parse(token)?;
        if token.expiry <= now_epoch_seconds {
            return Err(SasError::Expired);
        }

        let requested =
            ResourceScope::parse(requested_audience).map_err(|_| SasError::InvalidAudience)?;
        let token_scope =
            ResourceScope::parse(&token.resource).map_err(|_| SasError::InvalidAudience)?;
        if !token_scope.contains(&requested) {
            return Err(SasError::AudienceMismatch);
        }

        let rule = self.rule(&token.key_name).ok_or(SasError::UnknownRule)?;
        if !rule.scope().contains(&token_scope) {
            return Err(SasError::RuleScopeMismatch);
        }

        let signature = STANDARD
            .decode(token.signature.as_bytes())
            .map_err(|_| SasError::InvalidSignature)?;
        let string_to_sign = format!("{}\n{}", token.encoded_resource, token.expiry);
        let valid = rule
            .keys()
            .map(|key| signature_matches(key.expose(), string_to_sign.as_bytes(), &signature))
            .fold(false, |either, matches| either | matches);
        if !valid {
            return Err(SasError::InvalidSignature);
        }

        Ok(AccessGrant {
            subject: token.key_name,
            scope: token_scope,
            expires_at_epoch_seconds: token.expiry,
            permissions: rule.permissions(),
        })
    }
}

fn signature_matches(key: &[u8], input: &[u8], signature: &[u8]) -> bool {
    let Ok(mut hmac) = Hmac::<Sha256>::new_from_slice(key) else {
        return false;
    };
    hmac.update(input);
    hmac.verify_slice(signature).is_ok()
}

fn credential_matches(expected: &[u8], presented: &[u8]) -> bool {
    const PROOF: &[u8] = b"switchyard-sasl-plain-credential";
    let (Ok(mut expected_hmac), Ok(mut presented_hmac)) = (
        Hmac::<Sha256>::new_from_slice(expected),
        Hmac::<Sha256>::new_from_slice(presented),
    ) else {
        return false;
    };
    expected_hmac.update(PROOF);
    presented_hmac.update(PROOF);
    expected_hmac
        .verify_slice(&presented_hmac.finalize().into_bytes())
        .is_ok()
}

struct ParsedToken<'a> {
    encoded_resource: &'a str,
    resource: String,
    signature: String,
    expiry: u64,
    key_name: String,
}

impl<'a> ParsedToken<'a> {
    fn parse(token: &'a str) -> Result<Self, SasError> {
        let fields = token
            .strip_prefix(TOKEN_PREFIX)
            .ok_or(SasError::Malformed)?;
        let mut resource = None;
        let mut signature = None;
        let mut expiry = None;
        let mut key_name = None;

        for field in fields.split('&') {
            let (name, value) = field.split_once('=').ok_or(SasError::Malformed)?;
            let destination = match name {
                "sr" => &mut resource,
                "sig" => &mut signature,
                "se" => &mut expiry,
                "skn" => &mut key_name,
                _ => return Err(SasError::UnknownField),
            };
            if destination.replace(value).is_some() {
                return Err(SasError::DuplicateField);
            }
        }

        let encoded_resource = resource.ok_or(SasError::MissingField("sr"))?;
        let signature = decode_component(signature.ok_or(SasError::MissingField("sig"))?)?;
        let encoded_expiry = expiry.ok_or(SasError::MissingField("se"))?;
        if encoded_expiry.is_empty() || !encoded_expiry.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(SasError::InvalidExpiration);
        }
        let expiry = encoded_expiry
            .parse()
            .map_err(|_| SasError::InvalidExpiration)?;
        let key_name = decode_component(key_name.ok_or(SasError::MissingField("skn"))?)?;
        let resource = decode_component(encoded_resource)?;

        Ok(Self {
            encoded_resource,
            resource,
            signature,
            expiry,
            key_name,
        })
    }
}

fn decode_component(value: &str) -> Result<String, SasError> {
    validate_percent_encoding(value).map_err(|_| SasError::InvalidEncoding)?;
    percent_decode_str(value)
        .decode_utf8()
        .map(|value| value.into_owned())
        .map_err(|_| SasError::InvalidEncoding)
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SasError {
    #[error("the shared-access token is malformed")]
    Malformed,
    #[error("the shared-access token contains an unknown field")]
    UnknownField,
    #[error("the shared-access token contains a field more than once")]
    DuplicateField,
    #[error("the shared-access token is missing {0}")]
    MissingField(&'static str),
    #[error("the shared-access token contains invalid percent encoding")]
    InvalidEncoding,
    #[error("the shared-access token expiration is invalid")]
    InvalidExpiration,
    #[error("the shared-access token has expired")]
    Expired,
    #[error("the shared-access token names an invalid audience")]
    InvalidAudience,
    #[error("the shared-access token does not cover the requested audience")]
    AudienceMismatch,
    #[error("the shared-access rule does not cover the token audience")]
    RuleScopeMismatch,
    #[error("the shared-access token names an unknown rule")]
    UnknownRule,
    #[error("the shared-access token signature is invalid")]
    InvalidSignature,
    #[error("the shared-access credential is invalid")]
    InvalidCredential,
}

impl From<ResourceScopeError> for SasError {
    fn from(_: ResourceScopeError) -> Self {
        Self::InvalidAudience
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SharedAccessKey, SharedAccessRule};

    const HOST: &str = "tenant.servicebus.windows.net";
    const ORDERS: &str = "amqps://tenant.servicebus.windows.net/orders";
    const ENCODED_ORDERS: &str = "amqps%3A%2F%2Ftenant.servicebus.windows.net%2Forders";
    const EXPIRY: u64 = 2_000_000_000;
    const KNOWN_TOKEN: &str = "SharedAccessSignature \
        sr=amqps%3A%2F%2Ftenant.servicebus.windows.net%2Forders&\
        sig=R8KtgcCb7NeOCrECrMXtQ13KLGC8CiJYw0fUnUQCznw%3D&\
        se=2000000000&skn=send";

    fn policy(scope: ResourceScope, primary: &str, secondary: Option<&str>) -> SharedAccessPolicy {
        let rule = SharedAccessRule::new(
            "send",
            scope,
            SharedAccessKey::new(primary).unwrap(),
            secondary.map(|key| SharedAccessKey::new(key).unwrap()),
            PermissionSet::SEND,
        )
        .unwrap();
        SharedAccessPolicy::new([rule]).unwrap()
    }

    fn token(encoded_resource: &str, key_name: &str, expiry: u64, key: &str) -> String {
        let input = format!("{encoded_resource}\n{expiry}");
        let mut hmac = Hmac::<Sha256>::new_from_slice(key.as_bytes()).unwrap();
        hmac.update(input.as_bytes());
        let signature = STANDARD.encode(hmac.finalize().into_bytes());
        let signature = signature
            .replace('+', "%2B")
            .replace('/', "%2F")
            .replace('=', "%3D");
        format!(
            "SharedAccessSignature sr={encoded_resource}&sig={signature}&se={expiry}&skn={key_name}"
        )
    }

    #[test]
    fn a_known_service_bus_token_is_valid() {
        let policy = policy(ResourceScope::namespace(HOST).unwrap(), "secret", None);
        let grant = policy
            .validate_sas(KNOWN_TOKEN, ORDERS, EXPIRY - 1)
            .unwrap();

        assert_eq!(grant.subject(), "send");
        assert!(grant.permissions().allows(Permission::Send));
        assert!(
            grant
                .scope()
                .contains(&ResourceScope::parse(ORDERS).unwrap())
        );
    }

    #[test]
    fn either_rotation_key_can_sign() {
        let policy = policy(
            ResourceScope::namespace(HOST).unwrap(),
            "old-key",
            Some("new-key"),
        );
        let signed = token(ENCODED_ORDERS, "send", EXPIRY, "new-key");

        assert!(policy.validate_sas(&signed, ORDERS, EXPIRY - 1).is_ok());
    }

    #[test]
    fn plain_accepts_either_key_without_disclosing_which_part_failed() {
        let policy = policy(
            ResourceScope::namespace(HOST).unwrap(),
            "old-key",
            Some("new-key"),
        );

        assert!(policy.authenticate_plain("send", "old-key").is_ok());
        assert!(policy.authenticate_plain("send", "new-key").is_ok());
        assert_eq!(
            policy.authenticate_plain("send", "wrong"),
            Err(SasError::InvalidCredential)
        );
        assert_eq!(
            policy.authenticate_plain("unknown", "old-key"),
            Err(SasError::InvalidCredential)
        );
    }

    #[test]
    fn tampering_and_expiry_fail_closed() {
        let policy = policy(ResourceScope::namespace(HOST).unwrap(), "secret", None);
        let tampered = KNOWN_TOKEN.replace("R8Kt", "A8Kt");

        assert_eq!(
            policy.validate_sas(&tampered, ORDERS, EXPIRY - 1),
            Err(SasError::InvalidSignature)
        );
        assert_eq!(
            policy.validate_sas(KNOWN_TOKEN, ORDERS, EXPIRY),
            Err(SasError::Expired)
        );
    }

    #[test]
    fn duplicate_and_malformed_fields_are_refused() {
        let policy = policy(ResourceScope::namespace(HOST).unwrap(), "secret", None);
        let duplicate = format!("{KNOWN_TOKEN}&se={EXPIRY}");
        let malformed = KNOWN_TOKEN.replace("%3A", "%XZ");

        assert_eq!(
            policy.validate_sas(&duplicate, ORDERS, EXPIRY - 1),
            Err(SasError::DuplicateField)
        );
        assert_eq!(
            policy.validate_sas(&malformed, ORDERS, EXPIRY - 1),
            Err(SasError::InvalidEncoding)
        );
    }

    #[test]
    fn an_entity_rule_cannot_mint_a_namespace_token() {
        let policy = policy(ResourceScope::parse(ORDERS).unwrap(), "secret", None);
        let encoded_namespace = "amqps%3A%2F%2Ftenant.servicebus.windows.net";
        let signed = token(encoded_namespace, "send", EXPIRY, "secret");

        assert_eq!(
            policy.validate_sas(&signed, ORDERS, EXPIRY - 1),
            Err(SasError::RuleScopeMismatch)
        );
    }

    #[test]
    fn a_token_never_authorizes_a_sibling_resource() {
        let policy = policy(ResourceScope::namespace(HOST).unwrap(), "secret", None);
        let sibling = "amqps://tenant.servicebus.windows.net/orders-archive";

        assert_eq!(
            policy.validate_sas(KNOWN_TOKEN, sibling, EXPIRY - 1),
            Err(SasError::AudienceMismatch)
        );
    }
}

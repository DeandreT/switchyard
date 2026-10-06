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
    issuer: GrantIssuer,
    scope: ResourceScope,
    expires_at_epoch_seconds: u64,
    permissions: PermissionSet,
}

#[derive(Clone, Eq, PartialEq)]
enum GrantIssuer {
    SharedAccess,
    Jwt(String),
}

impl std::fmt::Debug for GrantIssuer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::SharedAccess => "SharedAccess",
            Self::Jwt(_) => "Jwt(<redacted>)",
        })
    }
}

impl AccessGrant {
    /// Compares the verified issuer namespace and principal, not the resource scope.
    pub fn same_principal(&self, other: &Self) -> bool {
        self.issuer == other.issuer && self.subject == other.subject
    }

    pub(crate) fn verified_jwt(
        subject: String,
        issuer: String,
        scope: ResourceScope,
        expires_at_epoch_seconds: u64,
        permissions: PermissionSet,
    ) -> Self {
        Self {
            subject,
            issuer: GrantIssuer::Jwt(issuer),
            scope,
            expires_at_epoch_seconds,
            permissions,
        }
    }

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

    /// Converts an authenticated grant for use on AMQP transport endpoints only.
    pub fn into_amqp_scope(mut self) -> Self {
        self.scope = self.scope.into_amqp_scope();
        self
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
            issuer: GrantIssuer::SharedAccess,
            scope: rule.scope().clone(),
            expires_at_epoch_seconds: u64::MAX,
            permissions: rule.permissions(),
        })
    }

    /// Authenticates a token's own audience without authorizing a requested
    /// operation. Callers must check the returned grant with `AccessGrant::allows`.
    pub fn authenticate_sas(
        &self,
        token: &str,
        now_epoch_seconds: u64,
    ) -> Result<AccessGrant, SasError> {
        self.validate_sas_inner(token, None, now_epoch_seconds)
    }

    /// Validates one Service Bus shared-access token for the CBS audience.
    ///
    /// Positional transport control aliases apply only to CBS scope comparisons
    /// and the returned grant, never to native `authenticate_sas` grants.
    /// The HMAC input deliberately retains the token's encoded `sr` field.
    /// Re-encoding the decoded URI can change its bytes and invalidate a
    /// signature that Service Bus clients generated correctly.
    pub fn validate_sas(
        &self,
        token: &str,
        requested_audience: &str,
        now_epoch_seconds: u64,
    ) -> Result<AccessGrant, SasError> {
        self.validate_sas_inner(token, Some(requested_audience), now_epoch_seconds)
    }

    fn validate_sas_inner(
        &self,
        token: &str,
        requested_audience: Option<&str>,
        now_epoch_seconds: u64,
    ) -> Result<AccessGrant, SasError> {
        let token = ParsedToken::parse(token)?;
        if token.expiry <= now_epoch_seconds {
            return Err(SasError::Expired);
        }

        let requested = requested_audience
            .map(ResourceScope::parse)
            .transpose()
            .map_err(|_| SasError::InvalidAudience)?
            .map(ResourceScope::into_amqp_scope);
        let mut token_scope =
            ResourceScope::parse(&token.resource).map_err(|_| SasError::InvalidAudience)?;
        if requested_audience.is_some() {
            token_scope = token_scope.into_amqp_scope();
        }
        if requested
            .as_ref()
            .is_some_and(|requested| !token_scope.contains(requested))
        {
            return Err(SasError::AudienceMismatch);
        }

        let rule = self.rule(&token.key_name).ok_or(SasError::UnknownRule)?;
        let amqp_rule_scope = requested_audience.map(|_| rule.scope().clone().into_amqp_scope());
        let rule_scope = amqp_rule_scope.as_ref().unwrap_or(rule.scope());
        if !rule_scope.contains(&token_scope) {
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
            issuer: GrantIssuer::SharedAccess,
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
    fn native_signed_scopes_remain_literal_while_cbs_controls_are_explicit_aliases() {
        let sdk_resource = format!("amqps://{HOST}/Orders/$Management");
        let canonical_resource = format!("amqps://{HOST}/Orders/$management");
        let sdk_scope = ResourceScope::parse(&sdk_resource).unwrap();
        let canonical_scope = ResourceScope::parse(&canonical_resource).unwrap();
        let sdk_encoded = "amqps%3A%2F%2Ftenant.servicebus.windows.net%2FOrders%2F%24Management";
        let canonical_encoded =
            "amqps%3A%2F%2Ftenant.servicebus.windows.net%2FOrders%2F%24management";
        let sdk_token = token(sdk_encoded, "send", EXPIRY, "secret");
        let canonical_token = token(canonical_encoded, "send", EXPIRY, "secret");
        let namespace_policy = policy(ResourceScope::namespace(HOST).unwrap(), "secret", None);
        let scoped_token = namespace_policy
            .authenticate_sas(&sdk_token, EXPIRY - 1)
            .unwrap();
        assert_eq!(scoped_token.scope(), &sdk_scope);
        assert!(!scoped_token.allows(&canonical_scope, Permission::Send, EXPIRY - 1));
        let policy = policy(sdk_scope.clone(), "secret", None);

        let native = policy.authenticate_sas(&sdk_token, EXPIRY - 1).unwrap();
        assert_eq!(native.scope(), &sdk_scope);
        assert!(native.allows(&sdk_scope, Permission::Send, EXPIRY - 1));
        assert!(!native.allows(&canonical_scope, Permission::Send, EXPIRY - 1));
        assert_eq!(
            policy.authenticate_sas(&canonical_token, EXPIRY - 1),
            Err(SasError::RuleScopeMismatch)
        );

        for (token, requested) in [
            (&sdk_token, canonical_resource.as_str()),
            (&canonical_token, sdk_resource.as_str()),
        ] {
            let cbs = policy.validate_sas(token, requested, EXPIRY - 1).unwrap();
            assert_eq!(cbs.scope(), &canonical_scope);
            assert!(cbs.allows(&canonical_scope, Permission::Send, EXPIRY - 1));
        }

        let plain = policy.authenticate_plain("send", "secret").unwrap();
        assert_eq!(plain.scope(), &sdk_scope);
        assert!(!plain.allows(&canonical_scope, Permission::Send, EXPIRY - 1));
        assert!(
            plain
                .into_amqp_scope()
                .allows(&canonical_scope, Permission::Send, EXPIRY - 1)
        );
    }

    #[test]
    fn scoped_subscription_aliases_verify_only_the_original_signed_resource_bytes() {
        let policy = policy(
            ResourceScope::entity(HOST, "Orders/subscriptions/Accounting").unwrap(),
            "secret",
            None,
        );
        let sdk =
            "amqps%3A%2F%2Ftenant.servicebus.windows.net%2FOrders%2FSubscriptions%2FAccounting";
        let signed = token(sdk, "send", EXPIRY, "secret");
        let grant = policy
            .validate_sas(
                &signed,
                "amqps://tenant.servicebus.windows.net/Orders/subscriptions/Accounting",
                EXPIRY - 1,
            )
            .unwrap();
        assert!(
            grant.allows(
                &ResourceScope::entity(HOST, "Orders/SUBSCRIPTIONS/Accounting/$DeadLetterQueue")
                    .unwrap()
                    .into_amqp_scope(),
                Permission::Send,
                EXPIRY - 1,
            )
        );
        let changed_signed_resource = signed.replace("%2FSubscriptions%2F", "%2Fsubscriptions%2F");
        assert_eq!(
            policy.validate_sas(
                &changed_signed_resource,
                "amqps://tenant.servicebus.windows.net/Orders/subscriptions/Accounting",
                EXPIRY - 1,
            ),
            Err(SasError::InvalidSignature)
        );
        let encoded_control =
            "amqps%3A%2F%2Ftenant.servicebus.windows.net%2FOrders%2F%2553ubscriptions%2FAccounting";
        assert!(
            policy
                .validate_sas(
                    &token(encoded_control, "send", EXPIRY, "secret"),
                    "amqps://tenant.servicebus.windows.net/Orders/subscriptions/Accounting",
                    EXPIRY - 1,
                )
                .is_ok()
        );
    }

    #[test]
    fn subscription_tokens_do_not_alias_user_case_hosts_or_sibling_resources() {
        let policy = policy(
            ResourceScope::entity(HOST, "Orders/subscriptions/Accounting").unwrap(),
            "secret",
            None,
        );
        let sdk =
            "amqps%3A%2F%2Ftenant.servicebus.windows.net%2FOrders%2FSubscriptions%2FAccounting";
        let signed = token(sdk, "send", EXPIRY, "secret");
        let grant = policy
            .validate_sas(
                &signed,
                "amqps://tenant.servicebus.windows.net/Orders/subscriptions/Accounting",
                EXPIRY - 1,
            )
            .unwrap();
        for requested in [
            "amqps://tenant.servicebus.windows.net/orders/subscriptions/Accounting",
            "amqps://tenant.servicebus.windows.net/Orders/subscriptions/accounting",
            "amqps://tenant.servicebus.windows.net/Orders/subscriptions/Accounting-old",
            "amqps://tenant.servicebus.windows.net/Orders/subscriptions/Billing",
            "amqps://tenant.servicebus.windows.net/Orders-archive/subscriptions/Accounting",
            "amqps://other.servicebus.windows.net/Orders/subscriptions/Accounting",
        ] {
            assert_eq!(
                policy.validate_sas(&signed, requested, EXPIRY - 1),
                Err(SasError::AudienceMismatch),
                "{requested}"
            );
            assert!(!grant.allows(
                &ResourceScope::parse(requested).unwrap().into_amqp_scope(),
                Permission::Send,
                EXPIRY - 1
            ));
        }
        for resource in [
            sdk.replace("%2FOrders%2F", "%2Forders%2F"),
            sdk.replace("%2FAccounting", "%2Faccounting"),
            sdk.replace("tenant.servicebus", "other.servicebus"),
        ] {
            let requested = percent_decode_str(&resource).decode_utf8().unwrap();
            assert_eq!(
                policy.validate_sas(
                    &token(&resource, "send", EXPIRY, "secret"),
                    requested.as_ref(),
                    EXPIRY - 1,
                ),
                Err(SasError::RuleScopeMismatch)
            );
        }
    }

    #[test]
    fn management_only_token_does_not_authorize_a_base_or_dead_letter_receiver() {
        let policy = policy(ResourceScope::namespace(HOST).unwrap(), "secret", None);
        let sdk = "amqps%3A%2F%2Ftenant.servicebus.windows.net%2FOrders%2FSubscriptions%2FAccounting%2F%24management";
        let signed = token(sdk, "send", EXPIRY, "secret");
        let grant = policy
            .validate_sas(
                &signed,
                "amqps://tenant.servicebus.windows.net/Orders/subscriptions/Accounting/$management",
                EXPIRY - 1,
            )
            .unwrap();
        assert!(
            grant.allows(
                &ResourceScope::entity(HOST, "Orders/subscriptions/Accounting/$management")
                    .unwrap()
                    .into_amqp_scope(),
                Permission::Send,
                EXPIRY - 1
            )
        );
        for path in [
            "Orders/subscriptions/Accounting",
            "Orders/subscriptions/Accounting/$deadletterqueue",
            "Orders/subscriptions/Billing/$management",
            "Orders",
        ] {
            assert!(!grant.allows(
                &ResourceScope::entity(HOST, path).unwrap().into_amqp_scope(),
                Permission::Send,
                EXPIRY - 1
            ));
        }
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

    #[test]
    fn authentication_returns_the_verified_token_scope_and_permissions() {
        let policy = policy(ResourceScope::namespace(HOST).unwrap(), "secret", None);
        let grant = policy.authenticate_sas(KNOWN_TOKEN, EXPIRY - 1).unwrap();

        assert_eq!(grant.subject(), "send");
        assert_eq!(grant.scope(), &ResourceScope::parse(ORDERS).unwrap());
        assert_eq!(grant.expires_at_epoch_seconds(), EXPIRY);
        assert_eq!(grant.permissions(), PermissionSet::SEND);
        assert_eq!(
            grant,
            policy
                .validate_sas(KNOWN_TOKEN, ORDERS, EXPIRY - 1)
                .unwrap()
        );
    }

    #[test]
    fn authentication_is_not_authorization_for_a_sibling_or_namespace() {
        let policy = policy(ResourceScope::namespace(HOST).unwrap(), "secret", None);
        let grant = policy.authenticate_sas(KNOWN_TOKEN, EXPIRY - 1).unwrap();
        let sibling =
            ResourceScope::parse("amqps://tenant.servicebus.windows.net/orders-archive").unwrap();
        let namespace = ResourceScope::namespace(HOST).unwrap();
        let requested = ResourceScope::parse(ORDERS).unwrap();

        assert!(!grant.allows(&sibling, Permission::Send, EXPIRY - 1));
        assert!(!grant.allows(&namespace, Permission::Send, EXPIRY - 1));
        assert!(!grant.allows(&requested, Permission::Manage, EXPIRY - 1));
        assert!(!grant.allows(&requested, Permission::Send, EXPIRY));
        assert!(grant.allows(&requested, Permission::Send, EXPIRY - 1));
        assert_eq!(
            policy.validate_sas(
                KNOWN_TOKEN,
                "amqps://tenant.servicebus.windows.net/orders-archive",
                EXPIRY - 1
            ),
            Err(SasError::AudienceMismatch)
        );
    }

    #[test]
    fn authentication_accepts_rotated_keys_and_rejects_forged_signatures() {
        let policy = policy(
            ResourceScope::namespace(HOST).unwrap(),
            "old-key",
            Some("new-key"),
        );
        for key in ["old-key", "new-key"] {
            let signed = token(ENCODED_ORDERS, "send", EXPIRY, key);
            assert!(policy.authenticate_sas(&signed, EXPIRY - 1).is_ok());
        }

        let forged = token(ENCODED_ORDERS, "send", EXPIRY, "unknown-key");
        assert_eq!(
            policy.authenticate_sas(&forged, EXPIRY - 1),
            Err(SasError::InvalidSignature)
        );
        let invalid_base64 = KNOWN_TOKEN.replace(
            "R8KtgcCb7NeOCrECrMXtQ13KLGC8CiJYw0fUnUQCznw%3D",
            "not-base64",
        );
        assert_eq!(
            policy.authenticate_sas(&invalid_base64, EXPIRY - 1),
            Err(SasError::InvalidSignature)
        );
    }

    #[test]
    fn authentication_rejects_unknown_rules_and_rule_scope_escalation() {
        let namespace_policy = policy(ResourceScope::namespace(HOST).unwrap(), "secret", None);
        let unknown_rule = token(ENCODED_ORDERS, "unknown", EXPIRY, "secret");
        assert_eq!(
            namespace_policy.authenticate_sas(&unknown_rule, EXPIRY - 1),
            Err(SasError::UnknownRule)
        );

        let entity_policy = policy(ResourceScope::parse(ORDERS).unwrap(), "secret", None);
        let namespace_token = token(
            "amqps%3A%2F%2Ftenant.servicebus.windows.net",
            "send",
            EXPIRY,
            "secret",
        );
        let sibling_token = token(
            "amqps%3A%2F%2Ftenant.servicebus.windows.net%2Forders-archive",
            "send",
            EXPIRY,
            "secret",
        );
        for signed in [namespace_token, sibling_token] {
            assert_eq!(
                entity_policy.authenticate_sas(&signed, EXPIRY - 1),
                Err(SasError::RuleScopeMismatch)
            );
        }
    }

    #[test]
    fn authentication_reuses_the_strict_token_parser_and_expiration_check() {
        let policy = policy(ResourceScope::namespace(HOST).unwrap(), "secret", None);
        let cases = [
            ("not-a-token".to_owned(), SasError::Malformed),
            (
                format!("{KNOWN_TOKEN}&se={EXPIRY}"),
                SasError::DuplicateField,
            ),
            (
                format!("{KNOWN_TOKEN}&unexpected=x"),
                SasError::UnknownField,
            ),
            (KNOWN_TOKEN.replace("%3A", "%XZ"), SasError::InvalidEncoding),
            (
                KNOWN_TOKEN.replace("se=2000000000", "se=-1"),
                SasError::InvalidExpiration,
            ),
            (
                KNOWN_TOKEN.replace("&skn=send", ""),
                SasError::MissingField("skn"),
            ),
        ];
        for (token, error) in cases {
            assert_eq!(policy.authenticate_sas(&token, EXPIRY - 1), Err(error));
        }
        for now in [EXPIRY, EXPIRY + 1, u64::MAX] {
            assert_eq!(
                policy.authenticate_sas(KNOWN_TOKEN, now),
                Err(SasError::Expired)
            );
        }

        let invalid_audience = token("not-an-audience", "send", EXPIRY, "secret");
        assert_eq!(
            policy.authenticate_sas(&invalid_audience, EXPIRY - 1),
            Err(SasError::InvalidAudience)
        );
    }

    #[test]
    fn requested_audience_validation_retains_its_error_precedence() {
        let policy = policy(ResourceScope::namespace(HOST).unwrap(), "secret", None);
        assert_eq!(
            policy.validate_sas(KNOWN_TOKEN, "invalid", EXPIRY),
            Err(SasError::Expired)
        );
        assert_eq!(
            policy.validate_sas(KNOWN_TOKEN, "invalid", EXPIRY - 1),
            Err(SasError::InvalidAudience)
        );
        let forged = token(ENCODED_ORDERS, "send", EXPIRY, "forged-key");
        assert_eq!(
            policy.validate_sas(
                &forged,
                "amqps://tenant.servicebus.windows.net/orders-archive",
                EXPIRY - 1
            ),
            Err(SasError::AudienceMismatch)
        );
    }
}

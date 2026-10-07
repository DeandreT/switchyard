use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use url::Url;

use super::*;
use crate::{SharedAccessKey, SharedAccessRule};

const HOST: &str = "tenant.servicebus.windows.net";
const EXPIRY: u64 = 2_000_000_000;
const KEY: &str = "secret";

fn policy(
    scope: ResourceScope,
    permissions: PermissionSet,
    primary: &str,
    secondary: Option<&str>,
) -> SharedAccessPolicy {
    SharedAccessPolicy::new([SharedAccessRule::new(
        "manage",
        scope,
        SharedAccessKey::new(primary).expect("primary key"),
        secondary.map(|value| SharedAccessKey::new(value).expect("secondary key")),
        permissions,
    )
    .expect("rule")])
    .expect("policy")
}

fn namespace_policy() -> SharedAccessPolicy {
    policy(
        ResourceScope::namespace(HOST).expect("namespace"),
        PermissionSet::MANAGE,
        KEY,
        None,
    )
}

fn encoded(value: &str) -> String {
    utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
}

fn sign_encoded(resource: &str, key_name: &str, expiry: u64, key: &str) -> String {
    let input = format!("{resource}\n{expiry}");
    let mut hmac = Hmac::<Sha256>::new_from_slice(key.as_bytes()).expect("HMAC key");
    hmac.update(input.as_bytes());
    let signature = encoded(&STANDARD.encode(hmac.finalize().into_bytes()));
    format!(
        "SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn={}",
        encoded(key_name)
    )
}

fn token(resource: &str) -> String {
    sign_encoded(&encoded(resource), "manage", EXPIRY, KEY)
}

#[test]
fn atom_namespace_audience_accepts_default_https_port_and_requires_manage_at_use() {
    let policy = namespace_policy();
    let namespace = ResourceScope::namespace(HOST).expect("namespace");
    let entity = ResourceScope::entity(HOST, "orders").expect("entity");
    for resource in [
        format!("https://{HOST}"),
        format!("https://{HOST}/"),
        format!("https://{HOST}:443/"),
        format!("HTTPS://{}/", HOST.to_ascii_uppercase()),
    ] {
        let grant = policy
            .authenticate_atom_sas(&token(&resource), EXPIRY - 1)
            .expect("HTTPS grant");
        assert_eq!(grant.scope(), &namespace);
        assert_eq!(grant.subject(), "manage");
        assert_eq!(grant.expires_at_epoch_seconds(), EXPIRY);
        assert!(grant.allows(&namespace, Permission::Manage, EXPIRY - 1));
        assert!(grant.allows(&entity, Permission::Manage, EXPIRY - 1));
        assert!(!grant.allows(&entity, Permission::Manage, EXPIRY));
        assert!(!grant.allows(
            &ResourceScope::entity("foreign.servicebus.windows.net", "orders").expect("foreign"),
            Permission::Manage,
            EXPIRY - 1,
        ));
    }
}

#[test]
fn atom_namespace_grant_is_not_bound_to_the_request_socket_port() {
    let grant = namespace_policy()
        .authenticate_atom_sas(&token(&format!("https://{HOST}/")), EXPIRY - 1)
        .expect("namespace grant");
    let request_uri = Url::parse(&format!("https://{HOST}:43127/orders")).expect("request URI");
    assert_eq!(request_uri.host_str(), Some(HOST));
    assert_eq!(request_uri.port(), Some(43127));
    let trusted_operation = ResourceScope::entity(HOST, "orders").expect("configured operation");
    assert!(grant.allows(&trusted_operation, Permission::Manage, EXPIRY - 1));
    assert_eq!(
        namespace_policy().authenticate_atom_sas(&token(request_uri.as_str()), EXPIRY - 1),
        Err(SasError::InvalidAudience)
    );
}

#[test]
fn atom_https_and_native_cbs_token_profiles_do_not_accept_each_others_schemes() {
    let policy = namespace_policy();
    let https = token(&format!("https://{HOST}/orders"));
    let amqps = token(&format!("amqps://{HOST}/orders"));
    assert!(policy.authenticate_atom_sas(&https, EXPIRY - 1).is_ok());
    assert_eq!(
        policy.authenticate_sas(&https, EXPIRY - 1),
        Err(SasError::InvalidAudience)
    );
    assert_eq!(
        policy.validate_sas(&https, &format!("amqps://{HOST}/orders"), EXPIRY - 1),
        Err(SasError::InvalidAudience)
    );
    assert!(policy.authenticate_sas(&amqps, EXPIRY - 1).is_ok());
    assert!(
        policy
            .validate_sas(&amqps, &format!("amqps://{HOST}/orders"), EXPIRY - 1)
            .is_ok()
    );
    assert_eq!(
        policy.authenticate_atom_sas(&amqps, EXPIRY - 1),
        Err(SasError::InvalidAudience)
    );
}

#[test]
fn atom_grants_keep_literal_case_controls_and_exact_hierarchy_boundaries() {
    let policy = namespace_policy();
    for literal in [
        "Orders/$Management",
        "Orders/$DeadLetterQueue",
        "Orders/Subscriptions/Accounting",
    ] {
        let scope = ResourceScope::entity(HOST, literal).expect("literal scope");
        let grant = policy
            .authenticate_atom_sas(&token(&format!("https://{HOST}/{literal}")), EXPIRY - 1)
            .expect("grant");
        assert_eq!(grant.scope(), &scope);
        assert!(grant.allows(&scope, Permission::Manage, EXPIRY - 1));
        assert!(!grant.allows(
            &scope.clone().into_amqp_scope(),
            Permission::Manage,
            EXPIRY - 1
        ));
    }
    let grant = policy
        .authenticate_atom_sas(&token(&format!("https://{HOST}/Orders")), EXPIRY - 1)
        .expect("Orders grant");
    assert!(grant.allows(
        &ResourceScope::entity(HOST, "Orders/child").expect("child"),
        Permission::Manage,
        EXPIRY - 1
    ));
    for sibling in ["orders", "Orders-old", "Other", "orders/child"] {
        assert!(!grant.allows(
            &ResourceScope::entity(HOST, sibling).expect("sibling"),
            Permission::Manage,
            EXPIRY - 1
        ));
    }
}

#[test]
fn atom_entity_grant_cannot_list_namespace_or_mint_a_namespace_scope() {
    let entity_scope = ResourceScope::entity(HOST, "orders").expect("entity");
    let entity_policy = policy(entity_scope.clone(), PermissionSet::MANAGE, KEY, None);
    let grant = entity_policy
        .authenticate_atom_sas(&token(&format!("https://{HOST}/orders")), EXPIRY - 1)
        .expect("entity grant");
    assert!(grant.allows(&entity_scope, Permission::Manage, EXPIRY - 1));
    assert!(!grant.allows(
        &ResourceScope::namespace(HOST).expect("collection scope"),
        Permission::Manage,
        EXPIRY - 1
    ));
    for resource in [
        format!("https://{HOST}/"),
        format!("https://{HOST}/orders-old"),
        "https://foreign.servicebus.windows.net/orders".to_owned(),
    ] {
        assert_eq!(
            entity_policy.authenticate_atom_sas(&token(&resource), EXPIRY - 1),
            Err(SasError::RuleScopeMismatch)
        );
    }
}

#[test]
fn atom_send_and_listen_grants_are_authenticated_but_do_not_authorize_manage() {
    let scope = ResourceScope::entity(HOST, "orders").expect("entity");
    for permissions in [
        PermissionSet::SEND,
        PermissionSet::LISTEN,
        PermissionSet::SEND | PermissionSet::LISTEN,
    ] {
        let policy = policy(
            ResourceScope::namespace(HOST).expect("namespace"),
            permissions,
            KEY,
            None,
        );
        let grant = policy
            .authenticate_atom_sas(&token(&format!("https://{HOST}/orders")), EXPIRY - 1)
            .expect("authenticated only");
        assert_eq!(grant.permissions(), permissions);
        assert!(!grant.allows(&scope, Permission::Manage, EXPIRY - 1));
    }
}

#[test]
fn atom_encoded_resource_hmac_keeps_original_bytes_and_decodes_path_once() {
    let policy = namespace_policy();
    let resource = format!("https://{HOST}/orders");
    let original = encoded(&resource);
    let alternate = original.replace("%3A", "%3a");
    assert_ne!(original, alternate);
    let signed = sign_encoded(&original, "manage", EXPIRY, KEY);
    let tampered = signed.replacen(&format!("sr={original}"), &format!("sr={alternate}"), 1);
    assert_eq!(
        policy.authenticate_atom_sas(&tampered, EXPIRY - 1),
        Err(SasError::InvalidSignature)
    );
    let original_grant = policy
        .authenticate_atom_sas(&signed, EXPIRY - 1)
        .expect("original");
    let alternate_grant = policy
        .authenticate_atom_sas(&sign_encoded(&alternate, "manage", EXPIRY, KEY), EXPIRY - 1)
        .expect("alternate valid encoding");
    assert_eq!(original_grant, alternate_grant);
    let escaped_letter = policy
        .authenticate_atom_sas(&token(&format!("https://{HOST}/%6Frders")), EXPIRY - 1)
        .expect("escaped letter");
    assert_eq!(escaped_letter.scope(), original_grant.scope());
    let literal_percent = policy
        .authenticate_atom_sas(
            &token(&format!("https://{HOST}/orders%252fchild")),
            EXPIRY - 1,
        )
        .expect("literal percent sequence");
    assert!(literal_percent.allows(
        &ResourceScope::entity(HOST, "orders%2fchild").expect("literal percent"),
        Permission::Manage,
        EXPIRY - 1
    ));
    assert!(!literal_percent.allows(
        &ResourceScope::entity(HOST, "orders/child").expect("different hierarchy"),
        Permission::Manage,
        EXPIRY - 1
    ));
}

#[test]
fn atom_rotation_and_utf8_key_text_share_existing_signature_validation() {
    let utf8_key = "cl\u{e9}-\u{96ea}";
    let policy = policy(
        ResourceScope::namespace(HOST).expect("namespace"),
        PermissionSet::MANAGE,
        utf8_key,
        Some("c2VjcmV0"),
    );
    let resource = encoded(&format!("https://{HOST}/orders"));
    for key in [utf8_key, "c2VjcmV0"] {
        assert!(
            policy
                .authenticate_atom_sas(&sign_encoded(&resource, "manage", EXPIRY, key), EXPIRY - 1)
                .is_ok()
        );
    }
    for wrong in [KEY, "unknown-key"] {
        assert_eq!(
            policy.authenticate_atom_sas(
                &sign_encoded(&resource, "manage", EXPIRY, wrong),
                EXPIRY - 1
            ),
            Err(SasError::InvalidSignature)
        );
    }
    assert_eq!(
        policy.authenticate_atom_sas(
            &sign_encoded(&resource, "unknown", EXPIRY, utf8_key),
            EXPIRY - 1
        ),
        Err(SasError::UnknownRule)
    );
}

#[test]
fn atom_utf8_literal_paths_are_compared_without_case_folding_or_reencoding() {
    let literal = "Orders/caf\u{e9}";
    let expected = ResourceScope::entity(HOST, literal).expect("UTF-8 scope");
    let policy = policy(expected.clone(), PermissionSet::MANAGE, KEY, None);
    for resource in [
        format!("https://{HOST}/{literal}"),
        format!("https://{HOST}/Orders/caf%C3%A9"),
    ] {
        let grant = policy
            .authenticate_atom_sas(&token(&resource), EXPIRY - 1)
            .expect("UTF-8 grant");
        assert_eq!(grant.scope(), &expected);
        assert!(grant.allows(&expected, Permission::Manage, EXPIRY - 1));
        assert!(!grant.allows(
            &ResourceScope::entity(HOST, "Orders/CAF\u{c9}").expect("different case"),
            Permission::Manage,
            EXPIRY - 1
        ));
    }
}

#[test]
fn atom_strict_token_fields_and_exact_expiry_reuse_existing_errors() {
    let policy = namespace_policy();
    let signed = token(&format!("https://{HOST}/orders"));
    for (token, expected) in [
        ("not-a-token".to_owned(), SasError::Malformed),
        (format!("{signed}&se={EXPIRY}"), SasError::DuplicateField),
        (format!("{signed}&unknown=x"), SasError::UnknownField),
        (signed.replace("%3A", "%GG"), SasError::InvalidEncoding),
        (
            signed.replace("se=2000000000", "se=-1"),
            SasError::InvalidExpiration,
        ),
        (
            signed.replace("&skn=manage", ""),
            SasError::MissingField("skn"),
        ),
        (
            signed.replacen("sig=", "sig=not-base64", 1),
            SasError::InvalidSignature,
        ),
    ] {
        assert_eq!(
            policy.authenticate_atom_sas(&token, EXPIRY - 1),
            Err(expected)
        );
    }
    for now in [EXPIRY, EXPIRY + 1, u64::MAX] {
        assert_eq!(
            policy.authenticate_atom_sas(&signed, now),
            Err(SasError::Expired)
        );
    }
    let invalid_audience = sign_encoded(&encoded("not-a-uri"), "manage", EXPIRY, KEY);
    assert_eq!(
        policy.authenticate_atom_sas(&invalid_audience, EXPIRY),
        Err(SasError::Expired)
    );
    assert_eq!(
        policy.authenticate_atom_sas(&invalid_audience, EXPIRY - 1),
        Err(SasError::InvalidAudience)
    );
}

#[test]
fn atom_uri_profile_rejects_authority_controls_and_pre_normalization_path_ambiguity() {
    let policy = namespace_policy();
    for resource in [
        "http://tenant.servicebus.windows.net/orders",
        "amqps://tenant.servicebus.windows.net/orders",
        "https://tenant.servicebus.windows.net:444/orders",
        "https://tenant.servicebus.windows.net:43127/orders",
        "https://user@tenant.servicebus.windows.net/orders",
        "https://user:password@tenant.servicebus.windows.net/orders",
        "https://@tenant.servicebus.windows.net/orders",
        "https://%74enant.servicebus.windows.net/orders",
        "https://tenant.servicebus.windows.net/orders?x=1",
        "https://tenant.servicebus.windows.net/orders#fragment",
        " https://tenant.servicebus.windows.net/orders",
        "https://tenant.servicebus.windows.net/orders ",
        "https://tenant.servicebus.windows.net/ord\ters",
        "https://tenant.servicebus.windows.net/ord\ners",
        "https://tenant.servicebus.windows.net/ord\\ers",
        "https://tenant.servicebus.windows.net//",
        "https://tenant.servicebus.windows.net/orders//child",
        "https://tenant.servicebus.windows.net/orders/./child",
        "https://tenant.servicebus.windows.net/orders/../child",
        "https://tenant.servicebus.windows.net/%2e/orders",
        "https://tenant.servicebus.windows.net/orders/%2E%2e/child",
        "https://tenant.servicebus.windows.net/orders/.%2e/child",
        "https://tenant.servicebus.windows.net/orders/%2e./child",
        "https://tenant.servicebus.windows.net/orders%2fchild",
        "https://tenant.servicebus.windows.net/orders%2Fchild",
        "https://tenant.servicebus.windows.net/orders%5cchild",
        "https://tenant.servicebus.windows.net/orders%5Cchild",
        "https://tenant.servicebus.windows.net/orders%00child",
        "https://tenant.servicebus.windows.net/orders%09child",
        "https://tenant.servicebus.windows.net/orders%0achild",
        "https://tenant.servicebus.windows.net/orders%7fchild",
        "https://tenant.servicebus.windows.net/orders%C2%85child",
        "https://tenant.servicebus.windows.net/orders%FF",
        "https://tenant.servicebus.windows.net/orders%GG",
        "https://tenant.servicebus.windows.net/orders%2",
    ] {
        assert_eq!(
            policy.authenticate_atom_sas(&token(resource), EXPIRY - 1),
            Err(SasError::InvalidAudience),
            "{resource}"
        );
    }
}

fn pad_expiry_to(token: &str, bytes: usize) -> String {
    assert!(bytes >= token.len());
    token.replace(
        "se=2000000000",
        &format!("se={}2000000000", "0".repeat(bytes - token.len())),
    )
}

#[test]
fn atom_token_limit_precedes_parsing_and_is_not_added_to_native_or_cbs() {
    let policy = namespace_policy();
    let signed = token(&format!("https://{HOST}/orders"));
    let at_limit = pad_expiry_to(&signed, MAX_ATOM_SAS_TOKEN_BYTES);
    assert_eq!(at_limit.len(), MAX_ATOM_SAS_TOKEN_BYTES);
    assert!(policy.authenticate_atom_sas(&at_limit, EXPIRY - 1).is_ok());
    let too_large = pad_expiry_to(&signed, MAX_ATOM_SAS_TOKEN_BYTES + 1);
    assert_eq!(
        policy.authenticate_atom_sas(&too_large, EXPIRY - 1),
        Err(SasError::Malformed)
    );
    assert_eq!(
        policy.authenticate_atom_sas(&too_large, EXPIRY),
        Err(SasError::Malformed)
    );
    let native = pad_expiry_to(
        &token(&format!("amqps://{HOST}/orders")),
        MAX_ATOM_SAS_TOKEN_BYTES + 1,
    );
    assert!(policy.authenticate_sas(&native, EXPIRY - 1).is_ok());
    assert!(
        policy
            .validate_sas(&native, &format!("amqps://{HOST}/orders"), EXPIRY - 1)
            .is_ok()
    );
}

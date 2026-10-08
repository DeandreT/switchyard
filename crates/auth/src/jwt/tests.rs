use base64::engine::general_purpose::STANDARD;
use hmac::{Hmac, Mac};
use ring::{
    rand::SystemRandom,
    signature::{RSA_PKCS1_SHA256, RsaKeyPair},
};
use serde_json::{Value, json};
use sha2::Sha256;

use super::*;
use crate::{Permission, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};

// Public test material from jsonwebtoken v11.1.0's tests/rsa PKCS1 fixture.
const MODULUS: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";
const PRIVATE_DER: &str = concat!(
    "MIIEpAIBAAKCAQEAyRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTL",
    "UTv4l4sggh5/CYYi/cvI+SXVT9kPWSKXxJXBXd/4LkvcPuUakBoAkfh+eiFVMh2V",
    "rUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8H",
    "oGfG/AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBI",
    "Mc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi+yUod+j8MtvIj812dkS4QMiRVN/",
    "by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQIDAQABAoIBAHREk0I0O9DvECKd",
    "WUpAmF3mY7oY9PNQiu44Yaf+AoSuyRpRUGTMIgc3u3eivOE8ALX0BmYUO5JtuRNZ",
    "Dpvt4SAwqCnVUinIf6C+eH/wSurCpapSM0BAHp4aOA7igptyOMgMPYBHNA1e9A7j",
    "E0dCxKWMl3DSWNyjQTk4zeRGEAEfbNjHrq6YCtjHSZSLmWiG80hnfnYos9hOr5Jn",
    "LnyS7ZmFE/5P3XVrxLc/tQ5zum0R4cbrgzHiQP5RgfxGJaEi7XcgherCCOgurJSS",
    "bYH29Gz8u5fFbS+Yg8s+OiCss3cs1rSgJ9/eHZuzGEdUZVARH6hVMjSuwvqVTFaE",
    "8AgtleECgYEA+uLMn4kNqHlJS2A5uAnCkj90ZxEtNm3E8hAxUrhssktY5XSOAPBl",
    "xyf5RuRGIImGtUVIr4HuJSa5TX48n3Vdt9MYCprO/iYl6moNRSPt5qowIIOJmIjY",
    "2mqPDfDt/zw+fcDD3lmCJrFlzcnh0uea1CohxEbQnL3cypeLt+WbU6kCgYEAzSp1",
    "9m1ajieFkqgoB0YTpt/OroDx38vvI5unInJlEeOjQ+oIAQdN2wpxBvTrRorMU6P0",
    "7mFUbt1j+Co6CbNiw+X8HcCaqYLR5clbJOOWNR36PuzOpQLkfK8woupBxzW9B8gZ",
    "mY8rB1mbJ+/WTPrEJy6YGmIEBkWylQ2VpW8O4O0CgYEApdbvvfFBlwD9YxbrcGz7",
    "MeNCFbMz+MucqQntIKoKJ91ImPxvtc0y6e/Rhnv0oyNlaUOwJVu0yNgNG117w0g4",
    "t/+Q38mvVC5xV7/cn7x9UMFk6MkqVir3dYGEqIl/OP1grY2Tq9HtB5iyG9L8NIam",
    "QOLMyUqqMUILxdthHyFmiGkCgYEAn9+PjpjGMPHxL0gj8Q8VbzsFtou6b1deIRRA",
    "2CHmSltltR1gYVTMwXxQeUhPMmgkMqUXzs4/WijgpthY44hK1TaZEKIuoxrS70nJ",
    "4WQLf5a9k1065fDsFZD6yGjdGxvwEmlGMZgTwqV7t1I4X0Ilqhav5hcs5apYL7gn",
    "PYPeRz0CgYALHCj/Ji8XSsDoF/MhVhnGdIs2P99NNdmo3R2Pv0CuZbDKMU559LJH",
    "UvrKS8WkuWRDuKrz1W/EQKApFjDGpdqToZqriUFQzwy7mR3ayIiogzNtHcvbDHx8",
    "oFnGY0OFksX/ye0/XGpy2SFxYRwGU98HPYeBvAQQrVjdkzfy7BmXQQ==",
);

fn configuration() -> Value {
    json!({
        "version": 1,
        "issuer": "https://issuer.example/",
        "audience": "urn:switchyard:tenant",
        "keys": [{"kid": "key-1", "kty": "RSA", "alg": "RS256", "use": "sig", "n": MODULUS, "e": "AQAB"}],
        "bindings": [{"subject": "producer", "scope": "amqps://tenant.example/orders", "permissions": ["send"]}]
    })
}

fn policy() -> JwtPolicy {
    JwtPolicy::from_json(&configuration().to_string()).unwrap()
}

fn requested() -> ResourceScope {
    ResourceScope::parse("amqps://tenant.example/orders").unwrap()
}

fn header() -> Value {
    json!({"alg": "RS256", "kid": "key-1", "typ": "switchyard+jwt"})
}

fn claims() -> Value {
    json!({"iss": "https://issuer.example/", "sub": "producer", "aud": "urn:switchyard:tenant", "iat": 100, "exp": 200})
}

fn signed_raw(header: &str, claims: &str) -> String {
    let input = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(header),
        URL_SAFE_NO_PAD.encode(claims)
    );
    let key = RsaKeyPair::from_der(&STANDARD.decode(PRIVATE_DER).unwrap()).unwrap();
    let mut signature = vec![0; key.public().modulus_len()];
    key.sign(
        &RSA_PKCS1_SHA256,
        &SystemRandom::new(),
        input.as_bytes(),
        &mut signature,
    )
    .unwrap();
    format!("{input}.{}", URL_SAFE_NO_PAD.encode(signature))
}

fn signed(header: &Value, claims: &Value) -> String {
    signed_raw(&header.to_string(), &claims.to_string())
}

fn validate(claims: &Value, now: u64) -> Result<AccessGrant, JwtError> {
    policy().validate(&signed(&header(), claims), &requested(), now)
}

#[test]
fn pinned_rs256_grants_only_local_rights_and_original_expiry() {
    let grant = validate(&claims(), 100).unwrap();
    assert_eq!(grant.subject(), "producer");
    assert_eq!(grant.scope(), &requested());
    assert_eq!(grant.expires_at_epoch_seconds(), 200);
    assert_eq!(grant.permissions(), PermissionSet::SEND);
    assert!(grant.allows(&requested(), Permission::Send, 199));
    assert!(!grant.allows(&requested(), Permission::Listen, 199));
    assert!(!grant.allows(&requested(), Permission::Send, 200));

    let noncanonical = signed_raw(
        r#" { "typ": "switchyard+jwt", "kid": "key-1", "alg": "RS256" } "#,
        r#" { "exp": 200, "sub": "pro\u0064ucer", "aud": "urn:switchyard:tenant", "iss": "https://issuer.example/", "iat": 100 } "#,
    );
    assert_eq!(
        policy().validate(&noncanonical, &requested(), 100).unwrap(),
        grant
    );
    let (_, signature) = noncanonical.rsplit_once('.').unwrap();
    let rewritten = format!(
        "{}.{}.{}",
        URL_SAFE_NO_PAD.encode(header().to_string()),
        URL_SAFE_NO_PAD.encode(claims().to_string()),
        signature,
    );
    assert_ne!(noncanonical, rewritten);
    assert_eq!(
        policy().validate(&rewritten, &requested(), 100),
        Err(JwtError::InvalidSignature)
    );
}

#[test]
fn requested_scope_never_borrows_permissions_from_signed_roles() {
    let mut claims = claims();
    claims["roles"] = json!(["admin", "manage"]);
    claims["scope"] = json!("send listen manage");
    let token = signed(&header(), &claims);
    let policy = policy();
    assert_eq!(
        policy.validate(
            &token,
            &ResourceScope::namespace("tenant.example").unwrap(),
            100
        ),
        Err(JwtError::ScopeMismatch)
    );
    assert_eq!(
        policy.validate(
            &token,
            &ResourceScope::entity("other.example", "orders").unwrap(),
            100
        ),
        Err(JwtError::ScopeMismatch)
    );
    assert_eq!(
        policy.validate(
            &token,
            &ResourceScope::entity("tenant.example", "orders-archive").unwrap(),
            100
        ),
        Err(JwtError::ScopeMismatch)
    );
    let child = ResourceScope::entity("tenant.example", "orders/$DeadLetterQueue").unwrap();
    let grant = policy.validate(&token, &child, 100).unwrap();
    assert_eq!(grant.permissions(), PermissionSet::SEND);
    assert_eq!(grant.scope(), &requested());
}

#[test]
fn issuer_subject_namespace_cannot_alias_sas_or_another_issuer() {
    let grant = validate(&claims(), 100).unwrap();
    let sas = SharedAccessPolicy::new([SharedAccessRule::new(
        "producer",
        requested(),
        SharedAccessKey::new("secret").unwrap(),
        None,
        PermissionSet::SEND,
    )
    .unwrap()])
    .unwrap();
    let sas_grant = sas.authenticate_plain("producer", "secret").unwrap();
    assert!(!grant.same_principal(&sas_grant));
    assert!(sas_grant.same_principal(&sas.authenticate_plain("producer", "secret").unwrap()));
    assert!(grant.same_principal(&validate(&claims(), 150).unwrap()));
    let mut other_config = configuration();
    other_config["issuer"] = json!("https://other-issuer.example/");
    let other_policy = JwtPolicy::from_json(&other_config.to_string()).unwrap();
    let mut other_claims = claims();
    other_claims["iss"] = other_config["issuer"].clone();
    let other = other_policy
        .validate(&signed(&header(), &other_claims), &requested(), 100)
        .unwrap();
    assert!(!grant.same_principal(&other));
}

#[test]
fn duplicate_members_are_rejected_even_in_ignored_metadata_and_escaped_names() {
    assert_eq!(
        JwtPolicy::from_json("{\"version\":1,\"\\u0076ersion\":1}").unwrap_err(),
        JwtError::DuplicateMember
    );
    assert_eq!(
        policy().validate(
            &signed_raw(
                "{\"alg\":\"RS256\",\"kid\":\"key-1\",\"typ\":\"switchyard+jwt\",\"alg\":\"RS256\"}",
                &claims().to_string()
            ),
            &requested(),
            100
        ),
        Err(JwtError::DuplicateMember)
    );
    let raw = "{\"iss\":\"https://issuer.example/\",\"sub\":\"producer\",\"aud\":\"urn:switchyard:tenant\",\"iat\":100,\"exp\":200,\"metadata\":{\"x\":1,\"\\u0078\":2}}";
    assert_eq!(
        policy().validate(&signed_raw(&header().to_string(), raw), &requested(), 100),
        Err(JwtError::DuplicateMember)
    );
}

#[test]
fn json_node_and_depth_boundaries_include_unknown_claim_values() {
    assert!(json::parse(b"[0,0]", 2, 3).is_ok());
    assert_eq!(json::parse(b"[0,0]", 2, 2), Err(json::Failure::Nodes));
    assert_eq!(json::parse(b"[[0]]", 2, 3), Err(json::Failure::Depth));
    for limit in [MAX_CONFIG_NODES, MAX_HEADER_NODES, MAX_CLAIMS_NODES] {
        let exact = json!(vec![0; limit - 1]).to_string();
        assert!(bounded_json(exact.as_bytes(), limit).is_ok());
        let over = json!(vec![0; limit]).to_string();
        assert_eq!(
            bounded_json(over.as_bytes(), limit),
            Err(JwtError::TooLarge)
        );
    }
    let mut claims = claims();
    claims["ignored"] = json!(vec![0; MAX_CLAIMS_NODES - 7]);
    assert!(validate(&claims, 100).is_ok());
    let mut value = Value::Null;
    for _ in 0..6 {
        value = json!([value]);
    }
    claims["ignored"] = value;
    assert!(validate(&claims, 100).is_ok());
    let mut value = Value::Null;
    for _ in 0..8 {
        value = json!([value]);
    }
    claims["ignored"] = value;
    assert_eq!(validate(&claims, 100), Err(JwtError::TooDeep));
    claims["ignored"] = json!(vec![0; MAX_CLAIMS_NODES]);
    assert_eq!(validate(&claims, 100), Err(JwtError::TooLarge));
    assert_eq!(
        bounded_json(b"{} trailing", 64),
        Err(JwtError::MalformedJson)
    );
}

#[test]
fn json_byte_bounds_are_checked_before_policy_or_signature_work() {
    assert_eq!(
        JwtPolicy::from_json(&" ".repeat(MAX_CONFIG_BYTES + 1)).unwrap_err(),
        JwtError::TooLarge
    );
    assert_eq!(
        policy().validate(&"x".repeat(MAX_TOKEN_BYTES + 1), &requested(), 100),
        Err(JwtError::TooLarge)
    );
    assert_eq!(
        decode_segment(
            &URL_SAFE_NO_PAD.encode(vec![0; MAX_HEADER_BYTES + 1]),
            MAX_HEADER_BYTES
        ),
        Err(JwtError::TooLarge)
    );
    assert_eq!(
        decode_segment(
            &URL_SAFE_NO_PAD.encode(vec![0; MAX_CLAIMS_BYTES + 1]),
            MAX_CLAIMS_BYTES
        ),
        Err(JwtError::TooLarge)
    );
    assert_eq!(
        policy().validate(
            "x.x.x",
            &ResourceScope::entity("tenant.example", "x".repeat(MAX_RESOURCE_BYTES)).unwrap(),
            100
        ),
        Err(JwtError::TooLarge)
    );
}

#[test]
fn compact_encoding_is_exact_and_has_only_three_nonempty_segments() {
    for token in [
        "", "a.b", "a.b.c.d", ".a.a", "a..a", "a.a.", "ab=.a.a", "a+.a.a",
    ] {
        assert_eq!(
            policy().validate(token, &requested(), 100),
            Err(JwtError::InvalidEncoding)
        );
    }
    assert_eq!(decode_segment("Zh", 1), Err(JwtError::InvalidEncoding));
    assert_eq!(decode_segment("Zg", 1).unwrap(), b"f");
}

#[test]
fn jose_profile_rejects_other_algorithms_types_and_key_sources() {
    assert_eq!(
        policy().validate(
            &signed(&json!(["RS256", "key-1", "switchyard+jwt"]), &claims()),
            &requested(),
            100,
        ),
        Err(JwtError::InvalidHeader),
    );
    for algorithm in ["none", "HS256", "RS384", "PS256", "ES256"] {
        let mut header = header();
        header["alg"] = json!(algorithm);
        assert_eq!(
            policy().validate(&signed(&header, &claims()), &requested(), 100),
            Err(JwtError::InvalidHeader)
        );
    }
    for typ in [
        "JWT",
        "at+jwt",
        "at+JWT",
        "application/switchyard+jwt",
        "",
        "Switchyard+JWT",
    ] {
        let mut header = header();
        header["typ"] = json!(typ);
        assert_eq!(
            policy().validate(&signed(&header, &claims()), &requested(), 100),
            Err(JwtError::InvalidHeader)
        );
    }
    for field in [
        "crit", "b64", "jku", "x5u", "x5c", "jwk", "cty", "zip", "custom",
    ] {
        let mut header = header();
        header[field] = json!("ignored");
        assert_eq!(
            policy().validate(&signed(&header, &claims()), &requested(), 100),
            Err(JwtError::InvalidHeader)
        );
    }
    let confused_header = json!({"alg": "HS256", "kid": "key-1", "typ": "switchyard+jwt"});
    let input = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(confused_header.to_string()),
        URL_SAFE_NO_PAD.encode(claims().to_string())
    );
    let mut mac =
        Hmac::<Sha256>::new_from_slice(&URL_SAFE_NO_PAD.decode(MODULUS).unwrap()).unwrap();
    mac.update(input.as_bytes());
    let token = format!(
        "{input}.{}",
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    );
    assert_eq!(
        policy().validate(&token, &requested(), 100),
        Err(JwtError::InvalidHeader)
    );
    let mut header = header();
    header.as_object_mut().unwrap().remove("typ");
    assert_eq!(
        policy().validate(&signed(&header, &claims()), &requested(), 100),
        Err(JwtError::InvalidHeader)
    );
}

#[test]
fn unknown_key_and_wrong_signature_never_create_a_grant() {
    let mut unknown = header();
    unknown["kid"] = json!("missing");
    assert_eq!(
        policy().validate(&signed(&unknown, &claims()), &requested(), 100),
        Err(JwtError::UnknownKey)
    );
    let token = signed(&header(), &claims());
    let (input, signature) = token.rsplit_once('.').unwrap();
    let mut bytes = URL_SAFE_NO_PAD.decode(signature).unwrap();
    bytes[0] ^= 1;
    assert_eq!(
        policy().validate(
            &format!("{input}.{}", URL_SAFE_NO_PAD.encode(bytes)),
            &requested(),
            100
        ),
        Err(JwtError::InvalidSignature)
    );
    assert_eq!(
        policy().validate(
            &format!("{input}.{}", URL_SAFE_NO_PAD.encode([0; 255])),
            &requested(),
            100
        ),
        Err(JwtError::InvalidSignature)
    );
    assert_eq!(
        policy().validate(
            &format!("{input}.{}", URL_SAFE_NO_PAD.encode([0u8; 257])),
            &requested(),
            100
        ),
        Err(JwtError::TooLarge)
    );
    let replaced = format!(
        "{}.{}.{}",
        URL_SAFE_NO_PAD.encode(header().to_string()),
        URL_SAFE_NO_PAD.encode(claims().to_string().replace("producer", "attacker")),
        signature
    );
    assert_eq!(
        policy().validate(&replaced, &requested(), 100),
        Err(JwtError::InvalidSignature)
    );
    let mut config = configuration();
    let mut other_modulus = URL_SAFE_NO_PAD.decode(MODULUS).unwrap();
    other_modulus[128] ^= 2;
    config["keys"][0]["n"] = json!(URL_SAFE_NO_PAD.encode(other_modulus));
    assert_eq!(
        JwtPolicy::from_json(&config.to_string())
            .unwrap()
            .validate(&token, &requested(), 100),
        Err(JwtError::InvalidSignature)
    );
}

#[test]
fn issuer_resource_audience_and_subject_are_exact_pins() {
    for (field, value, error) in [
        ("iss", "https://issuer.example", JwtError::IssuerMismatch),
        ("iss", "https://other.example/", JwtError::IssuerMismatch),
        ("iss", "https://ISSUER.example/", JwtError::IssuerMismatch),
        ("aud", "URN:switchyard:tenant", JwtError::AudienceMismatch),
        (
            "aud",
            "amqps://tenant.example/orders",
            JwtError::AudienceMismatch,
        ),
        ("sub", "Producer", JwtError::UnknownSubject),
    ] {
        let mut claims = claims();
        claims[field] = json!(value);
        assert_eq!(validate(&claims, 100), Err(error));
    }
}

#[test]
fn audience_arrays_are_bounded_unique_and_still_match_exactly() {
    let mut claims = claims();
    claims["aud"] = json!(["other", "urn:switchyard:tenant"]);
    assert!(validate(&claims, 100).is_ok());
    for audience in [
        json!([]),
        json!(["urn:switchyard:tenant", "urn:switchyard:tenant"]),
        json!(vec!["other"; 9]),
        json!([1]),
        json!({"aud": "urn:switchyard:tenant"}),
    ] {
        claims["aud"] = audience;
        assert_eq!(validate(&claims, 100), Err(JwtError::InvalidClaims));
    }
}

#[test]
fn timestamps_are_integral_and_never_use_library_leeway_or_clock() {
    for field in ["iat", "exp", "nbf"] {
        for value in [
            json!(100.0),
            json!(-1),
            json!("100"),
            Value::Null,
            json!(true),
        ] {
            let mut claims = claims();
            claims[field] = value;
            assert_eq!(validate(&claims, 100), Err(JwtError::InvalidClaims));
        }
    }
    assert_eq!(validate(&claims(), 99), Err(JwtError::NotYetValid));
    assert!(validate(&claims(), 100).is_ok());
    assert!(validate(&claims(), 199).is_ok());
    assert_eq!(validate(&claims(), 200), Err(JwtError::Expired));
    let mut claims = claims();
    claims["nbf"] = json!(101);
    assert_eq!(validate(&claims, 100), Err(JwtError::NotYetValid));
    assert!(validate(&claims, 101).is_ok());
    claims["nbf"] = json!(200);
    assert_eq!(validate(&claims, 199), Err(JwtError::InvalidClaims));
}

#[test]
fn lifetime_uses_checked_subtraction_and_accepts_exactly_one_hour() {
    let mut claims = claims();
    claims["exp"] = json!(3700);
    assert!(validate(&claims, 100).is_ok());
    claims["exp"] = json!(3701);
    assert_eq!(validate(&claims, 100), Err(JwtError::InvalidClaims));
    for expiration in [0, 99, 100] {
        claims["exp"] = json!(expiration);
        assert_eq!(validate(&claims, 100), Err(JwtError::InvalidClaims));
    }
    claims["iat"] = json!(u64::MAX - 1);
    claims["exp"] = json!(u64::MAX);
    assert!(validate(&claims, u64::MAX - 1).is_ok());
    assert_eq!(validate(&claims, u64::MAX), Err(JwtError::Expired));
}

#[test]
fn required_claim_shapes_and_lengths_are_not_coerced() {
    assert_eq!(
        validate(
            &json!([
                "https://issuer.example/",
                "producer",
                "urn:switchyard:tenant",
                100,
                200,
                null,
            ]),
            100,
        ),
        Err(JwtError::InvalidClaims),
    );
    for field in ["iss", "sub", "aud", "iat", "exp"] {
        let mut claims = claims();
        claims.as_object_mut().unwrap().remove(field);
        assert_eq!(validate(&claims, 100), Err(JwtError::InvalidClaims));
    }
    for (field, length) in [
        ("iss", MAX_RESOURCE_BYTES + 1),
        ("sub", MAX_SUBJECT_BYTES + 1),
        ("aud", MAX_RESOURCE_BYTES + 1),
    ] {
        let mut claims = claims();
        claims[field] = json!("x".repeat(length));
        assert_eq!(validate(&claims, 100), Err(JwtError::InvalidClaims));
    }
    let mut claims = claims();
    claims["sub"] = json!("");
    assert_eq!(validate(&claims, 100), Err(JwtError::InvalidClaims));
}

#[test]
fn configuration_is_closed_versioned_and_has_no_private_key_or_url_fields() {
    let config = configuration();
    let positional = json!([
        config["version"],
        config["issuer"],
        config["audience"],
        config["keys"],
        config["bindings"],
    ]);
    assert_eq!(
        JwtPolicy::from_json(&positional.to_string()).unwrap_err(),
        JwtError::InvalidConfiguration,
    );
    for (collection, fields) in [
        ("keys", &["kid", "kty", "alg", "use", "n", "e"][..]),
        ("bindings", &["subject", "scope", "permissions"][..]),
    ] {
        let mut config = configuration();
        let positional: Vec<_> = fields
            .iter()
            .map(|field| config[collection][0][*field].clone())
            .collect();
        config[collection][0] = json!(positional);
        assert_eq!(
            JwtPolicy::from_json(&config.to_string()).unwrap_err(),
            JwtError::InvalidConfiguration,
        );
    }
    for field in ["jwks_uri", "discovery", "unknown"] {
        let mut config = configuration();
        config[field] = json!("https://other.example/");
        assert_eq!(
            JwtPolicy::from_json(&config.to_string()).unwrap_err(),
            JwtError::InvalidConfiguration
        );
    }
    for field in ["d", "p", "q", "x5u", "jku", "key_ops"] {
        let mut config = configuration();
        config["keys"][0][field] = json!("untrusted");
        assert_eq!(
            JwtPolicy::from_json(&config.to_string()).unwrap_err(),
            JwtError::InvalidConfiguration
        );
    }
    for version in [json!(0), json!(2), json!(1.0), json!("1")] {
        let mut config = configuration();
        config["version"] = version;
        assert_eq!(
            JwtPolicy::from_json(&config.to_string()).unwrap_err(),
            JwtError::InvalidConfiguration
        );
    }
}

#[test]
fn issuer_configuration_requires_a_bounded_https_pin_without_credentials() {
    for issuer in [
        "http://issuer.example/",
        "https://user@issuer.example/",
        "https://issuer.example/?q=x",
        "https://issuer.example/#fragment",
        "not a URI",
        "",
    ] {
        let mut config = configuration();
        config["issuer"] = json!(issuer);
        assert_eq!(
            JwtPolicy::from_json(&config.to_string()).unwrap_err(),
            JwtError::InvalidConfiguration
        );
    }
    let mut config = configuration();
    config["audience"] = json!("");
    assert_eq!(
        JwtPolicy::from_json(&config.to_string()).unwrap_err(),
        JwtError::InvalidConfiguration
    );
}

#[test]
fn keys_have_hard_algorithm_type_exponent_and_modulus_bounds() {
    for (field, value) in [
        ("alg", "HS256"),
        ("kty", "EC"),
        ("use", "enc"),
        ("e", "Aw"),
        ("e", "AAEAAQ"),
        ("kid", ""),
    ] {
        let mut config = configuration();
        config["keys"][0][field] = json!(value);
        assert_eq!(
            JwtPolicy::from_json(&config.to_string()).unwrap_err(),
            JwtError::InvalidKey
        );
    }
    for length in [255, 513] {
        let mut config = configuration();
        let mut modulus = vec![0xff; length];
        modulus[0] = 0x80;
        config["keys"][0]["n"] = json!(URL_SAFE_NO_PAD.encode(modulus));
        assert_eq!(
            JwtPolicy::from_json(&config.to_string()).unwrap_err(),
            JwtError::InvalidKey
        );
    }
    for modulus in [vec![0; 256], vec![0xfe; 256], {
        let mut n = vec![0xff; 257];
        n[0] = 0;
        n
    }] {
        let mut config = configuration();
        config["keys"][0]["n"] = json!(URL_SAFE_NO_PAD.encode(modulus));
        assert_eq!(
            JwtPolicy::from_json(&config.to_string()).unwrap_err(),
            JwtError::InvalidKey
        );
    }
    // Synthetic moduli pin geometry only, not signing-key provenance.
    for (bytes, first) in [(256, 0x7f), (513, 0x01)] {
        let mut modulus = vec![0xff; bytes];
        modulus[0] = first;
        let mut config = configuration();
        config["keys"][0]["n"] = json!(URL_SAFE_NO_PAD.encode(modulus));
        assert_eq!(
            JwtPolicy::from_json(&config.to_string()).unwrap_err(),
            JwtError::InvalidKey
        );
    }
    let mut modulus = vec![0xff; 512];
    modulus[0] = 0x80;
    let mut config = configuration();
    config["keys"][0]["n"] = json!(URL_SAFE_NO_PAD.encode(modulus));
    assert!(JwtPolicy::from_json(&config.to_string()).is_ok());

    let mut config = configuration();
    config["keys"][0]["kid"] = json!("x".repeat(MAX_KID_BYTES + 1));
    assert_eq!(
        JwtPolicy::from_json(&config.to_string()).unwrap_err(),
        JwtError::InvalidKey
    );
}

#[test]
fn key_and_subject_collections_are_finite_nonempty_and_unique() {
    let mut maximum = configuration();
    maximum["keys"] = json!(
        (0..MAX_KEYS)
            .map(|index| {
                let mut key = configuration()["keys"][0].clone();
                key["kid"] = json!(format!("key-{index}"));
                key
            })
            .collect::<Vec<_>>()
    );
    maximum["bindings"] = json!(
        (0..MAX_BINDINGS)
            .map(|index| {
                let mut binding = configuration()["bindings"][0].clone();
                binding["subject"] = json!(format!("principal-{index}"));
                binding
            })
            .collect::<Vec<_>>()
    );
    assert!(JwtPolicy::from_json(&maximum.to_string()).is_ok());
    for collection in ["keys", "bindings"] {
        let mut config = configuration();
        config[collection] = json!([]);
        assert_eq!(
            JwtPolicy::from_json(&config.to_string()).unwrap_err(),
            JwtError::InvalidConfiguration
        );
        let mut config = configuration();
        let entry = config[collection][0].clone();
        config[collection] = json!([entry.clone(), entry]);
        assert_eq!(
            JwtPolicy::from_json(&config.to_string()).unwrap_err(),
            JwtError::DuplicateIdentity
        );
    }
    let mut config = configuration();
    config["keys"] = json!(
        (0..9)
            .map(|index| {
                let mut key = configuration()["keys"][0].clone();
                key["kid"] = json!(format!("key-{index}"));
                key
            })
            .collect::<Vec<_>>()
    );
    assert_eq!(
        JwtPolicy::from_json(&config.to_string()).unwrap_err(),
        JwtError::InvalidConfiguration
    );
    let mut config = configuration();
    config["bindings"] = json!(
        (0..65)
            .map(|index| {
                let mut binding = configuration()["bindings"][0].clone();
                binding["subject"] = json!(format!("principal-{index}"));
                binding
            })
            .collect::<Vec<_>>()
    );
    assert_eq!(
        JwtPolicy::from_json(&config.to_string()).unwrap_err(),
        JwtError::InvalidConfiguration
    );
}

#[test]
fn local_permissions_are_explicit_unique_and_do_not_include_cluster_authority() {
    for permissions in [
        json!([]),
        json!(["send", "send"]),
        json!(["audit"]),
        json!(["cluster"]),
        json!(["Send"]),
    ] {
        let mut config = configuration();
        config["bindings"][0]["permissions"] = permissions;
        assert_eq!(
            JwtPolicy::from_json(&config.to_string()).unwrap_err(),
            JwtError::InvalidConfiguration
        );
    }
    let mut config = configuration();
    config["bindings"][0]["permissions"] = json!(["manage"]);
    let policy = JwtPolicy::from_json(&config.to_string()).unwrap();
    let grant = policy
        .validate(&signed(&header(), &claims()), &requested(), 100)
        .unwrap();
    assert!(grant.allows(&requested(), Permission::Send, 100));
    assert!(grant.allows(&requested(), Permission::Listen, 100));
    assert!(!grant.allows(&requested(), Permission::Cluster, 100));
}

#[test]
fn configuration_scopes_use_existing_resource_hierarchy_validation() {
    for scope in [
        "https://tenant.example/orders",
        "amqps://tenant.example/orders%2fother",
        "amqps://tenant.example/orders?x=y",
    ] {
        let mut config = configuration();
        config["bindings"][0]["scope"] = json!(scope);
        assert_eq!(
            JwtPolicy::from_json(&config.to_string()).unwrap_err(),
            JwtError::InvalidConfiguration
        );
    }
    let mut config = configuration();
    config["bindings"][0]["scope"] = json!("amqps://tenant.example");
    let policy = JwtPolicy::from_json(&config.to_string()).unwrap();
    assert!(
        policy
            .validate(&signed(&header(), &claims()), &requested(), 100)
            .is_ok()
    );
}

#[test]
fn debug_and_errors_do_not_traverse_keys_tokens_or_issuer_configuration() {
    let policy = policy();
    let debug = format!("{policy:?}");
    assert!(!debug.contains(MODULUS));
    assert!(!debug.contains("issuer.example"));
    assert!(!debug.contains("producer"));
    let grant = policy
        .validate(&signed(&header(), &claims()), &requested(), 100)
        .unwrap();
    assert!(!format!("{grant:?}").contains("issuer.example"));
    assert_eq!(
        JwtError::InvalidSignature.to_string(),
        "JWT signature verification failed"
    );
}

#[test]
fn retained_validity_start_rejects_backwards_injected_time() {
    let mut claims = claims();
    claims["nbf"] = json!(170);
    let grant = validate(&claims, 180).unwrap();
    assert_eq!(grant.valid_from_epoch_seconds(), 170);
    assert!(!grant.is_valid_at(100));
    assert!(!grant.allows(&requested(), Permission::Send, 169));
    assert!(grant.is_valid_at(170));
    assert!(grant.allows(&requested(), Permission::Send, 199));
    assert!(!grant.is_valid_at(200));

    claims["nbf"] = json!(90);
    let grant = validate(&claims, 100).unwrap();
    assert_eq!(grant.valid_from_epoch_seconds(), 100);
    assert!(!grant.is_valid_at(99));
    claims.as_object_mut().unwrap().remove("nbf");
    assert_eq!(validate(&claims, 100).unwrap(), grant);

    claims["iat"] = json!(0);
    claims["exp"] = json!(1);
    let grant = validate(&claims, 0).unwrap();
    assert!(grant.is_valid_at(0));
    assert!(!grant.is_valid_at(1));
}

#[test]
fn same_principal_does_not_union_scope_rights_or_validity() {
    let first = validate(&claims(), 100).unwrap();
    let mut config = configuration();
    config["bindings"][0]["scope"] = json!("amqps://tenant.example");
    config["bindings"][0]["permissions"] = json!(["listen"]);
    let mut refreshed = claims();
    refreshed["iat"] = json!(150);
    refreshed["exp"] = json!(250);
    let second = JwtPolicy::from_json(&config.to_string())
        .unwrap()
        .validate(&signed(&header(), &refreshed), &requested(), 150)
        .unwrap();
    assert!(first.same_principal(&second));
    assert_ne!(first, second);
    assert_eq!(first.scope(), &requested());
    assert_eq!(first.permissions(), PermissionSet::SEND);
    assert_eq!(first.valid_from_epoch_seconds(), 100);
    assert_eq!(first.expires_at_epoch_seconds(), 200);
    assert_eq!(second.permissions(), PermissionSet::LISTEN);
    assert!(!second.allows(&requested(), Permission::Send, 150));
    assert!(!second.allows(&requested(), Permission::Listen, 149));
    assert!(second.allows(&requested(), Permission::Listen, 150));
    config["bindings"].as_array_mut().unwrap().push(json!({
        "subject": "consumer", "scope": "amqps://tenant.example", "permissions": ["listen"],
    }));
    refreshed["sub"] = json!("consumer");
    let other = JwtPolicy::from_json(&config.to_string())
        .unwrap()
        .validate(&signed(&header(), &refreshed), &requested(), 150)
        .unwrap();
    assert!(!second.same_principal(&other));
}

#[test]
fn sas_and_plain_grants_keep_the_existing_identity_and_start_zero() {
    let policy = SharedAccessPolicy::new([SharedAccessRule::new(
        "producer",
        requested(),
        SharedAccessKey::new("secret").unwrap(),
        None,
        PermissionSet::SEND,
    )
    .unwrap()])
    .unwrap();
    let plain = policy.authenticate_plain("producer", "secret").unwrap();
    assert_eq!(plain.valid_from_epoch_seconds(), 0);
    assert!(plain.is_valid_at(0));
    assert!(plain.is_valid_at(u64::MAX - 1));
    assert!(!plain.is_valid_at(u64::MAX));
    let encoded = "amqps%3A%2F%2Ftenant.example%2Forders";
    let mut mac = Hmac::<Sha256>::new_from_slice(b"secret").unwrap();
    mac.update(format!("{encoded}\n200").as_bytes());
    let signature = STANDARD.encode(mac.finalize().into_bytes());
    let signature: String = url::form_urlencoded::byte_serialize(signature.as_bytes()).collect();
    let token = format!("SharedAccessSignature sr={encoded}&sig={signature}&se=200&skn=producer");
    let sas = policy
        .validate_sas(&token, "amqps://tenant.example/orders", 100)
        .unwrap();
    assert!(sas.same_principal(&plain));
    assert_eq!(sas.valid_from_epoch_seconds(), 0);
    assert_eq!(sas.expires_at_epoch_seconds(), 200);
    assert!(sas.allows(&requested(), Permission::Send, 0));
    assert!(!sas.is_valid_at(200));
    assert!(!sas.same_principal(&validate(&claims(), 100).unwrap()));
}

#[test]
fn exact_byte_bounds_accept_complete_inputs_and_refuse_one_more_byte() {
    let config = configuration().to_string();
    let padded = format!("{config}{}", " ".repeat(MAX_CONFIG_BYTES - config.len()));
    assert_eq!(padded.len(), MAX_CONFIG_BYTES);
    assert!(JwtPolicy::from_json(&padded).is_ok());
    assert_eq!(
        JwtPolicy::from_json(&(padded + " ")).unwrap_err(),
        JwtError::TooLarge
    );
    for limit in [MAX_HEADER_BYTES, MAX_CLAIMS_BYTES] {
        let bytes = vec![0; limit];
        assert_eq!(
            decode_segment(&URL_SAFE_NO_PAD.encode(&bytes), limit).unwrap(),
            bytes
        );
    }

    let claims = claims().to_string();
    let signature_bytes = URL_SAFE_NO_PAD.encode([0u8; 256]).len();
    for padding in 0..3 {
        let header = format!("{}{}", header(), " ".repeat(padding));
        let encoded_claims =
            MAX_TOKEN_BYTES - URL_SAFE_NO_PAD.encode(&header).len() - signature_bytes - 2;
        if encoded_claims % 4 == 1 {
            continue;
        }
        let decoded_claims = encoded_claims / 4 * 3
            + match encoded_claims % 4 {
                2 => 1,
                3 => 2,
                _ => 0,
            };
        assert!(decoded_claims <= MAX_CLAIMS_BYTES);
        let claims = format!("{claims}{}", " ".repeat(decoded_claims - claims.len()));
        let token = signed_raw(&header, &claims);
        assert_eq!(token.len(), MAX_TOKEN_BYTES);
        assert!(policy().validate(&token, &requested(), 100).is_ok());
        assert_eq!(
            policy().validate(&(token + " "), &requested(), 100),
            Err(JwtError::TooLarge)
        );
        return;
    }
    panic!("no exact compact byte boundary");
}

#[test]
fn numeric_date_lexical_forms_are_not_coerced() {
    for value in ["1e2", "100.0", "-0", "18446744073709551616"] {
        let raw = format!(
            "{{\"iss\":\"https://issuer.example/\",\"sub\":\"producer\",\"aud\":\"urn:switchyard:tenant\",\"iat\":{value},\"exp\":200}}"
        );
        assert_eq!(
            policy().validate(&signed_raw(&header().to_string(), &raw), &requested(), 100),
            Err(JwtError::InvalidClaims)
        );
    }
}

#[test]
fn key_and_signature_encodings_reject_padding_and_unused_bits() {
    let mut config = configuration();
    config["keys"][0]["n"] = json!(format!("{MODULUS}="));
    assert_eq!(
        JwtPolicy::from_json(&config.to_string()).unwrap_err(),
        JwtError::InvalidKey
    );
    let mut config = configuration();
    config["keys"][0]["e"] = json!("AQAB=");
    assert_eq!(
        JwtPolicy::from_json(&config.to_string()).unwrap_err(),
        JwtError::InvalidKey
    );
    let token = signed(&header(), &claims());
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let last = *token.as_bytes().last().unwrap();
    let index = alphabet.iter().position(|byte| *byte == last).unwrap();
    assert_eq!(index & 15, 0);
    let noncanonical = format!(
        "{}{}",
        &token[..token.len() - 1],
        char::from(alphabet[index + 1])
    );
    assert_eq!(
        policy().validate(&noncanonical, &requested(), 100),
        Err(JwtError::InvalidEncoding)
    );
}

#[test]
fn policy_keeps_main_literal_resource_and_control_name_boundaries() {
    let mut config = configuration();
    config["bindings"][0]["scope"] = json!("amqps://tenant.example/orders%25archive");
    let policy = JwtPolicy::from_json(&config.to_string()).unwrap();
    let token = signed(&header(), &claims());
    let literal = ResourceScope::entity("tenant.example", "orders%archive").unwrap();
    assert!(policy.validate(&token, &literal, 100).is_ok());
    let different = ResourceScope::entity("tenant.example", "orders%25archive").unwrap();
    assert_eq!(
        policy.validate(&token, &different, 100),
        Err(JwtError::ScopeMismatch)
    );

    config["bindings"][0]["scope"] = json!("amqps://TENANT.example/Events/Subscriptions/Gold");
    let policy = JwtPolicy::from_json(&config.to_string()).unwrap();
    let shadow = ResourceScope::entity(
        "tenant.example",
        "events/subscriptions/GOLD/$DeadLetterQueue",
    )
    .unwrap();
    assert!(policy.validate(&token, &shadow, 100).is_ok());
    let sibling =
        ResourceScope::entity("tenant.example", "events/subscriptions/gold-archive").unwrap();
    assert_eq!(
        policy.validate(&token, &sibling, 100),
        Err(JwtError::ScopeMismatch)
    );

    config["bindings"][0]["scope"] = json!("amqps://tenant.example/events/management");
    let policy = JwtPolicy::from_json(&config.to_string()).unwrap();
    let management = ResourceScope::entity("tenant.example", "events/$management").unwrap();
    assert_eq!(
        policy.validate(&token, &management, 100),
        Err(JwtError::ScopeMismatch)
    );
}

#[test]
fn all_error_categories_are_static_and_do_not_expose_input() {
    for error in [
        JwtError::TooLarge,
        JwtError::MalformedJson,
        JwtError::DuplicateMember,
        JwtError::TooDeep,
        JwtError::InvalidConfiguration,
        JwtError::DuplicateIdentity,
        JwtError::InvalidKey,
        JwtError::InvalidEncoding,
        JwtError::InvalidHeader,
        JwtError::UnknownKey,
        JwtError::InvalidSignature,
        JwtError::InvalidClaims,
        JwtError::IssuerMismatch,
        JwtError::AudienceMismatch,
        JwtError::NotYetValid,
        JwtError::Expired,
        JwtError::UnknownSubject,
        JwtError::ScopeMismatch,
    ] {
        for rendered in [error.to_string(), format!("{error:?}")] {
            for sensitive in [MODULUS, PRIVATE_DER, "issuer.example", "producer"] {
                assert!(!rendered.contains(sensitive));
            }
        }
    }
}

#[test]
fn exact_text_bounds_keep_pinned_claims_keys_and_resources_usable() {
    let mut config = configuration();
    let issuer = format!(
        "https://issuer.example/{}",
        "x".repeat(MAX_RESOURCE_BYTES - "https://issuer.example/".len())
    );
    let audience = "x".repeat(MAX_RESOURCE_BYTES);
    let subject = "s".repeat(MAX_SUBJECT_BYTES);
    let kid = "k".repeat(MAX_KID_BYTES);
    let scope = format!(
        "amqps://tenant.example/{}",
        "r".repeat(MAX_RESOURCE_BYTES - "amqps://tenant.example/".len())
    );
    config["issuer"] = json!(issuer);
    config["audience"] = json!(audience);
    config["bindings"][0]["subject"] = json!(subject);
    config["bindings"][0]["scope"] = json!(scope);
    config["keys"][0]["kid"] = json!(kid);
    let mut header = header();
    header["kid"] = config["keys"][0]["kid"].clone();
    let mut claims = claims();
    claims["iss"] = config["issuer"].clone();
    claims["aud"] = config["audience"].clone();
    claims["sub"] = config["bindings"][0]["subject"].clone();
    let requested = ResourceScope::parse(&scope).unwrap();
    let grant = JwtPolicy::from_json(&config.to_string())
        .unwrap()
        .validate(&signed(&header, &claims), &requested, 100)
        .unwrap();
    assert_eq!(grant.scope(), &requested);
    assert_eq!(grant.subject().len(), MAX_SUBJECT_BYTES);
    for field in ["issuer", "audience"] {
        let mut over = config.clone();
        over[field] = json!(format!("{}x", over[field].as_str().unwrap()));
        assert_eq!(
            JwtPolicy::from_json(&over.to_string()).unwrap_err(),
            JwtError::InvalidConfiguration
        );
    }
    for field in ["subject", "scope"] {
        let mut over = config.clone();
        over["bindings"][0][field] =
            json!(format!("{}x", over["bindings"][0][field].as_str().unwrap()));
        assert_eq!(
            JwtPolicy::from_json(&over.to_string()).unwrap_err(),
            JwtError::InvalidConfiguration
        );
    }
}

#[test]
fn shared_access_principal_identity_is_namespace_qualified() {
    let mut grants = Vec::new();
    for host in ["tenant.example", "TENANT.example", "other.example"] {
        let resource = format!("amqps://{host}/orders");
        let scope = ResourceScope::parse(&resource).unwrap();
        let policy = SharedAccessPolicy::new([SharedAccessRule::new(
            "producer",
            scope,
            SharedAccessKey::new("secret").unwrap(),
            None,
            PermissionSet::SEND,
        )
        .unwrap()])
        .unwrap();
        let plain = policy.authenticate_plain("producer", "secret").unwrap();
        let encoded: String = url::form_urlencoded::byte_serialize(resource.as_bytes()).collect();
        let mut mac = Hmac::<Sha256>::new_from_slice(b"secret").unwrap();
        mac.update(format!("{encoded}\n200").as_bytes());
        let signature = STANDARD.encode(mac.finalize().into_bytes());
        let signature: String =
            url::form_urlencoded::byte_serialize(signature.as_bytes()).collect();
        let token =
            format!("SharedAccessSignature sr={encoded}&sig={signature}&se=200&skn=producer");
        let sas = policy.validate_sas(&token, &resource, 100).unwrap();
        assert!(plain.same_principal(&sas));
        assert_eq!(plain.valid_from_epoch_seconds(), 0);
        assert_eq!(sas.valid_from_epoch_seconds(), 0);
        grants.push((plain, sas));
    }
    assert!(grants[0].0.same_principal(&grants[1].0));
    assert!(grants[0].1.same_principal(&grants[1].1));
    assert!(!grants[0].0.same_principal(&grants[2].0));
    assert!(!grants[0].1.same_principal(&grants[2].1));
    assert!(!grants[0].0.same_principal(&grants[2].1));
    assert!(!grants[0].1.same_principal(&grants[2].0));
}

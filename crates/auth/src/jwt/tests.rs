use base64::engine::general_purpose::STANDARD;
use jsonwebtoken::EncodingKey;
use serde_json::{Value, json};

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
    json!({"alg": "RS256", "kid": "key-1", "typ": "at+jwt"})
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
    let key = EncodingKey::from_rsa_der(&STANDARD.decode(PRIVATE_DER).unwrap());
    let signer = (DEFAULT_PROVIDER.signer_factory)(&Algorithm::RS256, &key).unwrap();
    let signature = signer.try_sign(input.as_bytes()).unwrap();
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
        r#" { "typ": "at+jwt", "kid": "key-1", "alg": "RS256" } "#,
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
                "{\"alg\":\"RS256\",\"kid\":\"key-1\",\"typ\":\"at+jwt\",\"alg\":\"RS256\"}",
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
            &signed(&json!(["RS256", "key-1", "at+jwt"]), &claims()),
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
    for typ in ["JWT", "application/at+jwt", "", "At+JWT"] {
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
}

#[test]
fn issuer_resource_audience_and_subject_are_exact_pins() {
    for (field, value, error) in [
        ("iss", "https://issuer.example", JwtError::IssuerMismatch),
        ("iss", "https://other.example/", JwtError::IssuerMismatch),
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
fn configuration_scopes_use_existing_amqp_hierarchy_validation() {
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
fn native_bindings_preserve_control_names_without_changing_amqp_aliases() {
    let token = signed(&header(), &claims());
    for path in [
        "orders/$Management",
        "orders/$DeadLetterQueue",
        "events/Subscriptions/sub",
        "events/Subscriptions/sub/$Management",
    ] {
        let mut config = configuration();
        config["bindings"][0]["scope"] = json!(format!("amqps://tenant.example/{path}"));
        config["bindings"][0]["permissions"] = json!(["manage"]);
        let policy = JwtPolicy::from_json(&config.to_string()).unwrap();
        let literal = ResourceScope::entity("tenant.example", path).unwrap();
        let canonical = literal.clone().into_amqp_scope();
        assert_ne!(literal, canonical);
        let native = policy.validate_native_scope(&token, &literal, 100).unwrap();
        assert_eq!(native.scope(), &literal);
        assert!(native.allows(&literal, Permission::Manage, 100));
        assert!(!native.allows(&canonical, Permission::Manage, 100));
        assert_eq!(
            policy.validate_native_scope(&token, &canonical, 100),
            Err(JwtError::ScopeMismatch)
        );
        for requested in [&literal, &canonical] {
            let amqp = policy.validate(&token, requested, 100).unwrap();
            assert_eq!(amqp.scope(), &canonical);
            assert!(amqp.allows(&canonical, Permission::Manage, 100));
        }
    }
}

#[test]
fn native_and_amqp_bindings_keep_ordinary_case_and_namespace_boundaries() {
    let mut config = configuration();
    config["bindings"][0]["scope"] = json!("amqps://tenant.example/Orders");
    config["bindings"][0]["permissions"] = json!(["manage"]);
    let policy = JwtPolicy::from_json(&config.to_string()).unwrap();
    let token = signed(&header(), &claims());
    let bound = ResourceScope::entity("tenant.example", "Orders").unwrap();
    for path in ["Orders", "Orders/child"] {
        let requested = ResourceScope::entity("tenant.example", path).unwrap();
        let native = policy
            .validate_native_scope(&token, &requested, 100)
            .unwrap();
        let amqp = policy.validate(&token, &requested, 100).unwrap();
        assert_eq!(native.scope(), &bound);
        assert_eq!(amqp.scope(), &bound);
        assert!(native.allows(&requested, Permission::Manage, 100));
        assert!(amqp.allows(&requested, Permission::Manage, 100));
    }
    for requested in [
        ResourceScope::entity("tenant.example", "orders").unwrap(),
        ResourceScope::entity("tenant.example", "Orders-archive").unwrap(),
        ResourceScope::entity("other.example", "Orders").unwrap(),
        ResourceScope::namespace("tenant.example").unwrap(),
    ] {
        assert_eq!(
            policy.validate_native_scope(&token, &requested, 100),
            Err(JwtError::ScopeMismatch)
        );
        assert_eq!(
            policy.validate(&token, &requested, 100),
            Err(JwtError::ScopeMismatch)
        );
    }
    config["bindings"][0]["scope"] = json!("amqps://tenant.example");
    let policy = JwtPolicy::from_json(&config.to_string()).unwrap();
    for requested in [
        ResourceScope::namespace("tenant.example").unwrap(),
        ResourceScope::entity("tenant.example", "Orders/child").unwrap(),
    ] {
        assert!(
            policy
                .validate_native_scope(&token, &requested, 100)
                .is_ok()
        );
        assert!(policy.validate(&token, &requested, 100).is_ok());
    }
    let foreign = ResourceScope::namespace("other.example").unwrap();
    assert_eq!(
        policy.validate_native_scope(&token, &foreign, 100),
        Err(JwtError::ScopeMismatch)
    );
    assert_eq!(
        policy.validate(&token, &foreign, 100),
        Err(JwtError::ScopeMismatch)
    );
}

#[test]
fn native_scope_validation_keeps_credential_time_and_resource_bounds() {
    let policy = policy();
    let requested = requested();
    let token = signed(&header(), &claims());
    let (input, signature) = token.rsplit_once('.').unwrap();
    let mut signature = URL_SAFE_NO_PAD.decode(signature).unwrap();
    signature[0] ^= 1;
    let mut bad_header = header();
    bad_header["alg"] = json!("HS256");
    let mut invalid = vec![
        ("x.x.x".to_owned(), 100, JwtError::InvalidEncoding),
        ("x".repeat(MAX_TOKEN_BYTES + 1), 100, JwtError::TooLarge),
        (token.clone(), 99, JwtError::NotYetValid),
        (token.clone(), 200, JwtError::Expired),
        (
            format!("{input}.{}", URL_SAFE_NO_PAD.encode(signature)),
            100,
            JwtError::InvalidSignature,
        ),
        (signed(&bad_header, &claims()), 100, JwtError::InvalidHeader),
    ];
    for (field, value, error) in [
        (
            "iss",
            json!("https://other.example/"),
            JwtError::IssuerMismatch,
        ),
        ("aud", json!("urn:other"), JwtError::AudienceMismatch),
        ("sub", json!("unknown"), JwtError::UnknownSubject),
        ("iat", json!(101), JwtError::NotYetValid),
        ("exp", json!(100), JwtError::InvalidClaims),
        ("nbf", json!(101), JwtError::NotYetValid),
    ] {
        let mut claims = claims();
        claims[field] = value;
        invalid.push((signed(&header(), &claims), 100, error));
    }
    for (token, now, error) in invalid {
        assert_eq!(
            policy.validate_native_scope(&token, &requested, now),
            Err(error)
        );
        assert_eq!(policy.validate(&token, &requested, now), Err(error));
    }
    let oversized =
        ResourceScope::entity("tenant.example", "x".repeat(MAX_RESOURCE_BYTES)).unwrap();
    assert_eq!(
        policy.validate_native_scope(&token, &oversized, 100),
        Err(JwtError::TooLarge)
    );
    assert_eq!(
        policy.validate(&token, &oversized, 100),
        Err(JwtError::TooLarge)
    );
    let mut claims = claims();
    claims["roles"] = json!(["manage"]);
    claims["scope"] = json!("manage");
    let token = signed(&header(), &claims);
    let native = policy
        .validate_native_scope(&token, &requested, 100)
        .unwrap();
    assert_eq!(native.permissions(), PermissionSet::SEND);
    assert!(!native.allows(&requested, Permission::Manage, 100));
}

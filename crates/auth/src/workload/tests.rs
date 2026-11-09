use super::*;

#[path = "../../tests/workload_support/mod.rs"]
pub(super) mod support;
use serde_json::json;
use support::*;

fn load(value: &serde_json::Value) -> Result<WorkloadPolicy, WorkloadError> {
    WorkloadPolicy::from_json(&value.to_string(), ResourceScope::parse(ROOT).unwrap())
}

#[test]
fn policy_has_local_segment_scopes_and_redacted_prepared_state() {
    let fixture = Fixture::new();
    let policy = load(&fixture.config()).unwrap();
    assert!(policy.requires_client_certificate());
    assert_eq!(policy.namespace(), &ResourceScope::parse(ROOT).unwrap());
    let binding = &policy.0.bindings[PRINCIPAL];
    assert!(binding.scope.contains(
        &ResourceScope::parse("amqps://TENANT.example/ORDERS/$DeadLetterQueue").unwrap()
    ));
    assert!(
        !binding
            .scope
            .contains(&ResourceScope::parse("amqps://tenant.example/orders-archive").unwrap())
    );
    assert!(binding.permissions.allows(crate::Permission::Send));
    assert!(!binding.permissions.allows(crate::Permission::Manage));
    let debug = format!("{policy:?}");
    for sensitive in ["BEGIN CERTIFICATE", PRINCIPAL, "example.test"] {
        assert!(!debug.contains(sensitive));
    }
    let mut config = fixture.config();
    config["client_auth"] = json!("optional");
    config["bindings"] = json!([]);
    assert!(!load(&config).unwrap().requires_client_certificate());
}

#[test]
fn exact_uri_and_domain_profile_refuses_normalization_aliases() {
    assert!(valid_principal(PRINCIPAL, "example.test"));
    assert!(valid_principal(
        "spiffe://example.test/A_b-1.c",
        "example.test"
    ));
    for uri in [
        "SPIFFE://example.test/a",
        "spiffe://EXAMPLE.test/a",
        "spiffe://example.test./a",
        "spiffe://example.test",
        "spiffe://example.test/",
        "spiffe://example.test/a/",
        "spiffe://example.test/a//b",
        "spiffe://example.test/a/../b",
        "spiffe://example.test/./a",
        "spiffe://example.test/a%2fb",
        "spiffe://example.test/%61",
        "spiffe://example.test/a\\b",
        "spiffe://example.test:443/a",
        "spiffe://user@example.test/a",
        "spiffe://example.test/a?q=1",
        "spiffe://example.test/a#f",
        "spiffe://foreign.test/a",
        "https://example.test/a",
        "spiffe://example.test/é",
        "spiffe://example.test/a\n",
        "spiffe://example.test/a b",
    ] {
        assert!(!valid_principal(uri, "example.test"), "{uri:?}");
    }
    for domain in [
        "",
        "EXAMPLE.test",
        "*.test",
        "a..b",
        "a.",
        "-a",
        "a-",
        "é.test",
        "a:1",
        "a/b",
    ] {
        assert!(!valid_domain(domain), "{domain:?}");
    }
    assert!(!valid_domain(&"x".repeat(254)));
    assert!(!valid_domain(&format!("{}.test", "x".repeat(64))));
    assert!(!valid_principal(
        &format!("spiffe://example.test/{}", "x".repeat(512)),
        "example.test"
    ));
}

#[test]
fn config_refuses_unknown_ambiguous_foreign_or_out_of_bounds_values() {
    let fixture = Fixture::new();
    let base = fixture.config();
    let mut invalid = Vec::new();
    for (field, value) in [
        ("version", json!(0)),
        ("version", json!(2)),
        ("version", json!("1")),
        ("unknown", json!(true)),
        ("trust_domain", json!("EXAMPLE.test")),
        ("roots_pem", json!([])),
        ("roots_pem", json!(vec![fixture.ca.cert.pem(); 9])),
        (
            "roots_pem",
            json!([fixture.ca.cert.pem(), fixture.ca.cert.pem()]),
        ),
        ("roots_pem", json!(["not PEM"])),
        ("roots_pem", json!([""])),
        ("bindings", json!(vec![base["bindings"][0].clone(); 65])),
        (
            "bindings",
            json!([base["bindings"][0].clone(), base["bindings"][0].clone()]),
        ),
        ("bindings", json!([[PRINCIPAL, RESOURCE, ["send"]]])),
        ("client_auth", json!("disabled")),
        ("client_auth", json!(null)),
        ("max_connection_seconds", json!(0)),
        ("max_connection_seconds", json!(3601)),
        ("max_connection_seconds", json!(-1)),
        ("max_connection_seconds", json!(1.5)),
        ("revoked_leaf_sha256", json!(["AB".repeat(32)])),
        ("revoked_leaf_sha256", json!(vec!["aa".repeat(32); 257])),
        (
            "revoked_leaf_sha256",
            json!(["aa".repeat(32), "aa".repeat(32)]),
        ),
    ] {
        let mut config = base.clone();
        config[field] = value;
        invalid.push(config);
    }
    for (field, value) in [
        ("principal", json!("spiffe://foreign.test/a")),
        ("principal", json!("spiffe://example.test/a%2fb")),
        ("scope", json!("amqps://other.example/orders")),
        ("scope", json!("https://tenant.example/orders")),
        ("permissions", json!([])),
        ("permissions", json!(["send", "send"])),
        ("permissions", json!(["audit"])),
        ("permissions", json!(["cluster"])),
        ("unknown", json!(true)),
    ] {
        let mut config = base.clone();
        config["bindings"][0][field] = value;
        invalid.push(config);
    }
    for config in invalid {
        assert!(load(&config).is_err(), "invalid shape accepted");
    }
    assert!(
        WorkloadPolicy::from_json(&base.to_string(), ResourceScope::parse(RESOURCE).unwrap())
            .is_err()
    );
    let too_large = " ".repeat(MAX_CONFIG_BYTES + 1);
    assert_eq!(
        WorkloadPolicy::from_json(&too_large, ResourceScope::parse(ROOT).unwrap()).unwrap_err(),
        WorkloadError::TooLarge
    );
    for value in [
        "[]".to_string(),
        "{bad}".to_string(),
        format!("{}0{}", "[".repeat(9), "]".repeat(9)),
        base.to_string()
            .replacen("\"version\":1", "\"version\":1,\"\\u0076ersion\":1", 1),
        base.to_string().replacen(
            "\"principal\":",
            "\"principal\":\"ignored\",\"principal\":",
            1,
        ),
    ] {
        assert!(WorkloadPolicy::from_json(&value, ResourceScope::parse(ROOT).unwrap()).is_err());
    }
}

#[test]
fn maintained_der_parser_rejects_trailing_duplicate_extension_and_ca_usage_errors() {
    let fixture = Fixture::new();
    let mut trailing = fixture.ca.cert.der().to_vec();
    trailing.push(0);
    assert!(certificate(&trailing).is_err());
    assert!(certificate(&vec![0; MAX_CERT_BYTES + 1]).is_err());
    assert!(certificate::require_ca(&certificate(fixture.client.cert.der()).unwrap()).is_err());
    let mut params = ca_parameters("duplicate", now() as i64 - 60, now() as i64 + 60);
    params
        .custom_extensions
        .push(rcgen::CustomExtension::from_oid_content(
            &[2, 5, 29, 19],
            vec![0x30, 0],
        ));
    let id = signed(params, &fixture.ca);
    assert!(certificate(id.cert.der()).is_err());
    for pem in [
        String::new(),
        "garbage".into(),
        format!("junk\n{}", fixture.ca.cert.pem()),
        format!("{}junk", fixture.ca.cert.pem()),
        format!("{}{}", fixture.ca.cert.pem(), fixture.ca.cert.pem()),
        format!("{}junk\n-----END CERTIFICATE-----", fixture.ca.cert.pem()),
    ] {
        assert!(certificate::pem_certificates(pem.as_bytes(), 1).is_err());
    }
}

#[test]
fn exact_collection_and_json_byte_limits_accept_the_last_valid_entry() {
    let f = Fixture::new();
    let mut config = f.config();
    config["bindings"] = json!(
        (0..64)
            .map(
                |i| json!({"principal":format!("spiffe://example.test/p{i}"),
        "scope":ROOT,"permissions":["send","listen","manage"]})
            )
            .collect::<Vec<_>>()
    );
    config["revoked_leaf_sha256"] =
        json!((0..256).map(|i| format!("{i:064x}")).collect::<Vec<_>>());
    let json = config.to_string();
    let exact = format!("{json}{}", " ".repeat(MAX_CONFIG_BYTES - json.len()));
    let policy = WorkloadPolicy::from_json(&exact, ResourceScope::parse(ROOT).unwrap()).unwrap();
    let binding = &policy.0.bindings["spiffe://example.test/p63"];
    assert!(binding.permissions.allows(crate::Permission::Manage));
    assert!(!binding.permissions.allows(crate::Permission::Audit));
    assert!(!binding.permissions.allows(crate::Permission::Cluster));
    assert_eq!(policy.0.revoked.len(), 256);
    assert!(
        WorkloadPolicy::from_json(&(exact + " "), ResourceScope::parse(ROOT).unwrap()).is_err()
    );
    let roots = (0..8)
        .map(|i| root(&format!("root-{i}")))
        .collect::<Vec<_>>();
    assert_eq!(
        load(&configuration(&roots.iter().collect::<Vec<_>>()))
            .unwrap()
            .0
            .roots
            .len(),
        8
    );
    let oversized_nodes = format!("{{\"extra\":[{}]}}", vec!["0"; MAX_CONFIG_NODES].join(","));
    assert!(
        WorkloadPolicy::from_json(&oversized_nodes, ResourceScope::parse(ROOT).unwrap()).is_err()
    );
}

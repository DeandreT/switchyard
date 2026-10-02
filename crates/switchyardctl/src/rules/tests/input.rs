use std::fs;

use clap::Parser;
use prost::Message;
use tempfile::TempDir;

use super::*;

#[test]
fn documented_filter_file_is_a_complete_sql_input() {
    let filter = filter::parse_json(include_bytes!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../examples/rules/red.json"
    )))
    .unwrap();
    assert_eq!(
        filter.filter,
        Some(rule_filter::Filter::SqlFilter(v1::SqlRuleFilter {
            expression: "color = 'red' AND priority >= 3".into(),
            semantic_version: None,
        }))
    );
}

#[test]
fn true_false_and_empty_correlation_are_explicit_filters() {
    assert_eq!(
        parse(json!({"type":"true"})).unwrap().filter,
        Some(rule_filter::Filter::TrueFilter(v1::TrueRuleFilter {}))
    );
    assert_eq!(
        parse(json!({"type":"false"})).unwrap().filter,
        Some(rule_filter::Filter::FalseFilter(v1::FalseRuleFilter {}))
    );
    let proto = parse(json!({"type":"correlation","properties":[]})).unwrap();
    assert_eq!(
        proto.filter,
        Some(rule_filter::Filter::CorrelationFilter(
            v1::CorrelationRuleFilter::default()
        ))
    );
    for bad in [
        json!(null),
        json!({}),
        json!({"type":"unknown"}),
        json!({"type":"true","expression":"secret"}),
    ] {
        assert!(parse(bad).is_err());
    }
}

#[test]
fn sql_source_and_versions_are_forwarded_without_local_compilation() {
    for version in [None, Some(0), Some(1), Some(u32::MAX)] {
        let mut input = json!({"type":"sql","expression":"broken ="});
        if let Some(version) = version {
            input["semantic_version"] = json!(version);
        }
        let proto = parse(input).unwrap();
        assert_eq!(
            proto.filter,
            Some(rule_filter::Filter::SqlFilter(v1::SqlRuleFilter {
                expression: "broken =".into(),
                semantic_version: version
            }))
        );
    }
    for bad in [
        json!({"type":"sql"}),
        json!({"type":"sql","expression":null}),
        json!({"type":"sql","expression":"TRUE","semantic_version":"1"}),
    ] {
        assert!(parse(bad).is_err());
    }
}

#[test]
fn unknown_duplicate_and_trailing_json_fields_are_refused_without_echo() {
    for raw in [
        r#"{"type":"true","type":"false"}"#,
        r#"{"type":"sql","expression":"secret-A","expression":"secret-B"}"#,
        r#"{"type":"correlation","properties":[],"properties":[]}"#,
        r#"{"type":"correlation","properties":[{"name":"p","name":"secret-property","value":{"type":"null"}}]}"#,
        r#"{"type":"correlation","properties":[{"name":"p","value":{"type":"bool","value":true,"value":false}}]}"#,
        r#"{"type":"correlation","properties":[{"name":"p","value":{"type":"null","unknown":"secret-property"}}]}"#,
        r#"{"type":"correlation","unknown":"secret-field"}"#,
        r#"{"type":"true"} {"type":"false"}"#,
        "{secret-invalid-json",
        "",
        "\u{feff}{\"type\":\"true\"}",
    ] {
        let error = filter::parse_json(raw.as_bytes())
            .expect_err("invalid JSON")
            .to_string();
        assert!(!error.contains("secret"), "raw input leaked: {error}");
        assert!(
            raw.is_empty() || !error.contains(raw),
            "raw input leaked: {error}"
        );
    }
    assert!(filter::parse_json(&[0xff, 0xfe]).is_err());
}

#[test]
fn duplicate_properties_and_missing_values_are_not_silently_collapsed() {
    for input in [
        json!({"type":"correlation","properties":[{"name":"p","value":{"type":"null"}},{"name":"p","value":{"type":"string","value":"x"}}]}),
        json!({"type":"correlation","properties":[{"name":"p"}]}),
        json!({"type":"correlation","properties":[{"value":{"type":"null"}}]}),
        json!({"type":"correlation","properties":[{"name":"p","value":null}]}),
        json!({"type":"correlation","properties":[{"name":"p","value":{}}]}),
    ] {
        assert!(parse(input).is_err());
    }
    assert!(parse(json!({"type":"correlation","properties":[{"name":"p","value":{"type":"null"}},{"name":"P","value":{"type":"null"}}]})).is_ok());
}

#[test]
fn all_eight_system_conditions_share_the_thirty_two_condition_bound() {
    let properties = (0..24)
        .map(|index| json!({"name":format!("p{index:02}"),"value":{"type":"null"}}))
        .collect::<Vec<_>>();
    let mut exact = json!({"type":"correlation","correlation_id":"","message_id":"","to":"","reply_to":"","subject":"","session_id":"","reply_to_session_id":"","content_type":"","properties":properties});
    assert!(parse(exact.clone()).is_ok());
    exact["properties"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"overflow","value":{"type":"null"}}));
    assert!(parse(exact).is_err());
    let properties = (0..32)
        .map(|index| json!({"name":format!("p{index:02}"),"value":{"type":"null"}}))
        .collect::<Vec<_>>();
    assert!(parse(json!({"type":"correlation","properties":properties})).is_ok());
}

#[test]
fn bounded_filter_file_accepts_exact_limit_and_masks_file_json_errors() {
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("secret-filter-file.json");
    let mut content = br#"{"type":"true"}"#.to_vec();
    content.resize(MAX_FILTER_FILE_BYTES, b' ');
    fs::write(&path, &content).unwrap();
    assert_eq!(
        filter::load(&path).unwrap(),
        parse(json!({"type":"true"})).unwrap()
    );
    content.push(b' ');
    fs::write(&path, content).unwrap();
    let error = filter::load(&path).unwrap_err().to_string();
    assert!(!error.contains("secret-filter-file"));
    assert!(error.contains("filter"));
    fs::write(&path, b"secret-invalid-json").unwrap();
    let error = filter::load(&path).unwrap_err().to_string();
    assert!(!error.contains("secret-invalid-json"));
    assert!(!error.contains("secret-filter-file"));
    assert!(filter::load(&directory.path().join("missing-secret.json")).is_err());
    assert!(filter::load(directory.path()).is_err());
}

#[cfg(unix)]
#[test]
fn regular_file_symlink_is_supported_and_fifo_is_refused_without_blocking() {
    use std::os::unix::fs::symlink;
    let directory = TempDir::new().unwrap();
    let path = directory.path().join("filter.json");
    let link = directory.path().join("link.json");
    fs::write(&path, br#"{"type":"false"}"#).unwrap();
    symlink(&path, &link).unwrap();
    assert_eq!(
        filter::load(&link).unwrap(),
        parse(json!({"type":"false"})).unwrap()
    );
    let fifo = directory.path().join("filter.fifo");
    rustix::fs::mkfifoat(
        rustix::fs::CWD,
        &fifo,
        rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
    )
    .unwrap();
    assert!(filter::load(&fifo).is_err());
    let broken = directory.path().join("broken-link");
    symlink(directory.path().join("absent"), &broken).unwrap();
    assert!(filter::load(&broken).is_err());
}

#[test]
fn request_encoding_boundary_is_checked_before_connect() {
    let request = |length| v1::CreateRuleRequest {
        namespace: NAMESPACE.into(),
        subscription_path: PATH.into(),
        name: "bytes".into(),
        filter: Some(v1::RuleFilter {
            filter: Some(rule_filter::Filter::CorrelationFilter(
                v1::CorrelationRuleFilter {
                    properties: vec![v1::CorrelationProperty {
                        name: "p".into(),
                        value: Some(scalar_value(rule_scalar_value::Value::BinaryValue(
                            vec![0; length],
                        ))),
                    }],
                    ..Default::default()
                },
            )),
        }),
    };
    let mut low = 0;
    let mut high = MAX_REQUEST_BYTES;
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if request(middle).encoded_len() <= MAX_REQUEST_BYTES {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    let exact = request(low);
    assert_eq!(exact.encoded_len(), MAX_REQUEST_BYTES);
    assert!(validate_request_size(&exact).is_ok());
    assert_eq!(request(low + 1).encoded_len(), MAX_REQUEST_BYTES + 1);
    assert!(validate_request_size(&request(low + 1)).is_err());
    assert!(
        validate_request_size(&v1::ListRulesRequest {
            namespace: NAMESPACE.into(),
            subscription_path: PATH.into(),
            include_actions: true,
        })
        .is_ok()
    );
}

#[test]
fn command_parser_requires_filter_file_and_has_no_inline_action_upsert_or_paging() {
    use crate::{Arguments, Command};
    let parsed = Arguments::try_parse_from([
        "switchyardctl",
        "rule",
        "create",
        "Orders",
        "Alpha",
        "Rule",
        "--filter-file",
        "filter.json",
    ])
    .unwrap();
    assert!(
        matches!(parsed.command, Command::Rule { command:RuleCommand::Create(input) } if input.topic=="Orders" && input.subscription=="Alpha" && input.name=="Rule" && input.filter_file==std::path::Path::new("filter.json"))
    );
    for args in [
        vec!["rule", "create", "Orders", "Alpha", "Rule"],
        vec![
            "rule",
            "create",
            "Orders",
            "Alpha",
            "Rule",
            "--filter-file",
            "filter.json",
            "--action",
            "TRUE",
        ],
        vec!["rule", "update", "Orders", "Alpha", "Rule"],
        vec!["rule", "list", "Orders", "Alpha", "--page-size", "1"],
        vec![
            "rule",
            "delete",
            "Orders",
            "Alpha",
            "Rule",
            "--filter-file",
            "filter.json",
        ],
        vec!["rule", "get", "Orders", "Alpha"],
    ] {
        let mut argv = vec!["switchyardctl"];
        argv.extend(args);
        assert!(Arguments::try_parse_from(argv).is_err());
    }
}

#[test]
fn rule_names_and_subscription_components_preserve_native_literal_bytes() {
    for name in ["$Default", " Priority ", &"\u{1f600}".repeat(25)] {
        assert!(validate_rule_name(name).is_ok());
    }
    for name in [
        "",
        "   ",
        "a/b",
        "a\\b",
        "a@b",
        "a?b",
        "a#b",
        "a*b",
        "a\nb",
        &"x".repeat(51),
        &"\u{1f600}".repeat(26),
    ] {
        assert!(validate_rule_name(name).is_err());
    }
    let command = RuleCommand::Get {
        topic: "/Orders/$Management".into(),
        subscription: "Subscriptions".into(),
        name: " Priority ".into(),
    };
    let PreparedCommand::Get(input) = prepare(NAMESPACE, &command).unwrap() else {
        panic!("get");
    };
    assert_eq!(input.namespace, NAMESPACE);
    assert_eq!(
        input.subscription_path,
        "/Orders/$Management/subscriptions/Subscriptions"
    );
    assert_eq!(input.name, " Priority ");
    for (topic, subscription) in [
        ("Orders/subscriptions/nested", "Alpha"),
        ("Orders", "bad_"),
        ("Orders", "/Alpha"),
        ("Orders/$DeadLetterQueue", "Alpha"),
    ] {
        assert!(
            prepare(
                NAMESPACE,
                &RuleCommand::List {
                    topic: topic.into(),
                    subscription: subscription.into()
                }
            )
            .is_err()
        );
    }
}

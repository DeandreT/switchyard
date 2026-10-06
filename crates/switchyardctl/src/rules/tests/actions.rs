use std::{fs, path::Path};

use clap::Parser;
use prost::Message;
use tempfile::TempDir;

use super::*;

fn parse_action(input: Value) -> Result<v1::SqlRuleAction, CliError> {
    action::parse_json(&serde_json::to_vec(&input).unwrap())
}

#[test]
fn documented_action_file_is_a_complete_sql_input() {
    assert_eq!(
        action::parse_json(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../examples/rules/remove-audit.json"
        )))
        .unwrap(),
        v1::SqlRuleAction {
            expression: "REMOVE user.audit;".into(),
            semantic_version: Some(1),
        }
    );
}

#[test]
fn sql_action_sources_and_typed_versions_remain_server_decisions() {
    let source = " /* exact source */ REMOVE user.[colour];\n";
    for version in [None, Some(0), Some(1), Some(2), Some(u32::MAX)] {
        let mut input = json!({"type":"sql","expression":source});
        if let Some(version) = version {
            input["semantic_version"] = json!(version);
        }
        assert_eq!(
            parse_action(input).unwrap(),
            v1::SqlRuleAction {
                expression: source.into(),
                semantic_version: version
            }
        );
    }
    for source in ["SET user.secret = 'value'", "REMOVE", "", &"x".repeat(5000)] {
        assert_eq!(
            parse_action(json!({"type":"sql","expression":source}))
                .unwrap()
                .expression,
            source
        );
    }
}

#[test]
fn action_json_requires_the_sql_discriminant_and_strict_field_types() {
    for input in [
        json!(null),
        json!({}),
        json!({"expression":"REMOVE secret"}),
        json!({"type":"remove","expression":"REMOVE secret"}),
        json!({"type":"sql"}),
        json!({"type":"sql","expression":null}),
        json!({"type":"sql","expression":42}),
        json!({"type":"sql","expression":"REMOVE secret","parameters":[]}),
        json!({"type":"sql","expression":"REMOVE secret","semantic_version":"1"}),
        json!({"type":"sql","expression":"REMOVE secret","semantic_version":null}),
        json!({"type":"sql","expression":"REMOVE secret","semantic_version":-1}),
        json!({"type":"sql","expression":"REMOVE secret","semantic_version":1.5}),
        json!({"type":"sql","expression":"REMOVE secret","semantic_version":4294967296_u64}),
    ] {
        let error = parse_action(input).unwrap_err().to_string();
        assert!(!error.contains("secret"));
    }
}

#[test]
fn action_json_refuses_duplicate_fields_trailing_data_and_invalid_encoding() {
    for input in [
        r#"{"type":"sql","type":"sql","expression":"REMOVE secret"}"#,
        r#"{"type":"sql","expression":"REMOVE secret","expression":"REMOVE other"}"#,
        r#"{"type":"sql","expression":"REMOVE secret","semantic_version":1,"semantic_version":2}"#,
        r#"{"type":"sql","expression":"REMOVE secret"} {}"#,
        "secret-invalid-json",
        "",
        "\u{feff}{\"type\":\"sql\",\"expression\":\"REMOVE secret\"}",
    ] {
        let error = action::parse_json(input.as_bytes())
            .unwrap_err()
            .to_string();
        assert!(!error.contains("secret"));
    }
    assert!(action::parse_json(&[0xff, 0xfe]).is_err());
}

#[test]
fn action_file_accepts_exact_cap_and_refuses_larger_or_nonregular_files_privately() {
    let files = TempDir::new().unwrap();
    let path = files.path().join("private-action.json");
    let mut bytes = br#"{"type":"sql","expression":"REMOVE x"}"#.to_vec();
    bytes.resize(MAX_ACTION_FILE_BYTES, b' ');
    fs::write(&path, &bytes).unwrap();
    assert_eq!(action::load(&path).unwrap().expression, "REMOVE x");
    bytes.push(b' ');
    fs::write(&path, &bytes).unwrap();
    assert!(action::parse_json(&bytes).is_err());
    for path in [
        path,
        files.path().join("private-missing.json"),
        files.path().into(),
    ] {
        let error = action::load(&path).unwrap_err().to_string();
        assert!(error.contains("action"));
        assert!(!error.contains("private"));
    }
}

#[cfg(unix)]
#[test]
fn action_file_reuses_regular_symlink_and_nonblocking_fifo_rules() {
    use std::os::unix::fs::symlink;
    let files = TempDir::new().unwrap();
    let regular = files.path().join("action.json");
    let link = files.path().join("link.json");
    fs::write(&regular, br#"{"type":"sql","expression":"REMOVE x"}"#).unwrap();
    symlink(&regular, &link).unwrap();
    assert_eq!(
        action::load(&link).unwrap(),
        action::load(&regular).unwrap()
    );
    let fifo = files.path().join("action.fifo");
    rustix::fs::mkfifoat(
        rustix::fs::CWD,
        &fifo,
        rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
    )
    .unwrap();
    assert!(action::load(&fifo).is_err());
    let fifo_link = files.path().join("fifo-link");
    symlink(&fifo, &fifo_link).unwrap();
    assert!(action::load(&fifo_link).is_err());
}

#[test]
fn action_file_is_optional_but_never_replaces_the_required_filter_file() {
    use crate::{Arguments, Command};
    let parsed = Arguments::try_parse_from([
        "switchyardctl",
        "rule",
        "create",
        "Orders",
        "Alpha",
        "Action",
        "--filter-file",
        "filter.json",
        "--action-file",
        "action.json",
    ])
    .unwrap();
    assert!(matches!(parsed.command,
        Command::Rule { command: RuleCommand::Create(input) }
            if input.filter_file == Path::new("filter.json")
                && input.action_file.as_deref() == Some(Path::new("action.json"))));
    for args in [
        vec![
            "rule",
            "create",
            "Orders",
            "Alpha",
            "Action",
            "--action-file",
            "action.json",
        ],
        vec![
            "rule",
            "create",
            "Orders",
            "Alpha",
            "Action",
            "--filter-file",
            "filter.json",
            "--action-file",
        ],
        vec![
            "rule",
            "get",
            "Orders",
            "Alpha",
            "Action",
            "--action-file",
            "action.json",
        ],
    ] {
        let mut argv = vec!["switchyardctl"];
        argv.extend(args);
        assert!(Arguments::try_parse_from(argv).is_err());
    }
}

#[test]
fn prepared_action_and_plain_creates_select_distinct_requests() {
    let files = TempDir::new().unwrap();
    let filter_file = files.path().join("filter.json");
    let action_file = files.path().join("action.json");
    fs::write(&filter_file, br#"{"type":"true"}"#).unwrap();
    fs::write(&action_file, br#"{"type":"sql","expression":"REMOVE [x]"}"#).unwrap();
    let command = |action_file| {
        RuleCommand::Create(RuleCreate {
            topic: "Orders".into(),
            subscription: "Alpha".into(),
            name: "Action".into(),
            filter_file: filter_file.clone(),
            action_file,
        })
    };
    let PreparedCommand::Create(plain) = prepare(NAMESPACE, &command(None)).unwrap() else {
        panic!("plain create must use its original RPC");
    };
    assert_eq!(plain.name, "Action");
    let PreparedCommand::CreateWithAction(action) =
        prepare(NAMESPACE, &command(Some(action_file.clone()))).unwrap()
    else {
        panic!("action create must use its dedicated RPC");
    };
    assert_eq!(action.filter, plain.filter);
    assert_eq!(action.action.unwrap().expression, "REMOVE [x]");
    fs::write(&action_file, b"invalid-private-action").unwrap();
    assert!(prepare(NAMESPACE, &command(Some(action_file.clone()))).is_err());
    fs::write(&action_file, br#"{"type":"sql","expression":"REMOVE x"}"#).unwrap();
    fs::write(&filter_file, b"invalid-private-filter").unwrap();
    assert!(prepare(NAMESPACE, &command(Some(action_file))).is_err());
}

#[test]
fn action_request_combined_encoded_limit_is_checked_before_connect() {
    let request = |length| v1::CreateRuleWithActionRequest {
        namespace: NAMESPACE.into(),
        subscription_path: PATH.into(),
        name: "Action".into(),
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
        action: Some(v1::SqlRuleAction {
            expression: "REMOVE x".into(),
            semantic_version: None,
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
    assert_eq!(request(low).encoded_len(), MAX_REQUEST_BYTES);
    assert!(validate_request_size(&request(low)).is_ok());
    assert_eq!(request(low + 1).encoded_len(), MAX_REQUEST_BYTES + 1);
    assert!(validate_request_size(&request(low + 1)).is_err());
}

#[test]
fn get_and_list_explicitly_request_action_visibility() {
    let get = RuleCommand::Get {
        topic: "Orders".into(),
        subscription: "Alpha".into(),
        name: "Action".into(),
    };
    let PreparedCommand::Get(get) = prepare(NAMESPACE, &get).unwrap() else {
        panic!("get");
    };
    assert!(get.include_actions);
    let list = RuleCommand::List {
        topic: "Orders".into(),
        subscription: "Alpha".into(),
    };
    let PreparedCommand::List(list) = prepare(NAMESPACE, &list).unwrap() else {
        panic!("list");
    };
    assert!(list.include_actions);
}

#[test]
fn action_output_preserves_exact_source_and_omits_absent_actions() {
    let source = " /* exact */ REMOVE user.[colour];\n";
    let mut input = valid_rule("Action");
    input.action = Some(v1::SqlRuleAction {
        expression: source.into(),
        semantic_version: Some(1),
    });
    let value = serde_json::to_value(output::rule(input, NAMESPACE, PATH, Some("Action")).unwrap())
        .unwrap();
    assert_eq!(
        value["action"],
        json!({"type":"sql","expression":source,"semantic_version":1})
    );
    let plain =
        serde_json::to_value(output::rule(valid_rule("Plain"), NAMESPACE, PATH, None).unwrap())
            .unwrap();
    assert!(plain.get("action").is_none());
}

#[test]
fn action_output_requires_supported_present_versions_and_source_bounds() {
    assert!(
        action::from_protobuf(v1::SqlRuleAction {
            expression: "SET number=7".into(),
            semantic_version: Some(2)
        })
        .is_ok()
    );
    for version in [None, Some(0), Some(3), Some(u32::MAX)] {
        assert!(
            action::from_protobuf(v1::SqlRuleAction {
                expression: "REMOVE x".into(),
                semantic_version: version
            })
            .is_err()
        );
    }
    let exact = "\u{0800}".repeat(1024);
    assert!(
        action::from_protobuf(v1::SqlRuleAction {
            expression: exact,
            semantic_version: Some(1)
        })
        .is_ok()
    );
    for source in ["x".repeat(1025), "x".repeat(4097), "\u{1f600}".repeat(513)] {
        let mut input = valid_rule("Action");
        input.action = Some(v1::SqlRuleAction {
            expression: source,
            semantic_version: Some(1),
        });
        assert!(output::rule(input, NAMESPACE, PATH, None).is_err());
    }
}

#[test]
fn late_invalid_action_refuses_the_complete_list_without_a_partial_result() {
    let mut last = valid_rule("Z");
    last.action = Some(v1::SqlRuleAction {
        expression: "REMOVE private".into(),
        semantic_version: None,
    });
    let error = output::list(
        v1::ListRulesResponse {
            rules: vec![valid_rule("A"), last],
        },
        NAMESPACE,
        PATH,
    )
    .unwrap_err()
    .to_string();
    assert!(!error.contains("private"));
    assert!(error.contains("invalid rule response"));
}

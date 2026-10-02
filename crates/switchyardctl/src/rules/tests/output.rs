use prost::Message;

use super::*;

#[test]
fn complete_rule_output_has_exact_identity_and_decimal_timestamp() {
    let mut input = valid_rule(" Literal Rule ");
    input.created_at_unix_millis = u64::MAX;
    let output = output::rule(input, NAMESPACE, PATH, Some(" Literal Rule ")).unwrap();
    assert_eq!(
        serde_json::to_value(output).unwrap(),
        json!({
            "namespace":NAMESPACE,"subscription_path":PATH,"name":" Literal Rule ",
            "filter":{"type":"true"},"created_at_unix_millis":"18446744073709551615",
        })
    );
    let mut input = valid_rule("sql");
    input.filter = Some(v1::RuleFilter {
        filter: Some(rule_filter::Filter::SqlFilter(v1::SqlRuleFilter {
            expression: "color = 'blue'".into(),
            semantic_version: Some(1),
        })),
    });
    let output =
        serde_json::to_value(output::rule(input, NAMESPACE, PATH, Some("sql")).unwrap()).unwrap();
    assert_eq!(
        output["filter"],
        json!({"type":"sql","expression":"color = 'blue'","semantic_version":1})
    );
    assert_eq!(output["created_at_unix_millis"], "1000");
}

#[test]
fn rule_response_rejects_wrong_identity_missing_filter_and_invalid_names() {
    let valid = valid_rule("A");
    let mut invalid = Vec::new();
    let mut wrong = valid.clone();
    wrong.namespace = "Tenant".into();
    invalid.push(wrong);
    let mut wrong = valid.clone();
    wrong.subscription_path = "orders/subscriptions/Alpha".into();
    invalid.push(wrong);
    let mut wrong = valid.clone();
    wrong.subscription_path = "Orders/SUBSCRIPTIONS/Alpha".into();
    invalid.push(wrong);
    let mut wrong = valid.clone();
    wrong.name = "a".into();
    invalid.push(wrong);
    let mut wrong = valid.clone();
    wrong.filter = None;
    invalid.push(wrong);
    let mut wrong = valid.clone();
    wrong.filter = Some(v1::RuleFilter::default());
    invalid.push(wrong);
    let mut wrong = valid.clone();
    wrong.name = "bad/name".into();
    invalid.push(wrong);
    let mut wrong = valid.clone();
    wrong.name = "\u{1f600}".repeat(26);
    invalid.push(wrong);
    for response in invalid {
        assert!(output::rule(response, NAMESPACE, PATH, Some("A")).is_err());
    }
    assert!(output::rule(valid, NAMESPACE, PATH, Some("A")).is_ok());
}

#[test]
fn sql_responses_require_current_semantic_version_without_echoing_source() {
    for version in [None, Some(0), Some(2), Some(u32::MAX)] {
        let mut response = valid_rule("sql");
        response.filter = Some(v1::RuleFilter {
            filter: Some(rule_filter::Filter::SqlFilter(v1::SqlRuleFilter {
                expression: "secret-sql-source".into(),
                semantic_version: version,
            })),
        });
        let error = match output::rule(response, NAMESPACE, PATH, Some("sql")) {
            Ok(_) => panic!("unsupported response version"),
            Err(error) => error.to_string(),
        };
        assert!(!error.contains("secret-sql-source"));
    }
}

#[test]
fn correlation_response_validates_complete_properties_before_output() {
    let response = |properties| {
        let mut rule = valid_rule("correlation");
        rule.filter = Some(v1::RuleFilter {
            filter: Some(rule_filter::Filter::CorrelationFilter(
                v1::CorrelationRuleFilter {
                    properties,
                    ..Default::default()
                },
            )),
        });
        rule
    };
    let property = |name: &str, value| v1::CorrelationProperty {
        name: name.into(),
        value,
    };
    for properties in [
        vec![property("p", None)],
        vec![property("p", Some(v1::RuleScalarValue::default()))],
        vec![property(
            "p",
            Some(scalar_value(rule_scalar_value::Value::UbyteValue(256))),
        )],
        vec![
            property(
                "p",
                Some(scalar_value(rule_scalar_value::Value::NullValue(
                    v1::RuleNullValue {},
                ))),
            ),
            property(
                "p",
                Some(scalar_value(rule_scalar_value::Value::BoolValue(true))),
            ),
        ],
        (0..33)
            .map(|index| {
                property(
                    &format!("p{index:02}"),
                    Some(scalar_value(rule_scalar_value::Value::NullValue(
                        v1::RuleNullValue {},
                    ))),
                )
            })
            .collect(),
    ] {
        assert!(output::rule(response(properties), NAMESPACE, PATH, Some("correlation")).is_err());
    }
    let properties = vec![property(
        "bits",
        Some(scalar_value(rule_scalar_value::Value::DoubleBits(
            0x7ff8_0000_0000_0001,
        ))),
    )];
    let output = serde_json::to_value(
        output::rule(response(properties), NAMESPACE, PATH, Some("correlation")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        output["filter"]["properties"][0],
        json!({"name":"bits","value":{"type":"double_bits","value":"7ff8000000000001"}})
    );
}

#[test]
fn whole_list_is_bounded_sorted_and_rejects_any_bad_member_before_return() {
    let response = |names: &[&str]| v1::ListRulesResponse {
        rules: names.iter().map(|name| valid_rule(name)).collect(),
    };
    assert_eq!(
        serde_json::to_value(output::list(response(&[]), NAMESPACE, PATH).unwrap()).unwrap(),
        json!({"rules":[]})
    );
    let valid = output::list(response(&["$Default", "A", "a"]), NAMESPACE, PATH).unwrap();
    assert_eq!(
        serde_json::to_value(valid).unwrap()["rules"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    for names in [vec!["A", "A"], vec!["a", "A"], vec!["A", "$Default"]] {
        assert!(output::list(response(&names), NAMESPACE, PATH).is_err());
    }
    let mut last_bad = response(&["A", "B", "C"]);
    last_bad.rules[2].filter = None;
    assert!(output::list(last_bad, NAMESPACE, PATH).is_err());
    let mut sibling = response(&["A", "B"]);
    sibling.rules[1].subscription_path = "Orders/subscriptions/Beta".into();
    assert!(output::list(sibling, NAMESPACE, PATH).is_err());
    let exact = v1::ListRulesResponse {
        rules: (0..32)
            .map(|index| valid_rule(&format!("rule{index:02}")))
            .collect(),
    };
    assert!(output::list(exact.clone(), NAMESPACE, PATH).is_ok());
    let mut overflow = exact;
    overflow.rules.push(valid_rule("rule32"));
    assert!(output::list(overflow, NAMESPACE, PATH).is_err());
}

#[test]
fn mutation_output_reports_only_the_exact_synchronous_request_identity() {
    assert_eq!(
        serde_json::to_value(output::mutation(NAMESPACE, PATH, " Literal Rule ")).unwrap(),
        json!({
            "namespace":NAMESPACE,"subscription_path":PATH,"name":" Literal Rule ","completed":true,
        })
    );
}

#[test]
fn whole_list_encoded_response_limit_is_checked_before_conversion() {
    let response = |last_length| v1::ListRulesResponse {
        rules: (0..32)
            .map(|index| {
                let mut rule = valid_rule(&format!("rule{index:02}"));
                let length = if index == 31 { last_length } else { 32 * 1024 };
                rule.filter = Some(v1::RuleFilter {
                    filter: Some(rule_filter::Filter::CorrelationFilter(
                        v1::CorrelationRuleFilter {
                            properties: vec![v1::CorrelationProperty {
                                name: "p".into(),
                                value: Some(scalar_value(rule_scalar_value::Value::StringValue(
                                    "x".repeat(length),
                                ))),
                            }],
                            ..Default::default()
                        },
                    )),
                });
                rule
            })
            .collect(),
    };
    let mut low = 0_usize;
    let mut high = 64 * 1024 - 1;
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if response(middle).encoded_len() <= MAX_RESPONSE_BYTES {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    let exact = response(low);
    assert_eq!(exact.encoded_len(), MAX_RESPONSE_BYTES);
    assert!(output::list(exact, NAMESPACE, PATH).is_ok());
    let oversized = response(low + 1);
    assert_eq!(oversized.encoded_len(), MAX_RESPONSE_BYTES + 1);
    assert!(output::list(oversized, NAMESPACE, PATH).is_err());
}

use domain::{NamespaceName, RuleDefinition, SqlAction, StateMachine};
use prost::Message;
use storage::MemoryStore;

use super::super::action;
use super::*;
use crate::{Broker, LocalProposer, ManualClock, NativeAdminService};

fn input(expression: &str, semantic_version: Option<u32>) -> v1::SqlRuleAction {
    v1::SqlRuleAction {
        expression: expression.to_owned(),
        semantic_version,
    }
}

fn definition(name: &str, bytes: usize, action: Option<SqlAction>) -> RuleDefinition {
    RuleDefinition {
        name: rule_name(name).unwrap(),
        filter: RuleFilter::Correlation(CorrelationFilter {
            properties: BTreeMap::from([(String::from("p"), MessageValue::Binary(vec![0; bytes]))]),
            ..Default::default()
        }),
        created_at: Timestamp::UNIX_EPOCH,
        action,
    }
}

fn payload(rule: &mut RuleDefinition) -> &mut Vec<u8> {
    let RuleFilter::Correlation(filter) = &mut rule.filter else {
        panic!("correlation fixture");
    };
    let Some(MessageValue::Binary(bytes)) = filter.properties.get_mut("p") else {
        panic!("binary fixture");
    };
    bytes
}

fn response_service() -> (Broker, NativeAdminService, RuleTarget) {
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(MemoryStore::default()),
        ManualClock::at(1_000),
    ));
    let service = NativeAdminService::new(broker.handle(), NamespaceName::new("tenant").unwrap());
    let target = RuleTarget::parse("Orders/subscriptions/Alpha").unwrap();
    (broker, service, target)
}

#[test]
fn actions_require_presence_and_preserve_original_source_with_semantic_version() {
    assert_eq!(
        action::read(None).unwrap_err().code(),
        Code::InvalidArgument
    );
    let source = "  remove [Secret]; /* retained source */ REMOVE user.marker;  ";
    for version in [None, Some(domain::SQL_ACTION_SEMANTIC_VERSION)] {
        let output = action::write(&action::read(Some(&input(source, version))).unwrap());
        assert_eq!(output.expression, source);
        assert_eq!(output.semantic_version, Some(1));
        assert_ne!(output.semantic_version, Some(20));
    }
}

#[test]
fn action_version_refusal_precedes_source_bounds_and_does_not_echo_source() {
    let source = format!(
        "private-action-source{}",
        "x".repeat(domain::MAX_SQL_EXPRESSION_BYTES + 1)
    );
    for version in [0, 2, 20, u32::MAX] {
        let error = action::read(Some(&input(&source, Some(version)))).unwrap_err();
        assert_eq!(error.code(), Code::Unimplemented);
        assert_eq!(error.message(), "unsupported SQL action semantic version");
        assert!(!error.message().contains("private-action-source"));
    }
}

#[test]
fn borrowed_action_source_bounds_count_utf16_before_copying_or_compilation() {
    let exact = format!("REMOVE[{}]", "\u{1f600}".repeat(508));
    assert_eq!(
        exact.encode_utf16().count(),
        domain::MAX_SQL_EXPRESSION_UTF16_UNITS
    );
    assert_eq!(
        action::read(Some(&input(&exact, None)))
            .unwrap()
            .expression(),
        exact,
    );
    for source in [
        format!("REMOVE[{}]", "\u{1f600}".repeat(509)),
        "x".repeat(domain::MAX_SQL_EXPRESSION_BYTES + 1),
    ] {
        let error = action::read(Some(&input(&source, None))).unwrap_err();
        assert_eq!(error.code(), Code::ResourceExhausted);
        assert_eq!(error.message(), "SQL source limit reached");
    }
}

#[test]
fn action_grammar_tokens_and_statement_limits_have_static_statuses() {
    for (source, code, message) in [
        (
            "REMOVE private_action, other",
            Code::InvalidArgument,
            "invalid SQL action syntax",
        ),
        ("REMOVE", Code::InvalidArgument, "invalid SQL action syntax"),
        ("", Code::InvalidArgument, "invalid SQL action syntax"),
        (
            "SET private_action = 1",
            Code::Unimplemented,
            "unsupported SQL action",
        ),
        (
            "REMOVE sys.Subject",
            Code::Unimplemented,
            "unsupported SQL action",
        ),
        (
            "REMOVE property('private_action')",
            Code::Unimplemented,
            "unsupported SQL action",
        ),
    ] {
        let error = action::read(Some(&input(source, None))).unwrap_err();
        assert_eq!(error.code(), code);
        assert_eq!(error.message(), message);
        assert!(!error.message().contains("private_action"));
    }
    for source in [
        "REMOVE[a];".repeat(33),
        format!("{}REMOVE[a]", "/*x*/".repeat(127)),
    ] {
        let error = action::read(Some(&input(&source, None))).unwrap_err();
        assert_eq!(error.code(), Code::ResourceExhausted);
        assert_eq!(error.message(), "SQL compilation limit reached");
    }
    assert!(action::read(Some(&input(&"REMOVE[a];".repeat(32), None))).is_ok());
    assert!(
        action::read(Some(&input(
            &format!("{}REMOVE[a]", "/*x*/".repeat(126)),
            None
        )))
        .is_ok()
    );
}

#[test]
fn combined_definition_counts_action_bytes_at_the_exact_stored_limit() {
    let action = SqlAction::new(format!("/*{}*/REMOVE[p]", "x".repeat(400))).unwrap();
    let mut rule = definition("bounded", domain::MAX_RULE_BYTES / 2, Some(action));
    let overhead = domain::codec::encode(&rule).unwrap().len() - payload(&mut rule).len();
    payload(&mut rule).resize(domain::MAX_RULE_BYTES - overhead, 0);
    assert_eq!(rule.encoded_size().unwrap(), domain::MAX_RULE_BYTES);
    payload(&mut rule).push(0);
    assert_eq!(
        status::input(rule.encoded_size().unwrap_err()).code(),
        Code::ResourceExhausted
    );
    rule.action = None;
    assert!(rule.encoded_size().unwrap() < domain::MAX_RULE_BYTES);
}

#[test]
fn action_opt_in_preserves_no_action_responses_and_refuses_whole_legacy_lists() {
    let (_broker, service, target) = response_service();
    let plain = definition("a-plain", 0, None);
    let with_action = definition("z-action", 0, Some(SqlAction::new(" REMOVE[p]; ").unwrap()));
    let legacy_plain = service
        .rule_response(&target, plain.clone(), false)
        .unwrap();
    assert_eq!(
        legacy_plain,
        service.rule_response(&target, plain.clone(), true).unwrap()
    );
    assert_eq!(legacy_plain.action, None);
    let error = service
        .rule_response(&target, with_action.clone(), false)
        .unwrap_err();
    assert_eq!(error.code(), Code::Unimplemented);
    assert_eq!(error.message(), "rule action metadata was not requested");
    assert_eq!(
        service
            .rule_list_response(&target, vec![plain.clone(), with_action.clone()], false)
            .unwrap_err()
            .code(),
        Code::Unimplemented
    );
    let complete = service
        .rule_list_response(&target, vec![plain, with_action], true)
        .unwrap();
    assert_eq!(complete.rules.len(), 2);
    assert_eq!(complete.rules[0].action, None);
    assert_eq!(
        complete.rules[1].action,
        Some(input(" REMOVE[p]; ", Some(1)))
    );
}

#[test]
fn complete_action_lists_fit_the_exact_response_limit_or_refuse_without_truncation() {
    let (_broker, service, target) = response_service();
    let action = SqlAction::new("REMOVE[p];").unwrap();
    let mut rules: Vec<_> = (0..domain::MAX_SUBSCRIPTION_RULES)
        .map(|index| definition(&format!("r-{index:02}"), 32 * 1024, Some(action.clone())))
        .collect();
    let full = v1::ListRulesResponse {
        rules: rules
            .iter()
            .cloned()
            .map(|rule| service.rule_response(&target, rule, true).unwrap())
            .collect(),
    };
    let excess = full.encoded_len() - crate::NATIVE_ADMIN_RESPONSE_LIMIT;
    assert!(excess > 0 && excess < 32 * 1024);
    let last = rules.last_mut().unwrap();
    payload(last).truncate(32 * 1024 - excess);
    for rule in &rules {
        rule.encoded_size().unwrap();
    }
    let response = service
        .rule_list_response(&target, rules.clone(), true)
        .unwrap();
    assert_eq!(response.encoded_len(), crate::NATIVE_ADMIN_RESPONSE_LIMIT);
    assert_eq!(response.rules.len(), domain::MAX_SUBSCRIPTION_RULES);
    assert!(
        response
            .rules
            .iter()
            .all(|rule| rule.action.as_ref() == Some(&input("REMOVE[p];", Some(1))))
    );
    payload(rules.last_mut().unwrap()).push(0);
    let error = service
        .rule_list_response(&target, rules, true)
        .unwrap_err();
    assert_eq!(error.code(), Code::ResourceExhausted);
    assert_eq!(
        error.message(),
        "rule response exceeds its encoded-byte limit"
    );
}

#[test]
fn list_response_count_limit_is_not_raised_for_action_metadata() {
    let (_broker, service, target) = response_service();
    let rule = definition("a", 0, Some(SqlAction::new("REMOVE[p]").unwrap()));
    let error = service
        .rule_list_response(
            &target,
            vec![rule; domain::MAX_SUBSCRIPTION_RULES + 1],
            true,
        )
        .unwrap_err();
    assert_eq!(error.code(), Code::Internal);
    assert_eq!(error.message(), "invalid stored rule set");
}

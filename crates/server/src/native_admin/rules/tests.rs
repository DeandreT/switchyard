use std::collections::BTreeMap;

use admin_api::v1::{self, rule_filter::Filter, rule_scalar_value::Value};
use domain::{
    BrokerError, CorrelationFilter, MessageValue, RuleFilter, SqlCompileError, SqlCompileLimit,
    Timestamp,
};
use tonic::Code;

use super::{RuleTarget, filter, rule_name, scalar, status};
use crate::{ProposeError, SubmitError};

mod actions;

fn scalar_input(value: Value) -> v1::RuleScalarValue {
    v1::RuleScalarValue { value: Some(value) }
}

fn filter_input(filter: Filter) -> v1::RuleFilter {
    v1::RuleFilter {
        filter: Some(filter),
    }
}

fn property(name: &str, value: Value) -> v1::CorrelationProperty {
    v1::CorrelationProperty {
        name: name.to_owned(),
        value: Some(scalar_input(value)),
    }
}

fn scalars() -> Vec<MessageValue> {
    vec![
        MessageValue::Null,
        MessageValue::Bool(false),
        MessageValue::Ubyte(u8::MAX),
        MessageValue::Ushort(u16::MAX),
        MessageValue::Uint(u32::MAX),
        MessageValue::Ulong(u64::MAX),
        MessageValue::Byte(i8::MIN),
        MessageValue::Short(i16::MIN),
        MessageValue::Int(i32::MIN),
        MessageValue::Long(i64::MIN),
        MessageValue::Float(0x7fc0_1234),
        MessageValue::Double(0xfff0_0000_0000_0000),
        MessageValue::Decimal32([1, 2, 3, 4]),
        MessageValue::Decimal64([0xff; 8]),
        MessageValue::Decimal128([0x80; 16]),
        MessageValue::Char('\u{10ffff}'),
        MessageValue::Timestamp(i64::MIN),
        MessageValue::Uuid([0x55; 16]),
        MessageValue::Binary(vec![0, 255]),
        MessageValue::String(String::new()),
        MessageValue::Symbol(String::new()),
    ]
}

#[test]
fn all_scalar_constructors_roundtrip_without_width_or_bit_coercion() {
    for value in scalars() {
        let encoded = scalar::write(&value).expect("valid stored scalar");
        assert_eq!(scalar::read(&encoded).expect("valid request scalar"), value);
    }
    for value in [
        MessageValue::Float(0x8000_0000),
        MessageValue::Float(0x7f80_0000),
        MessageValue::Double(0x7ff8_0000_0000_0123),
        MessageValue::Double(0x8000_0000_0000_0000),
    ] {
        assert_eq!(
            scalar::read(&scalar::write(&value).unwrap()).unwrap(),
            value
        );
    }
    assert_ne!(
        scalar::write(&MessageValue::Uint(0)).unwrap(),
        scalar::write(&MessageValue::Ulong(0)).unwrap()
    );
}

#[test]
fn malformed_scalar_widths_characters_and_presence_are_invalid() {
    let invalid = [
        Value::UbyteValue(256),
        Value::UshortValue(65536),
        Value::ByteValue(-129),
        Value::ShortValue(32768),
        Value::Decimal32Bytes(vec![0; 3]),
        Value::Decimal64Bytes(vec![0; 9]),
        Value::Decimal128Bytes(vec![0; 15]),
        Value::UuidBytes(vec![0; 17]),
        Value::CharCodepoint(0xd800),
        Value::CharCodepoint(0x110000),
        Value::SymbolValue(String::from("\u{00e9}")),
    ];
    for value in invalid {
        assert_eq!(
            scalar::read(&scalar_input(value)).unwrap_err().code(),
            Code::InvalidArgument
        );
    }
    assert_eq!(
        scalar::read(&v1::RuleScalarValue { value: None })
            .unwrap_err()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(
        scalar::read(&scalar_input(Value::BinaryValue(vec![
            0;
            domain::MAX_RULE_BYTES
                + 1
        ])))
        .unwrap_err()
        .code(),
        Code::ResourceExhausted
    );
}

#[test]
fn stored_compounds_and_non_ascii_symbols_never_escape_as_scalars() {
    for value in [
        MessageValue::List(vec![]),
        MessageValue::Map(vec![]),
        MessageValue::Array(vec![]),
        MessageValue::Symbol(String::from("\u{00e9}")),
    ] {
        assert_eq!(scalar::write(&value).unwrap_err().code(), Code::Internal);
    }
}

#[test]
fn correlation_roundtrip_preserves_empty_system_conditions_and_exact_names() {
    let filter = RuleFilter::Correlation(CorrelationFilter {
        correlation_id: Some(String::new()),
        message_id: Some(String::from("id")),
        to: Some(String::new()),
        reply_to: Some(String::new()),
        subject: Some(String::new()),
        session_id: Some(String::new()),
        reply_to_session_id: Some(String::new()),
        content_type: Some(String::new()),
        properties: scalars()
            .into_iter()
            .enumerate()
            .map(|(index, value)| (format!("property-{index}"), value))
            .collect(),
    });
    assert_eq!(
        filter::read(Some(&filter::write(&filter).unwrap())).unwrap(),
        filter
    );
    let input = filter_input(Filter::CorrelationFilter(v1::CorrelationRuleFilter {
        properties: vec![
            property("kind", Value::BoolValue(false)),
            property("Kind", Value::NullValue(v1::RuleNullValue {})),
        ],
        ..Default::default()
    }));
    let RuleFilter::Correlation(result) = filter::read(Some(&input)).unwrap() else {
        panic!("correlation filter");
    };
    assert_eq!(result.properties.len(), 2);
}

#[test]
fn correlation_duplicate_names_missing_values_and_caps_are_local_rejections() {
    let duplicate = filter_input(Filter::CorrelationFilter(v1::CorrelationRuleFilter {
        properties: vec![
            property("p", Value::UintValue(0)),
            property("p", Value::UlongValue(0)),
        ],
        ..Default::default()
    }));
    assert_eq!(
        filter::read(Some(&duplicate)).unwrap_err().code(),
        Code::InvalidArgument
    );
    let missing = filter_input(Filter::CorrelationFilter(v1::CorrelationRuleFilter {
        properties: vec![v1::CorrelationProperty {
            name: String::from("p"),
            value: None,
        }],
        ..Default::default()
    }));
    assert_eq!(
        filter::read(Some(&missing)).unwrap_err().code(),
        Code::InvalidArgument
    );
    let capped = filter_input(Filter::CorrelationFilter(v1::CorrelationRuleFilter {
        correlation_id: Some(String::new()),
        properties: (0..domain::MAX_CORRELATION_RULE_CONDITIONS)
            .map(|index| property(&index.to_string(), Value::NullValue(v1::RuleNullValue {})))
            .collect(),
        ..Default::default()
    }));
    assert_eq!(
        filter::read(Some(&capped)).unwrap_err().code(),
        Code::ResourceExhausted
    );
    let oversized = filter_input(Filter::CorrelationFilter(v1::CorrelationRuleFilter {
        subject: Some("x".repeat(domain::MAX_RULE_BYTES + 1)),
        ..Default::default()
    }));
    assert_eq!(
        filter::read(Some(&oversized)).unwrap_err().code(),
        Code::ResourceExhausted
    );
}

#[test]
fn exact_rule_envelope_overhead_is_counted_after_borrowed_payload_admission() {
    let filter = filter_input(Filter::CorrelationFilter(v1::CorrelationRuleFilter {
        properties: vec![property(
            "p",
            Value::BinaryValue(vec![0; domain::MAX_RULE_BYTES]),
        )],
        ..Default::default()
    }));
    assert_eq!(
        filter::read(Some(&filter)).unwrap_err().code(),
        Code::ResourceExhausted
    );
    let rule = domain::RuleDefinition {
        name: rule_name("bounded").unwrap(),
        created_at: Timestamp::UNIX_EPOCH,
        action: None,
        filter: RuleFilter::Correlation(CorrelationFilter {
            properties: BTreeMap::from([(
                String::new(),
                MessageValue::Binary(vec![0; domain::MAX_RULE_BYTES]),
            )]),
            ..Default::default()
        }),
    };
    assert_eq!(
        status::input(rule.encoded_size().unwrap_err()).code(),
        Code::ResourceExhausted
    );
}

#[test]
fn sql_presence_versions_source_and_parser_failures_remain_distinct() {
    for version in [None, Some(domain::SQL_FILTER_SEMANTIC_VERSION)] {
        let input = filter_input(Filter::SqlFilter(v1::SqlRuleFilter {
            expression: String::from("  colour = 'red'  "),
            semantic_version: version,
        }));
        let output = filter::write(&filter::read(Some(&input)).unwrap()).unwrap();
        let Some(Filter::SqlFilter(output)) = output.filter else {
            panic!("SQL filter");
        };
        assert_eq!(output.expression, "  colour = 'red'  ");
        assert_eq!(
            output.semantic_version,
            Some(domain::SQL_FILTER_SEMANTIC_VERSION)
        );
    }
    for version in [0, 2, u32::MAX] {
        let input = filter_input(Filter::SqlFilter(v1::SqlRuleFilter {
            expression: "x".repeat(domain::MAX_SQL_EXPRESSION_BYTES + 1),
            semantic_version: Some(version),
        }));
        assert_eq!(
            filter::read(Some(&input)).unwrap_err().code(),
            Code::Unimplemented
        );
    }
    for (source, code) in [
        (String::from("colour ="), Code::InvalidArgument),
        (String::from("sys.NoSuchProperty"), Code::Unimplemented),
        (
            "x".repeat(domain::MAX_SQL_EXPRESSION_UTF16_UNITS + 1),
            Code::ResourceExhausted,
        ),
    ] {
        let input = filter_input(Filter::SqlFilter(v1::SqlRuleFilter {
            expression: source,
            semantic_version: None,
        }));
        assert_eq!(filter::read(Some(&input)).unwrap_err().code(), code);
    }
}

#[test]
fn explicit_boolean_filters_and_missing_oneof_do_not_conflate() {
    for (input, expected) in [
        (Filter::TrueFilter(v1::TrueRuleFilter {}), RuleFilter::True),
        (
            Filter::FalseFilter(v1::FalseRuleFilter {}),
            RuleFilter::False,
        ),
    ] {
        assert_eq!(filter::read(Some(&filter_input(input))).unwrap(), expected);
    }
    for input in [None, Some(v1::RuleFilter { filter: None })] {
        assert_eq!(
            filter::read(input.as_ref()).unwrap_err().code(),
            Code::InvalidArgument
        );
    }
}

#[test]
fn subscription_targets_canonicalize_only_the_structural_marker() {
    let target = RuleTarget::parse("Orders/SuBsCrIpTiOnS/Priority").unwrap();
    assert_eq!(target.path.as_str(), "Orders/subscriptions/Priority");
    assert_eq!(target.topic.as_str(), "Orders");
    assert_eq!(target.subscription.as_str(), "Priority");
    for path in [
        "orders",
        "orders/$DeadLetterQueue",
        "orders/Subscriptions/p/$DeadLetterQueue",
    ] {
        assert_eq!(
            RuleTarget::parse(path).err().unwrap().code(),
            Code::InvalidArgument
        );
    }
    assert!(rule_name("$Default").is_ok());
    assert!(rule_name(&"x".repeat(domain::MAX_RULE_NAME_LENGTH + 1)).is_err());
}

fn submit(error: BrokerError) -> SubmitError {
    SubmitError::Propose(ProposeError::Broker(error))
}

#[test]
fn rule_statuses_are_local_and_stored_failures_are_redacted() {
    assert_eq!(
        status::mutation(submit(BrokerError::RuleAlreadyExists)).code(),
        Code::AlreadyExists
    );
    assert_eq!(
        status::mutation(submit(BrokerError::RuleNotFound)).code(),
        Code::NotFound
    );
    for error in [
        BrokerError::RuleLimitExceeded { maximum: 32 },
        BrokerError::RuleTooLarge {
            maximum_bytes: 65536,
        },
        BrokerError::RuleSetTooLarge {
            maximum_bytes: 262144,
        },
    ] {
        assert_eq!(
            status::mutation(submit(error)).code(),
            Code::ResourceExhausted
        );
    }
    for error in [
        BrokerError::DanglingRuleMetadata,
        BrokerError::InvalidRule {
            reason: String::from("private stored detail"),
        },
        BrokerError::SqlRuleCompilation(SqlCompileError::Unsupported {
            feature: "private stored feature",
        }),
    ] {
        let status = status::read(submit(error));
        assert_eq!(status.code(), Code::Internal);
        assert_eq!(status.message(), "rule operation failed");
    }
    assert_eq!(
        status::read(submit(BrokerError::SqlRuleCompilation(
            SqlCompileError::Limit {
                kind: SqlCompileLimit::AggregateNodes,
                maximum: 1,
            }
        )))
        .code(),
        Code::ResourceExhausted
    );
}

#[test]
fn action_compilation_statuses_remain_static_and_distinct() {
    for (error, code) in [
        (SqlCompileError::Syntax, Code::InvalidArgument),
        (
            SqlCompileError::Unsupported {
                feature: "private-action-source",
            },
            Code::Unimplemented,
        ),
        (
            SqlCompileError::Limit {
                kind: domain::SqlCompileLimit::Nodes,
                maximum: 32,
            },
            Code::ResourceExhausted,
        ),
    ] {
        let status = status::mutation(submit(BrokerError::SqlActionCompilation(error)));
        assert_eq!(status.code(), code);
        assert!(!status.message().contains("private-action-source"));
    }
    assert_eq!(
        status::read(submit(BrokerError::SqlActionCompilation(
            SqlCompileError::Limit {
                kind: domain::SqlCompileLimit::AggregateSourceBytes,
                maximum: domain::MAX_SQL_COMPILE_SOURCE_BYTES,
            }
        )))
        .code(),
        Code::ResourceExhausted,
    );
}

#[test]
fn owner_and_clock_unavailability_preserve_the_established_mapping() {
    assert_eq!(
        status::read(SubmitError::BrokerStopped).code(),
        Code::Unavailable
    );
    assert_eq!(
        status::mutation(SubmitError::Propose(ProposeError::ClockWentBackward {
            last_applied: Timestamp::from_millis(2),
            now: Timestamp::from_millis(1),
            allowed_millis: 0,
        }))
        .code(),
        Code::Unavailable
    );
    assert_eq!(
        status::read(submit(BrokerError::EntityBindingStale)).code(),
        Code::NotFound
    );
    let unexpected = status::mutation(SubmitError::Propose(ProposeError::UnexpectedOutcome {
        outcome: String::from("private outcome"),
    }));
    assert_eq!(unexpected.code(), Code::Internal);
    assert_eq!(unexpected.message(), "rule operation failed");
}

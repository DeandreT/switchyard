use admin_api::{FILE_DESCRIPTOR_SET, PROTOBUF_PACKAGE, v1};
use prost::Message;
use prost_types::{
    DescriptorProto, FileDescriptorProto, FileDescriptorSet,
    field_descriptor_proto::{Label, Type},
};

fn descriptor() -> FileDescriptorProto {
    FileDescriptorSet::decode(FILE_DESCRIPTOR_SET)
        .expect("generated descriptors")
        .file
        .into_iter()
        .find(|file| file.package.as_deref() == Some(PROTOBUF_PACKAGE))
        .expect("native admin package")
}

fn message<'a>(file: &'a FileDescriptorProto, name: &str) -> &'a DescriptorProto {
    file.message_type
        .iter()
        .find(|message| message.name.as_deref() == Some(name))
        .expect("new rule message")
}

#[test]
fn rule_service_keeps_original_operations_and_appends_action_creation() {
    let descriptor = descriptor();
    let service = descriptor
        .service
        .iter()
        .find(|service| service.name.as_deref() == Some("RuleService"))
        .unwrap();
    let prefix = format!(".{PROTOBUF_PACKAGE}.");
    let expected = [
        ("CreateRule", "CreateRuleRequest", "RuleMutationResponse"),
        ("GetRule", "GetRuleRequest", "Rule"),
        ("ListRules", "ListRulesRequest", "ListRulesResponse"),
        ("DeleteRule", "DeleteRuleRequest", "RuleMutationResponse"),
        (
            "CreateRuleWithAction",
            "CreateRuleWithActionRequest",
            "RuleMutationResponse",
        ),
    ];
    assert_eq!(service.method.len(), expected.len());
    for (method, (name, input, output)) in service.method.iter().zip(expected) {
        assert_eq!(method.name.as_deref(), Some(name));
        assert_eq!(
            method.input_type.as_deref(),
            Some(format!("{prefix}{input}").as_str())
        );
        assert_eq!(
            method.output_type.as_deref(),
            Some(format!("{prefix}{output}").as_str())
        );
        assert!(!method.client_streaming.unwrap_or(false));
        assert!(!method.server_streaming.unwrap_or(false));
    }
    for (name, fields) in [
        (
            "CreateRuleRequest",
            vec![
                ("namespace", 1),
                ("subscription_path", 2),
                ("name", 3),
                ("filter", 4),
            ],
        ),
        (
            "GetRuleRequest",
            vec![
                ("namespace", 1),
                ("subscription_path", 2),
                ("name", 3),
                ("include_actions", 4),
            ],
        ),
        (
            "ListRulesRequest",
            vec![
                ("namespace", 1),
                ("subscription_path", 2),
                ("include_actions", 3),
            ],
        ),
        (
            "CreateRuleWithActionRequest",
            vec![
                ("namespace", 1),
                ("subscription_path", 2),
                ("name", 3),
                ("filter", 4),
                ("action", 5),
            ],
        ),
        (
            "DeleteRuleRequest",
            vec![("namespace", 1), ("subscription_path", 2), ("name", 3)],
        ),
        (
            "Rule",
            vec![
                ("namespace", 1),
                ("subscription_path", 2),
                ("name", 3),
                ("filter", 4),
                ("created_at_unix_millis", 5),
                ("action", 6),
            ],
        ),
        ("ListRulesResponse", vec![("rules", 1)]),
        ("RuleMutationResponse", vec![]),
    ] {
        let actual = message(&descriptor, name)
            .field
            .iter()
            .map(|field| (field.name.as_deref().unwrap(), field.number.unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(actual, fields, "{name}");
    }
    let list = &message(&descriptor, "ListRulesResponse").field[0];
    assert_eq!(list.label, Some(Label::Repeated as i32));
    assert_eq!(
        list.type_name.as_deref(),
        Some(format!("{prefix}Rule").as_str())
    );
}

#[test]
fn action_contract_preserves_presence_and_default_false_read_opt_in() {
    let file = descriptor();
    let prefix = format!(".{PROTOBUF_PACKAGE}.");
    let action = message(&file, "SqlRuleAction");
    assert_eq!(action.field.len(), 2);
    assert_eq!(action.field[0].name.as_deref(), Some("expression"));
    assert_eq!(action.field[0].number, Some(1));
    assert_eq!(action.field[0].r#type, Some(Type::String as i32));
    assert_eq!(action.field[1].name.as_deref(), Some("semantic_version"));
    assert_eq!(action.field[1].number, Some(2));
    assert_eq!(action.field[1].r#type, Some(Type::Uint32 as i32));
    assert_eq!(action.field[1].proto3_optional, Some(true));
    for (owner, number) in [("CreateRuleWithActionRequest", 5), ("Rule", 6)] {
        let field = message(&file, owner)
            .field
            .iter()
            .find(|field| field.number == Some(number))
            .unwrap();
        assert_eq!(field.name.as_deref(), Some("action"));
        assert_eq!(field.r#type, Some(Type::Message as i32));
        assert_eq!(
            field.type_name.as_deref(),
            Some(format!("{prefix}SqlRuleAction").as_str())
        );
    }
    for (owner, number) in [("GetRuleRequest", 4), ("ListRulesRequest", 3)] {
        let field = message(&file, owner)
            .field
            .iter()
            .find(|field| field.number == Some(number))
            .unwrap();
        assert_eq!(field.name.as_deref(), Some("include_actions"));
        assert_eq!(field.r#type, Some(Type::Bool as i32));
        assert!(!field.proto3_optional.unwrap_or(false));
    }
    for (version, bytes) in [
        (None, vec![]),
        (Some(0), vec![0x10, 0]),
        (Some(1), vec![0x10, 1]),
    ] {
        let action = v1::SqlRuleAction {
            expression: String::new(),
            semantic_version: version,
        };
        assert_eq!(action.encode_to_vec(), bytes);
        assert_eq!(v1::SqlRuleAction::decode(bytes.as_slice()).unwrap(), action);
    }
    assert!(v1::GetRuleRequest::default().encode_to_vec().is_empty());
    assert!(v1::ListRulesRequest::default().encode_to_vec().is_empty());
    assert_eq!(
        v1::GetRuleRequest {
            include_actions: true,
            ..Default::default()
        }
        .encode_to_vec(),
        [0x20, 1]
    );
    assert_eq!(
        v1::ListRulesRequest {
            include_actions: true,
            ..Default::default()
        }
        .encode_to_vec(),
        [0x18, 1]
    );
    assert!(v1::CreateRuleWithActionRequest::default().action.is_none());
    assert!(v1::Rule::default().action.is_none());
    let source = " /* exact source */ REMOVE user.[audit]; ";
    let action = v1::SqlRuleAction {
        expression: source.into(),
        semantic_version: Some(1),
    };
    let request = v1::CreateRuleWithActionRequest {
        action: Some(action.clone()),
        ..Default::default()
    };
    assert_eq!(
        v1::CreateRuleWithActionRequest::decode(request.encode_to_vec().as_slice()).unwrap(),
        request
    );
    let response = v1::Rule {
        action: Some(action),
        ..Default::default()
    };
    assert_eq!(
        v1::Rule::decode(response.encode_to_vec().as_slice()).unwrap(),
        response
    );
}

#[test]
fn original_create_request_encoding_cannot_carry_an_action() {
    let request = v1::CreateRuleRequest {
        namespace: "n".into(),
        subscription_path: "p".into(),
        name: "r".into(),
        filter: Some(v1::RuleFilter {
            filter: Some(v1::rule_filter::Filter::TrueFilter(v1::TrueRuleFilter {})),
        }),
    };
    assert_eq!(
        request.encode_to_vec(),
        [
            0x0a, 1, b'n', 0x12, 1, b'p', 0x1a, 1, b'r', 0x22, 2, 0x0a, 0
        ]
    );
    assert_eq!(
        v1::CreateRuleRequest::decode(request.encode_to_vec().as_slice()).unwrap(),
        request
    );
}

#[test]
fn filters_are_explicit_and_sql_version_preserves_presence() {
    let descriptor = descriptor();
    let filter = message(&descriptor, "RuleFilter");
    assert_eq!(filter.oneof_decl.len(), 1);
    assert_eq!(filter.oneof_decl[0].name.as_deref(), Some("filter"));
    for (field, (name, number, type_name)) in filter.field.iter().zip([
        ("true_filter", 1, "TrueRuleFilter"),
        ("false_filter", 2, "FalseRuleFilter"),
        ("correlation_filter", 3, "CorrelationRuleFilter"),
        ("sql_filter", 4, "SqlRuleFilter"),
    ]) {
        assert_eq!(field.name.as_deref(), Some(name));
        assert_eq!(field.number, Some(number));
        assert_eq!(field.r#type, Some(Type::Message as i32));
        assert_eq!(field.oneof_index, Some(0));
        assert_eq!(
            field.type_name.as_deref(),
            Some(format!(".{PROTOBUF_PACKAGE}.{type_name}").as_str())
        );
    }
    assert_eq!(filter.field.len(), 4);
    for marker in ["TrueRuleFilter", "FalseRuleFilter", "RuleNullValue"] {
        assert!(message(&descriptor, marker).field.is_empty());
    }
    let sql = message(&descriptor, "SqlRuleFilter");
    assert_eq!(sql.field.len(), 2);
    assert_eq!(sql.field[0].name.as_deref(), Some("expression"));
    assert_eq!(sql.field[0].number, Some(1));
    assert_eq!(sql.field[1].name.as_deref(), Some("semantic_version"));
    assert_eq!(sql.field[1].number, Some(2));
    assert_eq!(sql.field[1].r#type, Some(Type::Uint32 as i32));
    assert_eq!(sql.field[1].proto3_optional, Some(true));
    for (version, bytes) in [
        (None, vec![]),
        (Some(0), vec![0x10, 0]),
        (Some(1), vec![0x10, 1]),
    ] {
        let sql = v1::SqlRuleFilter {
            expression: String::new(),
            semantic_version: version,
        };
        assert_eq!(sql.encode_to_vec(), bytes);
        assert_eq!(v1::SqlRuleFilter::decode(bytes.as_slice()).unwrap(), sql);
    }
    assert_eq!(
        v1::RuleFilter {
            filter: Some(v1::rule_filter::Filter::TrueFilter(v1::TrueRuleFilter {}))
        }
        .encode_to_vec(),
        [0x0a, 0]
    );
    assert_eq!(
        v1::RuleFilter {
            filter: Some(v1::rule_filter::Filter::FalseFilter(v1::FalseRuleFilter {}))
        }
        .encode_to_vec(),
        [0x12, 0]
    );
    assert!(v1::RuleFilter { filter: None }.encode_to_vec().is_empty());
}

#[test]
fn correlation_optional_system_fields_and_repeated_properties_keep_exact_tags() {
    let descriptor = descriptor();
    let correlation = message(&descriptor, "CorrelationRuleFilter");
    let names = [
        "correlation_id",
        "message_id",
        "to",
        "reply_to",
        "subject",
        "session_id",
        "reply_to_session_id",
        "content_type",
    ];
    assert_eq!(correlation.field.len(), 9);
    for (index, name) in names.into_iter().enumerate() {
        let field = &correlation.field[index];
        assert_eq!(field.name.as_deref(), Some(name));
        assert_eq!(field.number, Some(index as i32 + 1));
        assert_eq!(field.r#type, Some(Type::String as i32));
        assert_eq!(field.proto3_optional, Some(true));
    }
    assert_eq!(correlation.field[8].name.as_deref(), Some("properties"));
    assert_eq!(correlation.field[8].number, Some(9));
    assert_eq!(correlation.field[8].label, Some(Label::Repeated as i32));
    let property = message(&descriptor, "CorrelationProperty");
    assert_eq!(property.field.len(), 2);
    assert_eq!(property.field[0].name.as_deref(), Some("name"));
    assert_eq!(property.field[0].number, Some(1));
    assert_eq!(property.field[1].name.as_deref(), Some("value"));
    assert_eq!(property.field[1].number, Some(2));
    let value = v1::CorrelationRuleFilter {
        correlation_id: Some(String::new()),
        properties: vec![
            v1::CorrelationProperty {
                name: String::from("same"),
                value: None,
            },
            v1::CorrelationProperty {
                name: String::from("same"),
                value: None,
            },
        ],
        ..Default::default()
    };
    let bytes = value.encode_to_vec();
    assert_eq!(&bytes[..2], &[0x0a, 0]);
    assert_eq!(
        v1::CorrelationRuleFilter::decode(bytes.as_slice()).unwrap(),
        value
    );
}

#[test]
fn scalar_constructor_tags_and_wire_types_are_stable() {
    let descriptor = descriptor();
    let scalar = message(&descriptor, "RuleScalarValue");
    let fields = [
        ("null_value", Type::Message),
        ("bool_value", Type::Bool),
        ("ubyte_value", Type::Uint32),
        ("ushort_value", Type::Uint32),
        ("uint_value", Type::Uint32),
        ("ulong_value", Type::Uint64),
        ("byte_value", Type::Sint32),
        ("short_value", Type::Sint32),
        ("int_value", Type::Sint32),
        ("long_value", Type::Sint64),
        ("float_bits", Type::Fixed32),
        ("double_bits", Type::Fixed64),
        ("decimal32_bytes", Type::Bytes),
        ("decimal64_bytes", Type::Bytes),
        ("decimal128_bytes", Type::Bytes),
        ("char_codepoint", Type::Uint32),
        ("timestamp_millis", Type::Sint64),
        ("uuid_bytes", Type::Bytes),
        ("binary_value", Type::Bytes),
        ("string_value", Type::String),
        ("symbol_value", Type::String),
    ];
    assert_eq!(scalar.field.len(), fields.len());
    assert_eq!(scalar.oneof_decl.len(), 1);
    assert_eq!(scalar.oneof_decl[0].name.as_deref(), Some("value"));
    for (index, (name, kind)) in fields.into_iter().enumerate() {
        let field = &scalar.field[index];
        assert_eq!(field.name.as_deref(), Some(name));
        assert_eq!(field.number, Some(index as i32 + 1));
        assert_eq!(field.r#type, Some(kind as i32));
        assert_eq!(field.oneof_index, Some(0));
    }
}

#[test]
fn scalar_goldens_preserve_null_false_zero_empty_signed_and_float_bits() {
    use v1::rule_scalar_value::Value;
    for (value, bytes) in [
        (Value::NullValue(v1::RuleNullValue {}), vec![0x0a, 0]),
        (Value::BoolValue(false), vec![0x10, 0]),
        (Value::UbyteValue(0), vec![0x18, 0]),
        (Value::UshortValue(0), vec![0x20, 0]),
        (Value::UintValue(0), vec![0x28, 0]),
        (Value::UlongValue(0), vec![0x30, 0]),
        (Value::ByteValue(-1), vec![0x38, 1]),
        (Value::ShortValue(-1), vec![0x40, 1]),
        (Value::IntValue(-1), vec![0x48, 1]),
        (Value::LongValue(-1), vec![0x50, 1]),
        (Value::FloatBits(0x8000_0000), vec![0x5d, 0, 0, 0, 0x80]),
        (
            Value::DoubleBits(0x8000_0000_0000_0000),
            vec![0x61, 0, 0, 0, 0, 0, 0, 0, 0x80],
        ),
        (Value::CharCodepoint(0), vec![0x80, 1, 0]),
        (Value::TimestampMillis(-1), vec![0x88, 1, 1]),
        (Value::BinaryValue(vec![]), vec![0x9a, 1, 0]),
        (Value::StringValue(String::new()), vec![0xa2, 1, 0]),
        (Value::SymbolValue(String::new()), vec![0xaa, 1, 0]),
    ] {
        let scalar = v1::RuleScalarValue { value: Some(value) };
        assert_eq!(scalar.encode_to_vec(), bytes);
        assert_eq!(
            v1::RuleScalarValue::decode(bytes.as_slice()).unwrap(),
            scalar
        );
    }
    assert!(
        v1::RuleScalarValue { value: None }
            .encode_to_vec()
            .is_empty()
    );
    assert!(v1::RuleMutationResponse {}.encode_to_vec().is_empty());
}

#[test]
fn decimal_and_uuid_octets_and_ieee_nan_payloads_roundtrip_exactly() {
    use v1::rule_scalar_value::Value;
    for value in [
        Value::Decimal32Bytes(vec![0, 1, 128, 255]),
        Value::Decimal64Bytes(vec![255; 8]),
        Value::Decimal128Bytes(vec![128; 16]),
        Value::UuidBytes(vec![85; 16]),
        Value::FloatBits(0x7fc0_1234),
        Value::DoubleBits(0x7ff8_0000_0000_4321),
    ] {
        let scalar = v1::RuleScalarValue { value: Some(value) };
        assert_eq!(
            v1::RuleScalarValue::decode(scalar.encode_to_vec().as_slice()).unwrap(),
            scalar
        );
    }
}

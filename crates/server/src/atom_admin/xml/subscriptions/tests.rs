use super::super::super::{
    ATOM_NS, AtomXmlError, Budget, MAX_ATTRIBUTES, MAX_BODY_BYTES, MAX_DEPTH, MAX_EVENTS,
    MAX_PROPERTIES, SERVICE_BUS_NS, XSI_NS, bounded_add,
};
use super::super::{MAX_MESSAGE_BYTES, decode_definition};
use super::*;

fn document(properties: &str) -> String {
    format!(
        "<entry xmlns=\"{ATOM_NS}\"><content type=\"application/xml\"><SubscriptionDescription xmlns=\"{SERVICE_BUS_NS}\">{properties}</SubscriptionDescription></content></entry>"
    )
}

fn rule(parameters: &str) -> String {
    format!(
        "<DefaultRuleDescription><Filter xmlns:i=\"{XSI_NS}\" i:type=\"TrueFilter\"><SqlExpression>1=1</SqlExpression>{parameters}</Filter><Name>$Default</Name></DefaultRuleDescription>"
    )
}

fn decode(properties: &str) -> Result<SubscriptionConfig, SubscriptionXmlError> {
    decode_definition(document(properties).as_bytes())
}

#[test]
fn both_pin_source_default_rule_shape_has_exact_core_defaults() {
    let properties = format!(
        "<LockDuration>PT1M</LockDuration><RequiresSession>false</RequiresSession><DeadLetteringOnMessageExpiration>false</DeadLetteringOnMessageExpiration><DeadLetteringOnFilterEvaluationExceptions>true</DeadLetteringOnFilterEvaluationExceptions>{}<MaxDeliveryCount>10</MaxDeliveryCount><EnableBatchedOperations>true</EnableBatchedOperations><Status>Active</Status>",
        rule("<Parameters />"),
    );
    assert_eq!(
        decode(&properties),
        Ok(SubscriptionConfig {
            lock_duration_millis: 60_000,
            max_delivery_count: 10,
            default_time_to_live_millis: None,
            max_message_bytes: 262_144,
            requires_session: false,
            dead_lettering_on_message_expiration: false,
            dead_lettering_on_filter_evaluation_exceptions: true,
        })
    );
}

#[test]
fn omitted_ttl_is_unlimited_and_explicit_false_is_preserved() {
    assert_eq!(decode(""), Ok(SubscriptionConfig::default()));
    assert_eq!(
        decode(
            "<LockDuration>PT5S</LockDuration><MaxDeliveryCount>1</MaxDeliveryCount><DefaultMessageTimeToLive>PT1.001S</DefaultMessageTimeToLive><RequiresSession>0</RequiresSession><DeadLetteringOnMessageExpiration>1</DeadLetteringOnMessageExpiration><DeadLetteringOnFilterEvaluationExceptions>0</DeadLetteringOnFilterEvaluationExceptions>"
        ),
        Ok(SubscriptionConfig {
            lock_duration_millis: 5_000,
            max_delivery_count: 1,
            default_time_to_live_millis: Some(1_001),
            dead_lettering_on_message_expiration: true,
            dead_lettering_on_filter_evaluation_exceptions: false,
            ..SubscriptionConfig::default()
        })
    );
}

#[test]
fn default_rule_allows_only_optional_empty_parameters() {
    for parameters in ["", "<Parameters/>", "<Parameters> \n\t </Parameters>"] {
        assert_eq!(decode(&rule(parameters)), Ok(SubscriptionConfig::default()));
    }
    let referenced = rule("")
        .replace("$Default", "$Def&#97;ult")
        .replace("1=1", "1&#61;1");
    assert_eq!(decode(&referenced), Ok(SubscriptionConfig::default()));
}

#[test]
fn unsupported_subscription_fields_are_never_silently_ignored() {
    for field in [
        "ForwardTo",
        "ForwardDeadLetteredMessagesTo",
        "UserMetadata",
        "AutoDeleteOnIdle",
        "MaxSizeInMegabytes",
        "MaxMessageSizeInKilobytes",
        "RequiresDuplicateDetection",
        "EnablePartitioning",
        "EnableExpress",
        "SupportOrdering",
        "AuthorizationRules",
        "SizeInBytes",
        "MessageCount",
        "CreatedAt",
        "Unknown",
    ] {
        assert_eq!(
            decode(&format!("<{field}/>")),
            Err(SubscriptionXmlError::UnsupportedDefinition),
            "{field}"
        );
    }
    for property in [
        "<RequiresSession>true</RequiresSession>",
        "<EnableBatchedOperations>false</EnableBatchedOperations>",
        "<Status>Disabled</Status>",
    ] {
        assert_eq!(
            decode(property),
            Err(SubscriptionXmlError::UnsupportedDefinition)
        );
    }
}

#[test]
fn custom_or_incomplete_rule_shapes_are_refused() {
    let default = rule("<Parameters/>");
    for unsupported in [
        default.replace("$Default", "custom"),
        default.replace("$Default", " $Default "),
        default.replace("TrueFilter", "SqlFilter"),
        default.replace("1=1", "1=0"),
        default.replace(
            "<Parameters/>",
            "<Parameters><KeyValueOfstringanyType/></Parameters>",
        ),
        default.replace(
            "</DefaultRuleDescription>",
            "<Action/></DefaultRuleDescription>",
        ),
        default.replace(
            "</DefaultRuleDescription>",
            "<CreatedAt/></DefaultRuleDescription>",
        ),
        default.replace("<Name>$Default</Name>", ""),
        default.replace("<SqlExpression>1=1</SqlExpression>", ""),
        String::from("<DefaultRuleDescription/>"),
    ] {
        assert_eq!(
            decode(&unsupported),
            Err(SubscriptionXmlError::UnsupportedDefinition),
            "{unsupported}"
        );
    }
}

#[test]
fn duplicate_properties_rule_children_and_expanded_attributes_are_malformed() {
    assert_eq!(
        decode(&rule("<Parameters>value</Parameters>")),
        Err(SubscriptionXmlError::Malformed)
    );
    for duplicate in [
        String::from(
            "<MaxDeliveryCount>1</MaxDeliveryCount><MaxDeliveryCount>2</MaxDeliveryCount>",
        ),
        format!("{}{}", rule(""), rule("")),
        rule("<Parameters/><Parameters/>"),
        rule("").replace("</Filter>", "<SqlExpression>1=1</SqlExpression></Filter>"),
        rule("").replace(
            "</DefaultRuleDescription>",
            "<Name>$Default</Name></DefaultRuleDescription>",
        ),
        rule("").replace(
            " i:type=\"TrueFilter\"",
            &format!(" xmlns:j=\"{XSI_NS}\" i:type=\"TrueFilter\" j:type=\"TrueFilter\""),
        ),
        rule("").replace(
            " i:type=\"TrueFilter\"",
            " i:type=\"TrueFilter\" i:type=\"TrueFilter\"",
        ),
    ] {
        assert_eq!(
            decode(&duplicate),
            Err(SubscriptionXmlError::Malformed),
            "{duplicate}"
        );
    }
}

#[test]
fn namespace_aliases_are_resolved_and_filter_type_is_required() {
    let aliases = format!(
        "<a:entry xmlns:a=\"{ATOM_NS}\" xmlns:s=\"{SERVICE_BUS_NS}\" xmlns:z=\"{XSI_NS}\"><a:content type=\"application/xml\"><s:SubscriptionDescription><s:DefaultRuleDescription><s:Name>$Default</s:Name><s:Filter xmlns=\"{SERVICE_BUS_NS}\" z:type=\"TrueFilter\"><s:SqlExpression>1=1</s:SqlExpression></s:Filter></s:DefaultRuleDescription></s:SubscriptionDescription></a:content></a:entry>"
    );
    assert_eq!(
        decode_definition(aliases.as_bytes()),
        Ok(SubscriptionConfig::default())
    );
    assert_eq!(
        decode(&rule("").replace(" i:type=\"TrueFilter\"", "")),
        Err(SubscriptionXmlError::UnsupportedDefinition)
    );
    assert_eq!(
        decode(&rule("").replace("i:type", "type")),
        Err(SubscriptionXmlError::Malformed)
    );
    assert_eq!(
        decode(&rule("").replace(XSI_NS, SERVICE_BUS_NS)),
        Err(SubscriptionXmlError::Malformed)
    );
    assert_eq!(
        decode(&rule("").replace("TrueFilter", "s:TrueFilter")),
        Err(SubscriptionXmlError::UnsupportedDefinition)
    );
    for invalid in [
        document("").replace(SERVICE_BUS_NS, "urn:foreign"),
        document("").replace("SubscriptionDescription", "QueueDescription"),
        document("").replace("xmlns=", "xmlns:xml="),
        document("").replace("type=\"application/xml\"", ""),
    ] {
        assert_eq!(
            decode_definition(invalid.as_bytes()),
            Err(SubscriptionXmlError::Malformed)
        );
    }
}

#[test]
fn lexical_hazards_and_non_documents_fail_closed() {
    let valid = document("");
    for invalid in [
        format!("<!DOCTYPE entry [<!ENTITY x 'value'>]>{valid}"),
        valid.replace(
            "<SubscriptionDescription",
            "<!-- comment --><SubscriptionDescription",
        ),
        valid.replace(
            "<SubscriptionDescription",
            "<?pi value?><SubscriptionDescription",
        ),
        valid.replace(
            "</SubscriptionDescription>",
            "<![CDATA[ ]]></SubscriptionDescription>",
        ),
        valid.replace(
            "</SubscriptionDescription>",
            "&unknown;</SubscriptionDescription>",
        ),
        valid.replace("type=\"application/xml\"", "type=\"application<xml\""),
        valid.replace(
            "type=\"application/xml\"",
            "type=\"application/xml\"extra=\"x\"",
        ),
        valid.replace(
            "</SubscriptionDescription>",
            "]]> </SubscriptionDescription>",
        ),
        format!("{valid}{valid}"),
        format!("\u{FEFF}\u{FEFF}{valid}"),
        format!("{valid}\0"),
        format!("{valid}\u{FFFE}"),
    ] {
        assert_eq!(
            decode_definition(invalid.as_bytes()),
            Err(SubscriptionXmlError::Malformed),
            "{invalid:?}"
        );
    }
    assert_eq!(
        decode_definition(&[0xFF]),
        Err(SubscriptionXmlError::Malformed)
    );
    assert_eq!(
        decode_definition(format!("\u{FEFF}{valid}").as_bytes()),
        Ok(SubscriptionConfig::default())
    );
    assert_eq!(
        decode_definition(
            format!("<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>{valid}")
                .as_bytes()
        ),
        Ok(SubscriptionConfig::default())
    );
    assert_eq!(
        decode_definition(format!("<?xml version=\"1.1\"?>{valid}").as_bytes()),
        Err(SubscriptionXmlError::Malformed)
    );
    assert_eq!(
        decode_definition(format!("<?xml standalone=\"yes\"?>{valid}").as_bytes()),
        Err(SubscriptionXmlError::Malformed)
    );
    assert_eq!(
        decode_definition(format!(" <?xml version=\"1.0\"?>{valid}").as_bytes()),
        Err(SubscriptionXmlError::Malformed)
    );
}

#[test]
fn scalar_boundaries_remain_core_valid_and_sdk_representable() {
    for invalid in [
        "<LockDuration>PT4.999S</LockDuration>",
        "<LockDuration>PT5M0.001S</LockDuration>",
        "<DefaultMessageTimeToLive>PT0.999S</DefaultMessageTimeToLive>",
        "<DefaultMessageTimeToLive>PT0S</DefaultMessageTimeToLive>",
        "<DefaultMessageTimeToLive>PT922337203685.478S</DefaultMessageTimeToLive>",
        "<LockDuration>PT5.0000001S</LockDuration>",
        "<LockDuration>P1Y</LockDuration>",
        "<MaxDeliveryCount>0</MaxDeliveryCount>",
        "<MaxDeliveryCount>2147483648</MaxDeliveryCount>",
        "<MaxDeliveryCount>18446744073709551616</MaxDeliveryCount>",
        "<MaxDeliveryCount>-1</MaxDeliveryCount>",
        "<RequiresSession>False</RequiresSession>",
    ] {
        assert_eq!(
            decode(invalid),
            Err(SubscriptionXmlError::InvalidDefinition),
            "{invalid}"
        );
    }
    let maximum = decode("<LockDuration>PT5M</LockDuration><MaxDeliveryCount>2147483647</MaxDeliveryCount><DefaultMessageTimeToLive>PT922337203685.477S</DefaultMessageTimeToLive>").expect("inclusive SDK boundaries");
    assert_eq!(maximum.lock_duration_millis, 300_000);
    assert_eq!(maximum.max_delivery_count, i32::MAX as u32);
    assert_eq!(
        maximum.default_time_to_live_millis,
        Some(922_337_203_685_477)
    );
}

#[test]
fn current_configuration_and_name_preflight_refuse_lossy_profiles() {
    for config in [
        SubscriptionConfig {
            requires_session: true,
            ..SubscriptionConfig::default()
        },
        SubscriptionConfig {
            max_message_bytes: MAX_MESSAGE_BYTES - 1,
            ..SubscriptionConfig::default()
        },
    ] {
        assert_eq!(
            validate_config(&config),
            Err(SubscriptionXmlError::UnsupportedDefinition)
        );
        assert_eq!(
            encode_entry(&SubscriptionName::new("audit").unwrap(), &config),
            Err(SubscriptionXmlError::UnsupportedDefinition)
        );
    }
    assert_eq!(
        validate_config(&SubscriptionConfig {
            max_delivery_count: 0,
            ..SubscriptionConfig::default()
        }),
        Err(SubscriptionXmlError::InvalidDefinition)
    );
    for value in ["audit", "Orders.v2-West_1", &"n".repeat(50)] {
        assert_eq!(
            validate_name(&SubscriptionName::new(value).unwrap()),
            Ok(())
        );
    }
    for value in [".", "..", "a/b", "a\\b", "a&b", &"n".repeat(51)] {
        assert!(SubscriptionName::new(value).is_err());
    }
}

#[test]
fn body_and_event_limits_are_enforced_before_final_rule_validation() {
    let valid = document("");
    let mut at_limit = valid.clone();
    at_limit.push_str(&" ".repeat(MAX_BODY_BYTES - valid.len()));
    assert_eq!(
        decode_definition(at_limit.as_bytes()),
        Ok(SubscriptionConfig::default())
    );
    at_limit.push(' ');
    assert_eq!(
        decode_definition(at_limit.as_bytes()),
        Err(SubscriptionXmlError::WorkLimitExceeded)
    );
    let events = rule("").replace("$Default", &"&#36;".repeat(MAX_EVENTS));
    assert!(document(&events).len() < MAX_BODY_BYTES);
    assert_eq!(
        decode(&events),
        Err(SubscriptionXmlError::WorkLimitExceeded)
    );
}

fn aliases(prefix: &str, count: usize) -> String {
    (0..count)
        .map(|index| format!(" xmlns:{prefix}{index}=\"{ATOM_NS}\""))
        .collect()
}

#[test]
fn attribute_and_active_namespace_bounds_have_exact_edges() {
    let with_attributes =
        |count| document("").replacen("<entry ", &format!("<entry{} ", aliases("r", count)), 1);
    assert_eq!(
        decode_definition(with_attributes(MAX_ATTRIBUTES - 1).as_bytes()),
        Ok(SubscriptionConfig::default())
    );
    assert_eq!(
        decode_definition(with_attributes(MAX_ATTRIBUTES).as_bytes()),
        Err(SubscriptionXmlError::WorkLimitExceeded)
    );
    let with_namespaces = |last| {
        format!(
            "<entry xmlns=\"{ATOM_NS}\"{}><content type=\"application/xml\"{}><SubscriptionDescription xmlns=\"{SERVICE_BUS_NS}\"{}/></content></entry>",
            aliases("r", 30),
            aliases("c", 30),
            aliases("d", last),
        )
    };
    assert_eq!(
        decode_definition(with_namespaces(2).as_bytes()),
        Ok(SubscriptionConfig::default())
    );
    assert_eq!(
        decode_definition(with_namespaces(3).as_bytes()),
        Err(SubscriptionXmlError::WorkLimitExceeded)
    );
}

#[test]
fn reused_budget_guards_are_finite_and_do_not_advance_on_refusal() {
    assert_eq!(Budget::depth(MAX_DEPTH), Ok(()));
    assert_eq!(
        Budget::depth(MAX_DEPTH + 1),
        Err(AtomXmlError::WorkLimitExceeded)
    );
    let mut budget = Budget::default();
    for _ in 0..MAX_EVENTS {
        assert_eq!(budget.event(), Ok(()));
    }
    assert_eq!(budget.event(), Err(AtomXmlError::WorkLimitExceeded));
    assert_eq!(budget.events, MAX_EVENTS);
    let mut properties = Budget::default();
    for _ in 0..MAX_PROPERTIES {
        assert_eq!(properties.property(), Ok(()));
    }
    assert_eq!(properties.property(), Err(AtomXmlError::WorkLimitExceeded));
    assert_eq!(properties.properties, MAX_PROPERTIES);
    let mut decoded = Budget::default();
    assert_eq!(decoded.decoded(MAX_BODY_BYTES), Ok(()));
    assert_eq!(decoded.decoded(1), Err(AtomXmlError::WorkLimitExceeded));
    assert_eq!(decoded.decoded, MAX_BODY_BYTES);
    let mut scratch = MAX_BODY_BYTES;
    assert_eq!(
        bounded_add(&mut scratch, 1, MAX_BODY_BYTES),
        Err(AtomXmlError::WorkLimitExceeded)
    );
    assert_eq!(scratch, MAX_BODY_BYTES);
    let mut overflow = usize::MAX;
    assert_eq!(
        bounded_add(&mut overflow, 1, usize::MAX),
        Err(AtomXmlError::WorkLimitExceeded)
    );
    assert_eq!(overflow, usize::MAX);
}

#[test]
fn responses_use_leaf_title_and_only_supported_configuration_scalars() {
    let name = SubscriptionName::new("Audit.v2-West_1").unwrap();
    let mut config = SubscriptionConfig {
        lock_duration_millis: 5_000,
        max_delivery_count: 3,
        dead_lettering_on_message_expiration: true,
        dead_lettering_on_filter_evaluation_exceptions: false,
        ..SubscriptionConfig::default()
    };
    for ttl in [None, Some(1_001), Some(922_337_203_685_477)] {
        config.default_time_to_live_millis = ttl;
        let text = String::from_utf8(encode_entry(&name, &config).unwrap()).unwrap();
        let title = format!("<title>{name}</title>");
        assert!(text.contains(&title));
        assert!(!text.contains("/subscriptions/"));
        assert_eq!(text.contains("DefaultMessageTimeToLive"), ttl.is_some());
        for absent in [
            "DefaultRuleDescription",
            "MaxSizeInMegabytes",
            "MaxMessageSizeInKilobytes",
            "SizeInBytes",
            "MessageCount",
            "CreatedAt",
        ] {
            assert!(!text.contains(absent));
        }
        assert_eq!(
            decode_definition(text.replacen(&title, "", 1).as_bytes()),
            Ok(config)
        );
    }
}

#[test]
fn scalar_escaping_and_static_errors_do_not_echo_input() {
    let mut writer = Writer::new(Reply::default());
    scalar(&mut writer, "title", "A&<>\"'").unwrap();
    assert_eq!(
        String::from_utf8(writer.into_inner().0).unwrap(),
        "<title>A&amp;&lt;&gt;&quot;&apos;</title>"
    );
    for error in [
        SubscriptionXmlError::Malformed,
        SubscriptionXmlError::WorkLimitExceeded,
        SubscriptionXmlError::InvalidDefinition,
        SubscriptionXmlError::UnsupportedDefinition,
        SubscriptionXmlError::ReplyLimitExceeded,
    ] {
        let text = String::from_utf8(encode_error(error).unwrap()).unwrap();
        assert!(text.contains(error.code()));
        assert!(text.contains(error.detail()));
        assert!(!text.contains("queue"));
        assert!(!text.contains("credential"));
    }
}

#[test]
fn output_sink_and_pre_escape_boundaries_preserve_prior_bytes_on_refusal() {
    let mut reply = Reply::default();
    reply.write_all(&vec![b'x'; MAX_REPLY_BYTES]).unwrap();
    assert_eq!(reply.0.len(), MAX_REPLY_BYTES);
    assert!(reply.write(b"y").is_err());
    assert_eq!(reply.0, vec![b'x'; MAX_REPLY_BYTES]);
    let mut budget = MAX_REPLY_BYTES - 6;
    assert_eq!(preflight(&mut budget, 1, 0), Ok(()));
    assert_eq!(budget, MAX_REPLY_BYTES);
    assert_eq!(
        preflight(&mut budget, 1, 0),
        Err(SubscriptionXmlError::ReplyLimitExceeded)
    );
    assert_eq!(budget, MAX_REPLY_BYTES);
    assert_eq!(
        preflight(&mut 0, usize::MAX, 0),
        Err(SubscriptionXmlError::ReplyLimitExceeded)
    );
}

use super::super::decode_update_definition;

fn decode_update(properties: &str) -> Result<SubscriptionConfig, SubscriptionXmlError> {
    decode_update_definition(document(properties).as_bytes())
}

#[test]
fn full_update_definition_preserves_all_exposed_scalars() {
    assert_eq!(
        decode_update(
            "<LockDuration>PT15S</LockDuration><RequiresSession>false</RequiresSession><DefaultMessageTimeToLive>PT45S</DefaultMessageTimeToLive><DeadLetteringOnMessageExpiration>true</DeadLetteringOnMessageExpiration><DeadLetteringOnFilterEvaluationExceptions>false</DeadLetteringOnFilterEvaluationExceptions><MaxDeliveryCount>3</MaxDeliveryCount><EnableBatchedOperations>true</EnableBatchedOperations><Status>Active</Status>"
        ),
        Ok(SubscriptionConfig {
            lock_duration_millis: 15_000,
            max_delivery_count: 3,
            default_time_to_live_millis: Some(45_000),
            max_message_bytes: MAX_MESSAGE_BYTES,
            requires_session: false,
            dead_lettering_on_message_expiration: true,
            dead_lettering_on_filter_evaluation_exceptions: false,
        })
    );
}

#[test]
fn full_update_omissions_reset_defaults_and_unlimited_ttl() {
    let changed = "<LockDuration>PT5S</LockDuration><MaxDeliveryCount>1</MaxDeliveryCount><DefaultMessageTimeToLive>PT1S</DefaultMessageTimeToLive><DeadLetteringOnMessageExpiration>true</DeadLetteringOnMessageExpiration><DeadLetteringOnFilterEvaluationExceptions>false</DeadLetteringOnFilterEvaluationExceptions>";
    assert_ne!(
        decode_update(changed).unwrap(),
        SubscriptionConfig::default()
    );
    assert_eq!(decode_update(""), Ok(SubscriptionConfig::default()));
    assert_eq!(
        decode_update(
            "<LockDuration>PT10S</LockDuration><RequiresSession>0</RequiresSession><DeadLetteringOnMessageExpiration>0</DeadLetteringOnMessageExpiration><DeadLetteringOnFilterEvaluationExceptions>1</DeadLetteringOnFilterEvaluationExceptions>"
        ),
        Ok(SubscriptionConfig {
            lock_duration_millis: 10_000,
            ..SubscriptionConfig::default()
        })
    );
    assert_eq!(
        decode_update("<DefaultMessageTimeToLive>PT922337203685.477S</DefaultMessageTimeToLive>")
            .unwrap()
            .default_time_to_live_millis,
        Some(922_337_203_685_477)
    );
    assert_eq!(decode_update("").unwrap().default_time_to_live_millis, None);
}

#[test]
fn full_update_refuses_every_default_rule_description() {
    for properties in [
        String::from("<DefaultRuleDescription/>"),
        rule(""),
        rule("<Parameters/>"),
        rule("").replace("$Default", "custom"),
        rule("").replace("TrueFilter", "FalseFilter"),
        format!("{}{}", rule(""), rule("")),
        format!("<s:DefaultRuleDescription xmlns:s=\"{SERVICE_BUS_NS}\"/>"),
    ] {
        assert_eq!(
            decode_update(&properties),
            Err(SubscriptionXmlError::UnsupportedDefinition),
            "{properties}"
        );
    }
    for parameters in ["", "<Parameters/>", "<Parameters> \n\t </Parameters>"] {
        assert_eq!(decode(&rule(parameters)), Ok(SubscriptionConfig::default()));
    }
}

#[test]
fn full_update_reuses_closed_grammar_and_existing_bounds() {
    for (properties, error) in [
        (
            "<MaxDeliveryCount>1</MaxDeliveryCount><MaxDeliveryCount>2</MaxDeliveryCount>",
            SubscriptionXmlError::Malformed,
        ),
        (
            "<RequiresSession>true</RequiresSession>",
            SubscriptionXmlError::UnsupportedDefinition,
        ),
        ("<ForwardTo/>", SubscriptionXmlError::UnsupportedDefinition),
        (
            "<LockDuration>PT4.999S</LockDuration>",
            SubscriptionXmlError::InvalidDefinition,
        ),
        (
            "<DefaultMessageTimeToLive>PT0S</DefaultMessageTimeToLive>",
            SubscriptionXmlError::InvalidDefinition,
        ),
        (
            "<MaxDeliveryCount>2147483648</MaxDeliveryCount>",
            SubscriptionXmlError::InvalidDefinition,
        ),
    ] {
        assert_eq!(decode_update(properties), Err(error), "{properties}");
    }
    let valid = document("");
    for invalid in [
        format!("<?xml standalone=\"yes\"?>{valid}"),
        format!("<?xml version=\"1.1\"?>{valid}"),
        format!("<!DOCTYPE entry>{valid}"),
        format!("{valid}{valid}"),
        valid.replace(SERVICE_BUS_NS, "urn:foreign"),
    ] {
        assert_eq!(
            decode_update_definition(invalid.as_bytes()),
            Err(SubscriptionXmlError::Malformed)
        );
    }
    let mut at_limit = valid.clone();
    at_limit.push_str(&" ".repeat(MAX_BODY_BYTES - valid.len()));
    assert_eq!(
        decode_update_definition(at_limit.as_bytes()),
        Ok(SubscriptionConfig::default())
    );
    at_limit.push(' ');
    assert_eq!(
        decode_update_definition(at_limit.as_bytes()),
        Err(SubscriptionXmlError::WorkLimitExceeded)
    );
    let events = document(&format!(
        "<LockDuration>{}</LockDuration>",
        "&#32;".repeat(MAX_EVENTS)
    ));
    assert!(events.len() < MAX_BODY_BYTES);
    assert_eq!(
        decode_update_definition(events.as_bytes()),
        Err(SubscriptionXmlError::WorkLimitExceeded)
    );
}

#[test]
fn default_rule_bare_type_requires_the_service_bus_default_namespace() {
    let prefixed = format!(
        "<entry xmlns=\"{ATOM_NS}\" xmlns:s=\"{SERVICE_BUS_NS}\"><content type=\"application/xml\"><s:SubscriptionDescription><s:DefaultRuleDescription><s:Filter xmlns:i=\"{XSI_NS}\" i:type=\"TrueFilter\"><s:SqlExpression>1=1</s:SqlExpression><s:Parameters/></s:Filter><s:Name>$Default</s:Name></s:DefaultRuleDescription></s:SubscriptionDescription></content></entry>"
    );
    assert_eq!(
        decode_definition(prefixed.as_bytes()),
        Err(SubscriptionXmlError::Malformed)
    );
    let bound = prefixed.replace(
        "<s:Filter ",
        &format!("<s:Filter xmlns=\"{SERVICE_BUS_NS}\" "),
    );
    assert_eq!(
        decode_definition(bound.as_bytes()),
        Ok(SubscriptionConfig::default())
    );
    let no_default = bound.replace(
        &format!("<s:Filter xmlns=\"{SERVICE_BUS_NS}\" "),
        "<s:Filter xmlns=\"\" ",
    );
    assert_eq!(
        decode_definition(no_default.as_bytes()),
        Err(SubscriptionXmlError::Malformed)
    );
}

use std::collections::BTreeMap;

use domain::{CorrelationFilter, MessageValue, RuleFilter, RuleName};

use super::super::super::{
    AtomXmlError, Budget, MAX_ATTRIBUTES, MAX_BODY_BYTES, MAX_DEPTH, MAX_EVENTS,
    MAX_NAMESPACE_BINDINGS, MAX_PROPERTIES, bounded_add,
};
use super::super::{decode_definition, validate_definition, validate_name};
use super::*;

fn document(properties: &str) -> String {
    format!(
        "<entry xmlns=\"{ATOM_NS}\"><content type=\"application/xml\"><RuleDescription xmlns=\"{SERVICE_BUS_NS}\">{properties}</RuleDescription></content></entry>"
    )
}

fn filter(kind: &str, expression: &str, parameters: &str) -> String {
    format!(
        "<Filter xmlns:i=\"{XSI_NS}\" i:type=\"{kind}\"><SqlExpression>{expression}</SqlExpression>{parameters}</Filter>"
    )
}

fn definition(name: &str, filter: RuleFilter) -> AtomRuleDefinition {
    AtomRuleDefinition {
        name: RuleName::new(name).expect("valid test name"),
        filter,
        action: None,
    }
}

fn decode(properties: &str) -> Result<AtomRuleDefinition, RuleXmlError> {
    decode_definition(document(properties).as_bytes())
}

fn properties(name: &str, kind: &str, expression: &str) -> String {
    format!(
        "{}<Name>{name}</Name>",
        filter(kind, expression, "<Parameters/>")
    )
}

fn aliases(prefix: &str, count: usize) -> String {
    (0..count)
        .map(|index| format!(" xmlns:{prefix}{index}=\"{ATOM_NS}\""))
        .collect()
}

#[test]
fn both_pin_sdk_filters_decode_without_defaults() {
    for (kind, expression, expected) in [
        ("TrueFilter", "1=1", RuleFilter::True),
        ("FalseFilter", "1=0", RuleFilter::False),
    ] {
        for parameters in ["", "<Parameters/>", "<Parameters></Parameters>"] {
            let value = format!(
                "{}<Name>$Default</Name>",
                filter(kind, expression, parameters)
            );
            assert_eq!(decode(&value), Ok(definition("$Default", expected.clone())));
            let reordered = format!(
                "<Name>$Default</Name>{}",
                filter(kind, expression, parameters)
            );
            assert_eq!(
                decode(&reordered),
                Ok(definition("$Default", expected.clone()))
            );
        }
    }
}

#[test]
fn required_name_filter_and_expression_are_not_fabricated() {
    for value in [
        String::new(),
        "<Name>$Default</Name>".to_owned(),
        filter("TrueFilter", "1=1", ""),
        format!("<Filter xmlns:i=\"{XSI_NS}\" i:type=\"TrueFilter\"/><Name>x</Name>"),
        "<Filter><SqlExpression>1=1</SqlExpression></Filter><Name>x</Name>".to_owned(),
        properties("", "TrueFilter", "1=1"),
    ] {
        assert_eq!(
            decode(&value),
            Err(RuleXmlError::InvalidDefinition),
            "{value}"
        );
    }
}

#[test]
fn filter_type_and_expression_must_match_exactly() {
    for (kind, expression) in [
        ("TrueFilter", "1=0"),
        ("FalseFilter", "1=1"),
        ("TrueFilter", " 1=1"),
        ("FalseFilter", "1=0 "),
        ("TrueFilter", "true"),
        ("FalseFilter", ""),
    ] {
        assert_eq!(
            decode(&properties("x", kind, expression)),
            Err(RuleXmlError::InvalidDefinition)
        );
    }
    for kind in ["CorrelationFilter", "truefilter", "i:TrueFilter", ""] {
        assert_eq!(
            decode(&properties("x", kind, "1=1")),
            Err(RuleXmlError::UnsupportedDefinition)
        );
    }
}

#[test]
fn sql_filter_sources_and_both_pin_shapes_are_exact() {
    for expression in [
        "1=1",
        "1=0",
        "  user.colour = 'Red & <x>' OR sys.Label IS NULL  ",
        "p('@literal') = 1",
        "[\u{03B1}] = '\u{00E9}'",
        "user.colour = 'Red'\r\n",
    ] {
        let escaped = quick_xml::escape::escape(expression);
        let expected = definition(
            " SQL ",
            RuleFilter::Sql(domain::SqlFilter::new(expression).unwrap()),
        );
        for parameters in ["", "<Parameters/>", "<Parameters></Parameters>"] {
            let value = format!(
                "{}<Name> SQL </Name>",
                filter("SqlFilter", &escaped, parameters)
            );
            assert_eq!(decode(&value), Ok(expected.clone()));
            let reordered = format!(
                "<Name> SQL </Name>{}",
                filter("SqlFilter", &escaped, parameters)
            );
            assert_eq!(decode(&reordered), Ok(expected.clone()));
        }
        let RuleFilter::Sql(sql) = &expected.filter else {
            panic!("SQL constants must not be relabelled as Boolean filters");
        };
        assert_eq!(sql.expression(), expression);
        assert_eq!(sql.semantic_version(), domain::SQL_FILTER_SEMANTIC_VERSION);
    }
    assert_eq!(
        decode(&properties(
            "x",
            "SqlFilter",
            "&#32;user.colour&#32;=&#32;'Red'&#13;&#10;"
        )),
        Ok(definition(
            "x",
            RuleFilter::Sql(domain::SqlFilter::new(" user.colour = 'Red'\r\n").unwrap())
        ))
    );
}

#[test]
fn sql_source_width_and_compile_limits_stay_definition_errors() {
    use domain::{SqlCompileError, SqlCompileLimit, SqlProgram};

    let exact = format!(
        "'{}'",
        "a".repeat(domain::MAX_SQL_EXPRESSION_UTF16_UNITS - 2)
    );
    assert!(decode(&properties("x", "SqlFilter", &exact)).is_ok());
    let tokens = format!("{}TRUE", " ".repeat(domain::MAX_SQL_EXPRESSION_TOKENS - 1));
    assert!(decode(&properties("x", "SqlFilter", &tokens)).is_ok());
    let in_items = std::iter::repeat_n("1", domain::MAX_SQL_IN_ITEMS)
        .collect::<Vec<_>>()
        .join(",");
    assert!(decode(&properties("x", "SqlFilter", &format!("x IN ({in_items})"))).is_ok());
    for (expression, kind) in [
        (
            "a".repeat(domain::MAX_SQL_EXPRESSION_BYTES + 1),
            SqlCompileLimit::SourceBytes,
        ),
        (
            "a".repeat(domain::MAX_SQL_EXPRESSION_UTF16_UNITS + 1),
            SqlCompileLimit::SourceUtf16Units,
        ),
        (format!(" {tokens}"), SqlCompileLimit::PhysicalTokens),
        (
            format!(
                "{}TRUE{}",
                "(".repeat(domain::MAX_SQL_PARSER_DEPTH + 1),
                ")".repeat(domain::MAX_SQL_PARSER_DEPTH + 1)
            ),
            SqlCompileLimit::ParserDepth,
        ),
        (
            std::iter::repeat_n("x", domain::MAX_SQL_EXPRESSION_DEPTH + 1)
                .collect::<Vec<_>>()
                .join("+"),
            SqlCompileLimit::ExpressionDepth,
        ),
        (format!("x IN ({in_items},1)"), SqlCompileLimit::InItems),
    ] {
        assert!(matches!(
            SqlProgram::compile(&expression),
            Err(SqlCompileError::Limit { kind: actual, .. }) if actual == kind
        ));
        assert_eq!(
            decode(&properties("x", "SqlFilter", &expression)),
            Err(RuleXmlError::InvalidDefinition)
        );
    }
    for expression in ["", "broken =", "lower(name)", "?", "1=1;"] {
        assert_eq!(
            decode(&properties("x", "SqlFilter", expression)),
            Err(RuleXmlError::InvalidDefinition)
        );
    }
}

#[test]
fn sql_unsupported_parameters_actions_and_types_are_closed() {
    let valid = properties("x", "SqlFilter", "1=1");
    for (parameters, error) in [
        ("<Parameters>x</Parameters>", RuleXmlError::Malformed),
        ("<Parameters>&#32;</Parameters>", RuleXmlError::Malformed),
        (
            "<Parameters><KeyValueOfstringanyType><Key>p</Key><Value>1</Value></KeyValueOfstringanyType></Parameters>",
            RuleXmlError::UnsupportedDefinition,
        ),
        ("<Parameters/><Parameters/>", RuleXmlError::Malformed),
    ] {
        assert_eq!(
            decode(&format!(
                "{}<Name>x</Name>",
                filter("SqlFilter", "1=1", parameters)
            )),
            Err(error)
        );
    }
    for extra in [
        "<Action xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\" i:type=\"EmptyRuleAction\"/>",
        "<Unknown/>",
        "<CreatedAt/>",
    ] {
        assert_eq!(
            decode(&format!("{valid}{extra}")),
            Err(RuleXmlError::UnsupportedDefinition)
        );
    }
    for kind in ["sqlfilter", "i:SqlFilter", "CorrelationFilter"] {
        assert_eq!(
            decode(&properties("x", kind, "1=1")),
            Err(RuleXmlError::UnsupportedDefinition)
        );
    }
    assert_eq!(
        decode(&properties("x", "SqlFilter", "1=1").replace(
            "<SqlExpression>1=1</SqlExpression>",
            "<SqlExpression><Name>x</Name></SqlExpression>"
        )),
        Err(RuleXmlError::Malformed)
    );
}

#[test]
fn sql_filter_type_qname_reuses_current_namespace_proof() {
    let prefixed = format!(
        "<entry xmlns=\"{ATOM_NS}\" xmlns:s=\"{SERVICE_BUS_NS}\"><content type=\"application/xml\"><s:RuleDescription><s:Filter xmlns:i=\"{XSI_NS}\" i:type=\"SqlFilter\"><s:SqlExpression>1=1</s:SqlExpression><s:Parameters/></s:Filter><s:Name>x</s:Name></s:RuleDescription></content></entry>"
    );
    assert_eq!(
        decode_definition(prefixed.as_bytes()),
        Err(RuleXmlError::Malformed)
    );
    let bound = prefixed.replace(
        "<s:Filter ",
        &format!("<s:Filter xmlns=\"{SERVICE_BUS_NS}\" "),
    );
    assert_eq!(
        decode_definition(bound.as_bytes()),
        Ok(definition(
            "x",
            RuleFilter::Sql(domain::SqlFilter::new("1=1").unwrap())
        ))
    );
    let no_default = bound.replace(
        &format!("<s:Filter xmlns=\"{SERVICE_BUS_NS}\" "),
        "<s:Filter xmlns=\"\" ",
    );
    assert_eq!(
        decode_definition(no_default.as_bytes()),
        Err(RuleXmlError::Malformed)
    );
}

#[test]
fn sql_projection_validates_decoded_source_and_xml_chars() {
    let bytes = domain::codec::encode(&(1_u32, "broken =")).unwrap();
    let malformed = domain::codec::decode::<domain::SqlFilter>(&bytes).unwrap();
    let value = definition("x", RuleFilter::Sql(malformed));
    assert_eq!(
        validate_definition(&value),
        Err(RuleXmlError::InvalidDefinition)
    );
    assert_eq!(encode_entry(&value), Err(RuleXmlError::InvalidDefinition));
    assert!(
        domain::codec::decode::<domain::SqlFilter>(
            &domain::codec::encode(&(2_u32, "1=1")).unwrap()
        )
        .is_err()
    );
    let source = "'\u{FFFE}' = 'x'";
    let value = definition(
        "x",
        RuleFilter::Sql(domain::SqlFilter::new(source).unwrap()),
    );
    assert_eq!(
        validate_definition(&value),
        Err(RuleXmlError::UnsupportedDefinition)
    );
    assert_eq!(
        encode_entry(&value),
        Err(RuleXmlError::UnsupportedDefinition)
    );
    assert_eq!(
        decode(&properties("x", "SqlFilter", "'&#xFFFE;' = 'x'")),
        Err(RuleXmlError::Malformed)
    );
    let mut values = vec![definition("x", RuleFilter::True); MAX_FEED_ENTRIES];
    values[MAX_FEED_ENTRIES - 1] = value;
    assert_eq!(
        encode_feed(&values),
        Err(RuleXmlError::UnsupportedDefinition)
    );
}

#[test]
fn sql_responses_preserve_cr_source_and_expression_preflight() {
    let source = " \r\nuser.colour = 'Red & <x>'\r\n ";
    let value = definition(
        " SQL & ",
        RuleFilter::Sql(domain::SqlFilter::new(source).unwrap()),
    );
    let bytes = encode_entry(&value).unwrap();
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(text.contains("<title> SQL &amp; </title>"));
    assert!(text.contains("<Name> SQL &amp; </Name>"));
    assert!(text.contains("i:type=\"SqlFilter\""));
    assert!(text.contains("&#13;\n"));
    assert!(text.contains("<Parameters></Parameters>"));
    for absent in ["<Action", "CreatedAt", "MessageCount", "SizeInBytes"] {
        assert!(!text.contains(absent), "{absent}");
    }
    let mut reader = quick_xml::Reader::from_str(text);
    let mut in_expression = false;
    let mut decoded = String::new();
    loop {
        match reader.read_event().unwrap() {
            Event::Start(start) if start.name().as_ref() == "SqlExpression" => {
                in_expression = true;
            }
            Event::End(end) if end.name().as_ref() == "SqlExpression" => {
                in_expression = false;
            }
            Event::Text(text) if in_expression => decoded.push_str(&text.xml10_content()),
            Event::GeneralRef(reference) if in_expression => {
                if let Some(value) = reference.resolve_char_ref().unwrap() {
                    decoded.push(value);
                } else {
                    decoded.push_str(
                        quick_xml::escape::resolve_xml_entity(reference.as_ref()).unwrap(),
                    );
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }
    assert_eq!(decoded, source);
    let text_bytes = value.name.as_str().len() * 2 + source.len();
    let reserved = text_bytes * 6 + 1_024;
    let mut total = MAX_REPLY_BYTES - reserved;
    assert_eq!(definition_budget(&mut total, &value), Ok(()));
    assert_eq!(total, MAX_REPLY_BYTES);
    assert_eq!(
        definition_budget(&mut total, &value),
        Err(RuleXmlError::ReplyLimitExceeded)
    );
    let mut one_short = MAX_REPLY_BYTES - reserved + 1;
    let before = one_short;
    assert_eq!(
        definition_budget(&mut one_short, &value),
        Err(RuleXmlError::ReplyLimitExceeded)
    );
    assert_eq!(one_short, before);
}

#[test]
fn optional_empty_parameters_are_closed() {
    for parameters in ["", "<Parameters/>", "<Parameters> \t\r\n </Parameters>"] {
        assert_eq!(
            decode(&format!(
                "{}<Name>x</Name>",
                filter("TrueFilter", "1=1", parameters)
            )),
            Ok(definition("x", RuleFilter::True))
        );
    }
    for (parameters, error) in [
        ("<Parameters>x</Parameters>", RuleXmlError::Malformed),
        ("<Parameters>&#32;</Parameters>", RuleXmlError::Malformed),
        (
            "<Parameters><KeyValueOfstringanyType/></Parameters>",
            RuleXmlError::UnsupportedDefinition,
        ),
        ("<Parameters/><Parameters/>", RuleXmlError::Malformed),
    ] {
        assert_eq!(
            decode(&format!(
                "{}<Name>x</Name>",
                filter("TrueFilter", "1=1", parameters)
            )),
            Err(error)
        );
    }
}

#[test]
fn actions_and_unsupported_properties_are_refused() {
    let valid = properties("x", "TrueFilter", "1=1");
    for extra in [
        "<Action xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\" i:type=\"EmptyRuleAction\"/>",
        "<Action xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\" i:type=\"UnknownRuleAction\"><SqlExpression>SET x=1</SqlExpression></Action>",
        "<CreatedAt>2026-01-01T00:00:00Z</CreatedAt>",
        "<Unknown/>",
        "<DefaultRuleDescription/>",
    ] {
        assert_eq!(
            decode(&format!("{valid}{extra}")),
            Err(RuleXmlError::UnsupportedDefinition)
        );
    }
    assert_eq!(
        decode(&valid.replace(
            "<Parameters/>",
            "<RequiresPreprocessing>false</RequiresPreprocessing>"
        )),
        Err(RuleXmlError::UnsupportedDefinition)
    );
}

#[test]
fn namespaces_and_expanded_attributes_are_checked() {
    let valid = document(&properties("x", "TrueFilter", "1=1"));
    let aliased = valid
        .replace("<Filter xmlns:i=", "<Filter xmlns:j=")
        .replace(" i:type=", " j:type=");
    assert_eq!(
        decode_definition(aliased.as_bytes()),
        Ok(definition("x", RuleFilter::True))
    );
    for invalid in [
        valid.replace(XSI_NS, "urn:foreign"),
        valid.replace("i:type", "type"),
        valid.replace("<Name>", "<Name xmlns=\"\">"),
        valid.replace("<Name>", "<Name xmlns=\"http://www.w3.org/2005/Atom\">"),
        valid.replace("<Name>", "<Name xml:lang=\"en\">"),
        valid.replace("<entry ", "<entry ignored=\"x\" "),
        valid.replace("application/xml", "text/xml"),
        valid.replace("xmlns:i=", "xmlns:xml="),
    ] {
        assert_eq!(
            decode_definition(invalid.as_bytes()),
            Err(RuleXmlError::Malformed)
        );
    }
}

#[test]
fn duplicate_nodes_and_attributes_are_rejected() {
    let valid = properties("x", "TrueFilter", "1=1");
    for invalid in [
        format!("{valid}<Name>x</Name>"),
        format!("{valid}{}", filter("FalseFilter", "1=0", "")),
        valid.replace(
            "<Parameters/>",
            "<SqlExpression>1=1</SqlExpression><Parameters/>",
        ),
        valid.replace(
            "i:type=\"TrueFilter\"",
            &format!("xmlns:j=\"{XSI_NS}\" i:type=\"TrueFilter\" j:type=\"TrueFilter\""),
        ),
        valid.replace(
            "i:type=\"TrueFilter\"",
            "i:type=\"TrueFilter\" i:type=\"TrueFilter\"",
        ),
    ] {
        assert_eq!(decode(&invalid), Err(RuleXmlError::Malformed));
    }
}

#[test]
fn closed_structure_rejects_scalar_children_and_extra_roots() {
    let valid = document(&properties("x", "TrueFilter", "1=1"));
    for invalid in [
        valid.replace("<Name>x</Name>", "<Name><Name>x</Name></Name>"),
        valid.replace(
            "<SqlExpression>1=1</SqlExpression>",
            "<SqlExpression><Name>x</Name></SqlExpression>",
        ),
        valid.replace(
            "</content>",
            "</content><content type=\"application/xml\"/>",
        ),
        valid.replace("<content ", "<title>x</title><content "),
        format!("{valid}{valid}"),
        "<entry xmlns=\"http://www.w3.org/2005/Atom\"/>".to_owned(),
        valid.replace("</Name>", "</Filter>"),
        format!("x{valid}"),
    ] {
        assert_eq!(
            decode_definition(invalid.as_bytes()),
            Err(RuleXmlError::Malformed)
        );
    }
}

#[test]
fn xml_declarations_references_and_encoding_are_bounded() {
    let valid = document(&properties("A&amp;B&#37;20", "TrueFilter", "1&#61;1"));
    for declaration in [
        "",
        "<?xml version=\"1.0\"?>",
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>",
        "<?xml version=\"1.0\" standalone=\"no\"?>",
    ] {
        assert_eq!(
            decode_definition(format!("{declaration}{valid}").as_bytes()),
            Ok(definition("A&B%20", RuleFilter::True))
        );
    }
    assert_eq!(
        decode_definition(format!("\u{FEFF}{valid}").as_bytes()),
        Ok(definition("A&B%20", RuleFilter::True))
    );
    for invalid in [
        format!("<?xml standalone=\"yes\" version=\"1.0\"?>{valid}"),
        format!("<?xml version=\"1.1\"?>{valid}"),
        format!("<?xml version=\"1.0\" encoding=\"UTF-16\"?>{valid}"),
        format!("<?xml version=\"1.0\" encoding=\"UTF-8\" encoding=\"UTF-8\"?>{valid}"),
        format!("<!DOCTYPE entry>{valid}"),
        format!("<!--comment-->{valid}"),
        format!("<?other?>{valid}"),
        valid.replace("A&amp;B&#37;20", "<![CDATA[x]]>"),
        valid.replace("A&amp;B&#37;20", "&unknown;"),
        valid.replace("A&amp;B&#37;20", "&#0;"),
        valid.replace("<Name>", "<Name bad='unterminated>"),
        format!("\u{FEFF}\u{FEFF}{valid}"),
    ] {
        assert_eq!(
            decode_definition(invalid.as_bytes()),
            Err(RuleXmlError::Malformed)
        );
    }
    assert_eq!(decode_definition(&[0xff]), Err(RuleXmlError::Malformed));
}

#[test]
fn names_preserve_utf16_case_spaces_and_literal_percent() {
    for name in [
        "$Default".to_owned(),
        " Rules ".to_owned(),
        "rules".to_owned(),
        "A&B%20".to_owned(),
        "x".repeat(50),
        "\u{1F600}".repeat(25),
        "\u{00E9}".to_owned(),
    ] {
        let expected = definition(&name, RuleFilter::False);
        assert_eq!(validate_name(&expected.name), Ok(()));
        let escaped = name.replace('&', "&amp;");
        assert_eq!(
            decode(&properties(&escaped, "FalseFilter", "1=0")),
            Ok(expected)
        );
    }
    assert_eq!(
        decode(&properties("&#32;Rules&#32;", "TrueFilter", "1=1")),
        Ok(definition(" Rules ", RuleFilter::True))
    );
}

#[test]
fn unsafe_and_xml_illegal_names_are_refused() {
    for name in [".", "..", "\u{FFFE}", "\u{FFFF}"] {
        let value = definition(name, RuleFilter::True);
        assert_eq!(
            validate_definition(&value),
            Err(RuleXmlError::UnsupportedDefinition)
        );
        assert_eq!(
            encode_entry(&value),
            Err(RuleXmlError::UnsupportedDefinition)
        );
    }
    for name in ["", " ", "a/b", "a\\b", "a@b", "a?b", "a#b", "a*b", "a\nb"] {
        assert!(RuleName::new(name).is_err(), "{name:?}");
    }
    for name in ["x".repeat(51), "\u{1F600}".repeat(26)] {
        assert_eq!(
            decode(&properties(&name, "TrueFilter", "1=1")),
            Err(RuleXmlError::InvalidDefinition)
        );
    }
    for name in [".", ".."] {
        assert_eq!(
            decode(&properties(name, "TrueFilter", "1=1")),
            Err(RuleXmlError::UnsupportedDefinition)
        );
    }
    assert_eq!(
        decode(&properties("&#xFFFE;", "TrueFilter", "1=1")),
        Err(RuleXmlError::Malformed)
    );
}

#[test]
fn body_and_event_work_limits_precede_final_name_validation() {
    let valid = document(&properties("x", "TrueFilter", "1=1"));
    let mut at_limit = valid.clone();
    at_limit.push_str(&" ".repeat(MAX_BODY_BYTES - valid.len()));
    assert_eq!(
        decode_definition(at_limit.as_bytes()),
        Ok(definition("x", RuleFilter::True))
    );
    at_limit.push(' ');
    assert_eq!(
        decode_definition(at_limit.as_bytes()),
        Err(RuleXmlError::WorkLimitExceeded)
    );
    let events = document(&properties(
        &"&#36;".repeat(MAX_EVENTS),
        "TrueFilter",
        "1=1",
    ));
    assert!(events.len() < MAX_BODY_BYTES);
    assert_eq!(
        decode_definition(events.as_bytes()),
        Err(RuleXmlError::WorkLimitExceeded)
    );
}

#[test]
fn attribute_and_active_namespace_limits_have_exact_edges() {
    let valid = document(&properties("x", "TrueFilter", "1=1"));
    let with_attributes =
        |count| valid.replacen("<entry ", &format!("<entry{} ", aliases("r", count)), 1);
    assert_eq!(
        decode_definition(with_attributes(MAX_ATTRIBUTES - 1).as_bytes()),
        Ok(definition("x", RuleFilter::True))
    );
    assert_eq!(
        decode_definition(with_attributes(MAX_ATTRIBUTES).as_bytes()),
        Err(RuleXmlError::WorkLimitExceeded)
    );
    let with_namespaces = |last| {
        format!(
            "<entry xmlns=\"{ATOM_NS}\"{}><content type=\"application/xml\"{}><RuleDescription xmlns=\"{SERVICE_BUS_NS}\"{}>{}</RuleDescription></content></entry>",
            aliases("r", 30),
            aliases("c", 30),
            aliases("d", last),
            properties("x", "TrueFilter", "1=1")
        )
    };
    assert_eq!(31 + 30 + 1 + 1 + 1, MAX_NAMESPACE_BINDINGS);
    assert_eq!(
        decode_definition(with_namespaces(1).as_bytes()),
        Ok(definition("x", RuleFilter::True))
    );
    assert_eq!(
        decode_definition(with_namespaces(2).as_bytes()),
        Err(RuleXmlError::WorkLimitExceeded)
    );
}

#[test]
fn reused_budget_limits_preserve_prior_counts() {
    assert_eq!(Budget::depth(MAX_DEPTH), Ok(()));
    assert_eq!(
        Budget::depth(MAX_DEPTH + 1),
        Err(AtomXmlError::WorkLimitExceeded)
    );
    assert_eq!(Budget::attributes(MAX_ATTRIBUTES), Ok(()));
    assert_eq!(
        Budget::attributes(MAX_ATTRIBUTES + 1),
        Err(AtomXmlError::WorkLimitExceeded)
    );
    let mut budget = Budget::default();
    for _ in 0..MAX_EVENTS {
        assert_eq!(budget.event(), Ok(()));
    }
    assert_eq!(budget.event(), Err(AtomXmlError::WorkLimitExceeded));
    assert_eq!(budget.events, MAX_EVENTS);
    for _ in 0..MAX_PROPERTIES {
        assert_eq!(budget.property(), Ok(()));
    }
    assert_eq!(budget.property(), Err(AtomXmlError::WorkLimitExceeded));
    assert_eq!(budget.properties, MAX_PROPERTIES);
    assert_eq!(budget.decoded(MAX_BODY_BYTES), Ok(()));
    assert_eq!(budget.decoded(1), Err(AtomXmlError::WorkLimitExceeded));
    assert_eq!(budget.decoded, MAX_BODY_BYTES);
    let mut overflow = usize::MAX;
    assert_eq!(
        bounded_add(&mut overflow, 1, usize::MAX),
        Err(AtomXmlError::WorkLimitExceeded)
    );
    assert_eq!(overflow, usize::MAX);
}

#[test]
fn responses_have_explicit_name_and_filter_without_runtime_or_action() {
    for (kind, expression, value) in [
        (
            "TrueFilter",
            "1=1",
            definition(" Rules &%20 ", RuleFilter::True),
        ),
        (
            "FalseFilter",
            "1=0",
            definition(" Rules &%20 ", RuleFilter::False),
        ),
    ] {
        let text = String::from_utf8(encode_entry(&value).unwrap()).unwrap();
        assert!(text.contains("<title> Rules &amp;%20 </title>"));
        assert!(text.contains("<Name> Rules &amp;%20 </Name>"));
        assert!(text.contains(&format!("i:type=\"{kind}\"")));
        assert!(text.contains(&format!("<SqlExpression>{expression}</SqlExpression>")));
        assert!(text.contains("<Parameters></Parameters>"));
        for absent in [
            "Action",
            "CreatedAt",
            "SizeInBytes",
            "MessageCount",
            "SubscriptionDescription",
            "SqlFilter",
        ] {
            assert!(!text.contains(absent), "{absent}");
        }
        let mut reader = quick_xml::Reader::from_str(&text);
        let mut names = Vec::new();
        loop {
            match reader.read_event().expect("well-formed response") {
                Event::Start(start) => names.push(start.name().as_ref().to_owned()),
                Event::Eof => break,
                _ => {}
            }
        }
        assert_eq!(
            names,
            [
                "entry",
                "title",
                "content",
                "RuleDescription",
                "Filter",
                "SqlExpression",
                "Parameters",
                "Name"
            ]
        );
        assert_eq!(
            decode_definition(text.as_bytes()),
            Err(RuleXmlError::Malformed)
        );
    }
}

#[test]
fn feed_counts_empty_tags_and_all_projection_preflight_are_bounded() {
    let empty = String::from_utf8(encode_feed(&[]).unwrap()).unwrap();
    assert_eq!(empty, format!("<feed xmlns=\"{ATOM_NS}\"></feed>"));
    let values = vec![definition("x", RuleFilter::True); MAX_FEED_ENTRIES];
    let text = String::from_utf8(encode_feed(&values).unwrap()).unwrap();
    assert_eq!(text.matches("<entry ").count(), MAX_FEED_ENTRIES);
    assert_eq!(text.matches("<Name>x</Name>").count(), MAX_FEED_ENTRIES);
    assert!(text.ends_with("</feed>"));
    let mut too_many = values.clone();
    too_many.push(definition("y", RuleFilter::False));
    assert_eq!(
        encode_feed(&too_many),
        Err(RuleXmlError::ReplyLimitExceeded)
    );
    let mut incompatible = values;
    incompatible[MAX_FEED_ENTRIES - 1].filter = RuleFilter::Correlation(CorrelationFilter {
        properties: BTreeMap::from([("opaque".into(), MessageValue::Null)]),
        ..CorrelationFilter::default()
    });
    assert_eq!(
        encode_feed(&incompatible),
        Err(RuleXmlError::UnsupportedDefinition)
    );
    assert_eq!(
        encode_entry(&incompatible[MAX_FEED_ENTRIES - 1]),
        Err(RuleXmlError::UnsupportedDefinition)
    );
}

#[test]
fn reply_sink_and_pre_escape_bounds_preserve_prior_bytes() {
    let mut reply = Reply::default();
    reply.write_all(&vec![b'x'; MAX_REPLY_BYTES]).unwrap();
    assert!(reply.write(b"y").is_err());
    assert_eq!(reply.0, vec![b'x'; MAX_REPLY_BYTES]);
    let mut total = MAX_REPLY_BYTES - 6;
    assert_eq!(preflight(&mut total, 1, 0), Ok(()));
    assert_eq!(total, MAX_REPLY_BYTES);
    assert_eq!(
        preflight(&mut total, 1, 0),
        Err(RuleXmlError::ReplyLimitExceeded)
    );
    assert_eq!(total, MAX_REPLY_BYTES);
    assert_eq!(
        preflight(&mut 0, usize::MAX, 0),
        Err(RuleXmlError::ReplyLimitExceeded)
    );
}

#[test]
fn error_text_is_bounded_escaped_and_taxonomy_is_static() {
    let mut writer = Writer::new(Reply::default());
    scalar(&mut writer, "Name", "A&<>\"'").unwrap();
    assert_eq!(
        String::from_utf8(writer.into_inner().0).unwrap(),
        "<Name>A&amp;&lt;&gt;&quot;&apos;</Name>"
    );
    for error in [
        RuleXmlError::Malformed,
        RuleXmlError::WorkLimitExceeded,
        RuleXmlError::InvalidDefinition,
        RuleXmlError::UnsupportedDefinition,
        RuleXmlError::ReplyLimitExceeded,
    ] {
        let text = String::from_utf8(encode_error(error.code(), error.detail()).unwrap()).unwrap();
        assert!(text.contains(error.code()));
        assert!(text.contains(error.detail()));
        assert_eq!(error.to_string(), error.detail());
        assert!(!text.contains("queue"));
        assert!(!text.contains("credential"));
    }
    assert_eq!(
        String::from_utf8(encode_error("A&B", "<detail>").unwrap()).unwrap(),
        "<Error><Code>A&amp;B</Code><Detail>&lt;detail&gt;</Detail></Error>"
    );
    assert_eq!(
        encode_error("x", "\u{FFFE}"),
        Err(RuleXmlError::ReplyLimitExceeded)
    );
    assert_eq!(
        encode_error("x", &"x".repeat(MAX_REPLY_BYTES)),
        Err(RuleXmlError::ReplyLimitExceeded)
    );
}

#[test]
fn bare_filter_type_qname_requires_the_service_bus_default_namespace() {
    for (kind, expression, expected) in [
        ("TrueFilter", "1=1", RuleFilter::True),
        ("FalseFilter", "1=0", RuleFilter::False),
    ] {
        let prefixed = format!(
            "<entry xmlns=\"{ATOM_NS}\" xmlns:s=\"{SERVICE_BUS_NS}\"><content type=\"application/xml\"><s:RuleDescription><s:Filter xmlns:i=\"{XSI_NS}\" i:type=\"{kind}\"><s:SqlExpression>{expression}</s:SqlExpression><s:Parameters/></s:Filter><s:Name>x</s:Name></s:RuleDescription></content></entry>"
        );
        assert_eq!(
            decode_definition(prefixed.as_bytes()),
            Err(RuleXmlError::Malformed)
        );
        let bound = prefixed.replace(
            "<s:Filter ",
            &format!("<s:Filter xmlns=\"{SERVICE_BUS_NS}\" "),
        );
        assert_eq!(
            decode_definition(bound.as_bytes()),
            Ok(definition("x", expected))
        );
        let no_default = bound.replace(
            &format!("<s:Filter xmlns=\"{SERVICE_BUS_NS}\" "),
            "<s:Filter xmlns=\"\" ",
        );
        assert_eq!(
            decode_definition(no_default.as_bytes()),
            Err(RuleXmlError::Malformed)
        );
    }
}

fn correlation_definition(body: &str) -> String {
    format!(
        "<Filter xmlns:i=\"{XSI_NS}\" i:type=\"CorrelationFilter\">{body}</Filter><Name>Correlation</Name>"
    )
}

fn correlation_property(key: &str, kind: &str, text: &str) -> String {
    format!(
        "<KeyValueOfstringanyType><Key>{key}</Key><Value xmlns:l28=\"{}\" i:type=\"l28:{kind}\">{text}</Value></KeyValueOfstringanyType>",
        super::super::correlation::XSD_NS
    )
}

fn correlation_decode(body: &str) -> Result<AtomRuleDefinition, RuleXmlError> {
    decode(&correlation_definition(body))
}

fn correlation_roundtrip(filter: CorrelationFilter) -> String {
    let expected = definition("Correlation", RuleFilter::Correlation(filter));
    let bytes = encode_entry(&expected).unwrap();
    let text = String::from_utf8(bytes).unwrap();
    let request = text.replace("<title>Correlation</title>", "");
    assert_eq!(decode_definition(request.as_bytes()), Ok(expected));
    text
}

#[test]
fn correlation_empty_and_all_system_fields_preserve_typed_conditions() {
    for body in [
        "",
        "<Properties/>",
        "<Properties></Properties>",
        "<Properties> \t\n </Properties>",
    ] {
        assert_eq!(
            correlation_decode(body),
            Ok(definition(
                "Correlation",
                RuleFilter::Correlation(CorrelationFilter::default())
            ))
        );
    }
    let filter = CorrelationFilter {
        correlation_id: Some("".into()),
        message_id: Some(" Message & <id> ".into()),
        to: Some(" TO ".into()),
        reply_to: Some("ReplyTo".into()),
        subject: Some("\u{e9}\u{3bb}".into()),
        session_id: Some("Session".into()),
        reply_to_session_id: Some("ReplySession".into()),
        content_type: Some(" \r\n ".into()),
        ..CorrelationFilter::default()
    };
    let body = super::super::correlation::fields(&filter)
        .into_iter()
        .map(|(name, value)| {
            format!(
                "<{name}>{}</{name}>",
                quick_xml::escape::escape(value.unwrap())
            )
        })
        .collect::<String>();
    assert_eq!(
        correlation_decode(&body),
        Ok(definition(
            "Correlation",
            RuleFilter::Correlation(filter.clone())
        ))
    );
    let text = correlation_roundtrip(filter);
    assert!(text.contains("i:type=\"CorrelationFilter\""));
    assert!(text.contains("<CorrelationId></CorrelationId>"));
    assert!(text.contains("<Label>"));
    assert!(text.contains("&#13;\n"));
    assert!(text.contains("<Properties></Properties>"));
    for absent in [
        "SqlExpression",
        "Parameters",
        "Action",
        "CreatedAt",
        "MessageCount",
    ] {
        assert!(!text.contains(absent), "{absent}");
    }
    let empty = correlation_roundtrip(CorrelationFilter::default());
    assert!(!empty.contains("TrueFilter"));
    let text = correlation_roundtrip(CorrelationFilter {
        properties: BTreeMap::from([(
            "\r Key \r".into(),
            MessageValue::String("\r\r&\r\n".into()),
        )]),
        ..CorrelationFilter::default()
    });
    assert!(text.contains("<Key>&#13; Key &#13;</Key>"));
    assert!(text.contains(">&#13;&#13;&amp;&#13;\n</Value>"));
}

#[test]
fn correlation_scalar_types_preserve_constructor_bits_and_dates() {
    for (kind, text, expected) in [
        ("string", "", MessageValue::String(String::new())),
        (
            "string",
            "  &amp;lt;\u{e9}&#13;&#10;  ",
            MessageValue::String("  &lt;\u{e9}\r\n  ".into()),
        ),
        ("int", "-2147483648", MessageValue::Int(i32::MIN)),
        ("int", "+2147483647", MessageValue::Int(i32::MAX)),
        ("long", "-9223372036854775808", MessageValue::Long(i64::MIN)),
        ("long", "9223372036854775807", MessageValue::Long(i64::MAX)),
        ("boolean", "true", MessageValue::Bool(true)),
        ("boolean", "0", MessageValue::Bool(false)),
        ("double", "-0", MessageValue::Double((-0.0_f64).to_bits())),
        ("double", "0e0", MessageValue::Double(0.0_f64.to_bits())),
        (
            "double",
            "1.7976931348623157E308",
            MessageValue::Double(f64::MAX.to_bits()),
        ),
        ("double", "5e-324", MessageValue::Double(1)),
        (
            "double",
            "INF",
            MessageValue::Double(f64::INFINITY.to_bits()),
        ),
        (
            "double",
            "-INF",
            MessageValue::Double(f64::NEG_INFINITY.to_bits()),
        ),
        (
            "dateTime",
            "1970-01-01T00:00:00.000Z",
            MessageValue::Timestamp(0),
        ),
        (
            "dateTime",
            "1969-12-31T23:59:59.999Z",
            MessageValue::Timestamp(-1),
        ),
        (
            "dateTime",
            "1970-01-01T01:00:00+01:00",
            MessageValue::Timestamp(0),
        ),
        (
            "dateTime",
            "1969-12-31T10:00:00-14:00",
            MessageValue::Timestamp(0),
        ),
        (
            "dateTime",
            "0001-01-01T00:00:00Z",
            MessageValue::Timestamp(-62_135_596_800_000),
        ),
        (
            "dateTime",
            "9999-12-31T23:59:59.9990000000Z",
            MessageValue::Timestamp(253_402_300_799_999),
        ),
    ] {
        let filter = CorrelationFilter {
            properties: BTreeMap::from([(" Key ".into(), expected)]),
            ..CorrelationFilter::default()
        };
        let body = format!(
            "<Properties>{}</Properties>",
            correlation_property(" Key ", kind, text)
        );
        assert_eq!(
            correlation_decode(&body),
            Ok(definition(
                "Correlation",
                RuleFilter::Correlation(filter.clone())
            )),
            "{kind}: {text}"
        );
        let encoded = correlation_roundtrip(filter);
        assert!(encoded.contains(&format!("i:type=\"l28:{kind}\"")));
        assert!(encoded.contains("<Key> Key </Key>"));
    }
    let filter = CorrelationFilter {
        properties: BTreeMap::from([
            ("same".into(), MessageValue::Int(1)),
            ("Other".into(), MessageValue::Long(1)),
        ]),
        ..CorrelationFilter::default()
    };
    let text = correlation_roundtrip(filter);
    assert!(text.contains("<Key>same</Key>"));
    assert!(text.contains("<Key>Other</Key>"));
}

#[test]
fn correlation_value_qnames_are_resolved_and_types_do_not_fall_back() {
    let property = correlation_property("key", "string", "text");
    let body = format!("<Properties>{property}</Properties>");
    let aliased = body
        .replace("xmlns:l28=", "xmlns:x=")
        .replace("l28:string", "x:string");
    assert_eq!(correlation_decode(&body), correlation_decode(&aliased));
    let bare = body
        .replace(
            &format!(
                "<Value xmlns:l28=\"{}\" i:type=\"l28:string\">",
                super::super::correlation::XSD_NS
            ),
            &format!(
                "<s:Value xmlns:s=\"{SERVICE_BUS_NS}\" xmlns=\"{}\" i:type=\"string\">",
                super::super::correlation::XSD_NS
            ),
        )
        .replace("</Value>", "</s:Value>");
    assert_eq!(correlation_decode(&bare), correlation_decode(&body));
    for invalid in [
        body.replace("l28:string", "string"),
        body.replace("l28:string", "missing:string"),
        body.replace("l28:string", "i:string"),
        body.replace("i:type=\"l28:string\"", "type=\"l28:string\""),
        body.replace("i:type=\"l28:string\"", "i:type=\" l28:string\""),
        body.replace(super::super::correlation::XSD_NS, "urn:foreign"),
        body.replace(
            "<Value ",
            "<Value xmlns=\"http://www.w3.org/2001/XMLSchema\" ",
        ),
        body.replace(">text</Value>", "><Key>text</Key></Value>"),
    ] {
        assert_eq!(
            correlation_decode(&invalid),
            Err(RuleXmlError::Malformed),
            "{invalid}"
        );
    }
    for kind in [
        "String",
        "unsignedInt",
        "unsignedLong",
        "short",
        "byte",
        "float",
        "decimal",
        "base64Binary",
        "duration",
        "guid",
        "unknown",
    ] {
        assert_eq!(
            correlation_decode(&format!(
                "<Properties>{}</Properties>",
                correlation_property("key", kind, "1")
            )),
            Err(RuleXmlError::UnsupportedDefinition),
            "{kind}"
        );
    }
    for forbidden in [
        "<SqlExpression>1=1</SqlExpression>",
        "<Parameters/>",
        "<Subject>x</Subject>",
        "<Unknown/>",
    ] {
        assert_eq!(
            correlation_decode(forbidden),
            Err(RuleXmlError::UnsupportedDefinition)
        );
    }
}

#[test]
fn correlation_duplicate_fields_keys_and_missing_parts_refuse() {
    let property = correlation_property("key", "int", "1");
    for invalid in [
        "<CorrelationId/><CorrelationId/>".into(),
        "<Properties/><Properties/>".into(),
        format!("<Properties>{property}{property}</Properties>"),
        format!("<Properties>{}</Properties>", property.replace("<Key>key</Key>", "<Key>key</Key><Key>key</Key>")),
        format!("<Properties>{}</Properties>", property.replace("</Value>", "</Value><Value xmlns:x=\"http://www.w3.org/2001/XMLSchema\" i:type=\"x:int\">1</Value>")),
    ] {
        assert_eq!(correlation_decode(&invalid), Err(RuleXmlError::Malformed));
    }
    for invalid in [
        "<Properties><KeyValueOfstringanyType/></Properties>".into(),
        format!("<Properties>{}</Properties>", property.replace("<Key>key</Key>", "")),
        "<Properties><KeyValueOfstringanyType><Key>key</Key></KeyValueOfstringanyType></Properties>".into(),
        format!("<Properties>{}</Properties>", property.replace(" i:type=\"l28:int\"", "")),
    ] {
        assert_eq!(correlation_decode(&invalid), Err(RuleXmlError::InvalidDefinition));
    }
    assert_eq!(
        correlation_decode("<Properties>text</Properties>"),
        Err(RuleXmlError::Malformed)
    );
    assert_eq!(
        correlation_decode("<Properties>&#32;</Properties>"),
        Err(RuleXmlError::Malformed)
    );
    assert_eq!(
        correlation_decode("<Properties><Unknown/></Properties>"),
        Err(RuleXmlError::UnsupportedDefinition)
    );
    let reordered = property
        .replace("<Key>key</Key>", "")
        .replace("</Value>", "</Value><Key>key</Key>");
    assert_eq!(
        correlation_decode(&format!("<Properties>{reordered}</Properties>")),
        correlation_decode(&format!("<Properties>{property}</Properties>"))
    );
    assert!(
        correlation_decode(&format!(
            "<Properties>{}</Properties>",
            correlation_property("", "string", "")
        ))
        .is_ok()
    );
}

#[test]
fn correlation_total_condition_limits_count_system_and_properties() {
    let properties = |count| {
        (0..count)
            .map(|n| correlation_property(&format!("p{n}"), "int", "1"))
            .collect::<String>()
    };
    let exact = format!("<Properties>{}</Properties>", properties(32));
    assert!(correlation_decode(&exact).is_ok());
    assert_eq!(
        correlation_decode(&format!("<Properties>{}</Properties>", properties(33))),
        Err(RuleXmlError::InvalidDefinition)
    );
    let fields = [
        "CorrelationId",
        "MessageId",
        "To",
        "ReplyTo",
        "Label",
        "SessionId",
        "ReplyToSessionId",
        "ContentType",
    ]
    .into_iter()
    .map(|name| format!("<{name}/>"))
    .collect::<String>();
    assert!(
        correlation_decode(&format!(
            "{fields}<Properties>{}</Properties>",
            properties(24)
        ))
        .is_ok()
    );
    assert_eq!(
        correlation_decode(&format!(
            "{fields}<Properties>{}</Properties>",
            properties(25)
        )),
        Err(RuleXmlError::InvalidDefinition)
    );
    let mut filter = CorrelationFilter {
        properties: (0..32)
            .map(|n| (format!("p{n}"), MessageValue::Int(1)))
            .collect(),
        ..CorrelationFilter::default()
    };
    assert_eq!(
        validate_definition(&definition(
            "Correlation",
            RuleFilter::Correlation(filter.clone())
        )),
        Ok(())
    );
    filter.correlation_id = Some(String::new());
    assert_eq!(
        validate_definition(&definition("Correlation", RuleFilter::Correlation(filter))),
        Err(RuleXmlError::InvalidDefinition)
    );
}

#[test]
fn correlation_scalar_refusals_and_xml_legality_are_exhaustive() {
    for value in [
        MessageValue::Null,
        MessageValue::Ubyte(1),
        MessageValue::Ushort(1),
        MessageValue::Uint(1),
        MessageValue::Ulong(1),
        MessageValue::Byte(1),
        MessageValue::Short(1),
        MessageValue::Float(1),
        MessageValue::Decimal32([0; 4]),
        MessageValue::Decimal64([0; 8]),
        MessageValue::Decimal128([0; 16]),
        MessageValue::Char('a'),
        MessageValue::Uuid([0; 16]),
        MessageValue::Binary(vec![]),
        MessageValue::Symbol("ascii".into()),
        MessageValue::Double(0x7ff8_0000_0000_0000),
        MessageValue::Double(0xfff8_0000_0000_0000),
        MessageValue::Double(0x7ff8_0000_0000_0001),
        MessageValue::Timestamp(-62_135_596_800_001),
        MessageValue::Timestamp(253_402_300_800_000),
        MessageValue::String("\u{FFFE}".into()),
    ] {
        let value = definition(
            "Correlation",
            RuleFilter::Correlation(CorrelationFilter {
                properties: BTreeMap::from([("key".into(), value)]),
                ..CorrelationFilter::default()
            }),
        );
        assert_eq!(
            validate_definition(&value),
            Err(RuleXmlError::UnsupportedDefinition)
        );
        assert_eq!(
            encode_entry(&value),
            Err(RuleXmlError::UnsupportedDefinition)
        );
    }
    for value in [
        MessageValue::List(vec![]),
        MessageValue::Map(vec![]),
        MessageValue::Array(vec![]),
        MessageValue::Described {
            descriptor: domain::MessageDescriptor::Code(1),
            value: Box::new(MessageValue::Null),
        },
    ] {
        let value = definition(
            "Correlation",
            RuleFilter::Correlation(CorrelationFilter {
                properties: BTreeMap::from([("key".into(), value)]),
                ..CorrelationFilter::default()
            }),
        );
        assert_eq!(
            validate_definition(&value),
            Err(RuleXmlError::InvalidDefinition)
        );
    }
    for name in [
        "CorrelationId",
        "MessageId",
        "To",
        "ReplyTo",
        "Label",
        "SessionId",
        "ReplyToSessionId",
        "ContentType",
    ] {
        let mut filter = CorrelationFilter::default();
        super::super::correlation::Field::named(name)
            .unwrap()
            .set(&mut filter, "\u{FFFE}".into());
        assert_eq!(
            validate_definition(&definition("Correlation", RuleFilter::Correlation(filter))),
            Err(RuleXmlError::UnsupportedDefinition)
        );
        assert_eq!(
            correlation_decode(&format!("<{name}>&#xFFFE;</{name}>")),
            Err(RuleXmlError::Malformed)
        );
    }
    let filter = CorrelationFilter {
        properties: BTreeMap::from([("\u{FFFE}".into(), MessageValue::Int(1))]),
        ..CorrelationFilter::default()
    };
    assert_eq!(
        validate_definition(&definition("Correlation", RuleFilter::Correlation(filter))),
        Err(RuleXmlError::UnsupportedDefinition)
    );
}

#[test]
fn correlation_numeric_and_datetime_lexicals_refuse_lossy_inputs() {
    for (kind, text) in [
        ("int", "2147483648"),
        ("int", "1.0"),
        ("long", "9223372036854775808"),
        ("boolean", "True"),
        ("boolean", "2"),
        ("double", "NaN"),
        ("double", "nan"),
        ("double", "Infinity"),
        ("double", "0x1p0"),
        ("double", "1e"),
        ("dateTime", "2000-01-01T00:00:00.000"),
        ("dateTime", "2000-01-01T00:00:00.0001Z"),
        ("dateTime", "2000-01-01T00:00:00.0000000001Z"),
        ("dateTime", "2000-01-01T00:00:60Z"),
        ("dateTime", "2000-01-01T00:00:00+14:01"),
        ("dateTime", "2000-01-01T00:00:00+01"),
        ("dateTime", "2000-02-30T00:00:00Z"),
        ("dateTime", "2000-01-01T24:00:00Z"),
        ("dateTime", "0000-01-01T00:00:00Z"),
        ("dateTime", "0000-12-31T23:59:59.999-00:01"),
        ("dateTime", "0001-01-01T00:00:00+01:00"),
        ("dateTime", "9999-12-31T23:59:59.999-01:00"),
        ("dateTime", "2000-01-01t00:00:00Z"),
    ] {
        assert!(
            correlation_decode(&format!(
                "<Properties>{}</Properties>",
                correlation_property("key", kind, text)
            ))
            .is_err(),
            "{kind}: {text}"
        );
    }
}

#[test]
fn correlation_projection_preflight_counts_all_text_and_property_markup() {
    let mut filter = CorrelationFilter {
        correlation_id: Some(" &\r\n ".into()),
        message_id: Some("Message".into()),
        to: Some("To".into()),
        reply_to: Some("Reply".into()),
        subject: Some("Subject".into()),
        session_id: Some("Session".into()),
        reply_to_session_id: Some("ReplySession".into()),
        content_type: Some("Content".into()),
        ..CorrelationFilter::default()
    };
    for n in 0..24 {
        filter.properties.insert(
            format!("key{n}"),
            match n % 6 {
                0 => MessageValue::String("&".repeat(100)),
                1 => MessageValue::Bool(false),
                2 => MessageValue::Int(i32::MIN),
                3 => MessageValue::Long(i64::MIN),
                4 => MessageValue::Double((-f64::MAX).to_bits()),
                _ => MessageValue::Timestamp(253_402_300_799_999),
            },
        );
    }
    let (filter_bytes, markup_bytes) = super::super::correlation::budget(&filter).unwrap();
    assert_eq!(
        markup_bytes,
        24 * super::super::correlation::PROPERTY_MARKUP_BYTES
    );
    let fields_bytes = super::super::correlation::fields(&filter)
        .into_iter()
        .filter_map(|(_, value)| value)
        .map(str::len)
        .sum::<usize>();
    let keys_bytes = filter.properties.keys().map(String::len).sum::<usize>();
    assert_eq!(
        filter_bytes,
        fields_bytes + keys_bytes + 4 * (100 + 5 + 11 + 20 + 24 + 24)
    );
    for value in filter.properties.values() {
        let text = super::super::correlation::text(value).unwrap();
        if !matches!(value, MessageValue::String(_)) {
            assert!(text.len() <= 24);
        }
    }
    let value = definition("Correlation", RuleFilter::Correlation(filter.clone()));
    let reserved = (2 * value.name.as_str().len() + filter_bytes) * 6 + 1_024 + markup_bytes;
    let mut total = MAX_REPLY_BYTES - reserved;
    assert_eq!(definition_budget(&mut total, &value), Ok(()));
    assert_eq!(total, MAX_REPLY_BYTES);
    let mut one_short = MAX_REPLY_BYTES - reserved + 1;
    let original = one_short;
    assert_eq!(
        definition_budget(&mut one_short, &value),
        Err(RuleXmlError::ReplyLimitExceeded)
    );
    assert_eq!(one_short, original);
    let text = correlation_roundtrip(filter);
    assert!(text.len() <= reserved);
    let small = definition(
        "Correlation",
        RuleFilter::Correlation(CorrelationFilter {
            properties: BTreeMap::from([("key0".into(), MessageValue::Int(1))]),
            ..CorrelationFilter::default()
        }),
    );
    let mut feed = vec![small; MAX_FEED_ENTRIES];
    assert!(encode_feed(&feed).is_ok());
    if let RuleFilter::Correlation(filter) = &mut feed[MAX_FEED_ENTRIES - 1].filter {
        filter.properties.insert("key0".into(), MessageValue::Null);
    }
    assert_eq!(encode_feed(&feed), Err(RuleXmlError::UnsupportedDefinition));
    let large = definition(
        "Correlation",
        RuleFilter::Correlation(CorrelationFilter {
            subject: Some("&".repeat(60_000)),
            ..CorrelationFilter::default()
        }),
    );
    assert!(validate_definition(&large).is_ok());
    assert_eq!(
        encode_feed(&[large.clone(), large.clone(), large]),
        Err(RuleXmlError::ReplyLimitExceeded)
    );
    let oversized_source = definition(
        "Correlation",
        RuleFilter::Correlation(CorrelationFilter {
            subject: Some("&".repeat(MAX_REPLY_BYTES / 6)),
            ..CorrelationFilter::default()
        }),
    );
    // Native stored-byte admission remains in the stamped planner, not this profile.
    assert_eq!(validate_definition(&oversized_source), Ok(()));
    assert_eq!(
        encode_entry(&oversized_source),
        Err(RuleXmlError::ReplyLimitExceeded)
    );
}

#[test]
fn correlation_number_grammar_and_double_finite_bits_roundtrip() {
    for (literal, expected) in [
        ("1", 1.0_f64),
        ("+1.25", 1.25),
        (".5", 0.5),
        ("1.", 1.0),
        ("-1e-2", -0.01),
    ] {
        let body = format!(
            "<Properties>{}</Properties>",
            correlation_property("key", "double", literal)
        );
        let expected = CorrelationFilter {
            properties: BTreeMap::from([("key".into(), MessageValue::Double(expected.to_bits()))]),
            ..CorrelationFilter::default()
        };
        assert_eq!(
            correlation_decode(&body),
            Ok(definition("Correlation", RuleFilter::Correlation(expected)))
        );
    }
    for bits in [
        0,
        1,
        0x8000_0000_0000_0000,
        0x8000_0000_0000_0001,
        0x0010_0000_0000_0000,
        0x7fef_ffff_ffff_ffff,
        0xffef_ffff_ffff_ffff,
        0x3ff0_0000_0000_0001,
        0x3fef_ffff_ffff_ffff,
        0x8010_0000_0000_0000,
    ] {
        correlation_roundtrip(CorrelationFilter {
            properties: BTreeMap::from([("key".into(), MessageValue::Double(bits))]),
            ..CorrelationFilter::default()
        });
    }
}

#[test]
fn correlation_ordinal_key_collisions_refuse_without_normalizing() {
    let casing = super::super::ordinal::KeyCasing::default();
    for (left, right) in [
        ("a", "A"),
        ("\u{e9}", "\u{c9}"),
        ("\u{b5}", "\u{39c}"),
        ("\u{250}", "\u{2c6f}"),
        ("\u{3c2}", "\u{3a3}"),
        ("\u{1c8a}", "\u{1c89}"),
        ("\u{10428}", "\u{10400}"),
        ("\u{16e60}", "\u{16e40}"),
        (" Key \u{e9}", " KEY \u{c9}"),
    ] {
        assert_eq!(casing.key(left), casing.key(right), "{left:?}, {right:?}");
        let filter = CorrelationFilter {
            properties: BTreeMap::from([
                (left.into(), MessageValue::Int(1)),
                (right.into(), MessageValue::Long(2)),
            ]),
            ..CorrelationFilter::default()
        };
        assert_eq!(filter.validate(), Ok(()));
        let value = definition("Correlation", RuleFilter::Correlation(filter));
        assert_eq!(
            validate_definition(&value),
            Err(RuleXmlError::UnsupportedDefinition)
        );
        assert_eq!(
            encode_entry(&value),
            Err(RuleXmlError::UnsupportedDefinition)
        );
        assert_eq!(
            encode_feed(&[value]),
            Err(RuleXmlError::UnsupportedDefinition)
        );
        let body = format!(
            "<Properties>{}{}</Properties>",
            correlation_property(&quick_xml::escape::escape(left), "int", "1"),
            correlation_property(&quick_xml::escape::escape(right), "long", "2")
        );
        assert_eq!(
            correlation_decode(&body),
            Err(RuleXmlError::UnsupportedDefinition)
        );
    }
    for (left, right) in [
        ("\u{131}", "I"),
        ("\u{17f}", "S"),
        ("\u{130}", "i"),
        ("\u{212a}", "K"),
        ("\u{df}", "\u{1e9e}"),
        ("\u{df}", "SS"),
        ("\u{fb00}", "ff"),
        ("\u{e9}", "e\u{301}"),
        ("\u{10d70}", "\u{10d50}"),
        ("\u{16ebb}", "\u{16ea0}"),
        (" Key ", "Key"),
        ("", " "),
    ] {
        assert_ne!(casing.key(left), casing.key(right), "{left:?}, {right:?}");
        let filter = CorrelationFilter {
            properties: BTreeMap::from([
                (left.into(), MessageValue::Int(1)),
                (right.into(), MessageValue::Long(2)),
            ]),
            ..CorrelationFilter::default()
        };
        let text = correlation_roundtrip(filter);
        for key in [left, right] {
            assert!(text.contains(&format!("<Key>{}</Key>", quick_xml::escape::escape(key))));
        }
    }
}

#[test]
fn correlation_ordinal_model_keeps_static_latin1_and_scalar_width() {
    let casing = super::super::ordinal::KeyCasing::default();
    for (first, last, output_first) in [(0x61, 0x7a, 0x41), (0xe0, 0xf6, 0xc0), (0xf8, 0xfe, 0xd8)]
    {
        for (scalar, uppercase) in (first..=last).zip(output_first..) {
            let scalar = char::from_u32(scalar).unwrap();
            let uppercase = char::from_u32(uppercase).unwrap();
            assert_eq!(casing.uppercase(scalar), uppercase);
        }
    }
    for scalar in 0..=0xff {
        if (0x61..=0x7a).contains(&scalar)
            || (0xe0..=0xf6).contains(&scalar)
            || (0xf8..=0xfe).contains(&scalar)
            || scalar == 0xb5
            || scalar == 0xff
        {
            continue;
        }
        let scalar = char::from_u32(scalar).unwrap();
        assert_eq!(casing.uppercase(scalar), scalar);
    }
    assert_eq!(casing.uppercase('\u{b5}'), '\u{39c}');
    assert_eq!(casing.uppercase('\u{ff}'), '\u{178}');
    assert_eq!(casing.uppercase('\u{131}'), '\u{131}');
    assert_eq!(casing.uppercase('\u{17f}'), '\u{17f}');
    assert_eq!(casing.uppercase('\u{1161}'), '\u{1161}');
    assert_eq!(casing.uppercase('\u{1c8a}'), '\u{1c89}');
    assert_eq!(casing.uppercase('\u{10428}'), '\u{10400}');
    assert_eq!(casing.uppercase('\u{10d70}'), '\u{10d70}');
    assert_eq!(casing.uppercase('\u{16ebb}'), '\u{16ebb}');
    let original = "\u{250}";
    let mapped = casing.key(original);
    assert_ne!(original.len(), mapped.len());
    assert_eq!(
        original.encode_utf16().count(),
        mapped.encode_utf16().count()
    );
}

fn action(kind: &str, expression: &str, parameters: &str) -> String {
    format!(
        "<Action xmlns:i=\"{XSI_NS}\" i:type=\"{kind}\"><SqlExpression>{expression}</SqlExpression>{parameters}</Action>"
    )
}

fn definition_with_action(name: &str, filter: RuleFilter, source: &str) -> AtomRuleDefinition {
    let mut value = definition(name, filter);
    value.action = Some(domain::SqlAction::new(source).expect("bounded v2 test action"));
    value
}

#[test]
fn sql_actions_decode_all_native_v2_literals_and_keep_filter_fields_separate() {
    for source in [
        "REMOVE user.[a.b]; REMOVE \"USER\".\"Case\";",
        "SET user.text='it''s'; SET enabled=TRUE; SET disabled=FALSE;",
        "SET minimum=-9223372036854775808; SET maximum=9223372036854775807; SET positive=+7;",
        " SET user.[caf\u{e9} & <\u{03bb}>]=' Red & <\u{03bb}>\nline '; REMOVE user.marker; ",
    ] {
        let escaped = quick_xml::escape::escape(source);
        for parameters in ["", "<Parameters/>", "<Parameters> \n\t </Parameters>"] {
            let action = action("SqlRuleAction", &escaped, parameters);
            for (filter_xml, filter) in [
                (
                    filter("TrueFilter", "1=1", "<Parameters/>"),
                    RuleFilter::True,
                ),
                (
                    filter("FalseFilter", "1=0", "<Parameters></Parameters>"),
                    RuleFilter::False,
                ),
                (
                    filter("SqlFilter", " user.colour = 'Red' ", "<Parameters/>"),
                    RuleFilter::Sql(domain::SqlFilter::new(" user.colour = 'Red' ").unwrap()),
                ),
                (
                    format!("<Filter xmlns:i=\"{XSI_NS}\" i:type=\"CorrelationFilter\"/>"),
                    RuleFilter::Correlation(CorrelationFilter::default()),
                ),
            ] {
                let expected = definition_with_action("Action", filter, source);
                for body in [
                    format!("{filter_xml}{action}<Name>Action</Name>"),
                    format!("<Name>Action</Name>{action}{filter_xml}"),
                ] {
                    assert_eq!(decode(&body), Ok(expected.clone()), "{body}");
                }
                assert_eq!(
                    expected.action.as_ref().unwrap().semantic_version(),
                    domain::SQL_ACTION_SEMANTIC_VERSION
                );
            }
        }
    }
}

#[test]
fn sql_actions_reject_parameters_missing_fields_and_ambiguous_shapes() {
    let base = properties("Action", "TrueFilter", "1=1");
    let valid = action("SqlRuleAction", "REMOVE x", "<Parameters/>");
    for (extra, error) in [
        ("<Action/>".to_owned(), RuleXmlError::InvalidDefinition),
        (
            format!("<Action xmlns:i=\"{XSI_NS}\" i:type=\"SqlRuleAction\"/>"),
            RuleXmlError::InvalidDefinition,
        ),
        (
            action("SqlRuleAction", "", ""),
            RuleXmlError::InvalidDefinition,
        ),
        (
            action("EmptyRuleAction", "REMOVE x", ""),
            RuleXmlError::UnsupportedDefinition,
        ),
        (
            action("UnknownRuleAction", "REMOVE x", ""),
            RuleXmlError::UnsupportedDefinition,
        ),
        (format!("{valid}{valid}"), RuleXmlError::Malformed),
        (
            valid.replace("<Parameters/>", "<Parameters/><Parameters/>"),
            RuleXmlError::Malformed,
        ),
        (
            valid.replace(
                "</SqlExpression>",
                "</SqlExpression><SqlExpression>REMOVE y</SqlExpression>",
            ),
            RuleXmlError::Malformed,
        ),
        (
            valid.replace(
                "<Parameters/>",
                "<Parameters><KeyValueOfstringanyType/></Parameters>",
            ),
            RuleXmlError::UnsupportedDefinition,
        ),
        (
            valid.replace("<Parameters/>", "<Parameters>x</Parameters>"),
            RuleXmlError::Malformed,
        ),
        (
            valid.replace("<Parameters/>", "<Parameters>&#32;</Parameters>"),
            RuleXmlError::Malformed,
        ),
        (
            valid.replace("<Parameters/>", "<Unknown/>"),
            RuleXmlError::UnsupportedDefinition,
        ),
        (
            valid.replace("i:type=\"SqlRuleAction\"", "i:nil=\"true\""),
            RuleXmlError::Malformed,
        ),
        (
            valid.replace("i:type=\"SqlRuleAction\"", "type=\"SqlRuleAction\""),
            RuleXmlError::Malformed,
        ),
        (
            valid.replace(XSI_NS, SERVICE_BUS_NS),
            RuleXmlError::Malformed,
        ),
        (
            valid.replace("i:type=\"SqlRuleAction\"", "i:type=\"i:SqlRuleAction\""),
            RuleXmlError::UnsupportedDefinition,
        ),
    ] {
        assert_eq!(decode(&format!("{base}{extra}")), Err(error), "{extra}");
    }
    for (filter_parameters, action_parameters) in [
        ("<Parameters><Parameter/></Parameters>", "<Parameters/>"),
        ("<Parameters/>", "<Parameters><Parameter/></Parameters>"),
    ] {
        assert_eq!(
            decode(&format!(
                "{}{}<Name>Action</Name>",
                filter("SqlFilter", "1=1", filter_parameters),
                action("SqlRuleAction", "REMOVE x", action_parameters),
            )),
            Err(RuleXmlError::UnsupportedDefinition)
        );
    }
}

#[test]
fn sql_actions_reuse_native_compile_and_source_bounds() {
    let base = properties("Action", "FalseFilter", "1=0");
    for source in [
        "SET sys.Label='changed'".to_owned(),
        "SET x=NULL".to_owned(),
        "SET x=y".to_owned(),
        "SET x=1+2".to_owned(),
        "SET x=1.5".to_owned(),
        "SET x=1e2".to_owned(),
        "SET x=9223372036854775808".to_owned(),
        "REMOVE[x];".repeat(33),
        format!(
            "{}REMOVE x",
            " ".repeat(domain::MAX_SQL_EXPRESSION_TOKENS + 1)
        ),
        "x".repeat(domain::MAX_SQL_EXPRESSION_BYTES + 1),
        format!(
            "SET x='{}'",
            "x".repeat(domain::MAX_SQL_EXPRESSION_UTF16_UNITS)
        ),
    ] {
        assert!(domain::SqlAction::new(&source).is_err());
        assert_eq!(
            decode(&format!(
                "{base}{}",
                action(
                    "SqlRuleAction",
                    &quick_xml::escape::escape(&source),
                    "<Parameters/>"
                ),
            )),
            Err(RuleXmlError::InvalidDefinition),
            "{source}"
        );
    }
}

#[test]
fn sql_actions_refuse_version_loss_and_invalid_stored_sources() {
    let mut value = definition("Action", RuleFilter::False);
    value.action =
        Some(domain::SqlAction::with_semantic_version("REMOVE user.marker;", 1).unwrap());
    assert_eq!(
        validate_definition(&value),
        Err(RuleXmlError::UnsupportedDefinition)
    );
    assert_eq!(
        encode_entry(&value),
        Err(RuleXmlError::UnsupportedDefinition)
    );
    let mut feed = vec![definition("Keep", RuleFilter::True); 2];
    feed[1] = value;
    assert_eq!(encode_feed(&feed), Err(RuleXmlError::UnsupportedDefinition));

    let invalid = domain::codec::decode::<domain::SqlAction>(
        &domain::codec::encode(&(2_u32, "SET x=y")).unwrap(),
    )
    .unwrap();
    let mut value = definition("Action", RuleFilter::True);
    value.action = Some(invalid);
    assert_eq!(
        validate_definition(&value),
        Err(RuleXmlError::InvalidDefinition)
    );
    assert_eq!(encode_entry(&value), Err(RuleXmlError::InvalidDefinition));

    value.action = Some(domain::SqlAction::new("SET text='\u{1}'").unwrap());
    assert_eq!(
        validate_definition(&value),
        Err(RuleXmlError::UnsupportedDefinition)
    );
    assert_eq!(
        encode_entry(&value),
        Err(RuleXmlError::UnsupportedDefinition)
    );
}

#[test]
fn sql_action_replies_preserve_source_and_checked_budgets() {
    let source = " \r\nSET user.[a&<b>]='caf\u{e9} & <\u{03bb}>\r\n'; REMOVE user.marker;\r\n ";
    let value = definition_with_action(
        "Action &",
        RuleFilter::Sql(domain::SqlFilter::new("1=0").unwrap()),
        source,
    );
    let bytes = encode_entry(&value).unwrap();
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(text.contains("i:type=\"SqlRuleAction\""));
    assert!(text.contains(&format!(
        "<SqlExpression>{}</SqlExpression>",
        quick_xml::escape::escape(source)
    )));
    assert!(text.contains("&#13;\n"));
    assert_eq!(text.matches("<Parameters>").count(), 2);
    assert!(text.find("</Filter>").unwrap() < text.find("<Action").unwrap());
    assert!(text.find("</Action>").unwrap() < text.find("<Name>").unwrap());
    let title = format!(
        "<title>{}</title>",
        quick_xml::escape::escape(value.name.as_str())
    );
    assert_eq!(
        decode_definition(text.replace(&title, "").as_bytes()),
        Ok(value.clone())
    );

    let text_bytes = 2 * value.name.as_str().len() + 3 + source.len();
    let reservation = 6 * text_bytes + 1_024 + 256;
    let mut total = 0;
    definition_budget(&mut total, &value).unwrap();
    assert_eq!(total, reservation);
    assert!(bytes.len() <= reservation);
    let mut at_limit = MAX_REPLY_BYTES - reservation;
    definition_budget(&mut at_limit, &value).unwrap();
    assert_eq!(at_limit, MAX_REPLY_BYTES);
    assert_eq!(
        definition_budget(&mut at_limit, &value),
        Err(RuleXmlError::ReplyLimitExceeded)
    );
    let mut overflow = usize::MAX;
    assert_eq!(
        definition_budget(&mut overflow, &value),
        Err(RuleXmlError::ReplyLimitExceeded)
    );
    let action_markup = format!(
        "<Action xmlns:i=\"{XSI_NS}\" i:type=\"SqlRuleAction\"><SqlExpression></SqlExpression><Parameters></Parameters></Action>"
    );
    assert!(action_markup.len() <= ACTION_MARKUP_BYTES);
    let feed = encode_feed(&[definition("Keep", RuleFilter::True), value]).unwrap();
    assert_eq!(
        std::str::from_utf8(&feed)
            .unwrap()
            .matches("<Action")
            .count(),
        1
    );

    let source = format!("user.text='{}'", "&".repeat(1_000));
    let mut value = definition(
        "x",
        RuleFilter::Sql(domain::SqlFilter::new(source).unwrap()),
    );
    assert!(encode_feed(&vec![value.clone(); MAX_FEED_ENTRIES]).is_ok());
    value.action =
        Some(domain::SqlAction::new(format!("SET text='{}'", "&".repeat(1_000))).unwrap());
    assert_eq!(
        encode_feed(&vec![value; MAX_FEED_ENTRIES]),
        Err(RuleXmlError::ReplyLimitExceeded)
    );
}

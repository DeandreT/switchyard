use domain::{CorrelationFilter, RuleFilter, RuleName};

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
    for extra in ["<Action/>", "<Unknown/>", "<CreatedAt/>"] {
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
        "<Action/>",
        "<Action xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\" i:type=\"SqlRuleAction\"><SqlExpression>SET x=1</SqlExpression></Action>",
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
    incompatible[MAX_FEED_ENTRIES - 1].filter =
        RuleFilter::Correlation(CorrelationFilter::default());
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

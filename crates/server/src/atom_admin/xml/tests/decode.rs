use super::*;

#[test]
fn empty_description_builds_complete_http_defaults() {
    assert_eq!(parse(""), Ok(default_definition()));
    let empty = document("").replace(
        &format!("<QueueDescription xmlns=\"{SERVICE_BUS_NS}\"></QueueDescription>"),
        &format!("<QueueDescription xmlns=\"{SERVICE_BUS_NS}\"/>"),
    );
    assert_eq!(
        decode_definition(empty.as_bytes()),
        Ok(default_definition())
    );
}

#[test]
fn pinned_serializer_field_shapes_map_real_settings_and_finite_capacity() {
    let properties = concat!(
        "<LockDuration>PT30S</LockDuration><MaxSizeInMegabytes>1024</MaxSizeInMegabytes>",
        "<RequiresDuplicateDetection>false</RequiresDuplicateDetection>",
        "<RequiresSession>false</RequiresSession>",
        "<DefaultMessageTimeToLive>P1DT2H3M4.005S</DefaultMessageTimeToLive>",
        "<DeadLetteringOnMessageExpiration>true</DeadLetteringOnMessageExpiration>",
        "<DuplicateDetectionHistoryTimeWindow>PT20S</DuplicateDetectionHistoryTimeWindow>",
        "<MaxDeliveryCount>3</MaxDeliveryCount><EnableBatchedOperations>true</EnableBatchedOperations>",
        "<Status>Active</Status><AuthorizationRules></AuthorizationRules>",
        "<IsAnonymousAccessible>false</IsAnonymousAccessible><SupportOrdering>false</SupportOrdering>",
        "<EnablePartitioning>false</EnablePartitioning><EnableExpress>false</EnableExpress>",
        "<MaxMessageSizeInKilobytes>128</MaxMessageSizeInKilobytes>",
    );
    let parsed = parse(properties).unwrap();
    assert_eq!(
        parsed.config,
        QueueConfig {
            lock_duration_millis: 30_000,
            max_delivery_count: 3,
            default_time_to_live_millis: Some(93_784_005),
            max_message_bytes: 128 * KIB,
            requires_session: false,
            requires_duplicate_detection: false,
            duplicate_detection_history_time_window_millis: 20_000,
            dead_lettering_on_message_expiration: true,
        }
    );
    assert_eq!(parsed.limit.bytes(), 1_024 * MIB);
    let with_xsi = document(properties).replacen(
        "<QueueDescription ",
        &format!("<QueueDescription xmlns:i=\"{XSI_NS}\" "),
        1,
    );
    assert_eq!(decode_definition(with_xsi.as_bytes()), Ok(parsed));
}

#[test]
fn complete_put_omissions_reset_ttl_and_inactive_window() {
    let changed = parse("<DefaultMessageTimeToLive>PT2S</DefaultMessageTimeToLive><DuplicateDetectionHistoryTimeWindow>PT2M</DuplicateDetectionHistoryTimeWindow>").unwrap();
    assert_eq!(changed.config.default_time_to_live_millis, Some(2_000));
    assert_eq!(
        changed
            .config
            .duplicate_detection_history_time_window_millis,
        120_000
    );
    let reset = parse("<SupportOrdering>false</SupportOrdering>").unwrap();
    assert_eq!(reset, default_definition());
    assert_eq!(parse("<AuthorizationRules/>"), Ok(reset));
}

#[test]
fn expanded_names_accept_alternate_unicode_prefixes_and_normalized_declarations() {
    let text = format!(
        "<a:entry xmlns:a=\"{ATOM_NS}\"><a:content type=\"application/xml\"><\u{03B1}:QueueDescription xmlns:\u{03B1}=\"{}\"><\u{03B1}:MaxDeliveryCount>7</\u{03B1}:MaxDeliveryCount></\u{03B1}:QueueDescription></a:content></a:entry>",
        SERVICE_BUS_NS.replace("http:", "http&#58;")
    );
    assert_eq!(
        decode_definition(text.as_bytes())
            .unwrap()
            .config
            .max_delivery_count,
        7
    );
    let declaration_after_use = format!(
        "<a:entry xmlns:a=\"{ATOM_NS}\"><a:content type=\"application/xml\"><s:QueueDescription xmlns:s=\"{SERVICE_BUS_NS}\"/></a:content></a:entry>"
    );
    assert_eq!(
        decode_definition(declaration_after_use.as_bytes()),
        Ok(default_definition())
    );
}

#[test]
fn empty_elements_roll_back_namespace_scope_and_end_uses_lexical_echo() {
    let valid = format!(
        "<entry xmlns=\"{ATOM_NS}\"><content type=\"application/xml\"><QueueDescription xmlns=\"{SERVICE_BUS_NS}\"><s:ForwardTo xmlns:s=\"{SERVICE_BUS_NS}\"/><MaxDeliveryCount>2</MaxDeliveryCount></QueueDescription></content></entry>"
    );
    assert_eq!(
        decode_definition(valid.as_bytes())
            .unwrap()
            .config
            .max_delivery_count,
        2
    );
    let leaked = valid.replace(
        "<MaxDeliveryCount>2</MaxDeliveryCount>",
        "<s:MaxDeliveryCount>2</s:MaxDeliveryCount>",
    );
    assert_eq!(
        decode_definition(leaked.as_bytes()),
        Err(AtomXmlError::Malformed)
    );
    let wrong_echo = format!(
        "<a:entry xmlns:a=\"{ATOM_NS}\" xmlns:b=\"{ATOM_NS}\"><a:content type=\"application/xml\"><QueueDescription xmlns=\"{SERVICE_BUS_NS}\"/></a:content></b:entry>"
    );
    assert_eq!(
        decode_definition(wrong_echo.as_bytes()),
        Err(AtomXmlError::Malformed)
    );
    let closing_space = document("").replace("</entry>", "</entry \t>");
    assert_eq!(
        decode_definition(closing_space.as_bytes()),
        Ok(default_definition())
    );
}

#[test]
fn unknown_wrong_and_reserved_namespaces_fail_closed() {
    for text in [
        document("").replace(ATOM_NS, "HTTP://www.w3.org/2005/Atom"),
        document("").replace(ATOM_NS, "http%3A//www.w3.org/2005/Atom"),
        document("").replace(SERVICE_BUS_NS, "urn:unknown"),
        document("").replace("<entry ", "<entry xmlns:p=\"\" "),
        document("").replace("<entry ", "<entry xmlns:xml=\"urn:wrong\" "),
        document("").replace("<entry ", &format!("<entry xmlns:p=\"{XML_NS}\" ")),
        document("").replace("<entry ", &format!("<entry xmlns=\"{XML_NS}\" ")),
        document("").replace("<entry ", "<entry xmlns:xmlns=\"urn:wrong\" "),
        format!("<xmlns:entry xmlns:xmlns=\"{ATOM_NS}\"/>"),
        document("")
            .replace("<entry ", "<p:entry ")
            .replace("</entry>", "</p:entry>"),
    ] {
        assert_eq!(
            decode_definition(text.as_bytes()),
            Err(AtomXmlError::Malformed),
            "{text}"
        );
    }
}

#[test]
fn attributes_are_fully_checked_without_tail_recovery() {
    for tail in [
        "type=\"application/xml\"type=\"application/xml\"",
        "type=\"application/xml\" bad=\"<\"",
        "type=\"application/xml\" broken",
        "type=\"application/xml\" type=\"application/xml\"",
        "type=\"application/xml\" p:type=\"application/xml\" xmlns:p=\"http://www.w3.org/2005/Atom\"",
        "type=\"application/xml\" xsi:nil=\"false\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"",
        "type=\"application/xml\" xml:base=\"https://other.invalid/\"",
        "type='application/xml'\u{A0}bad='x'",
        "type=application/xml",
    ] {
        let text = document("").replace("type=\"application/xml\"", tail);
        assert_eq!(
            decode_definition(text.as_bytes()),
            Err(AtomXmlError::Malformed),
            "{tail}"
        );
    }
    assert_eq!(
        decode_definition(
            document("")
                .replace("type=\"application/xml\"", "type='application/xml'")
                .as_bytes()
        ),
        Ok(default_definition())
    );
}

#[test]
fn qname_lexical_rules_are_independent_of_prefix_resolution() {
    for name in [
        "",
        "1name",
        ":name",
        "a:",
        "a:b:c",
        "a:1b",
        "a:b?",
        "a: b",
        "\u{B7}name",
    ] {
        assert_eq!(lexical::qname(name), Err(AtomXmlError::Malformed), "{name}");
    }
    for name in [
        "name",
        "_name",
        "\u{03B1}:name",
        "a:n\u{B7}",
        "\u{10000}:name",
    ] {
        assert_eq!(lexical::qname(name), Ok(()), "{name}");
    }
}

#[test]
fn declaration_is_complete_ordered_and_first_after_optional_bom() {
    let body = document("");
    for declaration in [
        "<?xml version='1.0'?>",
        "<?xml version=\"1.0\" encoding=\"uTf-8\" standalone=\"yes\"?>",
        "<?xml version='1.0' standalone='no'?>",
    ] {
        assert_eq!(
            decode_definition(format!("\u{FEFF}{declaration}{body}").as_bytes()),
            Ok(default_definition())
        );
    }
    for declaration in [
        "<?xml?>",
        "<?XML version='1.0'?>",
        "<?xml version='1.1'?>",
        "<?xml encoding='UTF-8' version='1.0'?>",
        "<?xml version='1.0' version='1.0'?>",
        "<?xml version='1.0' encoding='UTF-16'?>",
        "<?xml version='1.0' standalone='yes' encoding='UTF-8'?>",
        "<?xml version='1.0' standalone='maybe'?>",
        "<?xml version='1.0'encoding='UTF-8'?>",
        "<?xml version='1&#46;0'?>",
        "<?xml version='1.0' extra='x'?>",
    ] {
        assert_eq!(
            decode_definition(format!("{declaration}{body}").as_bytes()),
            Err(AtomXmlError::Malformed),
            "{declaration}"
        );
    }
    assert_eq!(
        decode_definition(format!(" <?xml version='1.0'?>{body}").as_bytes()),
        Err(AtomXmlError::Malformed)
    );
    assert_eq!(
        decode_definition(format!("<?xml version='1.0'?><?xml version='1.0'?>{body}").as_bytes()),
        Err(AtomXmlError::Malformed)
    );
    for prefix in [
        "\u{FEFF}\u{FEFF}",
        "\u{FEFF}\u{FEFF}<?xml version='1.0'?>",
        "\u{FEFF}\u{FEFF}\u{FEFF}",
        " \u{FEFF}",
        "\u{FEFF} \u{FEFF}",
    ] {
        assert_eq!(
            decode_definition(format!("{prefix}{body}").as_bytes()),
            Err(AtomXmlError::Malformed),
            "{prefix:?}"
        );
    }
}

#[test]
fn references_are_xml_only_once_and_legal_chars_are_checked() {
    assert_eq!(
        parse("<MaxDeliveryCount>&#49;&#x30;</MaxDeliveryCount>")
            .unwrap()
            .config
            .max_delivery_count,
        10
    );
    for value in [
        "&lt;", "&gt;", "&amp;", "&apos;", "&quot;", "&amp;lt;", "]]&gt;",
    ] {
        assert_eq!(
            parse(&property("UserMetadata", value)),
            Err(AtomXmlError::UnsupportedDefinition),
            "{value}"
        );
    }
    for value in [
        "&unknown;",
        "&nbsp;",
        "&amp",
        "&#0;",
        "&#xB;",
        "&#xFFFE;",
        "&#xD800;",
        "&#x110000;",
        "]]>",
    ] {
        assert_eq!(
            parse(&property("UserMetadata", value)),
            Err(AtomXmlError::Malformed),
            "{value}"
        );
    }
    assert_eq!(
        parse("<MaxDeliveryCount>\r\n1\r</MaxDeliveryCount>")
            .unwrap()
            .config
            .max_delivery_count,
        1
    );
    assert_eq!(
        parse("<MaxDeliveryCount>&#13;1&#13;</MaxDeliveryCount>")
            .unwrap()
            .config
            .max_delivery_count,
        1
    );
    assert_eq!(
        parse("<AuthorizationRules>&#32;</AuthorizationRules>"),
        Err(AtomXmlError::Malformed)
    );
    assert_eq!(decode_definition(b"\xff"), Err(AtomXmlError::Malformed));
    for illegal in ["\u{0}", "\u{B}", "\u{FFFE}", "\u{FFFF}"] {
        assert_eq!(
            parse(&property("UserMetadata", illegal)),
            Err(AtomXmlError::Malformed)
        );
    }
}

#[test]
fn dtd_pi_comments_cdata_and_incomplete_or_mixed_shapes_are_refused() {
    let body = document("");
    for text in [
        format!("<!DOCTYPE entry SYSTEM 'file:///secret'>{body}"),
        format!("<!DOCTYPE entry [<!ENTITY custom 'x'>]>{body}"),
        format!("<?target data?>{body}"),
        format!("<!-- comment -->{body}"),
        document("<MaxDeliveryCount><![CDATA[1]]></MaxDeliveryCount>"),
        document("<MaxDeliveryCount><Nested>1</Nested></MaxDeliveryCount>"),
        format!("<wrapper>{body}</wrapper>"),
        format!("{body}{body}"),
        format!("{body}trailing"),
        body.replace("</entry>", ""),
        body.replace("<content", "bad<content"),
        body.replace("<content", "<title>untrusted</title><content"),
        body.replace("</content>", "</content><content type='application/xml'/>"),
    ] {
        assert_eq!(
            decode_definition(text.as_bytes()),
            Err(AtomXmlError::Malformed),
            "{text}"
        );
    }
    for text in [
        format!("<entry xmlns='{ATOM_NS}'/>"),
        format!("<entry xmlns='{ATOM_NS}'><content type='application/xml'/></entry>"),
        "".into(),
    ] {
        assert_eq!(
            decode_definition(text.as_bytes()),
            Err(AtomXmlError::Malformed)
        );
    }
    assert_eq!(
        parse("<MaxDeliveryCount>1</MaxDeliveryCount><MaxDeliveryCount>2</MaxDeliveryCount>"),
        Err(AtomXmlError::Malformed)
    );
}

#[test]
fn unsupported_properties_cannot_be_inertly_enabled_or_preserved() {
    for (name, value) in [
        ("RequiresSession", "true"),
        ("RequiresDuplicateDetection", "1"),
        ("EnableBatchedOperations", "false"),
        ("Status", "Disabled"),
        ("IsAnonymousAccessible", "true"),
        ("EnablePartitioning", "true"),
        ("EnableExpress", "true"),
        ("SupportOrdering", "true"),
        ("ForwardTo", "other"),
        ("ForwardTo", " "),
        ("ForwardDeadLetteredMessagesTo", "other"),
        ("UserMetadata", "x"),
        ("AutoDeleteOnIdle", ""),
        ("AutoDeleteOnIdle", "P1D"),
        ("SizeInBytes", "0"),
    ] {
        assert_eq!(
            parse(&property(name, value)),
            Err(AtomXmlError::UnsupportedDefinition),
            "{name}"
        );
    }
    for text in [
        "<AuthorizationRules><Rule/></AuthorizationRules>",
        "<AuthorizationRules>data</AuthorizationRules>",
        "<AuthorizationRules xsi:nil='true' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'/>",
    ] {
        assert!(parse(text).is_err());
    }
    assert_eq!(
        parse(
            "<ForwardTo/><ForwardDeadLetteredMessagesTo></ForwardDeadLetteredMessagesTo><UserMetadata/>"
        ),
        Ok(default_definition())
    );
    assert_eq!(
        parse("<AuthorizationRules> \t\r\n </AuthorizationRules>"),
        Ok(default_definition())
    );
}

#[test]
fn numeric_boolean_and_supported_ranges_use_exact_conversion() {
    for value in [
        "0",
        "2147483648",
        "+1",
        "-1",
        "1.0",
        "1e2",
        "1,000",
        "",
        "\u{A0}1",
    ] {
        assert_eq!(
            parse(&property("MaxDeliveryCount", value)),
            Err(AtomXmlError::InvalidDefinition),
            "{value}"
        );
    }
    assert_eq!(
        parse("<MaxDeliveryCount> 2147483647 </MaxDeliveryCount>")
            .unwrap()
            .config
            .max_delivery_count,
        i32::MAX as u32
    );
    assert_eq!(
        parse("<MaxSizeInMegabytes>2147483647</MaxSizeInMegabytes>")
            .unwrap()
            .limit
            .bytes(),
        i32::MAX as u64 * MIB
    );
    for value in ["0", "2147483648", "18446744073709551616"] {
        assert_eq!(
            parse(&property("MaxSizeInMegabytes", value)),
            Err(AtomXmlError::InvalidDefinition)
        );
    }
    for value in ["0", "257", "1.5"] {
        assert_eq!(
            parse(&property("MaxMessageSizeInKilobytes", value)),
            Err(AtomXmlError::InvalidDefinition)
        );
    }
    for (value, expected) in [("true", true), ("1", true), ("false", false), ("0", false)] {
        assert_eq!(
            parse(&property("DeadLetteringOnMessageExpiration", value))
                .unwrap()
                .config
                .dead_lettering_on_message_expiration,
            expected
        );
    }
    for value in ["True", "yes", "", "2"] {
        assert_eq!(
            parse(&property("DeadLetteringOnMessageExpiration", value)),
            Err(AtomXmlError::InvalidDefinition)
        );
    }
    for (name, valid, invalid) in [
        ("LockDuration", "PT5S", "PT4.999S"),
        ("LockDuration", "PT5M", "PT300.001S"),
        ("DefaultMessageTimeToLive", "PT1S", "PT0.999S"),
        ("DuplicateDetectionHistoryTimeWindow", "PT20S", "PT19.999S"),
        ("DuplicateDetectionHistoryTimeWindow", "P7D", "P7DT0.001S"),
    ] {
        assert!(parse(&property(name, valid)).is_ok());
        assert_eq!(
            parse(&property(name, invalid)),
            Err(AtomXmlError::InvalidDefinition)
        );
    }
    for name in [
        "LockDuration",
        "DefaultMessageTimeToLive",
        "MaxDeliveryCount",
        "RequiresSession",
    ] {
        assert_eq!(
            parse(&format!("<{name}/>")),
            Err(AtomXmlError::InvalidDefinition)
        );
    }
}

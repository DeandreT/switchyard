use super::super::duration;
use super::*;

#[test]
fn response_title_uses_literal_validated_binding_and_is_safely_escaped() {
    let original = view("Mixed Case/percent%2F&<\"'\u{03B1}");
    let encoded = encode_entry(&original).unwrap();
    let text = std::str::from_utf8(&encoded).unwrap();
    assert!(text.contains("<title>Mixed Case/percent%2F&amp;&lt;&quot;&apos;\u{03B1}</title>"));
    assert_eq!(encode_entry(&original).unwrap(), encoded);
    assert_eq!(
        original.binding.target().as_str(),
        "Mixed Case/percent%2F&<\"'\u{03B1}"
    );
    let title_removed = text.replace(
        "<title>Mixed Case/percent%2F&amp;&lt;&quot;&apos;\u{03B1}</title>",
        "",
    );
    assert_eq!(
        decode_definition(title_removed.as_bytes()),
        Ok(default_definition())
    );
    assert!(decode_definition(text.as_bytes()).is_err());
}

#[test]
fn responses_expose_real_static_definition_not_runtime_usage_or_timestamps() {
    let mut original = view("q");
    original.config.lock_duration_millis = 5_000;
    original.config.default_time_to_live_millis = Some(1_005);
    original
        .config
        .duplicate_detection_history_time_window_millis = 120_000;
    original.config.max_message_bytes = KIB;
    original.config.max_delivery_count = 2;
    original.config.dead_lettering_on_message_expiration = true;
    original.capacity = QueueCapacityStatus::FiniteV1 {
        limit: FiniteQueueCapacity::new(7 * MIB).unwrap(),
        reserved_bytes: 999_999,
        message_count: 17,
    };
    let encoded = String::from_utf8(encode_entry(&original).unwrap()).unwrap();
    assert!(encoded.contains("<MaxSizeInMegabytes>7</MaxSizeInMegabytes>"));
    assert!(encoded.contains("<SupportOrdering>false</SupportOrdering>"));
    assert!(encoded.contains(
        "<DuplicateDetectionHistoryTimeWindow>PT2M</DuplicateDetectionHistoryTimeWindow>"
    ));
    for absent in [
        "SizeInBytes",
        "MessageCount",
        "999999",
        "CreatedAt",
        "UpdatedAt",
        "AccessedAt",
        "AutoDeleteOnIdle",
    ] {
        assert!(!encoded.contains(absent), "{absent}");
    }
    let input = encoded.replace("<title>q</title>", "");
    assert_eq!(
        decode_definition(input.as_bytes()),
        Ok(AtomQueueDefinition {
            config: original.config,
            limit: FiniteQueueCapacity::new(7 * MIB).unwrap(),
        })
    );
    // The SDK omits this inactive field on PUT; complete omission resets it.
    let sdk_omission = input.replace(
        "<DuplicateDetectionHistoryTimeWindow>PT2M</DuplicateDetectionHistoryTimeWindow>",
        "",
    );
    assert_eq!(
        decode_definition(sdk_omission.as_bytes())
            .unwrap()
            .config
            .duplicate_detection_history_time_window_millis,
        60_000
    );
}

#[test]
fn unrepresentable_modes_configs_and_bindings_refuse_without_rounding() {
    let original = view("q");
    let mut invalid = Vec::new();
    let mut changed = original.clone();
    changed.capacity = QueueCapacityStatus::NonFinite;
    invalid.push(changed);
    for bytes in [1, MIB + 1, (i32::MAX as u64 + 1) * MIB] {
        let mut changed = original.clone();
        changed.capacity = QueueCapacityStatus::FiniteV1 {
            limit: FiniteQueueCapacity::new(bytes).unwrap(),
            reserved_bytes: 0,
            message_count: 0,
        };
        invalid.push(changed);
    }
    for config in [
        QueueConfig {
            requires_session: true,
            ..original.config
        },
        QueueConfig {
            requires_duplicate_detection: true,
            ..original.config
        },
        QueueConfig {
            max_message_bytes: KIB + 1,
            ..original.config
        },
        QueueConfig {
            max_message_bytes: 257 * KIB,
            ..original.config
        },
        QueueConfig {
            lock_duration_millis: 4_999,
            ..original.config
        },
        QueueConfig {
            max_delivery_count: i32::MAX as u32 + 1,
            ..original.config
        },
        QueueConfig {
            default_time_to_live_millis: Some(999),
            ..original.config
        },
        QueueConfig {
            default_time_to_live_millis: Some(duration::MAX_DURATION_MILLIS + 1),
            ..original.config
        },
        QueueConfig {
            duplicate_detection_history_time_window_millis: 19_999,
            ..original.config
        },
    ] {
        let mut changed = original.clone();
        changed.config = config;
        invalid.push(changed);
    }
    for path in ["/q", "q/", "q//r", "q/./r", "q/../r", "q\\r", "q\u{FFFE}"] {
        invalid.push(view(path));
    }
    let mut shadow = original.clone();
    shadow.binding = EntityBinding::new(
        original.binding.namespace().clone(),
        EntityPath::new("q/$deadletterqueue").unwrap(),
        original.binding.owner().clone(),
        EntityIncarnationKind::Queue,
        1,
    )
    .unwrap();
    invalid.push(shadow);
    let mut topic = original.clone();
    topic.binding = EntityBinding::new(
        original.binding.namespace().clone(),
        EntityPath::new("q").unwrap(),
        original.binding.owner().clone(),
        EntityIncarnationKind::Topic,
        1,
    )
    .unwrap();
    invalid.push(topic);
    for changed in invalid {
        assert_eq!(
            validate_view(&changed),
            Err(AtomXmlError::UnsupportedDefinition)
        );
        assert_eq!(
            encode_entry(&changed),
            Err(AtomXmlError::UnsupportedDefinition)
        );
        assert_eq!(
            encode_feed(&[original.clone(), changed]),
            Err(AtomXmlError::UnsupportedDefinition)
        );
    }
}

#[test]
fn output_refuses_invalid_deserialized_binding_and_keeps_literal_control_case() {
    let mut bad = view("q");
    let payload =
        domain::codec::encode(&("test", "q", "q", EntityIncarnationKind::Queue, 0_u64)).unwrap();
    bad.binding = domain::codec::decode(&payload).unwrap();
    assert_eq!(
        validate_view(&bad),
        Err(AtomXmlError::UnsupportedDefinition)
    );
    let literal = view("orders/$Management");
    assert!(
        String::from_utf8(encode_entry(&literal).unwrap())
            .unwrap()
            .contains("<title>orders/$Management</title>")
    );
}

#[test]
fn exact_collection_collision_is_reserved_without_case_or_percent_aliases() {
    assert_eq!(
        validate_view(&view(QUEUE_COLLECTION_PATH)),
        Err(AtomXmlError::UnsupportedDefinition)
    );
    assert_eq!(
        encode_entry(&view(QUEUE_COLLECTION_PATH)),
        Err(AtomXmlError::UnsupportedDefinition)
    );
    for path in [
        "$resources/queues",
        "$Resources/Queues",
        "$Resources/%71ueues",
        "$Other/queues",
    ] {
        assert_eq!(validate_view(&view(path)), Ok(()), "{path}");
        assert!(
            String::from_utf8(encode_entry(&view(path)).unwrap())
                .unwrap()
                .contains(&format!("<title>{path}</title>"))
        );
    }
}

#[test]
fn empty_feed_is_explicit_and_feed_limit_does_not_publish_a_short_body() {
    let empty = String::from_utf8(encode_feed(&[]).unwrap()).unwrap();
    assert_eq!(empty, format!("<feed xmlns=\"{ATOM_NS}\"></feed>"));
    assert!(!empty.contains("/>"));
    let full = vec![view("q"); MAX_FEED_ENTRIES];
    let body = encode_feed(&full).unwrap();
    assert_eq!(
        std::str::from_utf8(&body)
            .unwrap()
            .matches("<entry ")
            .count(),
        MAX_FEED_ENTRIES
    );
    assert!(body.len() <= MAX_REPLY_BYTES);
    let excessive = vec![view("q"); MAX_FEED_ENTRIES + 1];
    assert_eq!(
        encode_feed(&excessive),
        Err(AtomXmlError::ReplyLimitExceeded)
    );
}

#[test]
fn error_display_debug_and_xml_are_closed_static_redacted_values() {
    for error in [
        AtomXmlError::Malformed,
        AtomXmlError::WorkLimitExceeded,
        AtomXmlError::InvalidDefinition,
        AtomXmlError::UnsupportedDefinition,
        AtomXmlError::ReplyLimitExceeded,
    ] {
        let body = String::from_utf8(encode_error(error).unwrap()).unwrap();
        assert_eq!(
            body,
            format!(
                "<Error><Code>{}</Code><Detail>{}</Detail></Error>",
                error.code(),
                error.detail()
            )
        );
        assert_eq!(error.to_string(), error.detail());
        assert!(!format!("{error:?}").contains("secret"));
        assert!(!body.contains("xmlns"));
    }
    let submitted = document("<UserMetadata>token-secret-private-path</UserMetadata>");
    let error = decode_definition(submitted.as_bytes()).unwrap_err();
    let diagnostics = format!(
        "{error} {error:?} {}",
        String::from_utf8(encode_error(error).unwrap()).unwrap()
    );
    assert!(!diagnostics.contains("token-secret-private-path"));
    assert!(!diagnostics.contains("UserMetadata"));
}

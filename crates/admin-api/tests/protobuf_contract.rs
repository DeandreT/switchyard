use admin_api::{FILE_DESCRIPTOR_SET, PROTOBUF_PACKAGE, v1};
use prost::Message;
use prost_types::{FileDescriptorProto, FileDescriptorSet};

fn descriptor() -> FileDescriptorProto {
    FileDescriptorSet::decode(FILE_DESCRIPTOR_SET)
        .expect("generated descriptor set")
        .file
        .into_iter()
        .find(|file| file.package.as_deref() == Some(PROTOBUF_PACKAGE))
        .expect("versioned native administration package")
}

#[test]
fn existing_entity_fields_keep_their_wire_numbers() {
    let descriptor = descriptor();
    for (name, expected) in [
        (
            "CreateEntityRequest",
            vec![
                ("namespace", 1),
                ("path", 2),
                ("kind", 3),
                ("placement_group_id", 4),
                ("max_size_bytes", 5),
                ("default_ttl_millis", 6),
                ("lock_duration_millis", 7),
                ("max_delivery_count", 8),
                ("requires_session", 9),
                ("queue_config", 10),
                ("topic_config", 11),
                ("subscription_config", 12),
            ],
        ),
        (
            "Entity",
            vec![
                ("namespace", 1),
                ("path", 2),
                ("kind", 3),
                ("placement_group_id", 4),
                ("max_size_bytes", 5),
                ("used_logical_bytes", 6),
                ("queue_config", 7),
                ("topic_config", 8),
                ("subscription_config", 9),
            ],
        ),
        (
            "ListEntitiesRequest",
            vec![
                ("namespace", 1),
                ("page_token", 2),
                ("page_size", 3),
                ("kind", 4),
                ("parent_topic", 5),
            ],
        ),
    ] {
        let message = descriptor
            .message_type
            .iter()
            .find(|message| message.name.as_deref() == Some(name))
            .expect("existing message");
        for (field_name, number) in expected {
            let field = message
                .field
                .iter()
                .find(|field| field.name.as_deref() == Some(field_name))
                .expect("existing field");
            assert_eq!(field.number, Some(number), "{name}.{field_name}");
        }
    }
}

#[test]
fn old_create_requests_decode_without_new_configuration() {
    let request = v1::CreateEntityRequest::decode(
        [
            0x0a, 6, b't', b'e', b'n', b'a', b'n', b't', 0x12, 6, b'o', b'r', b'd', b'e', b'r',
            b's', 0x18, 1, 0x30, 100, 0x38, 10, 0x40, 5, 0x48, 1,
        ]
        .as_slice(),
    )
    .expect("original wire tags");
    assert_eq!(request.namespace, "tenant");
    assert_eq!(request.path, "orders");
    assert_eq!(request.kind, v1::EntityKind::Queue as i32);
    assert_eq!(request.default_ttl_millis, 100);
    assert_eq!(request.lock_duration_millis, 10);
    assert_eq!(request.max_delivery_count, 5);
    assert!(request.requires_session);
    assert_eq!(request.queue_config, None);
    assert_eq!(request.topic_config, None);
    assert_eq!(request.subscription_config, None);
}

#[test]
fn old_list_requests_still_select_queues_without_a_parent() {
    let request = v1::ListEntitiesRequest::decode(
        [0x0a, 6, b't', b'e', b'n', b'a', b'n', b't', 0x18, 7].as_slice(),
    )
    .expect("original list fields");
    assert_eq!(request.namespace, "tenant");
    assert_eq!(request.page_size, 7);
    assert_eq!(request.kind, v1::EntityKind::Unspecified as i32);
    assert!(request.parent_topic.is_empty());
}

#[test]
fn topology_configuration_fields_have_additive_stable_numbers() {
    let descriptor = descriptor();
    for (name, expected) in [
        (
            "TopicConfiguration",
            vec![
                ("default_ttl_millis", 1),
                ("max_message_bytes", 2),
                ("requires_duplicate_detection", 3),
                ("duplicate_detection_history_time_window_millis", 4),
                ("default_ttl_unlimited", 5),
            ],
        ),
        (
            "SubscriptionConfiguration",
            vec![
                ("lock_duration_millis", 1),
                ("max_delivery_count", 2),
                ("default_ttl_millis", 3),
                ("max_message_bytes", 4),
                ("requires_session", 5),
                ("dead_lettering_on_message_expiration", 6),
                ("default_ttl_unlimited", 7),
                ("dead_lettering_on_filter_evaluation_exceptions", 8),
            ],
        ),
    ] {
        let message = descriptor
            .message_type
            .iter()
            .find(|message| message.name.as_deref() == Some(name))
            .expect("typed topology config");
        assert_eq!(message.field.len(), expected.len());
        for (field_name, number) in expected {
            let field = message
                .field
                .iter()
                .find(|field| field.name.as_deref() == Some(field_name))
                .expect("configuration field");
            assert_eq!(field.number, Some(number), "{name}.{field_name}");
            if !field_name.starts_with("default_ttl_") {
                assert_eq!(field.proto3_optional, Some(true), "{name}.{field_name}");
            }
        }
    }
}

#[test]
fn typed_topology_false_zero_and_all_ttl_forms_preserve_presence() {
    let mut topic_encodings = std::collections::BTreeSet::new();
    for default_time_to_live in [
        None,
        Some(v1::topic_configuration::DefaultTimeToLive::DefaultTtlMillis(0)),
        Some(v1::topic_configuration::DefaultTimeToLive::DefaultTtlMillis(60_000)),
        Some(
            v1::topic_configuration::DefaultTimeToLive::DefaultTtlUnlimited(
                v1::UnlimitedTimeToLive {},
            ),
        ),
    ] {
        let config = v1::TopicConfiguration {
            default_time_to_live,
            max_message_bytes: Some(0),
            requires_duplicate_detection: Some(false),
            duplicate_detection_history_time_window_millis: Some(0),
        };
        let bytes = config.encode_to_vec();
        assert!(topic_encodings.insert(bytes.clone()));
        assert_eq!(
            v1::TopicConfiguration::decode(bytes.as_slice()).expect("topic config"),
            config
        );
        assert_ne!(config, v1::TopicConfiguration::default());
    }
    let mut subscription_encodings = std::collections::BTreeSet::new();
    for default_time_to_live in [
        None,
        Some(v1::subscription_configuration::DefaultTimeToLive::DefaultTtlMillis(0)),
        Some(v1::subscription_configuration::DefaultTimeToLive::DefaultTtlMillis(60_000)),
        Some(
            v1::subscription_configuration::DefaultTimeToLive::DefaultTtlUnlimited(
                v1::UnlimitedTimeToLive {},
            ),
        ),
    ] {
        let config = v1::SubscriptionConfiguration {
            default_time_to_live,
            lock_duration_millis: Some(0),
            max_delivery_count: Some(0),
            max_message_bytes: Some(0),
            requires_session: Some(false),
            dead_lettering_on_message_expiration: Some(false),
            dead_lettering_on_filter_evaluation_exceptions: Some(false),
        };
        let bytes = config.encode_to_vec();
        assert!(subscription_encodings.insert(bytes.clone()));
        assert_eq!(
            v1::SubscriptionConfiguration::decode(bytes.as_slice()).expect("subscription config"),
            config
        );
        assert_ne!(config, v1::SubscriptionConfiguration::default());
    }
    assert!(v1::TopicConfiguration::default().encode_to_vec().is_empty());
    assert!(
        v1::SubscriptionConfiguration::default()
            .encode_to_vec()
            .is_empty()
    );
}

#[test]
fn filter_evaluation_dead_letter_flag_preserves_all_presence_states() {
    let mut encodings = std::collections::BTreeSet::new();
    for flag in [None, Some(false), Some(true)] {
        let config = v1::SubscriptionConfiguration {
            dead_lettering_on_filter_evaluation_exceptions: flag,
            ..Default::default()
        };
        let encoded = config.encode_to_vec();
        assert!(encodings.insert(encoded.clone()));
        assert_eq!(
            v1::SubscriptionConfiguration::decode(encoded.as_slice()).expect("config"),
            config
        );
    }
    assert_eq!(
        v1::SubscriptionConfiguration::decode([0x40, 0].as_slice())
            .expect("tag8 false")
            .dead_lettering_on_filter_evaluation_exceptions,
        Some(false)
    );
    assert_eq!(
        v1::SubscriptionConfiguration::decode([0x40, 1].as_slice())
            .expect("tag8 true")
            .dead_lettering_on_filter_evaluation_exceptions,
        Some(true)
    );
}

#[test]
fn older_entity_decoder_ignores_new_typed_configurations() {
    #[derive(Clone, PartialEq, prost::Message)]
    struct OldEntity {
        #[prost(string, tag = "1")]
        namespace: String,
        #[prost(string, tag = "2")]
        path: String,
        #[prost(int32, tag = "3")]
        kind: i32,
        #[prost(message, optional, tag = "7")]
        queue_config: Option<v1::QueueConfiguration>,
    }
    for (kind, topic_config, subscription_config) in [
        (
            v1::EntityKind::Topic,
            Some(v1::TopicConfiguration::default()),
            None,
        ),
        (
            v1::EntityKind::Subscription,
            None,
            Some(v1::SubscriptionConfiguration::default()),
        ),
    ] {
        let entity = v1::Entity {
            namespace: "tenant".into(),
            path: "events".into(),
            kind: kind as i32,
            topic_config,
            subscription_config,
            ..v1::Entity::default()
        };
        let old = OldEntity::decode(entity.encode_to_vec().as_slice())
            .expect("old decoder ignores unknown fields");
        assert_eq!(old.namespace, entity.namespace);
        assert_eq!(old.path, entity.path);
        assert_eq!(old.kind, entity.kind);
        assert!(old.queue_config.is_none());
    }
}

#[test]
fn entity_accounting_preserves_old_values_without_fabricating_absent_measurements() {
    let absent = v1::Entity::default();
    assert_eq!(absent.max_size_bytes, None);
    assert_eq!(absent.used_logical_bytes, None);
    let legacy = v1::Entity::decode([0x28, 0x80, 0x08, 0x30, 0].as_slice())
        .expect("the original integer field tags");
    assert_eq!(legacy.max_size_bytes, Some(1_024));
    assert_eq!(legacy.used_logical_bytes, Some(0));
    assert_ne!(legacy.encode_to_vec(), absent.encode_to_vec());
    let entity = descriptor()
        .message_type
        .into_iter()
        .find(|message| message.name.as_deref() == Some("Entity"))
        .expect("entity descriptor");
    for name in ["max_size_bytes", "used_logical_bytes"] {
        assert_eq!(
            entity
                .field
                .iter()
                .find(|field| field.name.as_deref() == Some(name))
                .expect("accounting field")
                .proto3_optional,
            Some(true)
        );
    }
}

#[test]
fn optional_false_and_zero_survive_instead_of_becoming_omission() {
    let patch = v1::QueueConfiguration {
        lock_duration_millis: Some(0),
        max_delivery_count: Some(0),
        max_message_bytes: Some(0),
        requires_session: Some(false),
        requires_duplicate_detection: Some(false),
        duplicate_detection_history_time_window_millis: Some(0),
        dead_lettering_on_message_expiration: Some(false),
        default_time_to_live: None,
    };
    let bytes = patch.encode_to_vec();
    assert!(!bytes.is_empty());
    assert_eq!(
        v1::QueueConfiguration::decode(bytes.as_slice()).expect("patch"),
        patch
    );
    let absent = v1::QueueConfiguration::default();
    assert!(absent.encode_to_vec().is_empty());
    assert_ne!(patch, absent);
}

#[test]
fn ttl_has_distinct_unchanged_finite_zero_and_unlimited_forms() {
    use v1::queue_configuration::DefaultTimeToLive;

    let variants = [
        None,
        Some(DefaultTimeToLive::DefaultTtlMillis(0)),
        Some(DefaultTimeToLive::DefaultTtlMillis(60_000)),
        Some(DefaultTimeToLive::DefaultTtlUnlimited(
            v1::UnlimitedTimeToLive {},
        )),
    ];
    let mut encodings = std::collections::BTreeSet::new();
    for default_time_to_live in variants {
        let config = v1::QueueConfiguration {
            default_time_to_live,
            ..v1::QueueConfiguration::default()
        };
        let encoded = config.encode_to_vec();
        assert!(encodings.insert(encoded.clone()));
        assert_eq!(
            v1::QueueConfiguration::decode(encoded.as_slice()).expect("TTL"),
            config
        );
    }
}

#[test]
fn generated_contract_includes_update_without_removing_existing_methods() {
    let descriptor = descriptor();
    let service = descriptor
        .service
        .iter()
        .find(|service| service.name.as_deref() == Some("EntityService"))
        .expect("entity service");
    let methods: std::collections::BTreeSet<_> = service
        .method
        .iter()
        .map(|method| method.name.as_deref().expect("method name"))
        .collect();
    assert_eq!(
        methods,
        [
            "CreateEntity",
            "DeleteEntity",
            "GetEntity",
            "ListEntities",
            "UpdateEntity"
        ]
        .into()
    );
    let update = v1::UpdateEntityRequest {
        namespace: "tenant".into(),
        path: "orders".into(),
        queue_config: Some(v1::QueueConfiguration {
            max_delivery_count: Some(4),
            ..v1::QueueConfiguration::default()
        }),
    };
    assert_eq!(
        v1::UpdateEntityRequest::decode(update.encode_to_vec().as_slice()).expect("update"),
        update
    );
}

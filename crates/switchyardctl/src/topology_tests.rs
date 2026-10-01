use super::*;
use crate::Command;
use clap::Parser;

fn arguments(options: &[&str]) -> Arguments {
    let mut argv = vec!["switchyardctl"];
    argv.extend_from_slice(options);
    Arguments::try_parse_from(argv).expect("valid topology command")
}

fn topic_config(options: &[&str]) -> TopicConfiguration {
    let mut argv = vec!["topic", "create", "Orders"];
    argv.extend_from_slice(options);
    let Command::Topic {
        command: TopicCommand::Create(input),
    } = arguments(&argv).command
    else {
        panic!("topic create")
    };
    input.configuration.protobuf()
}

fn subscription_config(options: &[&str]) -> SubscriptionConfiguration {
    let mut argv = vec!["subscription", "create", "Orders", "Alpha"];
    argv.extend_from_slice(options);
    let Command::Subscription {
        command: SubscriptionCommand::Create(input),
    } = arguments(&argv).command
    else {
        panic!("subscription create")
    };
    input.configuration.protobuf()
}

fn topic_patch(options: &[&str]) -> TopicConfiguration {
    let mut argv = vec!["topic", "update", "Orders"];
    argv.extend_from_slice(options);
    let Command::Topic {
        command: TopicCommand::Update(input),
    } = arguments(&argv).command
    else {
        panic!("topic update")
    };
    input.configuration.protobuf()
}

fn subscription_patch(options: &[&str]) -> SubscriptionConfiguration {
    let mut argv = vec!["subscription", "update", "Orders", "Alpha"];
    argv.extend_from_slice(options);
    let Command::Subscription {
        command: SubscriptionCommand::Update(input),
    } = arguments(&argv).command
    else {
        panic!("subscription update")
    };
    input.configuration.protobuf()
}

#[test]
fn updates_share_presence_preserving_arguments_without_defaults() {
    assert_eq!(topic_patch(&[]), TopicConfiguration::default());
    assert_eq!(
        subscription_patch(&[]),
        SubscriptionConfiguration::default()
    );
    let options = [
        "--ttl-unlimited",
        "--requires-duplicate-detection=false",
        "--max-message-bytes",
        "0",
    ];
    assert_eq!(topic_patch(&options), topic_config(&options));
    assert_eq!(
        topic_patch(&options).requires_duplicate_detection,
        Some(false)
    );
    let options = [
        "--lock-duration-millis",
        "0",
        "--max-delivery-count",
        "0",
        "--requires-session=false",
        "--dead-letter-on-expiration=false",
        "--dead-letter-on-filter-exceptions=false",
    ];
    assert_eq!(subscription_patch(&options), subscription_config(&options));
    let patch = subscription_patch(&options);
    assert_eq!(patch.default_time_to_live, None);
    assert_eq!(patch.requires_session, Some(false));
    assert_eq!(patch.dead_lettering_on_message_expiration, Some(false));
    assert_eq!(
        patch.dead_lettering_on_filter_evaluation_exceptions,
        Some(false)
    );
    let patch = subscription_patch(&["--dead-letter-on-filter-exceptions"]);
    assert_eq!(
        patch.dead_lettering_on_filter_evaluation_exceptions,
        Some(true)
    );
    assert_eq!(patch.dead_lettering_on_message_expiration, None);
    assert_eq!(patch.requires_session, None);
    assert_eq!(patch.default_time_to_live, None);
}

#[test]
fn update_commands_preserve_topic_and_member_spelling() {
    assert!(matches!(
        arguments(&["topic", "update", "/Orders/$Management"]).command,
        Command::Topic { command: TopicCommand::Update(input) } if input.path == "/Orders/$Management"
    ));
    let Command::Subscription {
        command: SubscriptionCommand::Update(input),
    } = arguments(&[
        "subscription",
        "update",
        "/Orders/$Management",
        "Subscriptions",
    ])
    .command
    else {
        panic!("subscription update")
    };
    assert_eq!(
        subscription_path(&input.topic, &input.name).unwrap(),
        "/Orders/$Management/subscriptions/Subscriptions"
    );
}

#[test]
fn omitted_configuration_preserves_presence() {
    assert_eq!(topic_config(&[]), TopicConfiguration::default());
    assert_eq!(
        subscription_config(&[]),
        SubscriptionConfiguration::default()
    );
}

#[test]
fn independent_flags_keep_explicit_false_zero_and_unlimited() {
    let topic = topic_config(&[
        "--default-ttl-millis",
        "0",
        "--requires-duplicate-detection=false",
        "--duplicate-detection-window-millis",
        "60000",
        "--max-message-bytes",
        "8192",
    ]);
    assert_eq!(
        topic.default_time_to_live,
        Some(TopicTimeToLive::DefaultTtlMillis(0))
    );
    assert_eq!(topic.requires_duplicate_detection, Some(false));
    assert_eq!(
        topic.duplicate_detection_history_time_window_millis,
        Some(60_000)
    );
    assert_eq!(topic.max_message_bytes, Some(8192));
    let subscription = subscription_config(&[
        "--lock-duration-millis",
        "5000",
        "--max-delivery-count",
        "7",
        "--ttl-unlimited",
        "--requires-session=false",
        "--dead-letter-on-expiration=false",
        "--dead-letter-on-filter-exceptions=false",
    ]);
    assert_eq!(subscription.lock_duration_millis, Some(5000));
    assert_eq!(subscription.max_delivery_count, Some(7));
    assert_eq!(subscription.requires_session, Some(false));
    assert_eq!(
        subscription.dead_lettering_on_message_expiration,
        Some(false)
    );
    assert_eq!(
        subscription.dead_lettering_on_filter_evaluation_exceptions,
        Some(false)
    );
    assert_eq!(
        subscription.default_time_to_live,
        Some(SubscriptionTimeToLive::DefaultTtlUnlimited(
            UnlimitedTimeToLive {}
        ))
    );
}

#[test]
fn filter_error_dead_letter_option_preserves_omitted_false_and_true() {
    for (options, expected) in [
        (vec![], None),
        (
            vec!["--dead-lettering-on-filter-evaluation-exceptions=false"],
            Some(false),
        ),
        (vec!["--dead-letter-on-filter-exceptions"], Some(true)),
        (vec!["--dead-letter-on-filter-exceptions=true"], Some(true)),
    ] {
        let config = subscription_config(&options);
        assert_eq!(
            config.dead_lettering_on_filter_evaluation_exceptions,
            expected
        );
        assert_eq!(config.dead_lettering_on_message_expiration, None);
        let output =
            serde_json::to_value(SubscriptionConfigurationOutput::from(config)).expect("JSON");
        assert_eq!(
            output["dead_lettering_on_filter_evaluation_exceptions"],
            serde_json::to_value(expected).expect("flag")
        );
    }
}

#[test]
fn wrong_kind_flags_and_conflicting_ttl_are_local_parse_errors() {
    for argv in [
        vec!["topic", "update", "Orders", "--requires-session"],
        vec![
            "topic",
            "update",
            "Orders",
            "--dead-letter-on-filter-exceptions",
        ],
        vec![
            "subscription",
            "update",
            "Orders",
            "Alpha",
            "--requires-duplicate-detection",
        ],
        vec![
            "topic",
            "update",
            "Orders",
            "--ttl-unlimited",
            "--default-ttl-millis",
            "50",
        ],
        vec![
            "subscription",
            "update",
            "Orders",
            "Alpha",
            "--ttl-unlimited",
            "--default-ttl-millis",
            "50",
        ],
        vec!["topic", "create", "Orders", "--requires-session"],
        vec![
            "topic",
            "create",
            "Orders",
            "--dead-letter-on-filter-exceptions",
        ],
        vec![
            "topic",
            "create",
            "Orders",
            "--lock-duration-millis",
            "5000",
        ],
        vec![
            "subscription",
            "create",
            "Orders",
            "Alpha",
            "--requires-duplicate-detection",
        ],
        vec![
            "subscription",
            "create",
            "Orders",
            "Alpha",
            "--duplicate-detection-window-millis",
            "60000",
        ],
        vec![
            "topic",
            "create",
            "Orders",
            "--ttl-unlimited",
            "--default-ttl-millis",
            "50",
        ],
        vec![
            "subscription",
            "create",
            "Orders",
            "Alpha",
            "--ttl-unlimited",
            "--default-ttl-millis",
            "50",
        ],
    ] {
        let mut full = vec!["switchyardctl"];
        full.extend(argv);
        assert!(Arguments::try_parse_from(full).is_err());
    }
}

#[test]
fn composed_paths_preserve_user_spelling_and_reserve_shadow_space() {
    assert_eq!(
        subscription_path("a/Subscriptions", "Subscriptions").unwrap(),
        "a/Subscriptions/subscriptions/Subscriptions"
    );
    assert_eq!(
        subscription_path("/a/$Management", "Alpha").unwrap(),
        "/a/$Management/subscriptions/Alpha"
    );
    for name in [
        "",
        "-alpha",
        "alpha_",
        "a/b",
        "a:b",
        "a b",
        "a\u{e9}b",
        &"a".repeat(51),
    ] {
        assert!(subscription_path("Orders", name).is_err(), "{name}");
    }
    let suffix_bytes = "/subscriptions/".len() + "Alpha".len() + "/$deadletterqueue".len();
    assert!(subscription_path(&"a".repeat(260 - suffix_bytes), "Alpha").is_ok());
    assert!(subscription_path(&"a".repeat(261 - suffix_bytes), "Alpha").is_err());
    assert!(validate_primary_path(&"a".repeat(260)).is_ok());
    for path in ["", "a/$DeadLetterQueue", "a/SUBSCRIPTIONS/Alpha", "a\n"] {
        assert!(validate_primary_path(path).is_err(), "{path}");
    }
}

#[test]
fn listing_defaults_and_parent_are_explicit() {
    assert!(
        matches!(arguments(&["topic", "list"]).command, Command::Topic { command: TopicCommand::List { page_size: 100, page_token } } if page_token.is_empty())
    );
    assert!(
        matches!(arguments(&["subscription", "list", "Orders", "--page-size", "1", "--page-token", "subscription.v1.cursor"]).command, Command::Subscription { command: SubscriptionCommand::List { topic, page_size: 1, page_token } } if topic == "Orders" && page_token == "subscription.v1.cursor")
    );
    for (size, token) in [
        (1025, "".to_owned()),
        (1, "x".repeat(513)),
        (1, "x\n".to_owned()),
        (1, "\u{e9}".to_owned()),
    ] {
        assert!(validate_page(size, &token).is_err());
    }
    assert!(validate_page(0, "").is_ok());
    assert!(validate_page(1024, &"x".repeat(512)).is_ok());
}

#[test]
fn typed_json_preserves_presence_without_changing_queue_fields() {
    let topic = EntityOutput::from(Entity {
        kind: EntityKind::Topic as i32,
        topic_config: Some(topic_config(&[
            "--default-ttl-millis",
            "0",
            "--requires-duplicate-detection=false",
        ])),
        ..Entity::default()
    });
    let json = serde_json::to_value(topic).unwrap();
    assert_eq!(json["kind"], "topic");
    assert_eq!(json["topic_config"]["default_time_to_live_millis"], 0);
    assert_eq!(json["topic_config"]["requires_duplicate_detection"], false);
    assert!(json.get("subscription_config").is_none());
    let subscription = EntityOutput::from(Entity {
        kind: EntityKind::Subscription as i32,
        subscription_config: Some(subscription_config(&[
            "--ttl-unlimited",
            "--requires-session=false",
        ])),
        ..Entity::default()
    });
    let json = serde_json::to_value(subscription).unwrap();
    assert_eq!(json["kind"], "subscription");
    assert!(json["subscription_config"]["default_time_to_live_millis"].is_null());
    assert_eq!(json["subscription_config"]["requires_session"], false);
    assert!(json.get("topic_config").is_none());
    let queue = serde_json::to_value(EntityOutput::from(Entity {
        kind: EntityKind::Queue as i32,
        ..Entity::default()
    }))
    .unwrap();
    assert!(queue.get("topic_config").is_none());
    assert!(queue.get("subscription_config").is_none());
    assert!(queue.get("queue_config").unwrap().is_null());
}

#[test]
fn wrong_kind_responses_do_not_print_a_misleading_entity() {
    assert!(
        write_entity(
            Entity {
                kind: EntityKind::Queue as i32,
                ..Entity::default()
            },
            EntityKind::Topic
        )
        .is_err()
    );
    assert!(
        write_list(
            ListEntitiesResponse {
                entities: vec![Entity {
                    kind: EntityKind::Topic as i32,
                    ..Entity::default()
                }],
                next_page_token: String::new()
            },
            EntityKind::Subscription
        )
        .is_err()
    );
}

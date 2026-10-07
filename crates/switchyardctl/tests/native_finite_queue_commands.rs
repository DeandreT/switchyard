//! The built CLI uses the separate finite-queue service over private-CA TLS.
#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]

#[path = "native_finite_queue_commands/fixture.rs"]
mod fixture;
#[path = "native_offline_jwt/process.rs"]
mod process;
#[path = "native_offline_jwt/fixtures.rs"]
mod tokens;

use domain::{
    Command as DomainCommand, CommandKind, CommandOutcome, DeleteEntityTarget, EntityPath,
    FiniteQueueCapacity, QueueCapacityCommandV1, QueueConfig, StateMachine, Timestamp, keys,
};
use serde_json::{Value as Json, json};
use storage::{MemoryStore, StateStore, StoreSnapshot};
use tonic::Code;

use fixture::{TestResult, assert_code, assert_failure, namespace, run};

fn config() -> QueueConfig {
    QueueConfig {
        lock_duration_millis: 30_000,
        max_delivery_count: 5,
        default_time_to_live_millis: Some(60_000),
        max_message_bytes: 16_384,
        requires_session: false,
        requires_duplicate_detection: false,
        duplicate_detection_history_time_window_millis: 60_000,
        dead_lettering_on_message_expiration: true,
    }
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn definition(
    operation: &str,
    path: &str,
    generation: Option<u64>,
    config: &QueueConfig,
    limit: u64,
) -> Vec<String> {
    let mut arguments = strings(&["finite-queue", operation, path]);
    for (flag, value) in [
        (
            "--lock-duration-millis",
            config.lock_duration_millis.to_string(),
        ),
        (
            "--max-delivery-count",
            config.max_delivery_count.to_string(),
        ),
        ("--max-message-bytes", config.max_message_bytes.to_string()),
        ("--requires-session", config.requires_session.to_string()),
        (
            "--requires-duplicate-detection",
            config.requires_duplicate_detection.to_string(),
        ),
        (
            "--duplicate-detection-history-time-window-millis",
            config
                .duplicate_detection_history_time_window_millis
                .to_string(),
        ),
        (
            "--dead-lettering-on-message-expiration",
            config.dead_lettering_on_message_expiration.to_string(),
        ),
        ("--reservation-limit-bytes", limit.to_string()),
    ] {
        arguments.extend([flag.to_owned(), value]);
    }
    match config.default_time_to_live_millis {
        Some(millis) => arguments.extend(["--default-ttl-millis".into(), millis.to_string()]),
        None => arguments.push("--ttl-unlimited".into()),
    }
    if let Some(generation) = generation {
        arguments.extend(["--expected-generation".into(), generation.to_string()]);
    }
    arguments
}

fn get(path: &str) -> Vec<String> {
    strings(&["finite-queue", "get", path])
}

fn without(arguments: &[String], flag: &str) -> Vec<String> {
    let mut arguments = arguments.to_vec();
    let index = arguments
        .iter()
        .position(|value| value == flag)
        .expect("original full flag");
    arguments.remove(index);
    if arguments
        .get(index)
        .is_some_and(|value| !value.starts_with("--"))
    {
        arguments.remove(index);
    }
    arguments
}

fn configuration_json(config: &QueueConfig) -> Json {
    json!({
        "lock_duration_millis": config.lock_duration_millis,
        "max_delivery_count": config.max_delivery_count,
        "default_time_to_live_millis": config.default_time_to_live_millis,
        "max_message_bytes": config.max_message_bytes,
        "requires_session": config.requires_session,
        "requires_duplicate_detection": config.requires_duplicate_detection,
        "duplicate_detection_history_time_window_millis": config.duplicate_detection_history_time_window_millis,
        "dead_lettering_on_message_expiration": config.dead_lettering_on_message_expiration,
    })
}

fn assert_view(
    value: &Json,
    path: &str,
    generation: u64,
    config: &QueueConfig,
    limit: u64,
    usage: u64,
    count: u64,
) {
    assert_eq!(
        value,
        &json!({
            "namespace": "tenant", "path": path, "generation": generation,
            "config": configuration_json(config), "reservation_limit_bytes": limit,
            "reserved_logical_bytes": usage, "retained_message_count": count,
        })
    );
    for (field, expected) in [
        ("generation", generation),
        ("reservation_limit_bytes", limit),
        ("reserved_logical_bytes", usage),
        ("retained_message_count", count),
    ] {
        assert_eq!(value[field].as_u64(), Some(expected));
    }
    assert!(value.get("max_size_bytes").is_none());
    assert!(value.get("used_logical_bytes").is_none());
}

fn unchanged_except(before: &StoreSnapshot, after: &StoreSnapshot, changed: &[Vec<u8>]) {
    let retain = |snapshot: &StoreSnapshot| {
        snapshot
            .entries()
            .iter()
            .filter(|(key, _)| !changed.contains(key))
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(retain(before), retain(after));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_finite_create_get_full_definition_and_unsigned_json() -> TestResult {
    run(false, |node| {
        Box::pin(async move {
            let initial = config();
            let before = node.checkpoint()?;
            let created = node
                .json(
                    "administrator",
                    &definition("create", "orders", None, &initial, u64::MAX),
                )
                .await?;
            assert_view(&created, "orders", 1, &initial, u64::MAX, 0, 0);
            let after = node.checkpoint()?;
            assert_eq!(after.commits, before.commits + 1);
            assert_eq!(after.clocks, before.clocks + 1);
            node.clock.manual.set(0);
            let before = node.checkpoint()?;
            assert_eq!(node.json("administrator", &get("orders")).await?, created);
            node.unchanged(&before, 0)?;
            node.clock.manual.set(2_000);
            let desired = QueueConfig {
                lock_duration_millis: 40_000,
                max_delivery_count: 6,
                default_time_to_live_millis: None,
                max_message_bytes: 8_192,
                requires_session: false,
                requires_duplicate_detection: false,
                duplicate_detection_history_time_window_millis: 120_000,
                dead_lettering_on_message_expiration: false,
            };
            let updated = node
                .json(
                    "administrator",
                    &definition("set-definition", "orders", Some(1), &desired, 1_048_576),
                )
                .await?;
            assert_view(&updated, "orders", 1, &desired, 1_048_576, 0, 0);
            assert_eq!(node.json("administrator", &get("orders")).await?, updated);
            let before = node.checkpoint()?;
            assert_eq!(
                node.json(
                    "administrator",
                    &definition("set-definition", "orders", Some(1), &desired, 1_048_576)
                )
                .await?,
                updated
            );
            node.unchanged(&before, 1)?;
            let desired = QueueConfig {
                lock_duration_millis: 50_000,
                ..desired
            };
            let again = node
                .json(
                    "administrator",
                    &definition("set-definition", "orders", Some(1), &desired, 1_048_576),
                )
                .await?;
            assert_view(&again, "orders", 1, &desired, 1_048_576, 0, 0);
            let compatibility = node
                .json("administrator", &strings(&["compatibility"]))
                .await?;
            assert_eq!(
                compatibility["finite_queue_operations"],
                json!(["create", "get", "set-definition"])
            );
            assert_eq!(
                compatibility["queue_operations"],
                json!(["create", "get", "list", "update", "delete"])
            );
            assert_eq!(
                compatibility["topic_operations"],
                compatibility["queue_operations"]
            );
            assert_eq!(
                compatibility["subscription_operations"],
                compatibility["queue_operations"]
            );
            assert_eq!(
                compatibility["rule_operations"],
                json!(["create", "get", "list", "delete"])
            );
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_full_shape_refuses_before_credentials_or_owner() -> TestResult {
    run(false, |node| {
        Box::pin(async move {
            let full = config();
            let fields = [
                "--lock-duration-millis",
                "--max-delivery-count",
                "--default-ttl-millis",
                "--max-message-bytes",
                "--requires-session",
                "--requires-duplicate-detection",
                "--duplicate-detection-history-time-window-millis",
                "--dead-lettering-on-message-expiration",
            ];
            let before = node.checkpoint()?;
            for operation in ["create", "set-definition"] {
                let arguments = definition(
                    operation,
                    "orders",
                    (operation == "set-definition").then_some(1),
                    &full,
                    1_048_576,
                );
                for field in fields {
                    let output = node
                        .run("missing-token-file", &without(&arguments, field))
                        .await?;
                    assert_failure(
                        output,
                        "finite queues require every configuration field and an explicit TTL",
                    );
                    node.untouched(&before)?;
                }
                for limit in [None, Some(0)] {
                    let mut arguments = without(&arguments, "--reservation-limit-bytes");
                    if let Some(limit) = limit {
                        arguments.extend(["--reservation-limit-bytes".into(), limit.to_string()]);
                    }
                    assert_failure(
                        node.run("missing-token-file", &arguments).await?,
                        "finite queues require positive --reservation-limit-bytes",
                    );
                    node.untouched(&before)?;
                }
            }
            for generation in [None, Some(0)] {
                let arguments =
                    definition("set-definition", "orders", generation, &full, 1_048_576);
                assert_failure(
                    node.run("missing-token-file", &arguments).await?,
                    "finite definition requires positive --expected-generation",
                );
                node.untouched(&before)?;
            }
            let zero = QueueConfig {
                lock_duration_millis: 0,
                ..full
            };
            assert_code(
                node.run(
                    "administrator",
                    &definition("create", "zero-numeric", None, &zero, 1_048_576),
                )
                .await?,
                Code::InvalidArgument,
            );
            node.unchanged(&before, 1)?;
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_retained_seed_below_usage_refusal_and_record_preservation() -> TestResult {
    run(false, |node| {
        Box::pin(async move {
            let initial = config();
            let limit = 1_048_576;
            let created = node
                .json(
                    "administrator",
                    &definition("create", "orders", None, &initial, limit),
                )
                .await?;
            let send = CommandKind::Send {
                message_id: "retained".into(),
                body: vec![5; 4_096],
                time_to_live_millis: Some(5_000),
                session_id: None,
            };
            let sequence = match node.submit("orders", send.clone()).await? {
                CommandOutcome::Sent { sequence } => sequence,
                _ => return Err("original retained owner seed did not send".into()),
            };
            let canonical = StateMachine::new(MemoryStore::default());
            let path = EntityPath::new("orders")?;
            canonical.apply_queue_capacity(&QueueCapacityCommandV1::CreateFinite {
                namespace: namespace()?,
                entity: path.clone(),
                issued_at: Timestamp::from_millis(1_000),
                config: initial,
                limit: FiniteQueueCapacity::new(limit)?,
            })?;
            canonical.apply(&DomainCommand::new(
                namespace()?,
                path.clone(),
                Timestamp::from_millis(1_000),
                send,
            ))?;
            let seeded = node.checkpoint()?;
            assert_eq!(
                seeded.image,
                canonical.store().snapshot()?,
                "the exact separately replayed deterministic seed image"
            );
            let current = node.json("administrator", &get("orders")).await?;
            let reserved = current["reserved_logical_bytes"]
                .as_u64()
                .expect("exact owner-observed reservation");
            assert!(reserved > 1);
            assert_view(
                &current,
                "orders",
                created["generation"].as_u64().unwrap(),
                &initial,
                limit,
                reserved,
                1,
            );
            let record_key = keys::message(&namespace()?, &path, sequence);
            let record = node
                .store
                .inner
                .get(&record_key)?
                .ok_or("original retained record absent")?;
            let desired = QueueConfig {
                lock_duration_millis: 90_000,
                default_time_to_live_millis: None,
                max_message_bytes: 1_024,
                dead_lettering_on_message_expiration: false,
                ..initial
            };
            let before = node.checkpoint()?;
            assert_code(
                node.run(
                    "administrator",
                    &definition("set-definition", "orders", Some(1), &desired, reserved - 1),
                )
                .await?,
                Code::ResourceExhausted,
            );
            node.unchanged(&before, 1)?;
            node.clock.manual.set(2_000);
            let updated = node
                .json(
                    "administrator",
                    &definition("set-definition", "orders", Some(1), &desired, reserved),
                )
                .await?;
            assert_view(&updated, "orders", 1, &desired, reserved, reserved, 1);
            let after = node.checkpoint()?;
            assert_eq!(after.commits, before.commits + 1);
            assert_eq!(after.attempts, before.attempts + 1);
            assert_eq!(after.clocks, before.clocks + 2);
            assert_eq!(node.store.inner.get(&record_key)?, Some(record));
            unchanged_except(
                &before.image,
                &after.image,
                &[
                    keys::clock(),
                    keys::queue_config(&namespace()?, &path),
                    keys::queue_config(&namespace()?, &path.dead_letter_queue()?),
                    keys::queue_capacity_mode(&namespace()?, &path),
                ],
            );
            assert_eq!(node.json("administrator", &get("orders")).await?, updated);
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_stale_incarnation_precedes_backward_clock() -> TestResult {
    run(false, |node| {
        Box::pin(async move {
            let full = config();
            let first = node
                .json(
                    "administrator",
                    &definition("create", "orders", None, &full, 1_048_576),
                )
                .await?;
            assert_eq!(
                node.submit(
                    "orders",
                    CommandKind::DeleteEntity {
                        target: DeleteEntityTarget::Queue
                    }
                )
                .await?,
                CommandOutcome::QueueDeleted
            );
            let second = node
                .json(
                    "administrator",
                    &definition("create", "orders", None, &full, 1_048_576),
                )
                .await?;
            let old = first["generation"].as_u64().unwrap();
            let current = second["generation"].as_u64().unwrap();
            assert_eq!(current, old + 1);
            node.clock.manual.set(0);
            let before = node.checkpoint()?;
            let invalid = QueueConfig {
                requires_session: true,
                max_message_bytes: 0,
                ..full
            };
            assert_code(
                node.run(
                    "administrator",
                    &definition("set-definition", "orders", Some(old), &invalid, 1_048_576),
                )
                .await?,
                Code::NotFound,
            );
            node.unchanged(&before, 0)?;
            assert_code(
                node.run(
                    "administrator",
                    &definition("set-definition", "orders", Some(current), &full, 1_048_576),
                )
                .await?,
                Code::Unavailable,
            );
            node.unchanged(&before, 1)?;
            let before = node.checkpoint()?;
            assert_eq!(node.json("administrator", &get("orders")).await?, second);
            node.unchanged(&before, 0)?;
            node.clock.manual.set(2_000);
            let desired = QueueConfig {
                lock_duration_millis: 40_000,
                ..full
            };
            assert_view(
                &node
                    .json(
                        "administrator",
                        &definition(
                            "set-definition",
                            "orders",
                            Some(current),
                            &desired,
                            1_048_576,
                        ),
                    )
                    .await?,
                "orders",
                current,
                &desired,
                1_048_576,
                0,
                0,
            );
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_private_ca_name_jwt_sas_denials_and_healthy_controls() -> TestResult {
    run(false, |node| {
        Box::pin(async move {
            let full = config();
            let created = node
                .json(
                    "administrator",
                    &definition("create", "orders", None, &full, 1_048_576),
                )
                .await?;
            for (ca, name) in [
                ("wrong-ca.pem", "localhost"),
                ("ca.pem", "wrong-name.example"),
            ] {
                let before = node.checkpoint()?;
                assert_failure(
                    node.run_tls("administrator", &get("orders"), ca, name)
                        .await?,
                    "could not establish the administration connection",
                );
                node.untouched(&before)?;
                assert_eq!(node.json("administrator", &get("orders")).await?, created);
            }
            let before = node.checkpoint()?;
            for arguments in [
                definition("create", "forbidden", None, &full, 1_048_576),
                get("orders"),
                definition("set-definition", "orders", Some(1), &full, 1_048_576),
            ] {
                assert_code(
                    node.run("sender", &arguments).await?,
                    Code::PermissionDenied,
                );
                node.untouched(&before)?;
                assert_code(
                    node.run("invalid", &arguments).await?,
                    Code::Unauthenticated,
                );
                node.untouched(&before)?;
            }
            assert_eq!(node.json("sas", &get("orders")).await?, created);
            assert_eq!(node.json("administrator", &get("orders")).await?, created);
            node.unchanged(&before, 0)?;
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_older_service_returns_unimplemented_without_legacy_fallback() -> TestResult {
    run(true, |node| {
        Box::pin(async move {
            let legacy = node
                .json("administrator", &strings(&["queue", "create", "legacy"]))
                .await?;
            assert_eq!(legacy["path"], "legacy");
            assert!(legacy.get("max_size_bytes").is_none());
            assert!(legacy.get("used_logical_bytes").is_none());
            let full = config();
            let before = node.checkpoint()?;
            for arguments in [
                definition("create", "new-finite", None, &full, 1_048_576),
                get("legacy"),
                definition("set-definition", "legacy", Some(1), &full, 1_048_576),
            ] {
                assert_code(
                    node.run("administrator", &arguments).await?,
                    Code::Unimplemented,
                );
                node.untouched(&before)?;
            }
            assert_eq!(
                node.json("administrator", &strings(&["queue", "get", "legacy"]))
                    .await?,
                legacy
            );
            node.unchanged(&before, 0)?;
            Ok(())
        })
    })
    .await
}

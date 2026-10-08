use std::task::Poll;

use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use sha2::Sha256;

use super::{fixture::*, *};

pub(super) async fn full_definition_responses_and_clock_free_get<P: StoreProvider>(
    provider: P,
) -> TestResult {
    run(provider, |node| {
        Box::pin(async move {
            let mut input = create("orders");
            input.config.as_mut().unwrap().lock_duration_millis = Some(30_000);
            input.config.as_mut().unwrap().default_time_to_live =
                Some(DefaultTimeToLive::DefaultTtlMillis(60_000));
            let before = node.checkpoint()?;
            let created = node.create(input.clone()).await?;
            assert_eq!(created.namespace, "tenant");
            assert_eq!(created.path, "orders");
            assert_eq!(created.generation, 1);
            assert_eq!(created.config, input.config);
            assert_eq!(
                created.reservation_limit_bytes,
                input.reservation_limit_bytes.unwrap()
            );
            assert_eq!(
                (
                    created.reserved_logical_bytes,
                    created.retained_message_count
                ),
                (0, 0)
            );
            let after = node.checkpoint()?;
            assert_eq!(after.clocks, before.clocks + 1);
            assert_eq!(after.commits, before.commits + 1);
            let before = node.checkpoint()?;
            node.clock.manual.set(0);
            assert_eq!(node.get(get("orders")).await?, created);
            node.unchanged(&before, 0)?;
            Ok(())
        })
    })
    .await
}

fn omit(config: &mut QueueConfiguration, field: usize) {
    match field {
        0 => config.lock_duration_millis = None,
        1 => config.max_delivery_count = None,
        2 => config.default_time_to_live = None,
        3 => config.max_message_bytes = None,
        4 => config.requires_session = None,
        5 => config.requires_duplicate_detection = None,
        6 => config.duplicate_detection_history_time_window_millis = None,
        7 => config.dead_lettering_on_message_expiration = None,
        _ => unreachable!("exact full configuration field"),
    }
}

pub(super) async fn strict_presence_and_positive_identity_refuse_before_owner<P: StoreProvider>(
    provider: P,
) -> TestResult {
    run(provider, |node| {
        Box::pin(async move {
            let current = node.create(create("orders")).await?;
            for field in 0..8 {
                let before = node.checkpoint()?;
                let mut input = create("missing");
                omit(input.config.as_mut().unwrap(), field);
                code(node.create(input).await, Code::InvalidArgument);
                let mut input = definition(&current);
                omit(input.config.as_mut().unwrap(), field);
                code(node.update(input).await, Code::InvalidArgument);
                node.untouched(&before)?;
            }
            for value in [None, Some(0)] {
                let before = node.checkpoint()?;
                let mut input = create("missing");
                input.reservation_limit_bytes = value;
                code(node.create(input).await, Code::InvalidArgument);
                let mut input = definition(&current);
                input.reservation_limit_bytes = value;
                code(node.update(input).await, Code::InvalidArgument);
                let mut input = definition(&current);
                input.expected_generation = value;
                code(node.update(input).await, Code::InvalidArgument);
                node.untouched(&before)?;
            }
            let before = node.checkpoint()?;
            let mut input = create("missing");
            input.config = None;
            code(node.create(input).await, Code::InvalidArgument);
            let mut input = definition(&current);
            input.config = None;
            code(node.update(input).await, Code::InvalidArgument);
            for path in ["", "orders/$deadletterqueue", "events/subscriptions/child"] {
                code(node.create(create(path)).await, Code::InvalidArgument);
                code(node.get(get(path)).await, Code::InvalidArgument);
                let mut input = definition(&current);
                input.path = path.into();
                code(node.update(input).await, Code::InvalidArgument);
            }
            if usize::try_from(u64::MAX).is_err() {
                let mut input = create("wide");
                input.config.as_mut().unwrap().max_message_bytes = Some(u64::MAX);
                code(node.create(input).await, Code::InvalidArgument);
                let mut input = definition(&current);
                input.config.as_mut().unwrap().max_message_bytes = Some(u64::MAX);
                code(node.update(input).await, Code::InvalidArgument);
            }
            node.untouched(&before)?;
            let mut input = create("full-unsigned-capacity");
            input.reservation_limit_bytes = Some(u64::MAX);
            assert_eq!(node.create(input).await?.reservation_limit_bytes, u64::MAX);
            let before = node.checkpoint()?;
            let mut input = definition(&current);
            input.expected_generation = Some(u64::MAX);
            code(node.update(input).await, Code::NotFound);
            node.unchanged(&before, 0)?;
            Ok(())
        })
    })
    .await
}

fn batch_keys(batch: &WriteBatch) -> Vec<Vec<u8>> {
    let mut keys = batch
        .mutations()
        .iter()
        .map(|mutation| match mutation {
            Mutation::Put { key, .. } => key.clone(),
            Mutation::Delete { .. } => panic!("finite definition does not delete rows"),
        })
        .collect::<Vec<_>>();
    keys.sort();
    assert!(keys.windows(2).all(|pair| pair[0] != pair[1]));
    keys
}

pub(super) async fn noop_and_exact_config_limit_batches_have_no_postcommit_read<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    run(provider, |node| {
        Box::pin(async move {
            let guarded = node.store.guard();
            let mut current = node.create(create("orders")).await?;
            drop(guarded);
            let namespace = namespace()?;
            let path = EntityPath::new("orders")?;
            let config_keys = vec![
                keys::queue_config(&namespace, &path),
                keys::queue_config(&namespace, &path.dead_letter_queue()?),
            ];
            for (turn, config_change, limit_change) in
                [(2, true, false), (3, false, true), (4, true, true)]
            {
                node.clock.manual.set(turn * 1_000);
                let before = node.checkpoint()?;
                let mut input = definition(&current);
                if config_change {
                    input.config.as_mut().unwrap().lock_duration_millis = Some(turn * 20_000);
                }
                if limit_change {
                    input.reservation_limit_bytes = Some(current.reservation_limit_bytes + 4096);
                }
                let guard = node.store.guard();
                let prepared = node.update(input.clone()).await?;
                drop(guard);
                assert_eq!(prepared.config, input.config);
                assert_eq!(
                    prepared.reservation_limit_bytes,
                    input.reservation_limit_bytes.unwrap()
                );
                assert_eq!(prepared.generation, current.generation);
                let after = node.checkpoint()?;
                assert_eq!(after.attempts, before.attempts + 1);
                assert_eq!(after.commits, before.commits + 1);
                assert_eq!(after.clocks, before.clocks + 1);
                node.metadata_probes(&before, "orders", 4)?;
                let mut changed = vec![keys::clock()];
                if config_change {
                    changed.extend(config_keys.clone());
                }
                if limit_change {
                    changed.push(keys::queue_capacity_mode(&namespace, &path));
                }
                changed.sort();
                {
                    let batches = node.store.observations.batches.lock().expect("batches");
                    assert_eq!(batches.len(), before.commits + 1);
                    assert_eq!(batch_keys(batches.last().unwrap()), changed);
                    assert_eq!(
                        batches
                            .last()
                            .unwrap()
                            .mutations()
                            .iter()
                            .find_map(|mutation| match mutation {
                                Mutation::Put { key, value } if key == &keys::clock() =>
                                    Some(value),
                                _ => None,
                            }),
                        Some(&codec::encode(&Timestamp::from_millis(turn * 1_000))?)
                    );
                }
                unchanged_except(&before.image, &after.image, &changed);
                assert_eq!(node.get(get("orders")).await?, prepared);
                current = prepared;
            }
            node.clock.manual.set(5_000);
            let before = node.checkpoint()?;
            assert_eq!(node.update(definition(&current)).await?, current);
            node.metadata_probes(&before, "orders", 4)?;
            node.unchanged(&before, 1)?;
            node.clock.manual.set(0);
            let before = node.checkpoint()?;
            code(node.update(definition(&current)).await, Code::Unavailable);
            // Host-clock refusal precedes the apply-side metadata checks.
            node.metadata_probes(&before, "orders", 1)?;
            node.unchanged(&before, 1)?;
            assert_eq!(node.get(get("orders")).await?, current);
            Ok(())
        })
    })
    .await
}

pub(super) async fn immutable_numeric_capacity_priority_rolls_back_both_settings<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    run(provider, |node| {
        Box::pin(async move {
            node.create(create("orders")).await?;
            node.submit(
                "orders",
                CommandKind::Send {
                    message_id: "retained".into(),
                    body: vec![7; 2048],
                    time_to_live_millis: None,
                    session_id: None,
                },
            )
            .await?;
            let current = node.get(get("orders")).await?;
            assert!(current.reserved_logical_bytes > 1);
            for fault in 0..4 {
                let before = node.checkpoint()?;
                let mut input = definition(&current);
                input.reservation_limit_bytes = Some(current.reserved_logical_bytes - 1);
                let config = input.config.as_mut().unwrap();
                config.lock_duration_millis = Some(90_000);
                let expected = match fault {
                    0 => {
                        config.requires_session = Some(true);
                        config.requires_duplicate_detection = Some(true);
                        config.max_message_bytes = Some(0);
                        Code::FailedPrecondition
                    }
                    1 => {
                        config.requires_duplicate_detection = Some(true);
                        config.max_message_bytes = Some(0);
                        Code::FailedPrecondition
                    }
                    2 => {
                        config.max_message_bytes = Some(0);
                        Code::InvalidArgument
                    }
                    _ => Code::ResourceExhausted,
                };
                let error = code(node.update(input).await, expected);
                if fault == 0 {
                    assert!(error.message().contains("requires_session"));
                }
                if fault == 1 {
                    assert!(error.message().contains("requires_duplicate_detection"));
                }
                node.unchanged(&before, 1)?;
            }
            Ok(())
        })
    })
    .await
}

pub(super) async fn explicit_incarnation_refuses_stale_before_host_clock<P: StoreProvider>(
    provider: P,
) -> TestResult {
    run(provider, |node| {
        Box::pin(async move {
            let first = node.create(create("orders")).await?;
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
            let second = node.create(create("orders")).await?;
            assert_eq!(second.generation, first.generation + 1);
            node.clock.manual.set(0);
            let before = node.checkpoint()?;
            let mut stale = definition(&first);
            stale.config.as_mut().unwrap().max_message_bytes = Some(0);
            code(node.update(stale).await, Code::NotFound);
            node.metadata_probes(&before, "orders", 0)?;
            node.unchanged(&before, 0)?;
            code(node.update(definition(&second)).await, Code::Unavailable);
            node.unchanged(&before, 1)?;
            node.clock.manual.set(2_000);
            let mut input = definition(&second);
            input.config.as_mut().unwrap().lock_duration_millis = Some(30_000);
            let third = node.update(input).await?;
            assert_eq!(third.generation, second.generation);
            let mut input = definition(&third);
            input.config.as_mut().unwrap().lock_duration_millis = Some(40_000);
            assert_eq!(
                node.update(input).await?.generation,
                third.generation,
                "generation is identity, not definition CAS"
            );
            Ok(())
        })
    })
    .await
}

pub(super) async fn future_size_and_ttl_preserve_retained_records_and_usage<P: StoreProvider>(
    provider: P,
) -> TestResult {
    run(provider, |node| {
        Box::pin(async move {
            let mut input = create("orders");
            input.config.as_mut().unwrap().default_time_to_live =
                Some(DefaultTimeToLive::DefaultTtlMillis(60_000));
            node.create(input).await?;
            let sequence = match node
                .submit(
                    "orders",
                    CommandKind::Send {
                        message_id: "retained".into(),
                        body: vec![5; 8192],
                        time_to_live_millis: Some(5_000),
                        session_id: None,
                    },
                )
                .await?
            {
                CommandOutcome::Sent { sequence } => sequence,
                _ => return Err("original retained send did not succeed".into()),
            };
            let current = node.get(get("orders")).await?;
            let namespace = namespace()?;
            let path = EntityPath::new("orders")?;
            let record_key = keys::message(&namespace, &path, sequence);
            let record = node
                .store
                .inner
                .get(&record_key)?
                .ok_or("retained record absent")?;
            let before = node.checkpoint()?;
            let mut input = definition(&current);
            input.config.as_mut().unwrap().max_message_bytes = Some(1024);
            input.config.as_mut().unwrap().default_time_to_live = Some(
                DefaultTimeToLive::DefaultTtlUnlimited(UnlimitedTimeToLive {}),
            );
            let updated = node.update(input).await?;
            assert_eq!(
                updated.reserved_logical_bytes,
                current.reserved_logical_bytes
            );
            assert_eq!(
                updated.retained_message_count,
                current.retained_message_count
            );
            assert_eq!(node.store.inner.get(&record_key)?, Some(record));
            let after = node.checkpoint()?;
            unchanged_except(
                &before.image,
                &after.image,
                &[
                    keys::clock(),
                    keys::queue_config(&namespace, &path),
                    keys::queue_config(&namespace, &path.dead_letter_queue()?),
                ],
            );
            let before = node.checkpoint()?;
            let error = node
                .submit(
                    "orders",
                    CommandKind::Send {
                        message_id: "future".into(),
                        body: vec![6; 8192],
                        time_to_live_millis: None,
                        session_id: None,
                    },
                )
                .await
                .expect_err("future size limit must refuse");
            assert!(matches!(
                error.downcast_ref::<server::SubmitError>(),
                Some(server::SubmitError::Propose(server::ProposeError::Broker(
                    domain::BrokerError::MessageTooLarge { .. }
                )))
            ));
            node.unchanged(&before, 1)?;
            Ok(())
        })
    })
    .await
}

pub(super) async fn unsupported_and_corrupt_profiles_are_never_repaired<P: StoreProvider>(
    provider: P,
) -> TestResult {
    run(provider, |node| {
        Box::pin(async move {
            let current = node.create(create("orders")).await?;
            for session in [true, false] {
                let before = node.checkpoint()?;
                let mut input = create("unsupported");
                if session {
                    input.config.as_mut().unwrap().requires_session = Some(true);
                } else {
                    input.config.as_mut().unwrap().requires_duplicate_detection = Some(true);
                }
                code(node.create(input).await, Code::FailedPrecondition);
                node.unchanged(&before, 1)?;
            }
            node.submit(
                "nonfinite",
                CommandKind::CreateQueue {
                    config: QueueConfig::default(),
                },
            )
            .await?;
            let before = node.checkpoint()?;
            code(node.get(get("nonfinite")).await, Code::FailedPrecondition);
            node.unchanged(&before, 0)?;
            let mut input = definition(&current);
            input.path = "nonfinite".into();
            code(node.update(input).await, Code::FailedPrecondition);
            node.unchanged(&before, 1)?;
            node.submit(
                "topic",
                CommandKind::CreateTopic {
                    config: TopicConfig::default(),
                },
            )
            .await?;
            let before = node.checkpoint()?;
            node.clock.manual.set(0);
            code(node.get(get("topic")).await, Code::InvalidArgument);
            let mut input = definition(&current);
            input.path = "topic".into();
            code(node.update(input).await, Code::NotFound);
            node.unchanged(&before, 0)?;
            let namespace = namespace()?;
            let path = EntityPath::new("orders")?;
            for key in [
                keys::queue_capacity_mode(&namespace, &path),
                keys::queue_capacity_usage(&namespace, &path),
                keys::queue_config(&namespace, &path.dead_letter_queue()?),
                keys::entity_incarnation(&namespace, &path),
            ] {
                let original = node
                    .store
                    .inner
                    .get(&key)?
                    .ok_or("original owner profile row absent")?;
                node.store
                    .inner
                    .apply(WriteBatch::default().put(key.clone(), vec![0]))?;
                let before = node.checkpoint()?;
                code(node.get(get("orders")).await, Code::Internal);
                let mut input = definition(&current);
                input.config.as_mut().unwrap().max_message_bytes = Some(0);
                code(node.update(input).await, Code::Internal);
                node.unchanged(&before, 0)?;
                node.store
                    .inner
                    .apply(WriteBatch::default().put(key, original))?;
            }
            let shadow_key = keys::queue_config(&namespace, &path.dead_letter_queue()?);
            let original = node
                .store
                .inner
                .get(&shadow_key)?
                .ok_or("original shadow absent")?;
            node.store
                .inner
                .apply(WriteBatch::default().delete(shadow_key.clone()))?;
            let before = node.checkpoint()?;
            code(node.get(get("orders")).await, Code::Internal);
            code(node.update(definition(&current)).await, Code::Internal);
            node.unchanged(&before, 0)?;
            node.store
                .inner
                .apply(WriteBatch::default().put(shadow_key, original))?;
            let orphan = EntityPath::new("orphan")?;
            let orphan_mode = keys::queue_capacity_mode(&namespace, &orphan);
            let mode = node
                .store
                .inner
                .get(&keys::queue_capacity_mode(&namespace, &path))?
                .unwrap();
            node.store
                .inner
                .apply(WriteBatch::default().put(orphan_mode.clone(), mode))?;
            let before = node.checkpoint()?;
            code(node.get(get("orphan")).await, Code::Internal);
            let mut input = definition(&current);
            input.path = "orphan".into();
            code(node.update(input).await, Code::NotFound);
            node.unchanged(&before, 0)?;
            node.store
                .inner
                .apply(WriteBatch::default().delete(orphan_mode))?;
            Ok(())
        })
    })
    .await
}

pub(super) async fn preapply_failure_reopen_and_retry_are_atomic<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    let mut saved = None;
    let observed = observe(async {
        let current = node.create(create("orders")).await?;
        let before = node.checkpoint()?;
        let mut input = definition(&current);
        input.config.as_mut().unwrap().lock_duration_millis = Some(90_000);
        input.reservation_limit_bytes = Some(current.reservation_limit_bytes + 4096);
        node.clock.manual.set(2_000);
        node.store
            .observations
            .fail_next
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let error = code(node.update(input.clone()).await, Code::Internal);
        assert!(!error.message().contains("private-finite-store-detail"));
        let after = node.checkpoint()?;
        assert_eq!(after.image, before.image);
        assert_eq!(after.attempts, before.attempts + 1);
        assert_eq!(after.commits, before.commits);
        assert_eq!(after.clocks, before.clocks + 1);
        saved = Some((before.image, current, input));
        Ok(())
    })
    .await;
    let node = node.reopen(observed)?;
    let observed = observe(async {
        let (image, current, input) = saved.ok_or("preapply boundary was not captured")?;
        assert_eq!(node.checkpoint()?.image, image);
        assert_eq!(node.get(get("orders")).await?, current);
        node.clock.manual.set(3_000);
        let result = node.update(input.clone()).await?;
        assert_eq!(result.config, input.config);
        assert_eq!(
            result.reservation_limit_bytes,
            input.reservation_limit_bytes.unwrap()
        );
        assert_eq!(node.get(get("orders")).await?, result);
        Ok(())
    })
    .await;
    node.finish(observed)
}

const HOST: &str = "tenant.servicebus.windows.net";
const KEY: &str = "public-finite-native-test-key";
const EXPIRY: u64 = 4_102_444_800;

fn policy() -> TestResult<SharedAccessPolicy> {
    Ok(SharedAccessPolicy::new([
        SharedAccessRule::new(
            "manage",
            ResourceScope::namespace(HOST)?,
            SharedAccessKey::new(KEY)?,
            None,
            PermissionSet::MANAGE,
        )?,
        SharedAccessRule::new(
            "send",
            ResourceScope::namespace(HOST)?,
            SharedAccessKey::new(KEY)?,
            None,
            PermissionSet::SEND,
        )?,
        SharedAccessRule::new(
            "listen",
            ResourceScope::namespace(HOST)?,
            SharedAccessKey::new(KEY)?,
            None,
            PermissionSet::LISTEN,
        )?,
    ])?)
}

fn token(resource: &str, rule: &str, expires: u64) -> TestResult<String> {
    let resource = url::form_urlencoded::byte_serialize(resource.as_bytes()).collect::<String>();
    let mut mac = Hmac::<Sha256>::new_from_slice(KEY.as_bytes())?;
    mac.update(format!("{resource}\n{expires}").as_bytes());
    let signature = url::form_urlencoded::byte_serialize(
        STANDARD.encode(mac.finalize().into_bytes()).as_bytes(),
    )
    .collect::<String>();
    Ok(format!(
        "SharedAccessSignature sr={resource}&sig={signature}&se={expires}&skn={rule}"
    ))
}

fn authorized<T>(body: T, token: &str) -> TestResult<Request<T>> {
    let mut request = Request::new(body);
    let mut value = token.parse::<tonic::metadata::MetadataValue<_>>()?;
    value.set_sensitive(true);
    request.metadata_mut().insert("authorization", value);
    Ok(request)
}

pub(super) async fn authorization_precedes_conversion_and_owner_work<P: StoreProvider>(
    provider: P,
) -> TestResult {
    run(provider, |node| {
        Box::pin(async move {
            let current = node.create(create("orders")).await?;
            let service = node
                .service
                .clone()
                .with_shared_access_policy(policy()?, HOST)?;
            for (token, expected) in [
                ("invalid".into(), Code::Unauthenticated),
                (
                    token(&format!("amqps://{HOST}"), "manage", 1)?,
                    Code::Unauthenticated,
                ),
                (
                    token(&format!("amqps://{HOST}"), "send", EXPIRY)?,
                    Code::PermissionDenied,
                ),
                (
                    token(&format!("amqps://{HOST}"), "listen", EXPIRY)?,
                    Code::PermissionDenied,
                ),
                (
                    token(&format!("amqps://{HOST}/other"), "manage", EXPIRY)?,
                    Code::PermissionDenied,
                ),
            ] {
                let before = node.checkpoint()?;
                let mut input = create("orders");
                input.config = None;
                code(
                    tokio::time::timeout(
                        DEADLINE,
                        service.create_finite_queue(authorized(input, &token)?),
                    )
                    .await?,
                    expected,
                );
                let mut input = definition(&current);
                input.config = None;
                code(
                    tokio::time::timeout(
                        DEADLINE,
                        service.set_finite_queue_definition(authorized(input, &token)?),
                    )
                    .await?,
                    expected,
                );
                code(
                    tokio::time::timeout(
                        DEADLINE,
                        service.get_finite_queue(authorized(get("orders"), &token)?),
                    )
                    .await?,
                    expected,
                );
                node.untouched(&before)?;
            }
            let namespace_token = token(&format!("amqps://{HOST}"), "manage", EXPIRY)?;
            let entity_token = token(&format!("amqps://{HOST}/orders"), "manage", EXPIRY)?;
            assert_eq!(
                tokio::time::timeout(
                    DEADLINE,
                    service.get_finite_queue(authorized(get("orders"), &entity_token)?)
                )
                .await??
                .into_inner(),
                current
            );
            let before = node.checkpoint()?;
            let mut foreign = create("other");
            foreign.namespace = "other".into();
            code(
                tokio::time::timeout(
                    DEADLINE,
                    service.create_finite_queue(authorized(foreign, &namespace_token)?),
                )
                .await?,
                Code::PermissionDenied,
            );
            code(
                tokio::time::timeout(
                    DEADLINE,
                    service.create_finite_queue(Request::new(create("other"))),
                )
                .await?,
                Code::Unauthenticated,
            );
            node.untouched(&before)?;
            let result = tokio::time::timeout(
                DEADLINE,
                service.create_finite_queue(authorized(create("healthy"), &namespace_token)?),
            )
            .await??
            .into_inner();
            assert_eq!(result.path, "healthy");
            Ok(())
        })
    })
    .await
}

pub(super) async fn legacy_entity_service_contract_remains_unchanged<P: StoreProvider>(
    provider: P,
) -> TestResult {
    run(provider, |node| {
        Box::pin(async move {
            let created = tokio::time::timeout(
                DEADLINE,
                node.service
                    .create_entity(Request::new(legacy_create("legacy"))),
            )
            .await??
            .into_inner();
            assert_eq!(
                (created.max_size_bytes, created.used_logical_bytes),
                (None, None)
            );
            let before = node.checkpoint()?;
            let mut unsupported = legacy_create("capacity");
            unsupported.max_size_bytes = 1;
            code(
                tokio::time::timeout(
                    DEADLINE,
                    node.service.create_entity(Request::new(unsupported)),
                )
                .await?,
                Code::Unimplemented,
            );
            node.untouched(&before)?;
            code(node.get(get("legacy")).await, Code::FailedPrecondition);
            node.unchanged(&before, 0)?;
            code(node.create(create("legacy")).await, Code::AlreadyExists);
            node.unchanged(&before, 1)?;
            assert_eq!(
                tokio::time::timeout(
                    DEADLINE,
                    node.service.get_entity(Request::new(GetEntityRequest {
                        namespace: "tenant".into(),
                        path: "legacy".into()
                    }))
                )
                .await??
                .into_inner(),
                created
            );
            Ok(())
        })
    })
    .await
}

pub(super) async fn stopped_owner_is_unavailable_without_effects<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let mut node = Node::start(provider)?;
    let observed = observe(async {
        let current = node.create(create("orders")).await?;
        drop(node.broker.take());
        let before = node.checkpoint()?;
        code(node.get(get("orders")).await, Code::Unavailable);
        code(node.create(create("new")).await, Code::Unavailable);
        code(node.update(definition(&current)).await, Code::Unavailable);
        node.untouched(&before)?;
        Ok(())
    })
    .await;
    node.finish(observed)
}

pub(super) async fn legacy_and_finite_clones_share_nonblocking_admission<P: StoreProvider>(
    provider: P,
) -> TestResult {
    run(provider, |node| {
        Box::pin(async move {
            node.create(create("orders")).await?;
            let before = node.checkpoint()?;
            let gate = node.gate(keys::queue_config(
                &namespace()?,
                &EntityPath::new("orders")?,
            ));
            let mut queries: Vec<Pin<Box<dyn Future<Output = TestResult> + '_>>> = Vec::new();
            for index in 0..128 {
                let mut future: Pin<Box<dyn Future<Output = TestResult> + '_>> = if index % 2 == 0 {
                    Box::pin(async {
                        node.service
                            .get_finite_queue(Request::new(get("orders")))
                            .await?;
                        Ok(())
                    })
                } else {
                    Box::pin(async {
                        node.service
                            .get_entity(Request::new(GetEntityRequest {
                                namespace: "tenant".into(),
                                path: "orders".into(),
                            }))
                            .await?;
                        Ok(())
                    })
                };
                std::future::poll_fn(|cx| {
                    assert!(future.as_mut().poll(cx).is_pending());
                    Poll::Ready(())
                })
                .await;
                queries.push(future);
            }
            gate.entered().await?;
            code(node.get(get("orders")).await, Code::ResourceExhausted);
            code(
                tokio::time::timeout(
                    DEADLINE,
                    node.service.get_entity(Request::new(GetEntityRequest {
                        namespace: "tenant".into(),
                        path: "orders".into(),
                    })),
                )
                .await?,
                Code::ResourceExhausted,
            );
            drop(queries.pop());
            let clone = node.service.clone();
            let mut replacement = Box::pin(clone.get_finite_queue(Request::new(get("orders"))));
            std::future::poll_fn(|cx| {
                assert!(replacement.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            code(node.get(get("orders")).await, Code::ResourceExhausted);
            gate.release();
            for query in queries {
                tokio::time::timeout(DEADLINE, query).await??;
            }
            tokio::time::timeout(DEADLINE, replacement).await??;
            node.get(get("orders")).await?;
            node.unchanged(&before, 0)?;
            Ok(())
        })
    })
    .await
}

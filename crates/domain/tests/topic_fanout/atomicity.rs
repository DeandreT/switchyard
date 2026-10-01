use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use storage::{Key, Mutation, StorageError, StoreSnapshot, Value};

use super::*;

fn invalid_late_inputs_and_stricter_duplicate_destinations_leave_every_record_unchanged<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = topic(
        provider,
        TopicConfig {
            max_message_bytes: 1024,
            requires_duplicate_detection: true,
            ..TopicConfig::default()
        },
    )?;
    subscribe(&fixture, "alpha", SubscriptionConfig::default(), 0)?;
    subscribe(
        &fixture,
        "zulu",
        SubscriptionConfig {
            max_message_bytes: 16,
            ..SubscriptionConfig::default()
        },
        0,
    )?;
    apply(&fixture, 1, legacy("known"))?;
    reject(
        &fixture,
        10,
        CommandKind::Send {
            message_id: "known".into(),
            body: vec![1; 17],
            time_to_live_millis: None,
            session_id: None,
        },
        BrokerError::MessageTooLarge {
            body_bytes: 17,
            maximum_bytes: 16,
        },
    )?;
    let mut invalid_duplicate = member("known");
    invalid_duplicate
        .envelope
        .application_properties
        .insert("bad".into(), MessageValue::List(vec![]));
    let before = fixture.machine.store().snapshot()?;
    assert!(matches!(
        apply(&fixture, 10, rich(invalid_duplicate)),
        Err(BrokerError::InvalidMessageContent { .. })
    ));
    assert_eq!(fixture.machine.store().snapshot()?, before);
    let mut valid = member("new");
    valid.body.clear();
    valid.envelope = MessageEnvelope::default();
    reject(
        &fixture,
        10,
        CommandKind::SendBatch {
            messages: vec![valid, member(&"x".repeat(129))],
        },
        BrokerError::MessageIdTooLong {
            length: 129,
            maximum: 128,
        },
    )?;
    let before = fixture.machine.store().snapshot()?;
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    effects(
        &apply(&fixture, 2, legacy("new"))?,
        &[
            fixture
                .entity
                .subscription(&SubscriptionName::new("alpha")?)?,
            fixture
                .entity
                .subscription(&SubscriptionName::new("zulu")?)?,
        ],
    );
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("topic counter")
            .next_sequence,
        3
    );
    Ok(())
}

fn duplicate_admission_at_the_exact_deadline_replaces_history_without_keeping_old_expiry<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let config = TopicConfig {
        requires_duplicate_detection: true,
        ..TopicConfig::default()
    };
    let fixture = topic(provider, config)?;
    let alpha = subscribe(&fixture, "alpha", SubscriptionConfig::default(), 0)?;
    let beta = subscribe(&fixture, "beta", SubscriptionConfig::default(), 0)?;
    apply(&fixture, 1, legacy("same"))?;
    effects(&apply(&fixture, 2, legacy("same"))?, &[]);
    let deadline =
        Timestamp::from_millis(1 + config.duplicate_detection_history_time_window_millis);
    let application = apply(&fixture, deadline.as_millis(), legacy("same"))?;
    assert_eq!(
        application.outcome,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(3)
        }
    );
    effects(&application, &[alpha.clone(), beta.clone()]);
    assert_eq!(
        fixture
            .machine
            .store()
            .get(&keys::duplicate_history_expiry(
                &fixture.namespace,
                &fixture.entity,
                deadline,
                "same"
            ))?,
        None
    );
    let new_deadline: Timestamp = codec::decode(
        &fixture
            .machine
            .store()
            .get(&keys::duplicate_history(
                &fixture.namespace,
                &fixture.entity,
                "same",
            ))?
            .expect("new topic history"),
    )?;
    assert_eq!(
        new_deadline,
        deadline.saturating_add_millis(config.duplicate_detection_history_time_window_millis)
    );
    for target in [&alpha, &beta] {
        for sequence in [1, 3] {
            assert!(
                fixture
                    .machine
                    .message(&fixture.namespace, target, SequenceNumber::new(sequence))?
                    .is_some()
            );
        }
        assert!(
            fixture
                .machine
                .message(&fixture.namespace, target, SequenceNumber::new(2))?
                .is_none()
        );
    }
    Ok(())
}

fn final_topic_sequence_is_shared_and_child_sequence_exhaustion_never_allocates<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = topic(
        provider,
        TopicConfig {
            requires_duplicate_detection: true,
            ..TopicConfig::default()
        },
    )?;
    let alpha = subscribe(&fixture, "alpha", SubscriptionConfig::default(), 0)?;
    let beta = subscribe(&fixture, "beta", SubscriptionConfig::default(), 0)?;
    fixture.machine.store().apply(
        WriteBatch::default()
            .put(
                keys::queue_counters(&fixture.namespace, &fixture.entity),
                codec::encode(&QueueCounters {
                    next_sequence: MAX_SEQUENCE_NUMBER,
                    next_lock_token: 1,
                })?,
            )
            .put(
                keys::queue_counters(&fixture.namespace, &alpha),
                codec::encode(&QueueCounters {
                    next_sequence: u64::MAX,
                    next_lock_token: 1,
                })?,
            ),
    )?;
    reject(
        &fixture,
        10,
        CommandKind::SendBatch {
            messages: vec![member("last"), member("last")],
        },
        BrokerError::QueueCounterExhausted {
            counter: QueueCounterKind::Sequence,
        },
    )?;
    let final_send = apply(&fixture, 1, legacy("last"))?;
    assert_eq!(
        final_send.outcome,
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(MAX_SEQUENCE_NUMBER)
        }
    );
    effects(&final_send, &[alpha.clone(), beta.clone()]);
    assert_eq!(
        counters(&fixture, &alpha)?
            .expect("seeded child counter")
            .next_sequence,
        u64::MAX
    );
    assert_eq!(counters(&fixture, &beta)?, None);
    for target in [&alpha, &beta] {
        assert!(
            fixture
                .machine
                .message(
                    &fixture.namespace,
                    target,
                    SequenceNumber::new(MAX_SEQUENCE_NUMBER)
                )?
                .is_some()
        );
    }
    reject(
        &fixture,
        10,
        legacy("last"),
        BrokerError::QueueCounterExhausted {
            counter: QueueCounterKind::Sequence,
        },
    )?;
    reject(
        &fixture,
        10,
        CommandKind::SendBatch {
            messages: vec![member("new"), member("other")],
        },
        BrokerError::QueueCounterExhausted {
            counter: QueueCounterKind::Sequence,
        },
    )?;
    let delivery =
        receive(&fixture, &alpha, 2, ReceiveMode::PeekLock)?.expect("shared sequence copy");
    assert_eq!(delivery.sequence, SequenceNumber::new(MAX_SEQUENCE_NUMBER));
    assert_eq!(delivery.lock.expect("independent lock").token.as_u64(), 1);
    assert_eq!(
        counters(&fixture, &alpha)?
            .expect("lock allocation counter")
            .next_sequence,
        u64::MAX
    );
    let before = fixture.machine.store().snapshot()?;
    effects(
        &apply(&fixture, 100, CommandKind::SendBatch { messages: vec![] })?,
        &[],
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

fn scheduled_inputs_accept_empty_and_duplicate_batches_without_retaining_duplicate_copies<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fixture = topic(
        provider,
        TopicConfig {
            requires_duplicate_detection: true,
            ..TopicConfig::default()
        },
    )?;
    let plain = subscribe(&fixture, "plain", SubscriptionConfig::default(), 0)?;
    let session = subscribe(
        &fixture,
        "session",
        SubscriptionConfig {
            requires_session: true,
            ..SubscriptionConfig::default()
        },
        0,
    )?;
    apply(&fixture, 1, legacy("known"))?;
    for kind in [
        CommandKind::Schedule { messages: vec![] },
        CommandKind::ScheduleEnvelopes { messages: vec![] },
    ] {
        let before = fixture.machine.store().snapshot()?;
        let application = apply(&fixture, 10, kind)?;
        assert_eq!(
            application.outcome,
            CommandOutcome::Scheduled { sequences: vec![] }
        );
        effects(&application, &[]);
        assert_eq!(fixture.machine.store().snapshot()?, before);
    }
    for (index, scheduled) in [0, 2, 100].into_iter().enumerate() {
        let mut message = member("known");
        message.scheduled_enqueue_time = Some(Timestamp::from_millis(scheduled));
        let application = apply(
            &fixture,
            2,
            CommandKind::SendBatch {
                messages: vec![member(&format!("new-{scheduled}")), message],
            },
        )?;
        let active = SequenceNumber::new(2 + index as u64 * 2);
        let discarded = SequenceNumber::new(active.as_u64() + 1);
        assert_eq!(
            application.outcome,
            CommandOutcome::BatchSent {
                sequences: vec![active, discarded]
            }
        );
        effects(&application, &[plain.clone(), session.dead_letter_queue()?]);
        assert!(
            fixture
                .machine
                .message(&fixture.namespace, &fixture.entity, discarded)?
                .is_none()
        );
        assert!(
            fixture
                .machine
                .message(&fixture.namespace, &plain, discarded)?
                .is_none()
        );
        assert!(
            fixture
                .machine
                .store()
                .get(&keys::scheduled(
                    &fixture.namespace,
                    &fixture.entity,
                    Timestamp::from_millis(scheduled),
                    discarded
                ))?
                .is_none()
        );
    }
    Ok(())
}

#[derive(Debug, Default)]
struct Observations {
    commits: usize,
    puts: Vec<Key>,
}

#[derive(Clone, Debug)]
struct ObservedStore<S> {
    inner: S,
    fail_next: Arc<AtomicBool>,
    fail_get: Arc<Mutex<Option<Key>>>,
    observations: Arc<Mutex<Observations>>,
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        let mut fail = self.fail_get.lock().expect("read failure");
        if fail.as_deref() == Some(key) {
            fail.take();
            return Err(StorageError::Backend {
                operation: "get",
                detail: "injected fanout read failure".into(),
            });
        }
        drop(fail);
        self.inner.get(key)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.inner.scan_from(prefix, start, limit)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        let mut observed = self.observations.lock().expect("observations");
        observed.commits += 1;
        observed
            .puts
            .extend(batch.mutations().iter().filter_map(|mutation| {
                if let Mutation::Put { key, .. } = mutation {
                    Some(key.clone())
                } else {
                    None
                }
            }));
        drop(observed);
        if self.fail_next.swap(false, Ordering::Relaxed) {
            return Err(StorageError::Backend {
                operation: "commit",
                detail: "injected fanout failure".into(),
            });
        }
        self.inner.apply(batch)
    }
}

struct ObservedProvider<P> {
    inner: P,
    fail_next: Arc<AtomicBool>,
    fail_get: Arc<Mutex<Option<Key>>>,
    observations: Arc<Mutex<Observations>>,
}

impl<P: StoreProvider> StoreProvider for ObservedProvider<P> {
    type Store = ObservedStore<P::Store>;
    fn open(&self) -> Result<Self::Store, StorageError> {
        Ok(ObservedStore {
            inner: self.inner.open()?,
            fail_next: self.fail_next.clone(),
            fail_get: self.fail_get.clone(),
            observations: self.observations.clone(),
        })
    }
}

fn one_commit_failure_retry_and_late_read_corruption_never_publish_partial_copies<
    P: StoreProvider,
>(
    provider: P,
) -> TestResult {
    let fail_next = Arc::new(AtomicBool::new(false));
    let fail_get = Arc::new(Mutex::new(None));
    let observations = Arc::new(Mutex::new(Observations::default()));
    let fixture = topic(
        ObservedProvider {
            inner: provider,
            fail_next: fail_next.clone(),
            fail_get: fail_get.clone(),
            observations: observations.clone(),
        },
        TopicConfig {
            requires_duplicate_detection: true,
            ..TopicConfig::default()
        },
    )?;
    let alpha = subscribe(&fixture, "alpha", SubscriptionConfig::default(), 0)?;
    let zulu = subscribe(&fixture, "zulu", SubscriptionConfig::default(), 0)?;
    let messages = vec![member("same"), member("same"), member("new")];
    *observations.lock().expect("observations") = Observations::default();
    *fail_get.lock().expect("read failure") = Some(keys::queue_config(&fixture.namespace, &zulu));
    reject(
        &fixture,
        10,
        CommandKind::SendBatch {
            messages: messages.clone(),
        },
        BrokerError::Storage(StorageError::Backend {
            operation: "get",
            detail: "injected fanout read failure".into(),
        }),
    )?;
    assert_eq!(observations.lock().expect("observations").commits, 0);
    *observations.lock().expect("observations") = Observations::default();
    fail_next.store(true, Ordering::Relaxed);
    reject(
        &fixture,
        10,
        CommandKind::SendBatch {
            messages: messages.clone(),
        },
        BrokerError::Storage(StorageError::Backend {
            operation: "commit",
            detail: "injected fanout failure".into(),
        }),
    )?;
    assert_eq!(observations.lock().expect("observations").commits, 1);
    let before = fixture.machine.store().snapshot()?;
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    *observations.lock().expect("observations") = Observations::default();
    let application = apply(&fixture, 10, CommandKind::SendBatch { messages })?;
    effects(&application, &[alpha.clone(), zulu.clone()]);
    assert_eq!(
        application.outcome,
        CommandOutcome::BatchSent {
            sequences: vec![
                SequenceNumber::new(1),
                SequenceNumber::new(2),
                SequenceNumber::new(3)
            ]
        }
    );
    let observed = observations.lock().expect("observations");
    assert_eq!(observed.commits, 1);
    assert_eq!(
        observed
            .puts
            .iter()
            .filter(|key| **key == keys::queue_counters(&fixture.namespace, &fixture.entity))
            .count(),
        1
    );
    for target in [&alpha, &zulu] {
        assert!(
            !observed
                .puts
                .contains(&keys::queue_counters(&fixture.namespace, target))
        );
        for sequence in [1, 3] {
            assert_eq!(
                observed
                    .puts
                    .iter()
                    .filter(|key| **key
                        == keys::message(&fixture.namespace, target, SequenceNumber::new(sequence)))
                    .count(),
                1
            );
        }
    }
    drop(observed);
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().delete(keys::queue_config(&fixture.namespace, &zulu)))?;
    reject(
        &fixture,
        11,
        legacy("newer"),
        BrokerError::DanglingSubscriptionMetadata,
    )?;
    fixture.machine.store().apply(
        WriteBatch::default().put(keys::queue_config(&fixture.namespace, &zulu), vec![255]),
    )?;
    let before = fixture.machine.store().snapshot()?;
    assert!(matches!(
        apply(&fixture, 11, legacy("newer")),
        Err(BrokerError::Codec(_))
    ));
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    invalid_late_inputs_and_stricter_duplicate_destinations_leave_every_record_unchanged,
    duplicate_admission_at_the_exact_deadline_replaces_history_without_keeping_old_expiry,
    final_topic_sequence_is_shared_and_child_sequence_exhaustion_never_allocates,
    scheduled_inputs_accept_empty_and_duplicate_batches_without_retaining_duplicate_copies,
    one_commit_failure_retry_and_late_read_corruption_never_publish_partial_copies,
}

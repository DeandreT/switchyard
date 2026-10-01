use super::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use storage::{Key, Mutation, StorageError, StoreSnapshot, Value, WriteBatch};

fn deduplication_is_topic_wide_across_session_and_null_routes<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, true)?;
    let plain = subscribe(&fixture, "plain", false)?;
    let required = subscribe(&fixture, "required", true)?;
    let shadow = required.dead_letter_queue()?;
    effects(
        &apply(&fixture, 1, legacy("known", Some("a")))?,
        vec![plain.clone(), required.clone()],
    );
    let application = apply(
        &fixture,
        2,
        CommandKind::SendBatch {
            messages: vec![
                member("known", None),
                member("new", None),
                member("", Some("b")),
                member("", None),
            ],
        },
    )?;
    assert_eq!(
        application.outcome,
        CommandOutcome::BatchSent {
            sequences: (2..=5).map(SequenceNumber::new).collect()
        }
    );
    effects(
        &application,
        vec![plain.clone(), required.clone(), shadow.clone()],
    );
    for (entity, expected) in [
        (&plain, vec![1, 3, 4, 5]),
        (&required, vec![1, 4]),
        (&shadow, vec![3, 5]),
    ] {
        for sequence in 1..=5 {
            assert_eq!(
                fixture
                    .machine
                    .message(&fixture.namespace, entity, SequenceNumber::new(sequence))?
                    .is_some(),
                expected.contains(&sequence)
            );
        }
        assert_eq!(counters(&fixture, entity)?, None);
    }
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("topic counter")
            .next_sequence,
        6
    );
    let before = fixture.machine.store().snapshot()?;
    effects(
        &apply(&fixture, 3, CommandKind::SendBatch { messages: vec![] })?,
        vec![],
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    let application = apply(
        &fixture,
        4,
        CommandKind::SendBatch {
            messages: vec![member("known", Some("other")), member("new", Some("other"))],
        },
    )?;
    assert_eq!(
        application.outcome,
        CommandOutcome::BatchSent {
            sequences: vec![SequenceNumber::new(6), SequenceNumber::new(7)]
        }
    );
    effects(&application, vec![]);
    for entity in [&plain, &required, &shadow] {
        for sequence in [6, 7] {
            assert_eq!(
                fixture.machine.message(
                    &fixture.namespace,
                    entity,
                    SequenceNumber::new(sequence)
                )?,
                None
            );
        }
    }
    assert_eq!(
        counters(&fixture, &fixture.entity)?
            .expect("topic counter")
            .next_sequence,
        8
    );
    Ok(())
}

fn independent_lock_session_and_ttl_timers_survive_reopen<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fixture = topic(provider, false)?;
    let plain = subscribe(&fixture, "plain", false)?;
    let first = subscribe(&fixture, "first", true)?;
    let second = subscribe(&fixture, "second", true)?;
    apply(&fixture, 1, rich(member("live", Some("a"))))?;
    let a = accept(&fixture, &first, 10, Some("a"))?.expect("first hold");
    let b = accept(&fixture, &second, 11, Some("a"))?.expect("second hold");
    let ordinary = receive(&fixture, &plain, 12, None)?.expect("plain lock");
    let first_delivery = receive(&fixture, &first, 13, Some(&a.hold()))?.expect("first lock");
    receive(&fixture, &second, 14, Some(&b.hold()))?.expect("second lock");
    at(
        &fixture,
        &first,
        15,
        CommandKind::RenewSessionLock {
            session: a.hold(),
            lock_duration_millis: Some(300),
        },
    )?;
    at(
        &fixture,
        &second,
        16,
        CommandKind::RenewSessionLock {
            session: b.hold(),
            lock_duration_millis: Some(400),
        },
    )?;
    at(
        &fixture,
        &first,
        17,
        CommandKind::SetSessionState {
            session: a.hold(),
            state: b"first".to_vec(),
        },
    )?;
    at(
        &fixture,
        &second,
        18,
        CommandKind::SetSessionState {
            session: b.hold(),
            state: b"second".to_vec(),
        },
    )?;
    let sibling = fixture
        .machine
        .message(&fixture.namespace, &second, SequenceNumber::new(1))?;
    assert_eq!(
        at(&fixture, &plain, 112, CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 1,
            dead_lettered: 0,
            dropped: 0
        }
    );
    assert_eq!(
        at(&fixture, &first, 113, CommandKind::ExpireLocks)?,
        CommandOutcome::LocksExpired {
            returned_to_ready: 1,
            dead_lettered: 0,
            dropped: 0
        }
    );
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &second, SequenceNumber::new(1))?,
        sibling
    );
    assert!(
        fixture
            .machine
            .store()
            .get(&keys::ready(&fixture.namespace, &plain, ordinary.sequence))?
            .is_some()
    );
    assert!(
        fixture
            .machine
            .store()
            .get(&keys::session_ready(
                &fixture.namespace,
                &first,
                &SessionId::new("a")?,
                first_delivery.sequence
            ))?
            .is_some()
    );
    let ordinary = receive(&fixture, &plain, 114, None)?.expect("plain redelivery");
    let first_delivery =
        receive(&fixture, &first, 115, Some(&a.hold()))?.expect("first redelivery");
    assert_eq!(ordinary.delivery_count, 2);
    assert_eq!(first_delivery.delivery_count, 2);
    let before = fixture.machine.store().snapshot()?;
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    complete(&fixture, &plain, 116, &ordinary)?;
    complete(&fixture, &first, 117, &first_delivery)?;
    assert_eq!(
        at(&fixture, &first, 315, CommandKind::ExpireSessionLocks)?,
        CommandOutcome::SessionLocksExpired { released: 1 }
    );
    let replacement = accept(&fixture, &first, 316, Some("a"))?.expect("expired hold replaced");
    assert_eq!(replacement.state, b"first");
    assert_eq!(
        at(
            &fixture,
            &second,
            317,
            CommandKind::GetSessionState { session: b.hold() }
        )?,
        CommandOutcome::SessionState(b"second".to_vec())
    );
    let mut expiring = member("ttl", Some("a"));
    expiring.time_to_live_millis = Some(10);
    apply(&fixture, 320, rich(expiring))?;
    for (entity, millis) in [(&plain, 330), (&first, 331), (&second, 332)] {
        assert_eq!(
            at(&fixture, entity, millis, CommandKind::ExpireMessages)?,
            CommandOutcome::MessagesExpired {
                dead_lettered: 0,
                dropped: 1,
                processed: 1
            }
        );
        assert_eq!(
            fixture
                .machine
                .message(&fixture.namespace, entity, SequenceNumber::new(2))?,
            None
        );
        assert!(
            fixture
                .machine
                .store()
                .get(&keys::ready(
                    &fixture.namespace,
                    entity,
                    SequenceNumber::new(2)
                ))?
                .is_none()
        );
        assert!(
            fixture
                .machine
                .store()
                .get(&keys::session_ready(
                    &fixture.namespace,
                    entity,
                    &SessionId::new("a")?,
                    SequenceNumber::new(2)
                ))?
                .is_none()
        );
    }
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &second, SequenceNumber::new(1))?
            .is_some()
    );
    Ok(())
}

#[derive(Clone, Debug)]
struct ObservedStore<S> {
    inner: S,
    fail_next: Arc<AtomicBool>,
    observations: Arc<Mutex<(usize, Vec<Key>)>>,
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
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
        {
            let mut observations = self.observations.lock().expect("observations");
            observations.0 += 1;
            observations.1.extend(
                batch
                    .mutations()
                    .iter()
                    .filter_map(|mutation| match mutation {
                        Mutation::Put { key, .. } => Some(key.clone()),
                        _ => None,
                    }),
            );
        }
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return Err(StorageError::Backend {
                operation: "commit",
                detail: "injected mixed session fanout failure".into(),
            });
        }
        self.inner.apply(batch)
    }
}

struct ObservedProvider<P> {
    inner: P,
    fail_next: Arc<AtomicBool>,
    observations: Arc<Mutex<(usize, Vec<Key>)>>,
}
impl<P: StoreProvider> StoreProvider for ObservedProvider<P> {
    type Store = ObservedStore<P::Store>;
    fn open(&self) -> Result<Self::Store, StorageError> {
        Ok(ObservedStore {
            inner: self.inner.open()?,
            fail_next: self.fail_next.clone(),
            observations: self.observations.clone(),
        })
    }
}

fn mixed_routes_commit_once_and_failed_commit_or_late_input_is_atomic<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let fail_next = Arc::new(AtomicBool::new(false));
    let observations = Arc::new(Mutex::new((0, Vec::new())));
    let fixture = topic(
        ObservedProvider {
            inner: provider,
            fail_next: fail_next.clone(),
            observations: observations.clone(),
        },
        true,
    )?;
    let plain = subscribe(&fixture, "plain", false)?;
    let required = subscribe(&fixture, "required", true)?;
    let shadow = required.dead_letter_queue()?;
    let messages = vec![
        member("same", Some("a")),
        member("same", None),
        member("null", None),
    ];
    *observations.lock().expect("observations") = (0, Vec::new());
    reject(
        &fixture,
        10,
        CommandKind::SendBatch {
            messages: vec![
                member("valid", Some("a")),
                member(&"x".repeat(domain::MAX_MESSAGE_ID_LENGTH + 1), None),
            ],
        },
        BrokerError::MessageIdTooLong {
            length: domain::MAX_MESSAGE_ID_LENGTH + 1,
            maximum: domain::MAX_MESSAGE_ID_LENGTH,
        },
    )?;
    assert_eq!(observations.lock().expect("observations").0, 0);
    fail_next.store(true, Ordering::SeqCst);
    reject(
        &fixture,
        10,
        CommandKind::SendBatch {
            messages: messages.clone(),
        },
        BrokerError::Storage(StorageError::Backend {
            operation: "commit",
            detail: "injected mixed session fanout failure".into(),
        }),
    )?;
    assert_eq!(observations.lock().expect("observations").0, 1);
    let before = fixture.machine.store().snapshot()?;
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    *observations.lock().expect("observations") = (0, Vec::new());
    let application = apply(&fixture, 10, CommandKind::SendBatch { messages })?;
    assert_eq!(
        application.outcome,
        CommandOutcome::BatchSent {
            sequences: (1..=3).map(SequenceNumber::new).collect()
        }
    );
    effects(
        &application,
        vec![plain.clone(), required.clone(), shadow.clone()],
    );
    {
        let observed = observations.lock().expect("observations");
        assert_eq!(observed.0, 1);
        assert_eq!(
            observed
                .1
                .iter()
                .filter(|key| **key == keys::queue_counters(&fixture.namespace, &fixture.entity))
                .count(),
            1
        );
        for entity in [&plain, &required, &shadow] {
            assert!(
                !observed
                    .1
                    .contains(&keys::queue_counters(&fixture.namespace, entity))
            );
        }
        for (entity, sequence) in [(&plain, 1), (&plain, 3), (&required, 1), (&shadow, 3)] {
            assert_eq!(
                observed
                    .1
                    .iter()
                    .filter(|key| **key
                        == keys::message(&fixture.namespace, entity, SequenceNumber::new(sequence)))
                    .count(),
                1
            );
        }
    }
    for entity in [&plain, &required, &shadow] {
        assert_eq!(
            fixture
                .machine
                .message(&fixture.namespace, entity, SequenceNumber::new(2))?,
            None
        );
    }
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> super::TestResult { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}
for_each_backend! {
    deduplication_is_topic_wide_across_session_and_null_routes,
    independent_lock_session_and_ttl_timers_survive_reopen,
    mixed_routes_commit_once_and_failed_commit_or_late_input_is_atomic,
}

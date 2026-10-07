//! Side effects describe the final committed batch, not tentative transitions.

use std::{
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use domain::{
    BrokerError, CommandApplication, CommandKind, CommandOutcome, Delivery, DeliveryBudget,
    EntityPath, QueueConfig, ReceiveMode, SequenceNumber, SessionHold, SessionId, Timestamp, keys,
};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::{QueueFixture, StoreProvider};

fn apply<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    kind: CommandKind,
) -> Result<CommandApplication, BrokerError> {
    fixture
        .machine
        .apply_with_effects(&fixture.command(millis, kind))
}

fn select_queue<P: StoreProvider>(
    fixture: &mut QueueFixture<P>,
    millis: u64,
    index: usize,
    config: QueueConfig,
) -> Result<(), Box<dyn Error>> {
    fixture.entity = EntityPath::new(format!("case-{index}"))?;
    let application = apply(fixture, millis, CommandKind::CreateQueue { config })?;
    assert_eq!(application.outcome, CommandOutcome::QueueCreated);
    assert!(!application.dead_letters_enqueued);
    Ok(())
}

fn send<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    id: &str,
    ttl: Option<u64>,
    session: Option<&SessionId>,
) -> Result<SequenceNumber, BrokerError> {
    let application = apply(
        fixture,
        millis,
        CommandKind::Send {
            message_id: id.to_owned(),
            body: b"payload".to_vec(),
            time_to_live_millis: ttl,
            session_id: session.cloned(),
        },
    )?;
    assert!(!application.dead_letters_enqueued);
    let CommandOutcome::Sent { sequence } = application.outcome else {
        panic!("send outcome")
    };
    Ok(sequence)
}

fn receive_kind(mode: ReceiveMode, session: Option<SessionHold>) -> CommandKind {
    CommandKind::Receive {
        mode,
        lock_duration_millis: Some(100),
        session,
    }
}

fn delivery(outcome: CommandOutcome) -> Option<Delivery> {
    let CommandOutcome::Received(delivery) = outcome else {
        panic!("receive outcome")
    };
    delivery
}

fn accept<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    session: &SessionId,
    duration: u64,
) -> Result<SessionHold, BrokerError> {
    let application = apply(
        fixture,
        millis,
        CommandKind::AcceptSession {
            session_id: Some(session.clone()),
            lock_duration_millis: Some(duration),
        },
    )?;
    assert!(!application.dead_letters_enqueued);
    let CommandOutcome::SessionAccepted(Some(accepted)) = application.outcome else {
        panic!("session accepted")
    };
    Ok(accepted.hold())
}

fn defer_next<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    hold: Option<&SessionHold>,
) -> Result<SequenceNumber, BrokerError> {
    let application = apply(
        fixture,
        millis,
        receive_kind(ReceiveMode::PeekLock, hold.cloned()),
    )?;
    assert!(!application.dead_letters_enqueued);
    let received = delivery(application.outcome).expect("live message");
    let deferred = apply(
        fixture,
        millis,
        CommandKind::Defer {
            sequence: received.sequence,
            lock_token: received.lock.expect("lock").token,
        },
    )?;
    assert_eq!(deferred.outcome, CommandOutcome::Deferred);
    assert!(!deferred.dead_letters_enqueued);
    Ok(received.sequence)
}

#[derive(Clone, Copy)]
enum DeferredApi {
    Legacy,
    Bounded,
    Held,
}

fn deferred_kind(
    api: DeferredApi,
    sequences: Vec<SequenceNumber>,
    mode: ReceiveMode,
    hold: Option<SessionHold>,
    max_bytes: u64,
) -> CommandKind {
    let budget = DeliveryBudget {
        max_bytes,
        per_message_overhead_bytes: 64,
    };
    match api {
        DeferredApi::Legacy => CommandKind::ReceiveDeferred {
            sequences,
            mode,
            lock_duration_millis: Some(100),
            session_id: hold.map(|hold| hold.session_id),
        },
        DeferredApi::Bounded => CommandKind::ReceiveDeferredBounded {
            sequences,
            mode,
            lock_duration_millis: Some(100),
            session_id: hold.map(|hold| hold.session_id),
            budget,
        },
        DeferredApi::Held => CommandKind::ReceiveDeferredHeld {
            sequences,
            mode,
            lock_duration_millis: Some(100),
            session: hold,
            budget,
        },
    }
}

fn assert_dead_letters<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    sequences: &[SequenceNumber],
) -> Result<(), Box<dyn Error>> {
    let shadow = fixture.entity.dead_letter_queue()?;
    let ready = fixture
        .machine
        .store()
        .scan_prefix(&keys::ready_prefix(&fixture.namespace, &shadow), usize::MAX)?;
    assert_eq!(
        ready
            .iter()
            .map(|(key, _)| keys::trailing_sequence(key).expect("sequence"))
            .collect::<Vec<_>>(),
        sequences
    );
    for sequence in sequences {
        assert!(
            fixture
                .machine
                .message(&fixture.namespace, &fixture.entity, *sequence)?
                .is_none()
        );
        assert!(
            fixture
                .machine
                .message(&fixture.namespace, &shadow, *sequence)?
                .expect("dead-letter message")
                .dead_letter
                .is_some()
        );
    }
    Ok(())
}

fn ordinary_lazy_expiry_reports_enqueues_with_or_without_a_live_delivery<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let mut fixture = QueueFixture::with_defaults(provider, "tenant", "unused")?;
    let mut index = 0;
    for mode in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete] {
        for include_live in [false, true] {
            let base = 10 + index as u64 * 100;
            select_queue(
                &mut fixture,
                base,
                index,
                QueueConfig {
                    dead_lettering_on_message_expiration: true,
                    ..QueueConfig::default()
                },
            )?;
            let first = send(&fixture, base, "expired-first", Some(5), None)?;
            let second = send(&fixture, base, "expired-second", Some(5), None)?;
            let live = include_live
                .then(|| send(&fixture, base, "live", None, None))
                .transpose()?;
            let result = apply(&fixture, base + 10, receive_kind(mode, None))?;
            assert!(result.dead_letters_enqueued);
            let received = delivery(result.outcome);
            assert_eq!(received.as_ref().map(|message| message.sequence), live);
            if let Some(received) = received {
                assert_eq!(received.delivery_count, 1);
                assert_eq!(received.lock.is_some(), mode == ReceiveMode::PeekLock);
            }
            assert_dead_letters(&fixture, &[first, second])?;
            let snapshot = fixture.machine.store().snapshot()?;
            fixture = fixture.restart()?;
            assert_eq!(fixture.machine.store().snapshot()?, snapshot);
            let repeated = apply(&fixture, base + 11, receive_kind(mode, None))?;
            assert_eq!(repeated.outcome, CommandOutcome::Received(None));
            assert!(!repeated.dead_letters_enqueued);
            assert_eq!(fixture.machine.store().snapshot()?, snapshot);
            index += 1;
        }
    }
    Ok(())
}

fn dropping_expired_ready_and_deferred_records_has_no_enqueue_effect<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let mut fixture = QueueFixture::with_defaults(provider, "tenant", "unused")?;
    for (index, mode) in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete]
        .into_iter()
        .enumerate()
    {
        let base = 10 + index as u64 * 100;
        select_queue(&mut fixture, base, index, QueueConfig::default())?;
        let deferred = send(&fixture, base, "deferred", Some(5), None)?;
        assert_eq!(defer_next(&fixture, base + 1, None)?, deferred);
        let ready = send(&fixture, base + 1, "ready", Some(5), None)?;
        let result = apply(&fixture, base + 10, receive_kind(mode, None))?;
        assert_eq!(result.outcome, CommandOutcome::Received(None));
        assert!(!result.dead_letters_enqueued);
        let result = apply(
            &fixture,
            base + 10,
            deferred_kind(DeferredApi::Held, vec![deferred], mode, None, u64::MAX),
        )?;
        assert_eq!(result.outcome, CommandOutcome::DeferredReceived(Vec::new()));
        assert!(!result.dead_letters_enqueued);
        for sequence in [ready, deferred] {
            assert!(
                fixture
                    .machine
                    .message(&fixture.namespace, &fixture.entity, sequence)?
                    .is_none()
            );
        }
        assert_dead_letters(&fixture, &[])?;
    }
    Ok(())
}

fn every_deferred_api_reports_committed_lazy_expiry_in_both_modes<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let mut fixture = QueueFixture::with_defaults(provider, "tenant", "unused")?;
    let mut index = 0;
    for api in [DeferredApi::Legacy, DeferredApi::Bounded, DeferredApi::Held] {
        for requires_session in [false, true] {
            for mode in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete] {
                for include_live in [false, true] {
                    let base = 10 + index as u64 * 100;
                    select_queue(
                        &mut fixture,
                        base,
                        index,
                        QueueConfig {
                            requires_session,
                            dead_lettering_on_message_expiration: true,
                            ..QueueConfig::default()
                        },
                    )?;
                    let session = requires_session
                        .then(|| SessionId::new("cart"))
                        .transpose()?;
                    let expired = send(&fixture, base, "expired", Some(5), session.as_ref())?;
                    let live = include_live
                        .then(|| send(&fixture, base, "live", None, session.as_ref()))
                        .transpose()?;
                    let hold = session
                        .as_ref()
                        .map(|session| accept(&fixture, base, session, 1_000))
                        .transpose()?;
                    assert_eq!(defer_next(&fixture, base + 1, hold.as_ref())?, expired);
                    if let Some(live) = live {
                        assert_eq!(defer_next(&fixture, base + 1, hold.as_ref())?, live);
                    }
                    let mut sequences = vec![expired];
                    sequences.extend(live);
                    fixture = fixture.restart()?;
                    let result = apply(
                        &fixture,
                        base + 10,
                        deferred_kind(api, sequences, mode, hold.clone(), u64::MAX),
                    )?;
                    assert!(result.dead_letters_enqueued);
                    let CommandOutcome::DeferredReceived(deliveries) = result.outcome else {
                        panic!("deferred response")
                    };
                    assert_eq!(
                        deliveries
                            .iter()
                            .map(|delivery| delivery.sequence)
                            .collect::<Vec<_>>(),
                        live.into_iter().collect::<Vec<_>>()
                    );
                    for delivery in deliveries {
                        assert_eq!(delivery.delivery_count, 2);
                        assert_eq!(delivery.session_id, session);
                        assert_eq!(delivery.lock.is_some(), mode == ReceiveMode::PeekLock);
                    }
                    assert_dead_letters(&fixture, &[expired])?;
                    let snapshot = fixture.machine.store().snapshot()?;
                    let result = apply(
                        &fixture,
                        base + 11,
                        deferred_kind(api, Vec::new(), mode, hold, u64::MAX),
                    )?;
                    assert_eq!(result.outcome, CommandOutcome::DeferredReceived(Vec::new()));
                    assert!(!result.dead_letters_enqueued);
                    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
                    index += 1;
                }
            }
        }
    }
    Ok(())
}

fn rejected_deferred_batches_do_not_commit_tentative_dead_letters<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let mut fixture = QueueFixture::with_defaults(provider, "tenant", "unused")?;
    let mut index = 0;
    for api in [DeferredApi::Legacy, DeferredApi::Bounded, DeferredApi::Held] {
        for mode in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete] {
            let base = 10 + index as u64 * 100;
            select_queue(
                &mut fixture,
                base,
                index,
                QueueConfig {
                    dead_lettering_on_message_expiration: true,
                    ..QueueConfig::default()
                },
            )?;
            let expired = send(&fixture, base, "expired", Some(5), None)?;
            let live = send(&fixture, base, "live", None, None)?;
            assert_eq!(defer_next(&fixture, base + 1, None)?, expired);
            assert_eq!(defer_next(&fixture, base + 1, None)?, live);
            let snapshot = fixture.machine.store().snapshot()?;
            let missing = SequenceNumber::new(999);
            assert_eq!(
                apply(
                    &fixture,
                    base + 10,
                    deferred_kind(api, vec![expired, missing], mode, None, u64::MAX),
                ),
                Err(BrokerError::MessageNotFound { sequence: missing })
            );
            assert_eq!(fixture.machine.store().snapshot()?, snapshot);
            if !matches!(api, DeferredApi::Legacy) {
                let cost = fixture
                    .machine
                    .message(&fixture.namespace, &fixture.entity, expired)?
                    .expect("expired record")
                    .delivery_size_upper_bound()
                    + 64;
                assert!(matches!(
                    apply(
                        &fixture,
                        base + 10,
                        deferred_kind(api, vec![expired, live], mode, None, cost),
                    ),
                    Err(BrokerError::MessageTooLarge { .. })
                ));
                assert_eq!(fixture.machine.store().snapshot()?, snapshot);
            }
            assert_dead_letters(&fixture, &[])?;
            fixture = fixture.restart()?;
            assert_eq!(fixture.machine.store().snapshot()?, snapshot);
            let retried = apply(
                &fixture,
                base + 10,
                deferred_kind(api, vec![expired, live], mode, None, u64::MAX),
            )?;
            assert!(retried.dead_letters_enqueued);
            let CommandOutcome::DeferredReceived(deliveries) = retried.outcome else {
                panic!("deferred response")
            };
            assert_eq!(deliveries.len(), 1);
            assert_eq!(deliveries[0].sequence, live);
            assert_eq!(deliveries[0].delivery_count, 2);
            assert_dead_letters(&fixture, &[expired])?;
            index += 1;
        }
    }
    Ok(())
}

fn expired_session_holds_do_not_publish_or_commit_expiration<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let mut fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_session: true,
            dead_lettering_on_message_expiration: true,
            ..QueueConfig::default()
        },
    )?;
    let session = SessionId::new("cart")?;
    let sequence = send(&fixture, 10, "expired", Some(5), Some(&session))?;
    let hold = accept(&fixture, 10, &session, 5)?;
    assert_eq!(defer_next(&fixture, 11, Some(&hold))?, sequence);
    let snapshot = fixture.machine.store().snapshot()?;
    for mode in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete] {
        assert_eq!(
            apply(
                &fixture,
                20,
                deferred_kind(
                    DeferredApi::Held,
                    vec![sequence],
                    mode,
                    Some(hold.clone()),
                    u64::MAX,
                ),
            ),
            Err(BrokerError::SessionLockExpired {
                session_id: session.clone(),
                locked_until: Timestamp::from_millis(15),
            })
        );
        assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    }
    fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    let release = apply(&fixture, 20, CommandKind::ExpireSessionLocks)?;
    assert!(!release.dead_letters_enqueued);
    let hold = accept(&fixture, 21, &session, 100)?;
    let result = apply(
        &fixture,
        22,
        deferred_kind(
            DeferredApi::Held,
            vec![sequence],
            ReceiveMode::ReceiveAndDelete,
            Some(hold),
            u64::MAX,
        ),
    )?;
    assert_eq!(result.outcome, CommandOutcome::DeferredReceived(Vec::new()));
    assert!(result.dead_letters_enqueued);
    assert_dead_letters(&fixture, &[sequence])?;
    Ok(())
}

#[derive(Clone, Debug, Default)]
struct StoreControls {
    fail_commit: Arc<AtomicBool>,
    refuse_shadow_reads: Arc<AtomicBool>,
    apply_calls: Arc<AtomicUsize>,
    commit_boundary: Arc<AtomicBool>,
    shadow_metadata: Arc<Mutex<Vec<Key>>>,
    shadow_metadata_reads: Arc<AtomicUsize>,
}

#[derive(Clone, Debug)]
struct ObservedStore<S> {
    inner: S,
    controls: StoreControls,
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        if self.controls.refuse_shadow_reads.load(Ordering::Relaxed)
            && keys::entity_scope_parts(key)
                .is_some_and(|(_, entity)| entity.ends_with("/$deadletterqueue"))
        {
            // Mandatory owner proof is pre-commit; effect publication needs no reads.
            if self.controls.commit_boundary.load(Ordering::Relaxed)
                || !self
                    .controls
                    .shadow_metadata
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|allowed| allowed == key)
            {
                return Err(StorageError::Backend {
                    operation: "read",
                    detail: String::from("unexpected shadow read"),
                });
            }
            self.controls
                .shadow_metadata_reads
                .fetch_add(1, Ordering::Relaxed);
        }
        self.inner.get(key)
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.controls.apply_calls.fetch_add(1, Ordering::Relaxed);
        self.controls.commit_boundary.store(true, Ordering::Relaxed);
        if self.controls.fail_commit.swap(false, Ordering::Relaxed) {
            return Err(StorageError::Backend {
                operation: "commit",
                detail: String::from("injected failure"),
            });
        }
        self.inner.apply(batch)
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
        if self.controls.refuse_shadow_reads.load(Ordering::Relaxed)
            && keys::entity_scope_parts(prefix)
                .is_some_and(|(_, entity)| entity.ends_with("/$deadletterqueue"))
        {
            return Err(StorageError::Backend {
                operation: "scan",
                detail: String::from("unexpected shadow scan"),
            });
        }
        self.inner.scan_from(prefix, start, limit)
    }
}

struct ObservedProvider<P> {
    inner: P,
    controls: StoreControls,
}

impl<P: StoreProvider> StoreProvider for ObservedProvider<P> {
    type Store = ObservedStore<P::Store>;

    fn open(&self) -> Result<Self::Store, StorageError> {
        Ok(ObservedStore {
            inner: self.inner.open()?,
            controls: self.controls.clone(),
        })
    }
}

fn storage_failure_cannot_publish_an_effect_and_success_needs_no_shadow_queries<
    P: StoreProvider,
>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let controls = StoreControls::default();
    let mut fixture = QueueFixture::new(
        ObservedProvider {
            inner: provider,
            controls: controls.clone(),
        },
        "tenant",
        "orders",
        QueueConfig {
            dead_lettering_on_message_expiration: true,
            ..QueueConfig::default()
        },
    )?;
    let sequence = send(&fixture, 10, "expired", Some(5), None)?;
    let snapshot = fixture.machine.store().snapshot()?;
    let attempts = controls.apply_calls.load(Ordering::Relaxed);
    let shadow = fixture.entity.dead_letter_queue()?;
    *controls.shadow_metadata.lock().unwrap() = vec![
        keys::queue_config(&fixture.namespace, &shadow),
        keys::topic_config(&fixture.namespace, &shadow),
        keys::queue_capacity_mode(&fixture.namespace, &shadow),
        keys::queue_capacity_usage(&fixture.namespace, &shadow),
    ];
    controls.fail_commit.store(true, Ordering::Relaxed);
    controls.refuse_shadow_reads.store(true, Ordering::Relaxed);
    controls.commit_boundary.store(false, Ordering::Relaxed);
    let command = receive_kind(ReceiveMode::ReceiveAndDelete, None);
    assert_eq!(
        apply(&fixture, 20, command.clone()),
        Err(BrokerError::Storage(StorageError::Backend {
            operation: "commit",
            detail: String::from("injected failure"),
        }))
    );
    assert_eq!(controls.apply_calls.load(Ordering::Relaxed), attempts + 1);
    assert_eq!(controls.shadow_metadata_reads.load(Ordering::Relaxed), 4);
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    controls.commit_boundary.store(false, Ordering::Relaxed);
    let result = apply(&fixture, 20, command.clone())?;
    assert_eq!(result.outcome, CommandOutcome::Received(None));
    assert!(result.dead_letters_enqueued);
    assert_eq!(controls.apply_calls.load(Ordering::Relaxed), attempts + 2);
    assert_eq!(controls.shadow_metadata_reads.load(Ordering::Relaxed), 8);
    let snapshot = fixture.machine.store().snapshot()?;
    controls.commit_boundary.store(false, Ordering::Relaxed);
    let no_op = apply(&fixture, 21, command)?;
    assert_eq!(no_op.outcome, CommandOutcome::Received(None));
    assert!(!no_op.dead_letters_enqueued);
    assert_eq!(controls.apply_calls.load(Ordering::Relaxed), attempts + 2);
    assert_eq!(controls.shadow_metadata_reads.load(Ordering::Relaxed), 12);
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    controls.refuse_shadow_reads.store(false, Ordering::Relaxed);
    assert_dead_letters(&fixture, &[sequence])?;
    Ok(())
}

fn read_only_noop_and_duplicate_acknowledgements_have_no_enqueue_effect<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            requires_duplicate_detection: true,
            ..QueueConfig::default()
        },
    )?;
    let snapshot = fixture.machine.store().snapshot()?;
    for kind in [
        receive_kind(ReceiveMode::PeekLock, None),
        CommandKind::Peek {
            from_sequence: SequenceNumber::new(0),
            max_messages: 10,
            session_id: None,
        },
        deferred_kind(
            DeferredApi::Held,
            Vec::new(),
            ReceiveMode::ReceiveAndDelete,
            None,
            0,
        ),
        CommandKind::ExpireLocks,
        CommandKind::ExpireMessages,
        CommandKind::ExpireSessionLocks,
        CommandKind::ActivateScheduled,
        CommandKind::ExpireDuplicateHistory,
    ] {
        assert!(!apply(&fixture, 10, kind)?.dead_letters_enqueued);
        assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    }
    let first = send(&fixture, 10, "duplicate", None, None)?;
    let second = send(&fixture, 11, "duplicate", None, None)?;
    assert_ne!(first, second);
    assert!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, second)?
            .is_none()
    );
    assert_dead_letters(&fixture, &[])?;
    Ok(())
}

fn explicit_automatic_and_timer_dead_letters_report_effects_but_shadow_actions_do_not<
    P: StoreProvider,
>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let mut fixture = QueueFixture::with_defaults(provider, "tenant", "unused")?;
    for index in 0..4 {
        let base = 10 + index as u64 * 200;
        select_queue(
            &mut fixture,
            base,
            index,
            QueueConfig {
                max_delivery_count: 1,
                dead_lettering_on_message_expiration: true,
                ..QueueConfig::default()
            },
        )?;
        let sequence = send(&fixture, base, "message", (index == 2).then_some(5), None)?;
        let (at, kind) = if index == 2 {
            (base + 10, CommandKind::ExpireMessages)
        } else {
            let result = apply(
                &fixture,
                base + 1,
                receive_kind(ReceiveMode::PeekLock, None),
            )?;
            assert!(!result.dead_letters_enqueued);
            let received = delivery(result.outcome).expect("message");
            let lock = received.lock.expect("lock");
            match index {
                0 => (
                    base + 2,
                    CommandKind::DeadLetter {
                        sequence,
                        lock_token: lock.token,
                        reason: String::from("invalid"),
                        description: String::from("explicit rejection"),
                    },
                ),
                1 => (
                    base + 2,
                    CommandKind::Abandon {
                        sequence,
                        lock_token: lock.token,
                    },
                ),
                3 => (lock.locked_until.as_millis(), CommandKind::ExpireLocks),
                _ => unreachable!(),
            }
        };
        assert!(apply(&fixture, at, kind)?.dead_letters_enqueued);
        assert_dead_letters(&fixture, &[sequence])?;
        let mut command = fixture.command(at, receive_kind(ReceiveMode::PeekLock, None));
        command.entity = fixture.entity.dead_letter_queue()?;
        let received = fixture.machine.apply_with_effects(&command)?;
        assert!(!received.dead_letters_enqueued);
        let received = delivery(received.outcome).expect("dead-letter delivery");
        command.kind = CommandKind::Abandon {
            sequence,
            lock_token: received.lock.expect("lock").token,
        };
        let abandoned = fixture.machine.apply_with_effects(&command)?;
        assert!(!abandoned.dead_letters_enqueued);
        assert_eq!(
            abandoned.outcome,
            CommandOutcome::Abandoned {
                dead_lettered: false,
                dropped: false,
            }
        );
        command.kind = receive_kind(ReceiveMode::ReceiveAndDelete, None);
        let received = fixture.machine.apply_with_effects(&command)?;
        assert!(!received.dead_letters_enqueued);
        assert_eq!(
            delivery(received.outcome).expect("shadow message").sequence,
            sequence
        );
        assert!(!apply(&fixture, at, CommandKind::ExpireMessages)?.dead_letters_enqueued);
    }
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory {
            $(
                #[test]
                fn $case() -> Result<(), Box<dyn std::error::Error>> {
                    super::$case(::testkit::MemoryProvider::new())
                }
            )+
        }

        mod durable {
            $(
                #[test]
                fn $case() -> Result<(), Box<dyn std::error::Error>> {
                    super::$case(::testkit::DurableProvider::temporary()?)
                }
            )+
        }
    };
}

for_each_backend! {
    ordinary_lazy_expiry_reports_enqueues_with_or_without_a_live_delivery,
    dropping_expired_ready_and_deferred_records_has_no_enqueue_effect,
    every_deferred_api_reports_committed_lazy_expiry_in_both_modes,
    rejected_deferred_batches_do_not_commit_tentative_dead_letters,
    expired_session_holds_do_not_publish_or_commit_expiration,
    storage_failure_cannot_publish_an_effect_and_success_needs_no_shadow_queries,
    read_only_noop_and_duplicate_acknowledgements_have_no_enqueue_effect,
    explicit_automatic_and_timer_dead_letters_report_effects_but_shadow_actions_do_not,
}

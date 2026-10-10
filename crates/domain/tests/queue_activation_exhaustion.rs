//! Qualified near-MAX counter seeds followed by actual queue activation.
//! Seeds and corruptions are not reachable-history or full-state-health proof.
//! RED records the outcome/MAX record before asserting refusal; subsequent
//! conservation and reopen witnesses are unreached when that assertion fails.

use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use domain::{
    BoundCommand, BrokerError, Command, CommandKind, CommandOutcome, DurableProposal, EntityPath,
    IndexedApplyError, IndexedApplyOutcome, IndexedWriter, MessageInput, MessageState,
    NamespaceName, QueueConfig, QueueCounters, ReceiveMode, SequenceNumber, SessionId,
    StateMachine, TIMER_SCAN_LIMIT, Timestamp, codec, keys,
};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::{DurableProvider, MemoryProvider, QueueFixture, StoreProvider};

type TestResult = Result<(), Box<dyn Error>>;

fn fixture<P: StoreProvider>(
    provider: P,
    sessions: bool,
) -> Result<QueueFixture<P>, Box<dyn Error>> {
    Ok(QueueFixture::new(
        provider,
        "tenant",
        "scheduled-exhaustion",
        QueueConfig {
            requires_session: sessions,
            ..QueueConfig::default()
        },
    )?)
}

fn input(id: &str, due: u64, ttl: Option<u64>, session: Option<&SessionId>) -> MessageInput {
    MessageInput {
        message_id: id.to_owned(),
        body: id.as_bytes().to_vec(),
        scheduled_enqueue_at: Some(Timestamp::from_millis(due)),
        time_to_live_millis: ttl,
        session_id: session.cloned(),
        ..MessageInput::default()
    }
}

fn send<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    at: u64,
    message: MessageInput,
) -> Result<SequenceNumber, Box<dyn Error>> {
    let outcome = fixture.at(
        at,
        CommandKind::Send {
            message_id: message.message_id,
            body: message.body,
            time_to_live_millis: message.time_to_live_millis,
            session_id: message.session_id,
            scheduled_enqueue_at: message.scheduled_enqueue_at,
            envelope: message.envelope,
        },
    )?;
    let CommandOutcome::Sent { sequence } = outcome else {
        panic!("expected a retained scheduled message, got {outcome:?}");
    };
    Ok(sequence)
}

fn counters<P: StoreProvider>(fixture: &QueueFixture<P>) -> Result<QueueCounters, Box<dyn Error>> {
    match fixture
        .machine
        .store()
        .get(&keys::queue_counters(&fixture.namespace, &fixture.entity))?
    {
        Some(raw) => Ok(codec::decode(&raw)?),
        None => Ok(QueueCounters::default()),
    }
}

fn seed_sequence<P: StoreProvider>(fixture: &QueueFixture<P>, next: u64) -> TestResult {
    let mut value = counters(fixture)?;
    value.next_sequence = next;
    fixture.machine.store().apply(WriteBatch::default().put(
        keys::queue_counters(&fixture.namespace, &fixture.entity),
        codec::encode(&value)?,
    ))?;
    Ok(())
}

fn raw<P: StoreProvider>(fixture: &QueueFixture<P>, key: Key, value: Option<Value>) -> TestResult {
    let mut batch = WriteBatch::default();
    match value {
        Some(value) => batch.push_put(key, value),
        None => batch.push_delete(key),
    }
    fixture.machine.store().apply(batch)?;
    Ok(())
}

fn activated<P: StoreProvider>(fixture: &QueueFixture<P>, at: u64, expected: u32) -> TestResult {
    assert_eq!(
        fixture.at(at, CommandKind::ActivateScheduled)?,
        CommandOutcome::ScheduledActivated {
            activated: expected,
            deliverable_entities: (expected > 0)
                .then(|| fixture.entity.clone())
                .into_iter()
                .collect(),
        }
    );
    Ok(())
}

fn refuse<P: StoreProvider>(fixture: &QueueFixture<P>, at: u64) -> TestResult {
    let before = fixture.machine.store().snapshot()?;
    let clock = fixture.machine.last_applied_time()?;
    let original_counters = counters(fixture)?;
    let result = fixture.at(at, CommandKind::ActivateScheduled);
    let max_record = fixture.machine.message(
        &fixture.namespace,
        &fixture.entity,
        SequenceNumber::new(u64::MAX),
    )?;
    assert_eq!(
        result,
        Err(BrokerError::SequenceNumberExhausted),
        "actual activation outcome above; stored MAX record: {max_record:?}"
    );
    // All witnesses below are unreached in RED if the refusal assertion fails.
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(fixture.machine.last_applied_time()?, clock);
    assert_eq!(counters(fixture)?, original_counters);
    Ok(())
}

fn unchanged_error<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    command: &Command,
    expected: BrokerError,
) -> TestResult {
    let before = fixture.machine.store().snapshot()?;
    let clock = fixture.machine.last_applied_time()?;
    assert_eq!(fixture.machine.apply(command), Err(expected));
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(fixture.machine.last_applied_time()?, clock);
    Ok(())
}

fn restart<P: StoreProvider>(fixture: QueueFixture<P>) -> Result<QueueFixture<P>, Box<dyn Error>> {
    let before = fixture.machine.store().snapshot()?;
    // No store clone is retained: the sole machine drops before provider.open.
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(fixture)
}

fn terminal<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = fixture(provider, false)?;
    let first = send(&fixture, 10, input("first", 100, Some(50), None))?;
    let second = send(&fixture, 11, input("second", 100, Some(80), None))?;
    let ready = send(
        &fixture,
        12,
        MessageInput {
            message_id: "already-ready".to_owned(),
            body: b"already-ready".to_vec(),
            ..MessageInput::default()
        },
    )?;
    seed_sequence(&fixture, u64::MAX)?;
    refuse(&fixture, 100)?;
    assert_eq!(
        fixture
            .machine
            .scheduled_sequences(&fixture.namespace, &fixture.entity, 10)?,
        vec![first, second]
    );
    let fixture = restart(fixture)?;
    assert_eq!(
        fixture.at(
            101,
            CommandKind::CancelScheduled {
                sequences: vec![first]
            }
        )?,
        CommandOutcome::ScheduledCancelled { cancelled: 1 }
    );
    let CommandOutcome::Received(Some(delivery)) = fixture.at(
        102,
        CommandKind::Receive {
            mode: ReceiveMode::ReceiveAndDelete,
            lock_duration_millis: None,
            session: None,
        },
    )?
    else {
        panic!("the original ready message remains usable after refusal/reopen");
    };
    assert_eq!(delivery.sequence, ready);
    assert_eq!(delivery.message_id, "already-ready");
    assert_eq!(counters(&fixture)?.next_sequence, u64::MAX);
    Ok(())
}

fn fitting<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = fixture(provider, false)?;
    let first = send(&fixture, 10, input("first", 100, Some(30), None))?;
    let later = send(&fixture, 11, input("later", 200, Some(70), None))?;
    seed_sequence(&fixture, u64::MAX - 1)?;
    activated(&fixture, 100, 1)?;
    let active = SequenceNumber::new(u64::MAX - 1);
    assert_eq!(counters(&fixture)?.next_sequence, u64::MAX);
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, first)?,
        None
    );
    let record = fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, active)?
        .unwrap();
    assert_eq!(record.message_id, "first");
    assert_eq!(record.state, MessageState::Ready);
    assert_eq!(record.enqueued_at, Timestamp::from_millis(100));
    assert_eq!(
        record.scheduled_enqueue_at,
        Some(Timestamp::from_millis(100))
    );
    assert_eq!(record.expires_at, Some(Timestamp::from_millis(130)));
    assert_eq!(
        fixture
            .machine
            .store()
            .get(&keys::ready(&fixture.namespace, &fixture.entity, active))?,
        Some(Vec::new())
    );
    assert_eq!(
        fixture.machine.store().get(&keys::expiry(
            &fixture.namespace,
            &fixture.entity,
            Timestamp::from_millis(130),
            active,
        ))?,
        Some(Vec::new())
    );
    let before_idle = fixture.machine.store().snapshot()?;
    activated(&fixture, 199, 0)?;
    assert_eq!(fixture.machine.store().snapshot()?, before_idle);
    refuse(&fixture, 200)?;
    let fixture = restart(fixture)?;
    assert_eq!(
        fixture.at(
            201,
            CommandKind::CancelScheduled {
                sequences: vec![later]
            }
        )?,
        CommandOutcome::ScheduledCancelled { cancelled: 1 }
    );
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, active)?,
        Some(record)
    );
    assert_eq!(counters(&fixture)?.next_sequence, u64::MAX);
    Ok(())
}

fn crossing<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = fixture(provider, true)?;
    let session = SessionId::new("Case-Sensitive")?;
    let first = send(&fixture, 10, input("first", 100, Some(30), Some(&session)))?;
    let second = send(&fixture, 11, input("second", 100, Some(70), Some(&session)))?;
    let CommandOutcome::SessionAccepted(Some(accepted)) = fixture.at(
        20,
        CommandKind::AcceptSession {
            session_id: Some(session.clone()),
            lock_duration_millis: Some(1_000),
        },
    )?
    else {
        panic!("a named empty session acquires an original hold");
    };
    seed_sequence(&fixture, u64::MAX - 1)?;
    let original_counters = counters(&fixture)?;
    let original_session =
        fixture
            .machine
            .session(&fixture.namespace, &fixture.entity, &session)?;
    refuse(&fixture, 100)?;
    assert_eq!(counters(&fixture)?, original_counters);
    assert_eq!(
        fixture
            .machine
            .session(&fixture.namespace, &fixture.entity, &session)?,
        original_session
    );
    assert_eq!(
        fixture.machine.session_ready_sequences(
            &fixture.namespace,
            &fixture.entity,
            &session,
            10
        )?,
        Vec::new()
    );
    assert_eq!(
        fixture.machine.store().get(&keys::expiry(
            &fixture.namespace,
            &fixture.entity,
            Timestamp::from_millis(130),
            SequenceNumber::new(u64::MAX - 1),
        ))?,
        None
    );
    let fixture = restart(fixture)?;
    assert_eq!(
        fixture.at(
            101,
            CommandKind::CancelScheduled {
                sequences: vec![first, second]
            }
        )?,
        CommandOutcome::ScheduledCancelled { cancelled: 2 }
    );
    assert_eq!(
        fixture.at(
            102,
            CommandKind::ReleaseSession {
                session: accepted.hold()
            }
        )?,
        CommandOutcome::SessionReleased
    );
    assert_eq!(counters(&fixture)?.next_sequence, u64::MAX - 1);
    assert_eq!(
        counters(&fixture)?.next_lock_token,
        original_counters.next_lock_token
    );
    Ok(())
}

fn idle_cancel<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = fixture(provider, false)?;
    let counter_key = keys::queue_counters(&fixture.namespace, &fixture.entity);
    assert_eq!(
        fixture.machine.store().get(&counter_key)?,
        None,
        "counters are lazy"
    );
    let before = fixture.machine.store().snapshot()?;
    activated(&fixture, 10, 0)?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(counters(&fixture)?, QueueCounters::default());
    let future = send(&fixture, 20, input("future", 1_000, Some(50), None))?;
    seed_sequence(&fixture, u64::MAX)?;
    let before = fixture.machine.store().snapshot()?;
    activated(&fixture, 999, 0)?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(
        fixture.machine.last_applied_time()?,
        Timestamp::from_millis(20)
    );
    assert_eq!(
        fixture.at(
            100,
            CommandKind::CancelScheduled {
                sequences: vec![future]
            }
        )?,
        CommandOutcome::ScheduledCancelled { cancelled: 1 }
    );
    assert_eq!(counters(&fixture)?.next_sequence, u64::MAX);
    let before = fixture.machine.store().snapshot()?;
    activated(&fixture, 1_001, 0)?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    let fixture = restart(fixture)?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

fn bounded_scan<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = fixture(provider, false)?;
    let messages = (0..=TIMER_SCAN_LIMIT)
        .map(|index| input(&format!("scheduled-{index}"), 100, Some(40), None))
        .collect();
    let CommandOutcome::BatchSent { sequences, stored } =
        fixture.at(10, CommandKind::SendBatch { messages })?
    else {
        panic!("all ordinary placeholders are created before the qualified seed");
    };
    assert_eq!(stored as usize, TIMER_SCAN_LIMIT + 1);
    let start = u64::MAX - u64::try_from(TIMER_SCAN_LIMIT)?;
    seed_sequence(&fixture, start)?;
    activated(&fixture, 100, u32::try_from(TIMER_SCAN_LIMIT)?)?;
    assert_eq!(counters(&fixture)?.next_sequence, u64::MAX);
    let remaining = *sequences.last().unwrap();
    assert_eq!(
        fixture
            .machine
            .scheduled_sequences(&fixture.namespace, &fixture.entity, 10)?,
        vec![remaining]
    );
    let ready = fixture.machine.ready_sequences(
        &fixture.namespace,
        &fixture.entity,
        TIMER_SCAN_LIMIT + 1,
    )?;
    assert_eq!(ready.len(), TIMER_SCAN_LIMIT);
    assert_eq!(ready.first(), Some(&SequenceNumber::new(start)));
    assert_eq!(ready.last(), Some(&SequenceNumber::new(u64::MAX - 1)));
    assert_eq!(
        fixture.machine.message(
            &fixture.namespace,
            &fixture.entity,
            SequenceNumber::new(u64::MAX)
        )?,
        None
    );
    refuse(&fixture, 100)?;
    let fixture = restart(fixture)?;
    assert_eq!(
        fixture.at(
            101,
            CommandKind::CancelScheduled {
                sequences: vec![remaining]
            }
        )?,
        CommandOutcome::ScheduledCancelled { cancelled: 1 }
    );
    assert_eq!(counters(&fixture)?.next_sequence, u64::MAX);
    Ok(())
}

fn priority<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = fixture(provider, false)?;
    let first = send(&fixture, 10, input("first", 100, Some(30), None))?;
    let second = send(&fixture, 20, input("second", 100, Some(70), None))?;
    seed_sequence(&fixture, u64::MAX)?;
    unchanged_error(
        &fixture,
        &fixture.command(19, CommandKind::ActivateScheduled),
        BrokerError::ClockRegression {
            last_applied: Timestamp::from_millis(20),
            proposed: Timestamp::from_millis(19),
        },
    )?;
    let missing = Command::new(
        fixture.namespace.clone(),
        EntityPath::new("missing")?,
        Timestamp::from_millis(100),
        CommandKind::ActivateScheduled,
    );
    unchanged_error(&fixture, &missing, BrokerError::QueueNotFound)?;
    let key = keys::message(&fixture.namespace, &fixture.entity, first);
    let original_raw = fixture.machine.store().get(&key)?.unwrap();
    let original = fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, first)?
        .unwrap();
    let mut wrong_state = original.clone();
    wrong_state.state = MessageState::Ready;
    let mut missing_deadline = original.clone();
    missing_deadline.scheduled_enqueue_at = None;
    let mut wrong_deadline = original.clone();
    wrong_deadline.scheduled_enqueue_at = Some(Timestamp::from_millis(101));
    // These are injected record shapes, not supported transition histories.
    for (record, expected) in [
        (
            wrong_state,
            BrokerError::MessageNotScheduled { sequence: first },
        ),
        (
            missing_deadline,
            BrokerError::ScheduledEnqueueTimeMissing { sequence: first },
        ),
        (wrong_deadline, BrokerError::MalformedIndexKey),
    ] {
        raw(&fixture, key.clone(), Some(codec::encode(&record)?))?;
        unchanged_error(
            &fixture,
            &fixture.command(100, CommandKind::ActivateScheduled),
            expected,
        )?;
        raw(&fixture, key.clone(), Some(original_raw.clone()))?;
    }
    raw(&fixture, key.clone(), None)?;
    unchanged_error(
        &fixture,
        &fixture.command(100, CommandKind::ActivateScheduled),
        BrokerError::DanglingIndexEntry { sequence: first },
    )?;
    raw(&fixture, key, Some(original_raw))?;
    let counter_key = keys::queue_counters(&fixture.namespace, &fixture.entity);
    let original_counter = fixture.machine.store().get(&counter_key)?.unwrap();
    let malformed = vec![0];
    let error = codec::decode::<QueueCounters>(&malformed).expect_err("truncated V1 counters");
    raw(&fixture, counter_key.clone(), Some(malformed))?;
    unchanged_error(
        &fixture,
        &fixture.command(100, CommandKind::ActivateScheduled),
        BrokerError::Codec(error),
    )?;
    raw(&fixture, counter_key, Some(original_counter))?;
    let second_key = keys::message(&fixture.namespace, &fixture.entity, second);
    let mut second_record = fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, second)?
        .unwrap();
    second_record.state = MessageState::Ready;
    raw(&fixture, second_key, Some(codec::encode(&second_record)?))?;
    // The first valid due row reaches exhaustion before a later injected fault.
    refuse(&fixture, 100)?;
    let fixture = restart(fixture)?;
    assert_eq!(counters(&fixture)?.next_sequence, u64::MAX);
    Ok(())
}

#[derive(Default)]
struct ReadProbe {
    deny: AtomicBool,
    applies: AtomicUsize,
}

#[derive(Clone)]
struct Observed<S> {
    inner: S,
    probe: Arc<ReadProbe>,
}

impl<S: StateStore> Observed<S> {
    fn read_allowed(&self) -> Result<(), StorageError> {
        if self.probe.deny.load(Ordering::SeqCst) {
            Err(StorageError::Backend {
                operation: "read during latest duplicate",
                detail: "qualified activation read-free witness".to_owned(),
            })
        } else {
            Ok(())
        }
    }
}

impl<S: StateStore> StateStore for Observed<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.read_allowed()?;
        self.inner.get(key)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.probe.applies.fetch_add(1, Ordering::SeqCst);
        self.inner.apply(batch)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.read_allowed()?;
        self.inner.snapshot()
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.read_allowed()?;
        self.inner.scan_from(prefix, start, limit)
    }
}

fn domain_rows(image: &StoreSnapshot) -> Vec<(Key, Value)> {
    image
        .entries()
        .iter()
        .filter(|(key, _)| key.first() != Some(&0xF1))
        .cloned()
        .collect()
}

fn indexed<P: StoreProvider>(provider: P) -> TestResult {
    let store = provider.open()?;
    let probe = Arc::new(ReadProbe::default());
    let mut writer = IndexedWriter::open(Observed {
        inner: store.clone(),
        probe: probe.clone(),
    })?;
    let namespace = NamespaceName::new("tenant")?;
    let entity = EntityPath::new("indexed-scheduled")?;
    let command = |at, kind| {
        Command::new(
            namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(at),
            kind,
        )
    };
    assert_eq!(
        writer.apply(
            1,
            &DurableProposal::unbound(command(
                10,
                CommandKind::CreateQueue {
                    config: QueueConfig::default()
                }
            ))
        )?,
        IndexedApplyOutcome::Applied(CommandOutcome::QueueCreated)
    );
    let mut ready = input("ready", 100, None, None);
    ready.scheduled_enqueue_at = None;
    assert_eq!(
        writer.apply(
            2,
            &DurableProposal::unbound(command(
                20,
                CommandKind::SendBatch {
                    messages: vec![
                        input("first", 100, Some(30), None),
                        input("second", 100, Some(70), None),
                        ready
                    ],
                }
            ))
        )?,
        IndexedApplyOutcome::Applied(CommandOutcome::BatchSent {
            sequences: vec![
                SequenceNumber::new(1),
                SequenceNumber::new(2),
                SequenceNumber::new(3)
            ],
            stored: 3,
        })
    );
    // Bootstrap completes before raw seeding; every writer/raw operation is serial.
    let machine = StateMachine::new(store.clone());
    let binding = machine.bind_entity(&namespace, &entity)?;
    drop(machine);
    let counter_key = keys::queue_counters(&namespace, &entity);
    let mut value: QueueCounters = codec::decode(&store.get(&counter_key)?.unwrap())?;
    value.next_sequence = u64::MAX;
    store.apply(WriteBatch::default().put(counter_key, codec::encode(&value)?))?;
    let before = store.snapshot()?;
    let proposal = DurableProposal::bound(BoundCommand::new(
        binding.clone(),
        command(100, CommandKind::ActivateScheduled),
    ))?;
    let result = writer.apply(3, &proposal);
    let max_record = StateMachine::new(store.clone()).message(
        &namespace,
        &entity,
        SequenceNumber::new(u64::MAX),
    )?;
    assert!(
        matches!(
            &result,
            Ok(IndexedApplyOutcome::Refused(
                BrokerError::SequenceNumberExhausted
            ))
        ),
        "actual indexed activation: {result:?}; stored MAX record: {max_record:?}"
    );
    // Checkpoint/read-free/reopen witnesses are unreached if the RED assertion fails.
    assert_eq!(
        result?,
        IndexedApplyOutcome::Refused(BrokerError::SequenceNumberExhausted)
    );
    assert_eq!(writer.applied_index()?, 3);
    let after = store.snapshot()?;
    assert_eq!(domain_rows(&after), domain_rows(&before));
    let metadata: Vec<_> = after
        .entries()
        .iter()
        .filter(|(key, _)| key.first() == Some(&0xF1))
        .collect();
    assert_eq!(metadata.len(), 2);
    assert_ne!(
        after, before,
        "F1 checkpoint intentionally advances on refusal"
    );
    let applies = probe.applies.load(Ordering::SeqCst);
    probe.deny.store(true, Ordering::SeqCst);
    assert_eq!(
        writer.apply(3, &proposal)?,
        IndexedApplyOutcome::AlreadyApplied
    );
    assert_eq!(
        writer.apply(
            3,
            &DurableProposal::bound(BoundCommand::new(
                binding.clone(),
                command(101, CommandKind::ActivateScheduled)
            ))?
        ),
        Err(IndexedApplyError::ConflictingProposal { index: 3 })
    );
    assert_eq!(probe.applies.load(Ordering::SeqCst), applies);
    probe.deny.store(false, Ordering::SeqCst);
    assert_eq!(store.snapshot()?, after);
    // Drop the writer's clone AND the raw handle before real Fjall directory open.
    drop(writer);
    drop(store);
    let store = provider.open()?;
    assert_eq!(store.snapshot()?, after);
    let mut writer = IndexedWriter::open(Observed {
        inner: store.clone(),
        probe: probe.clone(),
    })?;
    probe.deny.store(true, Ordering::SeqCst);
    assert_eq!(
        writer.apply(3, &proposal)?,
        IndexedApplyOutcome::AlreadyApplied
    );
    probe.deny.store(false, Ordering::SeqCst);
    let first_key = keys::message(&namespace, &entity, SequenceNumber::new(1));
    let original_raw = store.get(&first_key)?.unwrap();
    let malformed = vec![0];
    let error =
        codec::decode::<domain::MessageRecord>(&malformed).expect_err("truncated V1 message");
    store.apply(WriteBatch::default().put(first_key.clone(), malformed))?;
    let corrupt_before = store.snapshot()?;
    assert_eq!(
        writer.apply(
            4,
            &DurableProposal::bound(BoundCommand::new(
                binding.clone(),
                command(101, CommandKind::ActivateScheduled)
            ))?
        ),
        Err(IndexedApplyError::Domain(BrokerError::Codec(error)))
    );
    assert_eq!(writer.applied_index()?, 3);
    assert_eq!(
        store.snapshot()?,
        corrupt_before,
        "unmarked corruption does not checkpoint"
    );
    store.apply(WriteBatch::default().put(first_key, original_raw))?;
    assert_eq!(store.snapshot()?, after);
    assert_eq!(
        writer.apply(
            4,
            &DurableProposal::bound(BoundCommand::new(
                binding,
                command(
                    101,
                    CommandKind::CancelScheduled {
                        sequences: vec![SequenceNumber::new(1), SequenceNumber::new(2)],
                    }
                )
            ))?
        )?,
        IndexedApplyOutcome::Applied(CommandOutcome::ScheduledCancelled { cancelled: 2 })
    );
    let IndexedApplyOutcome::Applied(CommandOutcome::Received(Some(delivery))) = writer.apply(
        5,
        &DurableProposal::unbound(command(
            102,
            CommandKind::Receive {
                mode: ReceiveMode::ReceiveAndDelete,
                lock_duration_millis: None,
                session: None,
            },
        )),
    )?
    else {
        panic!("the original ready message remains usable after refusal/reopen");
    };
    assert_eq!(delivery.sequence, SequenceNumber::new(3));
    assert_eq!(delivery.message_id, "ready");
    drop(writer);
    drop(store);
    Ok(())
}

#[test]
fn memory_qualified_terminal_activation_refuses_without_effects() -> TestResult {
    terminal(MemoryProvider::new())
}

#[test]
fn fjall_qualified_terminal_activation_refuses_without_effects() -> TestResult {
    terminal(DurableProvider::temporary()?)
}

#[test]
fn memory_qualified_final_slot_preserves_ttl_and_later_refusal() -> TestResult {
    fitting(MemoryProvider::new())
}

#[test]
fn fjall_qualified_final_slot_preserves_ttl_and_later_refusal() -> TestResult {
    fitting(DurableProvider::temporary()?)
}

#[test]
fn memory_qualified_crossing_due_batch_preserves_session_and_whole_image() -> TestResult {
    crossing(MemoryProvider::new())
}

#[test]
fn fjall_qualified_crossing_due_batch_preserves_session_and_whole_image() -> TestResult {
    crossing(DurableProvider::temporary()?)
}

#[test]
fn memory_qualified_idle_cancel_and_lazy_counters_need_no_sequence() -> TestResult {
    idle_cancel(MemoryProvider::new())
}

#[test]
fn fjall_qualified_idle_cancel_and_lazy_counters_need_no_sequence() -> TestResult {
    idle_cancel(DurableProvider::temporary()?)
}

#[test]
fn memory_qualified_bounded_due_scan_exhausts_then_refuses_remaining() -> TestResult {
    bounded_scan(MemoryProvider::new())
}

#[test]
fn fjall_qualified_bounded_due_scan_exhausts_then_refuses_remaining() -> TestResult {
    bounded_scan(DurableProvider::temporary()?)
}

#[test]
fn memory_qualified_due_row_priority_and_corruption_remain_exact() -> TestResult {
    priority(MemoryProvider::new())
}

#[test]
fn fjall_qualified_due_row_priority_and_corruption_remain_exact() -> TestResult {
    priority(DurableProvider::temporary()?)
}

#[test]
fn memory_qualified_indexed_refusal_checkpoints_without_domain_effects() -> TestResult {
    indexed(MemoryProvider::new())
}

#[test]
fn fjall_qualified_indexed_refusal_checkpoints_without_domain_effects() -> TestResult {
    indexed(DurableProvider::temporary()?)
}

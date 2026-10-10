//! Qualified near-MAX counter seeds followed by ordinary queue operations.
//! The seeds are not reachable-history or full-state-health evidence. RED first
//! asserts refusal without naming the not-yet-added error variant; later checks
//! are deliberately unreached if that first assertion fails.

use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use domain::{
    BoundCommand, BrokerError, CodecError, Command, CommandKind, CommandOutcome, DurableProposal,
    EntityPath, IndexedApplyError, IndexedApplyOutcome, IndexedWriter, MAX_MESSAGE_ID_CHARACTERS,
    MessageEnvelope, MessageInput, MessageState, NamespaceName, QueueConfig, QueueCounters,
    ReceiveMode, SequenceNumber, SessionId, StateMachine, Timestamp, codec, keys,
};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::{DurableProvider, MemoryProvider, QueueFixture, StoreProvider};

type TestResult = Result<(), Box<dyn Error>>;
const EXHAUSTED: &str = "sequence number space exhausted";

fn input(id: &str) -> MessageInput {
    MessageInput {
        message_id: id.to_owned(),
        body: b"body".to_vec(),
        ..MessageInput::default()
    }
}

fn send(message: MessageInput) -> CommandKind {
    CommandKind::Send {
        message_id: message.message_id,
        body: message.body,
        time_to_live_millis: message.time_to_live_millis,
        session_id: message.session_id,
        scheduled_enqueue_at: message.scheduled_enqueue_at,
        envelope: message.envelope,
    }
}

fn counters<P: StoreProvider>(fixture: &QueueFixture<P>) -> Result<QueueCounters, Box<dyn Error>> {
    // Newly created queues may not yet have their lazy counters.
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

fn assert_exhausted(result: Result<CommandOutcome, BrokerError>) {
    // This is the intended first RED failure, not a match on an absent variant.
    assert!(
        result.is_err(),
        "expected sequence exhaustion refusal, got {result:?}"
    );
    assert_eq!(result.unwrap_err().to_string(), EXHAUSTED);
}

fn refuse<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    kind: CommandKind,
) -> TestResult {
    let before = fixture.machine.store().snapshot()?;
    let clock = fixture.machine.last_applied_time()?;
    let original_counters = counters(fixture)?;
    assert_exhausted(fixture.at(millis, kind));
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(fixture.machine.last_applied_time()?, clock);
    assert_eq!(counters(fixture)?, original_counters);
    Ok(())
}

fn restart_unchanged<P: StoreProvider>(
    fixture: QueueFixture<P>,
) -> Result<QueueFixture<P>, Box<dyn Error>> {
    let before = fixture.machine.store().snapshot()?;
    // QueueFixture consumes and drops its sole machine/store before reopening.
    // Memory only reopens a shared logical handle; Fjall reopens its directory.
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(fixture)
}

fn terminal_send<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    let CommandOutcome::Sent { sequence } = fixture.at(10, send(input("held")))? else {
        panic!("expected the ordinary setup send");
    };
    let CommandOutcome::Received(Some(delivery)) = fixture.at(
        20,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
    )?
    else {
        panic!("expected the setup delivery");
    };
    let lock = delivery.lock.expect("the original delivery is held");
    let record = fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, sequence)?;
    let lock_key = keys::lock(
        &fixture.namespace,
        &fixture.entity,
        lock.locked_until,
        sequence,
    );
    seed_sequence(&fixture, u64::MAX)?;
    let binding = fixture
        .machine
        .bind_entity(&fixture.namespace, &fixture.entity)?;
    let before = fixture.machine.store().snapshot()?;
    for scheduled in [None, Some(Timestamp::from_millis(1_000))] {
        let mut message = input("must-not-store");
        message.time_to_live_millis = Some(13);
        message.scheduled_enqueue_at = scheduled;
        message.envelope = Some(MessageEnvelope::new(b"opaque envelope".to_vec()));
        refuse(&fixture, 30, send(message.clone()))?;
        let bound = BoundCommand::new(binding.clone(), fixture.command(30, send(message)));
        assert_exhausted(fixture.machine.apply_bound(&bound));
        assert_eq!(fixture.machine.store().snapshot()?, before);
    }
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, sequence)?,
        record
    );
    assert_eq!(fixture.machine.store().get(&lock_key)?, Some(Vec::new()));
    assert_eq!(
        fixture.machine.message(
            &fixture.namespace,
            &fixture.entity,
            SequenceNumber::new(u64::MAX)
        )?,
        None
    );
    let fixture = restart_unchanged(fixture)?;
    refuse(&fixture, 30, send(input("after-reopen")))?;
    assert_eq!(
        fixture.at(
            40,
            CommandKind::Complete {
                sequence,
                lock_token: lock.token
            }
        )?,
        CommandOutcome::Completed
    );
    assert_eq!(fixture.machine.store().get(&lock_key)?, None);
    assert_eq!(counters(&fixture)?.next_sequence, u64::MAX);
    Ok(())
}

fn last_slot<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    seed_sequence(&fixture, u64::MAX - 1)?;
    let mut message = input("last");
    message.time_to_live_millis = Some(1_000);
    message.envelope = Some(MessageEnvelope::new(vec![0, 255, 7]));
    let sequence = SequenceNumber::new(u64::MAX - 1);
    assert_eq!(
        fixture.at(10, send(message.clone()))?,
        CommandOutcome::Sent { sequence }
    );
    let record = fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, sequence)?
        .unwrap();
    assert_eq!(record.body, message.body);
    assert_eq!(record.envelope, message.envelope);
    assert_eq!(record.expires_at, Some(Timestamp::from_millis(1_010)));
    assert_eq!(record.state, MessageState::Ready);
    assert_eq!(
        fixture.machine.store().get(&keys::expiry(
            &fixture.namespace,
            &fixture.entity,
            Timestamp::from_millis(1_010),
            sequence
        ))?,
        Some(Vec::new())
    );
    assert_eq!(counters(&fixture)?.next_sequence, u64::MAX);
    refuse(&fixture, 20, send(input("past-last")))?;
    let fixture = restart_unchanged(fixture)?;
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, sequence)?,
        Some(record)
    );
    let CommandOutcome::Received(Some(delivery)) = fixture.at(
        30,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
    )?
    else {
        panic!("the last allocatable message remains receivable");
    };
    assert_eq!(delivery.sequence, sequence);
    assert_eq!(
        fixture.at(
            40,
            CommandKind::Complete {
                sequence,
                lock_token: delivery.lock.unwrap().token,
            }
        )?,
        CommandOutcome::Completed
    );
    assert_eq!(counters(&fixture)?.next_sequence, u64::MAX);
    Ok(())
}

fn fitting_batch<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "orders")?;
    seed_sequence(&fixture, u64::MAX - 2)?;
    let mut ready = input("ready");
    ready.time_to_live_millis = Some(1_000);
    let mut scheduled = input("scheduled");
    scheduled.scheduled_enqueue_at = Some(Timestamp::from_millis(1_000));
    scheduled.time_to_live_millis = Some(13);
    let first = SequenceNumber::new(u64::MAX - 2);
    let second = SequenceNumber::new(u64::MAX - 1);
    assert_eq!(
        fixture.at(
            10,
            CommandKind::SendBatch {
                messages: vec![ready, scheduled]
            }
        )?,
        CommandOutcome::BatchSent {
            sequences: vec![first, second],
            stored: 2,
        }
    );
    assert_eq!(counters(&fixture)?.next_sequence, u64::MAX);
    assert_eq!(
        fixture
            .machine
            .ready_sequences(&fixture.namespace, &fixture.entity, 10)?,
        vec![first]
    );
    assert_eq!(
        fixture
            .machine
            .scheduled_sequences(&fixture.namespace, &fixture.entity, 10)?,
        vec![second]
    );
    let placeholder = fixture
        .machine
        .message(&fixture.namespace, &fixture.entity, second)?
        .unwrap();
    assert_eq!(placeholder.state, MessageState::Scheduled);
    assert_eq!(placeholder.expires_at, Some(Timestamp::from_millis(1_013)));
    assert_eq!(
        fixture.machine.store().get(&keys::expiry(
            &fixture.namespace,
            &fixture.entity,
            Timestamp::from_millis(1_013),
            second
        ))?,
        None
    );
    refuse(
        &fixture,
        20,
        CommandKind::SendBatch {
            messages: vec![input("past-batch")],
        },
    )?;
    let fixture = restart_unchanged(fixture)?;
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, second)?,
        Some(placeholder)
    );
    assert_eq!(
        fixture.at(
            30,
            CommandKind::CancelScheduled {
                sequences: vec![second]
            }
        )?,
        CommandOutcome::ScheduledCancelled { cancelled: 1 }
    );
    assert_eq!(
        fixture
            .machine
            .message(&fixture.namespace, &fixture.entity, second)?,
        None
    );
    assert_eq!(counters(&fixture)?.next_sequence, u64::MAX);
    Ok(())
}

fn duplicate_config() -> QueueConfig {
    QueueConfig {
        requires_duplicate_detection: true,
        duplicate_detection_history_millis: 20_000,
        ..QueueConfig::default()
    }
}

fn crossing_batches<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = QueueFixture::new(provider, "tenant", "orders", duplicate_config())?;
    fixture.at(10, send(input("stale")))?;
    fixture.at(30_000, send(input("seen")))?;
    let stale_deadline =
        fixture
            .machine
            .duplicate_history_deadline(&fixture.namespace, &fixture.entity, "stale")?;
    let seen_deadline =
        fixture
            .machine
            .duplicate_history_deadline(&fixture.namespace, &fixture.entity, "seen")?;
    assert_eq!(stale_deadline, Some(Timestamp::from_millis(20_010)));
    assert_eq!(seen_deadline, Some(Timestamp::from_millis(50_000)));
    seed_sequence(&fixture, u64::MAX - 1)?;
    let mut ready = input("new-ready");
    ready.time_to_live_millis = Some(1);
    let mut scheduled = input("new-scheduled");
    scheduled.scheduled_enqueue_at = Some(Timestamp::from_millis(70_000));
    scheduled.time_to_live_millis = Some(2);
    for messages in [
        vec![ready, scheduled],
        vec![input("stale"), input("new"), input("seen")],
        vec![input("seen"), input("seen")],
    ] {
        refuse(&fixture, 30_001, CommandKind::SendBatch { messages })?;
        assert_eq!(
            fixture.machine.duplicate_history_deadline(
                &fixture.namespace,
                &fixture.entity,
                "stale"
            )?,
            stale_deadline
        );
        assert_eq!(
            fixture.machine.duplicate_history_deadline(
                &fixture.namespace,
                &fixture.entity,
                "seen"
            )?,
            seen_deadline
        );
        for id in ["new-ready", "new-scheduled", "new"] {
            assert_eq!(
                fixture.machine.duplicate_history_deadline(
                    &fixture.namespace,
                    &fixture.entity,
                    id
                )?,
                None
            );
        }
        for raw in [u64::MAX - 1, u64::MAX] {
            assert_eq!(
                fixture.machine.message(
                    &fixture.namespace,
                    &fixture.entity,
                    SequenceNumber::new(raw)
                )?,
                None
            );
        }
    }
    let fixture = restart_unchanged(fixture)?;
    refuse(
        &fixture,
        30_001,
        CommandKind::SendBatch {
            messages: vec![input("after"), input("reopen")],
        },
    )?;
    Ok(())
}

fn duplicate_ack_slots<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = QueueFixture::new(provider, "tenant", "orders", duplicate_config())?;
    fixture.at(10, send(input("seen")))?;
    let deadline =
        fixture
            .machine
            .duplicate_history_deadline(&fixture.namespace, &fixture.entity, "seen")?;
    seed_sequence(&fixture, u64::MAX - 2)?;
    let sequences = vec![
        SequenceNumber::new(u64::MAX - 2),
        SequenceNumber::new(u64::MAX - 1),
    ];
    assert_eq!(
        fixture.at(
            20,
            CommandKind::SendBatch {
                messages: vec![input("seen"), input("seen")]
            }
        )?,
        CommandOutcome::BatchSent {
            sequences: sequences.clone(),
            stored: 0,
        }
    );
    for sequence in sequences {
        assert_eq!(
            fixture
                .machine
                .message(&fixture.namespace, &fixture.entity, sequence)?,
            None
        );
    }
    assert_eq!(counters(&fixture)?.next_sequence, u64::MAX);
    assert_eq!(
        fixture.machine.last_applied_time()?,
        Timestamp::from_millis(20)
    );
    assert_eq!(
        fixture
            .machine
            .duplicate_history_deadline(&fixture.namespace, &fixture.entity, "seen")?,
        deadline
    );
    refuse(&fixture, 30, send(input("seen")))?;
    refuse(
        &fixture,
        30,
        CommandKind::SendBatch {
            messages: vec![input("seen"), input("seen")],
        },
    )?;
    let fixture = restart_unchanged(fixture)?;
    assert_eq!(
        fixture
            .machine
            .ready_sequences(&fixture.namespace, &fixture.entity, 10)?,
        vec![SequenceNumber::new(1)]
    );
    assert_eq!(
        fixture
            .machine
            .duplicate_history_deadline(&fixture.namespace, &fixture.entity, "seen")?,
        deadline
    );
    Ok(())
}

fn priority<P: StoreProvider>(provider: P) -> TestResult {
    let fixture = QueueFixture::new(
        provider,
        "tenant",
        "orders",
        QueueConfig {
            max_message_bytes: 4,
            ..duplicate_config()
        },
    )?;
    fixture.at(10, send(input("seen")))?;
    seed_sequence(&fixture, u64::MAX)?;
    let before = fixture.machine.store().snapshot()?;
    let mut oversized = input("must-not-stage");
    oversized.body = vec![0; 5];
    let mut wrong_session = input("session");
    wrong_session.session_id = Some(SessionId::new("A")?);
    let long_id = input(&"x".repeat(MAX_MESSAGE_ID_CHARACTERS + 1));
    for (millis, kind, error) in [
        (
            9,
            send(input("new")),
            BrokerError::ClockRegression {
                last_applied: Timestamp::from_millis(10),
                proposed: Timestamp::from_millis(9),
            },
        ),
        (
            20,
            CommandKind::SendBatch { messages: vec![] },
            BrokerError::EmptyMessageBatch,
        ),
        (
            20,
            CommandKind::SendBatch {
                messages: vec![input("valid"), oversized],
            },
            BrokerError::MessageTooLarge {
                body_bytes: 5,
                maximum_bytes: 4,
            },
        ),
        (20, send(wrong_session), BrokerError::SessionNotSupported),
        (
            20,
            send(long_id),
            BrokerError::MessageIdTooLong {
                characters: MAX_MESSAGE_ID_CHARACTERS + 1,
                maximum: MAX_MESSAGE_ID_CHARACTERS,
            },
        ),
    ] {
        assert_eq!(fixture.at(millis, kind), Err(error));
        assert_eq!(fixture.machine.store().snapshot()?, before);
    }
    let binding = fixture
        .machine
        .bind_entity(&fixture.namespace, &fixture.entity)?;
    let mismatch = BoundCommand::new(
        binding,
        Command::new(
            fixture.namespace.clone(),
            EntityPath::new("other")?,
            Timestamp::from_millis(9),
            send(input("new")),
        ),
    );
    assert_eq!(
        fixture.machine.apply_bound(&mismatch),
        Err(BrokerError::InvalidEntityBinding)
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);

    // Controlled undecodable values, not absent counters or healthy-image claims.
    let history_key = keys::duplicate_id(&fixture.namespace, &fixture.entity, "seen");
    let original_history = fixture.machine.store().get(&history_key)?.unwrap();
    let counter_key = keys::queue_counters(&fixture.namespace, &fixture.entity);
    let original_counter = fixture.machine.store().get(&counter_key)?.unwrap();
    fixture.machine.store().apply(
        WriteBatch::default()
            .put(history_key.clone(), vec![99])
            .put(counter_key.clone(), Vec::new()),
    )?;
    let malformed = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture.at(20, send(input("seen"))),
        Err(BrokerError::Codec(CodecError::UnsupportedVersion {
            version: 99
        }))
    );
    assert_eq!(fixture.machine.store().snapshot()?, malformed);
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().put(history_key, original_history))?;
    let malformed_counter = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture.at(20, send(input("new"))),
        Err(BrokerError::Codec(CodecError::EmptyEnvelope))
    );
    assert_eq!(fixture.machine.store().snapshot()?, malformed_counter);
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().put(counter_key, original_counter))?;
    assert_eq!(fixture.machine.store().snapshot()?, before);

    let session_entity = EntityPath::new("sessions")?;
    let session_command = |kind| {
        Command::new(
            fixture.namespace.clone(),
            session_entity.clone(),
            Timestamp::from_millis(30),
            kind,
        )
    };
    fixture.machine.apply(&Command::new(
        fixture.namespace.clone(),
        session_entity.clone(),
        Timestamp::from_millis(20),
        CommandKind::CreateQueue {
            config: QueueConfig {
                requires_session: true,
                ..QueueConfig::default()
            },
        },
    ))?;
    let session_counter = keys::queue_counters(&fixture.namespace, &session_entity);
    let mut value: QueueCounters = match fixture.machine.store().get(&session_counter)? {
        Some(raw) => codec::decode(&raw)?,
        None => QueueCounters::default(),
    };
    value.next_sequence = u64::MAX;
    fixture
        .machine
        .store()
        .apply(WriteBatch::default().put(session_counter, codec::encode(&value)?))?;
    let before = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture
            .machine
            .apply(&session_command(send(input("missing-session")))),
        Err(BrokerError::SessionRequired)
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    let mut first = input("A");
    first.session_id = Some(SessionId::new("A")?);
    let mut second = input("B");
    second.session_id = Some(SessionId::new("B")?);
    assert_eq!(
        fixture
            .machine
            .apply(&session_command(CommandKind::SendBatch {
                messages: vec![first, second]
            })),
        Err(BrokerError::MessageBatchSessionMismatch)
    );
    assert_eq!(fixture.machine.store().snapshot()?, before);
    refuse(&fixture, 30, send(input("new")))?;
    let fixture = restart_unchanged(fixture)?;
    assert_eq!(fixture.machine.store().snapshot()?, before);
    Ok(())
}

#[derive(Default)]
struct ReadGate {
    deny: AtomicBool,
    applies: AtomicUsize,
}

#[derive(Clone)]
struct Observed<S> {
    inner: S,
    gate: Arc<ReadGate>,
}

impl<S: StateStore> Observed<S> {
    fn read_allowed(&self) -> Result<(), StorageError> {
        if self.gate.deny.load(Ordering::SeqCst) {
            Err(StorageError::Backend {
                operation: "read during latest duplicate",
                detail: "qualified read-free witness".to_owned(),
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
        self.gate.applies.fetch_add(1, Ordering::SeqCst);
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

fn indexed_refusal<P: StoreProvider>(provider: P) -> TestResult {
    // Raw seeding and every owner operation are sequential under exclusive writes.
    let store = provider.open()?;
    let gate = Arc::new(ReadGate::default());
    let mut writer = IndexedWriter::open(Observed {
        inner: store.clone(),
        gate: gate.clone(),
    })?;
    let namespace = NamespaceName::new("tenant")?;
    let entity = EntityPath::new("orders")?;
    let command = |millis, kind| {
        Command::new(
            namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(millis),
            kind,
        )
    };
    assert_eq!(
        writer.apply(
            1,
            &DurableProposal::unbound(command(
                10,
                CommandKind::CreateQueue {
                    config: duplicate_config()
                }
            ))
        )?,
        IndexedApplyOutcome::Applied(CommandOutcome::QueueCreated)
    );
    assert_eq!(
        writer.apply(
            2,
            &DurableProposal::unbound(command(20, send(input("seen"))))
        )?,
        IndexedApplyOutcome::Applied(CommandOutcome::Sent {
            sequence: SequenceNumber::new(1)
        })
    );
    let machine = StateMachine::new(store.clone());
    let binding = machine.bind_entity(&namespace, &entity)?;
    drop(machine);
    let counter_key = keys::queue_counters(&namespace, &entity);
    let mut original: QueueCounters = codec::decode(&store.get(&counter_key)?.unwrap())?;
    original.next_sequence = u64::MAX;
    store.apply(WriteBatch::default().put(counter_key, codec::encode(&original)?))?;
    let before = store.snapshot()?;
    let mut latest = None;
    for (index, kind) in [
        (3, send(input("new-single"))),
        (
            4,
            CommandKind::SendBatch {
                messages: vec![input("new-batch"), input("seen")],
            },
        ),
    ] {
        let proposal =
            DurableProposal::bound(BoundCommand::new(binding.clone(), command(30, kind)))?;
        let result = writer.apply(index, &proposal);
        // Baseline-compilable RED: don't refer to the future BrokerError variant.
        assert!(
            matches!(&result, Ok(IndexedApplyOutcome::Refused(_))),
            "expected checkpointed exhaustion refusal, got {result:?}"
        );
        let IndexedApplyOutcome::Refused(error) = result? else {
            unreachable!()
        };
        assert_eq!(error.to_string(), EXHAUSTED);
        assert_eq!(writer.applied_index()?, index);
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
            "F1 checkpoint metadata intentionally changes on refusal"
        );
        let applied = gate.applies.load(Ordering::SeqCst);
        gate.deny.store(true, Ordering::SeqCst);
        assert_eq!(
            writer.apply(index, &proposal)?,
            IndexedApplyOutcome::AlreadyApplied
        );
        assert_eq!(writer.applied_index()?, index);
        let different = DurableProposal::unbound(command(30, send(input("different"))));
        assert_eq!(
            writer.apply(index, &different),
            Err(IndexedApplyError::ConflictingProposal { index })
        );
        assert_eq!(gate.applies.load(Ordering::SeqCst), applied);
        gate.deny.store(false, Ordering::SeqCst);
        assert_eq!(store.snapshot()?, after);
        latest = Some(proposal);
    }
    let after = store.snapshot()?;
    // Drop BOTH the writer's cloned handle and the raw handle before Fjall open.
    drop(writer);
    drop(store);
    let store = provider.open()?;
    assert_eq!(store.snapshot()?, after);
    assert_eq!(domain_rows(&after), domain_rows(&before));
    let mut writer = IndexedWriter::open(Observed {
        inner: store.clone(),
        gate: gate.clone(),
    })?;
    assert_eq!(writer.applied_index()?, 4);
    gate.deny.store(true, Ordering::SeqCst);
    assert_eq!(
        writer.apply(4, &latest.unwrap())?,
        IndexedApplyOutcome::AlreadyApplied
    );
    gate.deny.store(false, Ordering::SeqCst);
    assert_eq!(store.snapshot()?, after);
    let IndexedApplyOutcome::Applied(CommandOutcome::Received(Some(delivery))) = writer.apply(
        5,
        &DurableProposal::unbound(command(
            40,
            CommandKind::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: None,
                session: None,
            },
        )),
    )?
    else {
        panic!("the original message remains usable after reopen");
    };
    assert_eq!(delivery.sequence, SequenceNumber::new(1));
    assert_eq!(
        writer.apply(
            6,
            &DurableProposal::unbound(command(
                50,
                CommandKind::Complete {
                    sequence: delivery.sequence,
                    lock_token: delivery.lock.unwrap().token,
                }
            ))
        )?,
        IndexedApplyOutcome::Applied(CommandOutcome::Completed)
    );
    drop(writer);
    drop(store);
    Ok(())
}

#[test]
fn memory_qualified_terminal_queue_send_refuses_without_effects() -> TestResult {
    terminal_send(MemoryProvider::new())
}

#[test]
fn fjall_qualified_terminal_queue_send_refuses_without_effects() -> TestResult {
    terminal_send(DurableProvider::temporary()?)
}

#[test]
fn memory_qualified_last_queue_sequence_remains_usable() -> TestResult {
    last_slot(MemoryProvider::new())
}

#[test]
fn fjall_qualified_last_queue_sequence_remains_usable() -> TestResult {
    last_slot(DurableProvider::temporary()?)
}

#[test]
fn memory_qualified_fitting_queue_batch_preserves_ttl_and_schedule() -> TestResult {
    fitting_batch(MemoryProvider::new())
}

#[test]
fn fjall_qualified_fitting_queue_batch_preserves_ttl_and_schedule() -> TestResult {
    fitting_batch(DurableProvider::temporary()?)
}

#[test]
fn memory_qualified_crossing_queue_batches_are_atomic() -> TestResult {
    crossing_batches(MemoryProvider::new())
}

#[test]
fn fjall_qualified_crossing_queue_batches_are_atomic() -> TestResult {
    crossing_batches(DurableProvider::temporary()?)
}

#[test]
fn memory_qualified_duplicate_suppression_consumes_queue_ack_slots() -> TestResult {
    duplicate_ack_slots(MemoryProvider::new())
}

#[test]
fn fjall_qualified_duplicate_suppression_consumes_queue_ack_slots() -> TestResult {
    duplicate_ack_slots(DurableProvider::temporary()?)
}

#[test]
fn memory_qualified_queue_exhaustion_preserves_existing_error_priority() -> TestResult {
    priority(MemoryProvider::new())
}

#[test]
fn fjall_qualified_queue_exhaustion_preserves_existing_error_priority() -> TestResult {
    priority(DurableProvider::temporary()?)
}

#[test]
fn memory_qualified_indexed_queue_refusal_changes_only_checkpoint() -> TestResult {
    indexed_refusal(MemoryProvider::new())
}

#[test]
fn fjall_qualified_indexed_queue_refusal_changes_only_checkpoint() -> TestResult {
    indexed_refusal(DurableProvider::temporary()?)
}

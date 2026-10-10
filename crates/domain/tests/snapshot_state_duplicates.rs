//! Paired exact history graphs; supported commands are distinct from injected
//! stale compatibility, corruption and observations of deliberately pending facets.

use domain::{
    BrokerError, Command, CommandKind, CommandOutcome, Delivery, EntityPath, MessageInput,
    NamespaceName, QueueConfig, QueueConfigError, QueueConfigUpdate, QueueCounters, ReceiveMode,
    SequenceNumber, SessionId, StateMachine, SubscriptionConfig, SubscriptionName,
    TIMER_SCAN_LIMIT, Timestamp, TopicConfig, codec, keys,
    snapshot_validation::{
        DuplicateRowsValidation, SnapshotCatalogError, SnapshotDuplicateError, SnapshotStateError,
        validate_duplicate_rows, validate_message_rows,
    },
};
use storage::{Key, StateStore, Value, WriteBatch};
use testkit::{DurableProvider, MemoryProvider, StoreProvider};

const WINDOW: u64 = 20_000;
type Image = Vec<(Key, Value)>;

struct Fixture<P: StoreProvider> {
    // Drop the final Fjall store handle before its temporary directory.
    machine: StateMachine<P::Store>,
    provider: P,
    namespace: NamespaceName,
    queue: EntityPath,
    plain: EntityPath,
}

impl<P: StoreProvider> Fixture<P> {
    fn new(provider: P) -> Self {
        let f = Self {
            machine: StateMachine::new(provider.open().unwrap()),
            provider,
            namespace: NamespaceName::new("tenant").unwrap(),
            queue: EntityPath::new("history").unwrap(),
            plain: EntityPath::new("plain").unwrap(),
        };
        f.at(
            &f.queue,
            1,
            CommandKind::CreateQueue {
                config: QueueConfig {
                    requires_duplicate_detection: true,
                    duplicate_detection_history_millis: WINDOW,
                    max_message_bytes: 8,
                    ..QueueConfig::default()
                },
            },
        );
        f.at(
            &f.plain,
            2,
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        );
        f
    }
    fn raw_at(
        &self,
        entity: &EntityPath,
        time: u64,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerError> {
        self.machine.apply(&Command::new(
            self.namespace.clone(),
            entity.clone(),
            Timestamp::from_millis(time),
            kind,
        ))
    }
    fn at(&self, entity: &EntityPath, time: u64, kind: CommandKind) -> CommandOutcome {
        self.raw_at(entity, time, kind).unwrap()
    }
    fn send_to(
        &self,
        entity: &EntityPath,
        time: u64,
        id: &str,
        scheduled: Option<u64>,
    ) -> CommandOutcome {
        self.at(
            entity,
            time,
            CommandKind::Send {
                message_id: id.to_owned(),
                body: vec![0, 255, 1],
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: scheduled.map(Timestamp::from_millis),
                envelope: None,
            },
        )
    }
    fn send(&self, time: u64, id: &str) -> CommandOutcome {
        self.send_to(&self.queue, time, id, None)
    }
    fn advance(&self, time: u64) {
        self.send_to(&self.plain, time, &format!("clock-{time}"), None);
    }
    fn receive(&self, time: u64, mode: ReceiveMode) -> Delivery {
        match self.at(
            &self.queue,
            time,
            CommandKind::Receive {
                mode,
                lock_duration_millis: None,
                session: None,
            },
        ) {
            CommandOutcome::Received(Some(delivery)) => delivery,
            outcome => panic!("expected delivery: {outcome:?}"),
        }
    }
    fn image(&self) -> Image {
        self.machine.store().snapshot().unwrap().entries().to_vec()
    }
    fn validate(&self, image: &Image) -> DuplicateRowsValidation {
        let original = image.clone();
        let stored = self.image();
        let clock = value(image, &keys::clock());
        let report = validate_duplicate_rows(image).expect("complete message/history graph");
        assert_eq!(image, &original, "all source bytes survive validation");
        assert_eq!(self.image(), stored, "no backend writes during validation");
        assert_eq!(value(image, &keys::clock()), clock);
        assert_eq!(
            report.catalog().catalog_rows()
                + report.pending_external_rows()
                + report.message_rows()
                + report.message_index_rows()
                + report.pending_session_rows()
                + report.lookup_rows()
                + report.expiry_rows(),
            image.len()
        );
        assert!(!report.session_relations_checked());
        assert!(!report.allocation_relations_checked());
        assert!(!report.external_relations_checked());
        report
    }
    fn reject(&self, image: &Image) -> SnapshotDuplicateError {
        let original = image.clone();
        let stored = self.image();
        let clock = image.iter().find(|(key, _)| key == &keys::clock()).cloned();
        let error = validate_duplicate_rows(image).expect_err("refuse injected corruption");
        assert_eq!(image, &original, "all source bytes survive refusal");
        assert_eq!(self.image(), stored, "no backend writes during refusal");
        assert_eq!(
            image.iter().find(|(key, _)| key == &keys::clock()).cloned(),
            clock
        );
        error
    }
    fn deadline(&self, image: &Image, id: &str) -> Timestamp {
        codec::decode(&value(
            image,
            &keys::duplicate_id(&self.namespace, &self.queue, id),
        ))
        .unwrap()
    }
    fn restart(self) -> Self {
        let Self {
            machine,
            provider,
            namespace,
            queue,
            plain,
        } = self;
        drop(machine);
        Self {
            machine: StateMachine::new(provider.open().unwrap()),
            provider,
            namespace,
            queue,
            plain,
        }
    }
    fn reopen_and_compare(self, image: &Image) {
        // Memory reopens logically; Fjall reopens only after the final store handle drops.
        let reopened = self.restart();
        assert_eq!(&reopened.image(), image);
        reopened.validate(image);
    }
}

fn input(id: &str) -> MessageInput {
    MessageInput {
        message_id: id.to_owned(),
        body: vec![1],
        ..MessageInput::default()
    }
}
fn value(image: &Image, key: &[u8]) -> Value {
    image
        .iter()
        .find(|(candidate, _)| candidate == key)
        .expect("exact retained row")
        .1
        .clone()
}
fn put(image: &mut Image, key: Key, raw: Value) {
    image.retain(|(candidate, _)| candidate != &key);
    image.push((key, raw));
    image.sort_by(|a, b| a.0.cmp(&b.0));
}
fn remove(image: &mut Image, key: &[u8]) {
    let count = image.len();
    image.retain(|(candidate, _)| candidate != key);
    assert_eq!(image.len() + 1, count, "remove one exact row");
}
fn row(image: &Image, key: &[u8]) -> usize {
    image
        .iter()
        .position(|(candidate, _)| candidate == key)
        .unwrap()
}
fn relation(error: SnapshotDuplicateError, ordinal: usize, detail: &'static str) {
    assert_eq!(
        error,
        SnapshotDuplicateError::InconsistentDuplicate {
            row: ordinal,
            detail
        }
    );
}
fn error_row(error: SnapshotDuplicateError) -> usize {
    match error {
        SnapshotDuplicateError::InconsistentDuplicate { row, .. }
        | SnapshotDuplicateError::State(
            SnapshotStateError::InvalidKey { row, .. }
            | SnapshotStateError::InvalidValue { row, .. }
            | SnapshotStateError::InconsistentMessage { row, .. },
        ) => row,
        SnapshotDuplicateError::State(SnapshotStateError::Catalog(
            SnapshotCatalogError::EmptyKey { row }
            | SnapshotCatalogError::InputOrder { row }
            | SnapshotCatalogError::UnsupportedTag { row, .. }
            | SnapshotCatalogError::InvalidKey { row, .. }
            | SnapshotCatalogError::InvalidValue { row, .. }
            | SnapshotCatalogError::InconsistentCatalog { row, .. },
        )) => row,
        SnapshotDuplicateError::State(SnapshotStateError::Catalog(
            SnapshotCatalogError::MissingClock,
        )) => {
            panic!("expected an original row ordinal")
        }
    }
}

fn hits_and_boundary<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    assert_eq!(
        f.send(3, "CaSe"),
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(1)
        }
    );
    let first = f.image();
    let deadline = f.deadline(&first, "CaSe");
    f.validate(&first);
    assert_eq!(
        f.send(4, "CaSe"),
        CommandOutcome::DuplicateSuppressed {
            sequence: SequenceNumber::new(2)
        }
    );
    assert_eq!(f.deadline(&f.image(), "CaSe"), deadline);
    for (time, id) in [
        (5, "case"),
        (6, "Unicode-\u{e9}\0tail"),
        (7, "tail\0"),
        (8, ""),
        (9, ""),
    ] {
        assert!(matches!(f.send(time, id), CommandOutcome::Sent { .. }));
        f.validate(&f.image());
    }
    let image = f.image();
    assert_eq!(f.validate(&image).lookup_rows(), 4);
    assert!(
        !image
            .iter()
            .any(|(key, _)| key == &keys::duplicate_id(&f.namespace, &f.queue, ""))
    );
    assert_eq!(
        f.send(deadline.as_millis(), "CaSe"),
        CommandOutcome::Sent {
            sequence: SequenceNumber::new(8)
        }
    );
    let replaced = f.image();
    assert_eq!(
        f.deadline(&replaced, "CaSe"),
        deadline.saturating_add_millis(WINDOW)
    );
    assert!(
        !replaced.iter().any(
            |(key, _)| key == &keys::duplicate_expiry(&f.namespace, &f.queue, deadline, "CaSe")
        )
    );
    assert_eq!(f.validate(&replaced).stale_compatibility_rows(), 0);
    let f = f.restart();
    assert_eq!(f.image(), replaced);
    assert_eq!(
        f.send(deadline.as_millis() + 1, "CaSe"),
        CommandOutcome::DuplicateSuppressed {
            sequence: SequenceNumber::new(9),
        }
    );
    let image = f.image();
    f.validate(&image);
    f.reopen_and_compare(&image);
}

#[test]
fn memory_exact_ids_hits_boundary_replacement_and_anonymous_sends() {
    hits_and_boundary(MemoryProvider::new());
}
#[test]
fn fjall_exact_ids_hits_boundary_replacement_and_anonymous_sends() {
    hits_and_boundary(DurableProvider::temporary().unwrap());
}

fn independent_history<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    let scheduled = match f.send_to(&f.queue, 3, "scheduled", Some(100)) {
        CommandOutcome::Sent { sequence } => sequence,
        other => panic!("scheduled outcome: {other:?}"),
    };
    f.send(4, "complete");
    let delivery = f.receive(5, ReceiveMode::PeekLock);
    f.at(
        &f.queue,
        6,
        CommandKind::Complete {
            sequence: delivery.sequence,
            lock_token: delivery.lock.unwrap().token,
        },
    );
    assert_eq!(
        f.at(
            &f.queue,
            7,
            CommandKind::CancelScheduled {
                sequences: vec![scheduled]
            }
        ),
        CommandOutcome::ScheduledCancelled { cancelled: 1 }
    );
    let retained = f.image();
    let report = f.validate(&retained);
    assert_eq!(report.message_rows(), 0);
    assert_eq!(report.lookup_rows(), 2);
    let original_deadline = f.deadline(&retained, "scheduled");
    assert!(matches!(
        f.send(8, "scheduled"),
        CommandOutcome::DuplicateSuppressed { .. }
    ));
    assert_eq!(
        f.at(
            &f.queue,
            9,
            CommandKind::UpdateQueue {
                update: QueueConfigUpdate {
                    duplicate_detection_history_millis: Some(40_000),
                    ..QueueConfigUpdate::default()
                }
            }
        ),
        CommandOutcome::QueueUpdated
    );
    assert_eq!(f.deadline(&f.image(), "scheduled"), original_deadline);
    f.send_to(&f.queue, 10, "activate", Some(100));
    let deadline = f.deadline(&f.image(), "activate");
    assert_eq!(deadline, Timestamp::from_millis(40_010));
    assert_eq!(
        f.at(&f.queue, 100, CommandKind::ActivateScheduled),
        CommandOutcome::ScheduledActivated {
            activated: 1,
            deliverable_entities: vec![f.queue.clone()],
        }
    );
    assert_eq!(f.deadline(&f.image(), "activate"), deadline);
    f.validate(&f.image());
    f.advance(20_004);
    let elapsed = f.image();
    f.validate(&elapsed); // Current expired history is retained until actual cleanup.
    assert_eq!(
        f.at(&f.queue, 20_004, CommandKind::ExpireDuplicateHistory),
        CommandOutcome::DuplicateHistoryExpired { removed: 2 }
    );
    let delivered = f.receive(20_005, ReceiveMode::ReceiveAndDelete);
    assert_eq!(delivered.body, vec![0, 255, 1]);
    let image = f.image();
    let report = f.validate(&image);
    assert_eq!(report.lookup_rows(), 1);
    assert_eq!(f.deadline(&image, "activate"), deadline);
    f.reopen_and_compare(&image);
}

#[test]
fn memory_completion_schedule_cancel_window_change_activation_and_cleanup() {
    independent_history(MemoryProvider::new());
}
#[test]
fn fjall_completion_schedule_cancel_window_change_activation_and_cleanup() {
    independent_history(DurableProvider::temporary().unwrap());
}

fn batches<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    assert_eq!(
        f.at(
            &f.queue,
            3,
            CommandKind::SendBatch {
                messages: ["same", "same", "unique", "", ""]
                    .into_iter()
                    .map(input)
                    .collect()
            }
        ),
        CommandOutcome::BatchSent {
            sequences: (1..=5).map(SequenceNumber::new).collect(),
            stored: 4,
        }
    );
    let first = f.image();
    assert_eq!(f.validate(&first).lookup_rows(), 2);
    assert_eq!(f.validate(&first).message_rows(), 4);
    assert_eq!(
        f.at(
            &f.queue,
            4,
            CommandKind::SendBatch {
                messages: ["same", "new", "same"].into_iter().map(input).collect()
            }
        ),
        CommandOutcome::BatchSent {
            sequences: (6..=8).map(SequenceNumber::new).collect(),
            stored: 1,
        }
    );
    let before = f.image();
    let invalid = MessageInput {
        body: vec![0; 9],
        ..input("bad")
    };
    assert_eq!(
        f.raw_at(
            &f.queue,
            5,
            CommandKind::SendBatch {
                messages: vec![input("would-poison"), invalid]
            }
        ),
        Err(BrokerError::MessageTooLarge {
            body_bytes: 9,
            maximum_bytes: 8
        })
    );
    assert_eq!(
        f.image(),
        before,
        "refused batch preserves Clock, counters and every history byte"
    );
    assert_eq!(
        f.machine
            .duplicate_history_deadline(&f.namespace, &f.queue, "would-poison")
            .unwrap(),
        None
    );
    f.validate(&before);
    let many = (0..=TIMER_SCAN_LIMIT)
        .map(|index| input(&format!("bounded-{index}")))
        .collect();
    assert!(
        matches!(f.at(&f.queue, 6, CommandKind::SendBatch { messages: many }),
        CommandOutcome::BatchSent { stored, .. } if stored as usize == TIMER_SCAN_LIMIT + 1)
    );
    let history_count = f.validate(&f.image()).lookup_rows();
    assert_eq!(history_count, 3 + TIMER_SCAN_LIMIT + 1);
    assert_eq!(
        f.at(&f.queue, 20_006, CommandKind::ExpireDuplicateHistory),
        CommandOutcome::DuplicateHistoryExpired {
            removed: TIMER_SCAN_LIMIT as u32
        }
    );
    assert_eq!(
        f.validate(&f.image()).lookup_rows(),
        history_count - TIMER_SCAN_LIMIT
    );
    assert_eq!(
        f.at(&f.queue, 20_006, CommandKind::ExpireDuplicateHistory),
        CommandOutcome::DuplicateHistoryExpired {
            removed: (history_count - TIMER_SCAN_LIMIT) as u32
        }
    );
    let image = f.image();
    assert_eq!(f.validate(&image).lookup_rows(), 0);
    f.reopen_and_compare(&image);
}

#[test]
fn memory_batch_ack_slots_suppression_and_refusal_conservation() {
    batches(MemoryProvider::new());
}
#[test]
fn fjall_batch_ack_slots_suppression_and_refusal_conservation() {
    batches(DurableProvider::temporary().unwrap());
}

fn current_corruptions<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    f.send(3, "CaSe");
    let image = f.image();
    let current = f.deadline(&image, "CaSe");
    let lookup = keys::duplicate_id(&f.namespace, &f.queue, "CaSe");
    let expiry = keys::duplicate_expiry(&f.namespace, &f.queue, current, "CaSe");
    let mut missing = image.clone();
    remove(&mut missing, &expiry);
    relation(
        f.reject(&missing),
        row(&missing, &lookup),
        "lookup is missing its exact current expiry",
    );
    let mut changed = image.clone();
    put(
        &mut changed,
        lookup.clone(),
        codec::encode(&current.saturating_add_millis(1)).unwrap(),
    );
    relation(
        f.reject(&changed),
        row(&changed, &lookup),
        "lookup is missing its exact current expiry",
    );
    let mut orphan = image.clone();
    remove(&mut orphan, &lookup);
    relation(
        f.reject(&orphan),
        row(&orphan, &expiry),
        "expiry has no exact current lookup",
    );
    for wrong in [
        keys::duplicate_expiry(&f.namespace, &f.queue, current, "case"),
        keys::duplicate_expiry(
            &NamespaceName::new("other").unwrap(),
            &f.queue,
            current,
            "CaSe",
        ),
        keys::duplicate_expiry(&f.namespace, &f.plain, current, "CaSe"),
    ] {
        let mut broken = image.clone();
        put(&mut broken, wrong.clone(), vec![]);
        let error = f.reject(&broken);
        assert!(matches!(
            error,
            SnapshotDuplicateError::InconsistentDuplicate { .. }
        ));
        assert_eq!(error_row(error), row(&broken, &wrong));
    }
    // Forward current lookup refusal wins even when a reverse orphan is present.
    let extra = keys::duplicate_expiry(&f.namespace, &f.queue, Timestamp::from_millis(3), "orphan");
    put(&mut missing, extra, vec![]);
    relation(
        f.reject(&missing),
        row(&missing, &lookup),
        "lookup is missing its exact current expiry",
    );
    f.reopen_and_compare(&image);
}

#[test]
fn memory_current_companions_exact_scope_case_and_original_ordinals() {
    current_corruptions(MemoryProvider::new());
}
#[test]
fn fjall_current_companions_exact_scope_case_and_original_ordinals() {
    current_corruptions(DurableProvider::temporary().unwrap());
}

fn stale_compatibility<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    f.send(3, "generation");
    let original = f.image();
    let current = f.deadline(&original, "generation");
    let lookup = keys::duplicate_id(&f.namespace, &f.queue, "generation");
    let current_expiry = keys::duplicate_expiry(&f.namespace, &f.queue, current, "generation");
    let clock = Timestamp::from_millis(3);
    let old_expiry = keys::duplicate_expiry(&f.namespace, &f.queue, clock, "generation");
    // Ordinary replacement deletes its prior expiry. This older row is injected
    // compatibility, matching the existing sweep-tolerance control, not a trace.
    let mut compatible = original.clone();
    put(&mut compatible, old_expiry.clone(), vec![]);
    assert_eq!(f.validate(&compatible).stale_compatibility_rows(), 1);
    assert_eq!(f.deadline(&compatible, "generation"), current);
    let mut missing_current = compatible.clone();
    remove(&mut missing_current, &current_expiry);
    relation(
        f.reject(&missing_current),
        row(&missing_current, &lookup),
        "lookup is missing its exact current expiry",
    );
    let mut no_lookup = compatible.clone();
    remove(&mut no_lookup, &lookup);
    relation(
        f.reject(&no_lookup),
        row(&no_lookup, &old_expiry),
        "expiry has no exact current lookup",
    );
    for deadline in [
        clock.saturating_add_millis(1),
        current.saturating_add_millis(1),
    ] {
        let mut broken = original.clone();
        let extra = keys::duplicate_expiry(&f.namespace, &f.queue, deadline, "generation");
        put(&mut broken, extra.clone(), vec![]);
        relation(
            f.reject(&broken),
            row(&broken, &extra),
            "expiry is neither current nor elapsed older compatibility",
        );
    }
    let mut no_clock = compatible.clone();
    remove(&mut no_clock, &keys::clock());
    assert_eq!(
        f.reject(&no_clock),
        SnapshotDuplicateError::State(SnapshotStateError::Catalog(
            SnapshotCatalogError::MissingClock
        ))
    );
    // Separately drive actual cleanup over the explicitly injected stored row.
    f.machine
        .store()
        .apply(WriteBatch::default().put(old_expiry, vec![]))
        .unwrap();
    assert_eq!(f.image(), compatible);
    f.validate(&f.image());
    assert_eq!(
        f.at(&f.queue, 3, CommandKind::ExpireDuplicateHistory),
        CommandOutcome::DuplicateHistoryExpired { removed: 1 }
    );
    assert_eq!(
        f.image(),
        original,
        "cleanup removes only the explicitly injected older expiry"
    );
    assert_eq!(f.deadline(&f.image(), "generation"), current);
    assert_eq!(f.validate(&f.image()).stale_compatibility_rows(), 0);
    assert!(matches!(
        f.send(4, "generation"),
        CommandOutcome::DuplicateSuppressed { .. }
    ));
    let image = f.image();
    f.reopen_and_compare(&image);
}

#[test]
fn memory_explicit_elapsed_older_compatibility_and_sweep_tolerance() {
    stale_compatibility(MemoryProvider::new());
}
#[test]
fn fjall_explicit_elapsed_older_compatibility_and_sweep_tolerance() {
    stale_compatibility(DurableProvider::temporary().unwrap());
}

fn canonical_and_priority<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    let id = "Unicode-\u{e9}\0tail";
    let sequence = match f.send(3, id) {
        CommandOutcome::Sent { sequence } => sequence,
        other => panic!("send: {other:?}"),
    };
    let image = f.image();
    let lookup = keys::duplicate_id(&f.namespace, &f.queue, id);
    let expiry = keys::duplicate_expiry(&f.namespace, &f.queue, f.deadline(&image, id), id);
    for key in [&lookup, &expiry] {
        for bad_key in [
            {
                let mut key = key.clone();
                key[1] = 255;
                key
            },
            {
                let mut key = key.clone();
                *key.last_mut().unwrap() = 255;
                key
            },
        ] {
            let mut broken = image.clone();
            remove(&mut broken, key);
            put(&mut broken, bad_key.clone(), value(&image, key));
            let error = f.reject(&broken);
            assert!(matches!(
                error,
                SnapshotDuplicateError::State(SnapshotStateError::InvalidKey { .. })
            ));
            assert_eq!(error_row(error), row(&broken, &bad_key));
        }
        let mut broken = image.clone();
        put(&mut broken, key.clone(), vec![255]);
        let error = f.reject(&broken);
        assert!(matches!(
            error,
            SnapshotDuplicateError::State(SnapshotStateError::InvalidValue { .. })
        ));
        assert_eq!(error_row(error), row(&broken, key));
    }
    let mut trailing = image.clone();
    let mut bytes = value(&image, &lookup);
    bytes.push(0);
    put(&mut trailing, lookup.clone(), bytes);
    assert_eq!(error_row(f.reject(&trailing)), row(&trailing, &lookup));
    let mut short = image.clone();
    remove(&mut short, &expiry);
    let prefix = keys::duplicate_expiry_prefix(&f.namespace, &f.queue);
    let truncated = expiry[..prefix.len() + 7].to_vec();
    put(&mut short, truncated.clone(), vec![]);
    assert!(matches!(
        f.reject(&short),
        SnapshotDuplicateError::State(SnapshotStateError::InvalidKey { .. })
    ));
    assert_eq!(error_row(f.reject(&short)), row(&short, &truncated));
    let mut catalog_first = image.clone();
    put(&mut catalog_first, keys::clock(), vec![255]);
    remove(&mut catalog_first, &expiry);
    assert!(matches!(
        f.reject(&catalog_first),
        SnapshotDuplicateError::State(SnapshotStateError::Catalog(
            SnapshotCatalogError::InvalidValue { row: 0, .. }
        ))
    ));
    let ready = keys::ready(&f.namespace, &f.queue, sequence);
    let mut canonical_first = image.clone();
    remove(&mut canonical_first, &ready);
    put(&mut canonical_first, expiry.clone(), vec![1]);
    assert!(matches!(
        f.reject(&canonical_first),
        SnapshotDuplicateError::State(SnapshotStateError::InvalidValue { .. })
    ));
    assert_eq!(
        error_row(f.reject(&canonical_first)),
        row(&canonical_first, &expiry)
    );
    let mut message_first = image.clone();
    remove(&mut message_first, &ready);
    remove(&mut message_first, &expiry);
    let old_error = validate_message_rows(&message_first).unwrap_err();
    let wrapped = f.reject(&message_first);
    assert_eq!(wrapped, SnapshotDuplicateError::State(old_error));
    assert_eq!(wrapped.to_string(), old_error.to_string());
    f.reopen_and_compare(&image);
}

#[test]
fn memory_canonical_history_forms_and_deterministic_phase_priority() {
    canonical_and_priority(MemoryProvider::new());
}
#[test]
fn fjall_canonical_history_forms_and_deterministic_phase_priority() {
    canonical_and_priority(DurableProvider::temporary().unwrap());
}

fn owner_roles_and_pending<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    f.send(3, "real");
    f.send_to(&f.plain, 4, "same", None);
    f.send_to(&f.plain, 5, "same", None);
    let before = f.image();
    assert_eq!(
        f.raw_at(
            &f.queue,
            6,
            CommandKind::UpdateQueue {
                update: QueueConfigUpdate {
                    requires_duplicate_detection: Some(false),
                    ..QueueConfigUpdate::default()
                }
            }
        ),
        Err(BrokerError::QueueConfig(
            QueueConfigError::RequiresDuplicateDetectionImmutable
        ))
    );
    assert_eq!(
        f.image(),
        before,
        "duplicate-disabled history is not a reachable profile update"
    );
    let topic = EntityPath::new("events").unwrap();
    f.at(
        &topic,
        7,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    );
    let subscription = match f.at(
        &topic,
        8,
        CommandKind::CreateSubscription {
            name: SubscriptionName::new("first").unwrap(),
            config: SubscriptionConfig::default(),
        },
    ) {
        CommandOutcome::SubscriptionCreated { entity } => entity,
        other => panic!("subscription: {other:?}"),
    };
    let image = f.image();
    let queue_shadow = f.queue.dead_letter_queue().unwrap();
    let subscription_shadow = subscription.dead_letter_queue().unwrap();
    let clock = Timestamp::from_millis(8);
    for endpoint in [
        &f.plain,
        &topic,
        &subscription,
        &queue_shadow,
        &subscription_shadow,
    ] {
        let lookup = keys::duplicate_id(&f.namespace, endpoint, "injected");
        let expiry = keys::duplicate_expiry(&f.namespace, endpoint, clock, "injected");
        let mut broken = image.clone();
        put(&mut broken, lookup.clone(), codec::encode(&clock).unwrap());
        put(&mut broken, expiry, vec![]);
        relation(
            f.reject(&broken),
            row(&broken, &lookup),
            "duplicate history endpoint is not an enabled ordinary queue",
        );
        let mut orphan = image.clone();
        let expiry = keys::duplicate_expiry(&f.namespace, endpoint, clock, "injected");
        put(&mut orphan, expiry.clone(), vec![]);
        relation(
            f.reject(&orphan),
            row(&orphan, &expiry),
            "duplicate history endpoint is not an enabled ordinary queue",
        );
    }
    let mut anonymous = image.clone();
    let empty = keys::duplicate_id(&f.namespace, &f.queue, "");
    put(
        &mut anonymous,
        empty.clone(),
        codec::encode(&clock).unwrap(),
    );
    put(
        &mut anonymous,
        keys::duplicate_expiry(&f.namespace, &f.queue, clock, ""),
        vec![],
    );
    relation(
        f.reject(&anonymous),
        row(&anonymous, &empty),
        "anonymous identifiers do not own duplicate history",
    );
    let mut disabled = image.clone();
    let config_key = keys::queue_config(&f.namespace, &f.queue);
    let mut config: QueueConfig = codec::decode(&value(&image, &config_key)).unwrap();
    config.requires_duplicate_detection = false; // Qualified impossible-update image, not history.
    put(&mut disabled, config_key, codec::encode(&config).unwrap());
    let lookup = keys::duplicate_id(&f.namespace, &f.queue, "real");
    relation(
        f.reject(&disabled),
        row(&disabled, &lookup),
        "duplicate history endpoint is not an enabled ordinary queue",
    );
    // These canonical relation corruptions belong to other pending facets.
    let mut pending = image.clone();
    put(
        &mut pending,
        keys::session_lock(
            &f.namespace,
            &f.queue,
            clock,
            &SessionId::new("orphan").unwrap(),
        ),
        vec![],
    );
    let counter_key = keys::queue_counters(&f.namespace, &f.queue);
    let mut counters: QueueCounters = codec::decode(&value(&image, &counter_key)).unwrap();
    assert!(counters.next_sequence > 1);
    counters.next_sequence = 1;
    put(&mut pending, counter_key, codec::encode(&counters).unwrap());
    put(&mut pending, vec![0xF0], vec![255]);
    put(&mut pending, vec![0xF1], vec![0, 255]);
    let old = validate_message_rows(&pending).unwrap();
    let partial = f.validate(&pending);
    assert_eq!(partial.pending_session_rows(), 1);
    assert_eq!(partial.pending_external_rows(), 2);
    assert_eq!(partial.pending_counter_rows(), old.pending_counter_rows());
    assert_eq!(partial.catalog(), old.catalog());
    assert_eq!(validate_message_rows(&pending).unwrap(), old);
    let zero = f.validate(&image);
    assert_eq!(zero.pending_session_rows(), 0);
    assert_eq!(zero.pending_external_rows(), 0);
    assert!(!zero.session_relations_checked());
    assert!(!zero.external_relations_checked());
    f.reopen_and_compare(&image);
}

#[test]
fn memory_enabled_owner_roles_and_explicit_pending_facets() {
    owner_roles_and_pending(MemoryProvider::new());
}
#[test]
fn fjall_enabled_owner_roles_and_explicit_pending_facets() {
    owner_roles_and_pending(DurableProvider::temporary().unwrap());
}

//! Paired immutable session graphs: supported command traces are distinct from
//! injected corruption and observations of deliberately pending facets.

use domain::{
    AcceptedSession, Command, CommandKind, CommandOutcome, Delivery, EntityPath, MessageRecord,
    MessageState, NamespaceName, QueueConfig, QueueCounters, ReceiveMode, SequenceNumber,
    SessionHold, SessionId, SessionRecord, StateMachine, SubscriptionConfig, SubscriptionName,
    Timestamp, TopicConfig, codec, keys,
    snapshot_validation::{
        SessionRowsValidation, SnapshotCatalogError, SnapshotSessionError, SnapshotStateError,
        validate_message_rows, validate_session_rows,
    },
};
use storage::{Key, StateStore, Value};
use testkit::{DurableProvider, MemoryProvider, StoreProvider};

type Image = Vec<(Key, Value)>;

struct Fixture<P: StoreProvider> {
    // Drop the final store handle before a temporary durable directory.
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
            queue: EntityPath::new("sessions").unwrap(),
            plain: EntityPath::new("plain").unwrap(),
        };
        f.at(
            &f.queue,
            1,
            CommandKind::CreateQueue {
                config: QueueConfig {
                    requires_session: true,
                    lock_duration_millis: 10,
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

    fn at(&self, entity: &EntityPath, time: u64, kind: CommandKind) -> CommandOutcome {
        self.machine
            .apply(&Command::new(
                self.namespace.clone(),
                entity.clone(),
                Timestamp::from_millis(time),
                kind,
            ))
            .unwrap()
    }

    fn send(
        &self,
        time: u64,
        id: &str,
        session: &SessionId,
        scheduled: Option<u64>,
    ) -> SequenceNumber {
        match self.at(
            &self.queue,
            time,
            CommandKind::Send {
                message_id: id.to_owned(),
                body: vec![0, 255, 3],
                time_to_live_millis: Some(100),
                session_id: Some(session.clone()),
                scheduled_enqueue_at: scheduled.map(Timestamp::from_millis),
                envelope: None,
            },
        ) {
            CommandOutcome::Sent { sequence } => sequence,
            outcome => panic!("expected session send: {outcome:?}"),
        }
    }

    fn accept(
        &self,
        time: u64,
        session: Option<SessionId>,
        duration: Option<u64>,
    ) -> AcceptedSession {
        match self.at(
            &self.queue,
            time,
            CommandKind::AcceptSession {
                session_id: session,
                lock_duration_millis: duration,
            },
        ) {
            CommandOutcome::SessionAccepted(Some(accepted)) => accepted,
            outcome => panic!("expected accepted session: {outcome:?}"),
        }
    }

    fn receive(&self, time: u64, session: &SessionHold) -> Delivery {
        match self.at(
            &self.queue,
            time,
            CommandKind::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: Some(100),
                session: Some(session.clone()),
            },
        ) {
            CommandOutcome::Received(Some(delivery)) => delivery,
            outcome => panic!("expected delivery: {outcome:?}"),
        }
    }

    fn advance(&self, time: u64) {
        self.at(
            &self.plain,
            time,
            CommandKind::Send {
                message_id: format!("clock-{time}"),
                body: vec![],
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: None,
                envelope: None,
            },
        );
    }

    fn image(&self) -> Image {
        self.machine.store().snapshot().unwrap().entries().to_vec()
    }

    fn validate(&self, image: &Image) -> SessionRowsValidation {
        let original = image.clone();
        let stored = self.image();
        let clock = value(image, &keys::clock());
        let report = validate_session_rows(image).expect("complete message/session graph");
        assert_eq!(
            image, &original,
            "all original input bytes survive validation"
        );
        assert_eq!(self.image(), stored, "the validator never writes storage");
        assert_eq!(value(image, &keys::clock()), clock);
        assert_eq!(
            report.catalog().catalog_rows()
                + report.pending_external_rows()
                + report.message_rows()
                + report.message_index_rows()
                + report.session_rows()
                + report.session_lock_rows()
                + report.pending_duplicate_rows(),
            image.len()
        );
        assert!(!report.duplicate_relations_checked());
        assert!(!report.allocation_relations_checked());
        assert!(!report.external_relations_checked());
        report
    }

    fn reject(&self, image: &Image) -> SnapshotSessionError {
        let original = image.clone();
        let stored = self.image();
        let clock = image.iter().find(|(key, _)| key == &keys::clock()).cloned();
        let error = validate_session_rows(image).expect_err("refuse injected corruption");
        assert_eq!(image, &original, "all original input bytes survive refusal");
        assert_eq!(self.image(), stored, "refusal never writes storage");
        assert_eq!(
            image.iter().find(|(key, _)| key == &keys::clock()).cloned(),
            clock
        );
        error
    }

    fn session(&self, image: &Image, id: &SessionId) -> SessionRecord {
        codec::decode(&value(
            image,
            &keys::session(&self.namespace, &self.queue, id),
        ))
        .unwrap()
    }

    fn record(&self, image: &Image, sequence: SequenceNumber) -> MessageRecord {
        MessageRecord::decode(&value(
            image,
            &keys::message(&self.namespace, &self.queue, sequence),
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
        // Memory is logical reopen; Fjall opens after every machine/store handle drops.
        let reopened = self.restart();
        assert_eq!(&reopened.image(), image);
        reopened.validate(image);
    }
}

fn id(text: &str) -> SessionId {
    SessionId::new(text).unwrap()
}
fn value(image: &Image, key: &[u8]) -> Value {
    image
        .iter()
        .find(|(candidate, _)| candidate == key)
        .expect("exact fixture row")
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
    assert_eq!(image.len() + 1, count, "remove exactly one retained row");
}
fn row(image: &Image, key: &[u8]) -> usize {
    image
        .iter()
        .position(|(candidate, _)| candidate == key)
        .unwrap()
}
fn relation(error: SnapshotSessionError, ordinal: usize, detail: &'static str) {
    assert_eq!(
        error,
        SnapshotSessionError::InconsistentSession {
            row: ordinal,
            detail
        }
    );
}
fn state_row(error: SnapshotSessionError) -> usize {
    match error {
        SnapshotSessionError::InconsistentSession { row, .. }
        | SnapshotSessionError::State(
            SnapshotStateError::InvalidKey { row, .. }
            | SnapshotStateError::InvalidValue { row, .. }
            | SnapshotStateError::InconsistentMessage { row, .. },
        ) => row,
        SnapshotSessionError::State(SnapshotStateError::Catalog(
            SnapshotCatalogError::EmptyKey { row }
            | SnapshotCatalogError::InputOrder { row }
            | SnapshotCatalogError::UnsupportedTag { row, .. }
            | SnapshotCatalogError::InvalidKey { row, .. }
            | SnapshotCatalogError::InvalidValue { row, .. }
            | SnapshotCatalogError::InconsistentCatalog { row, .. },
        )) => row,
        SnapshotSessionError::State(SnapshotStateError::Catalog(
            SnapshotCatalogError::MissingClock,
        )) => {
            panic!("expected an original row ordinal")
        }
    }
}

fn implicit_selection<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    let upper = id("CaSe");
    let lower = id("case");
    f.send(3, "upper", &upper, None);
    f.send(4, "lower", &lower, None);
    let implicit = f.image();
    let report = f.validate(&implicit);
    assert_eq!(report.session_rows(), 0);
    assert_eq!(report.session_ready_rows(), 2);
    assert!(!implicit.iter().any(|(key, _)| key[0] == 8 || key[0] == 10));
    let first = f.accept(5, None, None);
    assert_eq!(first.session_id, upper);
    let second = f.accept(6, None, None);
    assert_eq!(second.session_id, lower);
    f.validate(&f.image());
    let empty = f.accept(7, Some(id("empty")), None);
    assert_eq!(empty.state, Vec::<u8>::new());
    f.validate(&f.image());
    let opaque = vec![0, 255, 128, 1, 0];
    assert_eq!(
        f.at(
            &f.queue,
            8,
            CommandKind::SetSessionState {
                session: empty.hold(),
                state: opaque.clone(),
            }
        ),
        CommandOutcome::SessionStateSet
    );
    assert_eq!(
        f.at(
            &f.queue,
            9,
            CommandKind::ReleaseSession {
                session: empty.hold()
            }
        ),
        CommandOutcome::SessionReleased
    );
    let image = f.image();
    let record = f.session(&image, &empty.session_id);
    assert_eq!(record.lock, None);
    assert_eq!(record.state, opaque);
    assert_eq!(f.validate(&image).session_rows(), 3);
    f.reopen_and_compare(&image);
}

#[test]
fn memory_implicit_case_sensitive_selection_empty_named_and_opaque_state() {
    implicit_selection(MemoryProvider::new());
}
#[test]
fn fjall_implicit_case_sensitive_selection_empty_named_and_opaque_state() {
    implicit_selection(DurableProvider::temporary().unwrap());
}

fn renewal_and_expiry<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    let session = id("retained");
    let first = f.accept(3, Some(session.clone()), None);
    f.at(
        &f.queue,
        4,
        CommandKind::SetSessionState {
            session: first.hold(),
            state: vec![255, 0, 7],
        },
    );
    let old_key = keys::session_lock(&f.namespace, &f.queue, first.lock.locked_until, &session);
    let deadline = Timestamp::from_millis(35);
    assert_eq!(
        f.at(
            &f.queue,
            5,
            CommandKind::RenewSessionLock {
                session: first.hold(),
                lock_duration_millis: Some(30),
            }
        ),
        CommandOutcome::SessionLockRenewed {
            locked_until: deadline
        }
    );
    let renewed = f.image();
    assert!(!renewed.iter().any(|(key, _)| key == &old_key));
    assert_eq!(
        f.session(&renewed, &session).lock.unwrap().token,
        first.lock.token
    );
    f.validate(&renewed);
    f.advance(35); // Unrelated supported work advances Clock without sweeping this session.
    let elapsed = f.image();
    assert!(
        f.session(&elapsed, &session)
            .live_lock_at(Timestamp::from_millis(35))
            .is_none()
    );
    assert_eq!(f.validate(&elapsed).session_lock_rows(), 1);
    assert_eq!(
        f.at(&f.queue, 35, CommandKind::ExpireSessionLocks),
        CommandOutcome::SessionLocksExpired { released: 1 }
    );
    let expired = f.image();
    assert_eq!(f.session(&expired, &session).state, vec![255, 0, 7]);
    assert_eq!(f.validate(&expired).session_lock_rows(), 0);
    let next = f.accept(36, Some(session.clone()), None);
    assert_ne!(next.lock.token, first.lock.token);
    assert_eq!(next.state, vec![255, 0, 7]);
    f.validate(&f.image());
    f.advance(46);
    let unswept = f.image();
    f.validate(&unswept);
    let reaccepted = f.accept(47, Some(session.clone()), None);
    assert_ne!(reaccepted.lock.token, next.lock.token);
    assert!(!f.image().iter().any(|(key, _)| key
        == &keys::session_lock(&f.namespace, &f.queue, next.lock.locked_until, &session)));
    f.validate(&f.image());
    assert_eq!(
        f.at(
            &f.queue,
            48,
            CommandKind::ReleaseSession {
                session: reaccepted.hold()
            }
        ),
        CommandOutcome::SessionReleased
    );
    let immediate = f.accept(49, Some(id("zero-duration")), Some(0));
    assert_eq!(immediate.lock.locked_until, Timestamp::from_millis(49));
    let image = f.image();
    f.validate(&image); // A retained lock at Clock is not incorrectly clamped away.
    f.reopen_and_compare(&image);
}

#[test]
fn memory_renewal_elapsed_retention_expiry_and_reacceptance() {
    renewal_and_expiry(MemoryProvider::new());
}
#[test]
fn fjall_renewal_elapsed_retention_expiry_and_reacceptance() {
    renewal_and_expiry(DurableProvider::temporary().unwrap());
}

fn independent_message_locks<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    let session = id("delivery");
    let sequence = f.send(3, "one", &session, None);
    let accepted = f.accept(4, Some(session.clone()), None);
    let delivery = f.receive(5, &accepted.hold());
    let message_lock = delivery.lock.unwrap();
    f.at(
        &f.queue,
        6,
        CommandKind::ReleaseSession {
            session: accepted.hold(),
        },
    );
    let released = f.image();
    assert_eq!(f.session(&released, &session).lock, None);
    assert!(matches!(f.record(&released, sequence).state,
        MessageState::Locked { token, locked_until, .. }
            if token == message_lock.token && locked_until == message_lock.locked_until));
    f.validate(&released);
    // Settlement uses the original delivery token, not a surviving session hold.
    assert_eq!(
        f.at(
            &f.queue,
            7,
            CommandKind::Complete {
                sequence,
                lock_token: message_lock.token
            }
        ),
        CommandOutcome::Completed
    );
    let deferred = f.send(8, "defer", &session, None);
    let owner = f.accept(9, Some(session.clone()), None);
    let delivery = f.receive(10, &owner.hold());
    f.at(
        &f.queue,
        11,
        CommandKind::Defer {
            sequence: deferred,
            lock_token: delivery.lock.unwrap().token,
            replacement_envelope: None,
        },
    );
    f.at(
        &f.queue,
        12,
        CommandKind::ReleaseSession {
            session: owner.hold(),
        },
    );
    let image = f.image();
    assert_eq!(f.record(&image, deferred).state, MessageState::Deferred);
    f.validate(&image); // Deferred routing requires neither 09 nor a live session lock.
    let owner = f.accept(13, Some(session.clone()), None);
    let received = f.at(
        &f.queue,
        14,
        CommandKind::ReceiveDeferred {
            sequences: vec![deferred],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(100),
            session: Some(owner.hold()),
        },
    );
    let CommandOutcome::DeferredReceived(deliveries) = received else {
        panic!("deferred outcome")
    };
    assert_eq!(deliveries.len(), 1);
    let token = deliveries[0].lock.unwrap().token;
    f.advance(23);
    assert_eq!(
        f.at(&f.queue, 23, CommandKind::ExpireSessionLocks),
        CommandOutcome::SessionLocksExpired { released: 1 }
    );
    let expired = f.image();
    assert_eq!(f.session(&expired, &session).lock, None);
    assert!(matches!(
        f.record(&expired, deferred).state,
        MessageState::Locked { .. }
    ));
    f.validate(&expired);
    assert_eq!(
        f.at(
            &f.queue,
            24,
            CommandKind::Abandon {
                sequence: deferred,
                lock_token: token,
                replacement_envelope: None,
            }
        ),
        CommandOutcome::Abandoned {
            dead_lettered: false
        }
    );
    f.send(25, "scheduled", &session, Some(40));
    f.send(26, "ready", &session, None);
    f.advance(200); // Ready TTL and scheduled due time may remain unswept.
    let image = f.image();
    f.validate(&image);
    f.reopen_and_compare(&image);
}

#[test]
fn memory_independent_delivery_locks_deferred_and_unswept_routes() {
    independent_message_locks(MemoryProvider::new());
}
#[test]
fn fjall_independent_delivery_locks_deferred_and_unswept_routes() {
    independent_message_locks(DurableProvider::temporary().unwrap());
}

fn lock_corruptions<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    let session = id("CaSe");
    let accepted = f.accept(3, Some(session.clone()), None);
    let image = f.image();
    f.validate(&image);
    let primary = keys::session(&f.namespace, &f.queue, &session);
    let companion =
        keys::session_lock(&f.namespace, &f.queue, accepted.lock.locked_until, &session);
    let different = accepted.lock.locked_until.saturating_add_millis(1);
    assert_ne!(different, accepted.lock.locked_until);
    let wrong_deadline = keys::session_lock(&f.namespace, &f.queue, different, &session);
    let mut missing = image.clone();
    remove(&mut missing, &companion);
    relation(
        f.reject(&missing),
        row(&missing, &primary),
        "session is missing its exact lock index",
    );
    let mut changed = missing.clone();
    put(&mut changed, wrong_deadline.clone(), vec![]);
    relation(
        f.reject(&changed),
        row(&changed, &primary),
        "session is missing its exact lock index",
    );
    let mut orphan = image.clone();
    remove(&mut orphan, &primary);
    relation(
        f.reject(&orphan),
        row(&orphan, &companion),
        "session lock has no exact session record",
    );
    let mut unlocked = image.clone();
    let mut record = f.session(&image, &session);
    record.lock = None;
    put(
        &mut unlocked,
        primary.clone(),
        codec::encode(&record).unwrap(),
    );
    relation(
        f.reject(&unlocked),
        row(&unlocked, &companion),
        "session lock differs from its record deadline",
    );
    let mut extra = image.clone();
    put(&mut extra, wrong_deadline.clone(), vec![]);
    relation(
        f.reject(&extra),
        row(&extra, &wrong_deadline),
        "session lock differs from its record deadline",
    );
    let other_namespace = NamespaceName::new("other").unwrap();
    let other_endpoint = EntityPath::new("missing").unwrap();
    for wrong in [
        keys::session_lock(
            &f.namespace,
            &f.queue,
            accepted.lock.locked_until,
            &id("case"),
        ),
        keys::session_lock(
            &other_namespace,
            &f.queue,
            accepted.lock.locked_until,
            &session,
        ),
        keys::session_lock(
            &f.namespace,
            &other_endpoint,
            accepted.lock.locked_until,
            &session,
        ),
    ] {
        let mut broken = image.clone();
        put(&mut broken, wrong.clone(), vec![]);
        let error = f.reject(&broken);
        assert!(matches!(
            error,
            SnapshotSessionError::InconsistentSession { .. }
        ));
        assert_eq!(state_row(error), row(&broken, &wrong));
    }
    let mut both = missing.clone();
    let wrong = keys::session_lock(&f.namespace, &f.queue, different, &id("orphan"));
    put(&mut both, wrong, vec![]);
    relation(
        f.reject(&both),
        row(&both, &primary),
        "session is missing its exact lock index",
    );
    assert_eq!(f.image(), image);
    f.reopen_and_compare(&image);
}

#[test]
fn memory_bidirectional_lock_companions_deadlines_and_exact_scopes() {
    lock_corruptions(MemoryProvider::new());
}
#[test]
fn fjall_bidirectional_lock_companions_deadlines_and_exact_scopes() {
    lock_corruptions(DurableProvider::temporary().unwrap());
}

fn canonical_and_priority<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    let session = id("CaSe");
    let seq = f.send(3, "ready", &session, None);
    let accepted = f.accept(4, Some(session.clone()), None);
    let image = f.image();
    let primary = keys::session(&f.namespace, &f.queue, &session);
    let lock = keys::session_lock(&f.namespace, &f.queue, accepted.lock.locked_until, &session);
    let ready = keys::session_ready(&f.namespace, &f.queue, &session, seq);
    for key in [&primary, &lock, &ready] {
        let mut bad_key = key.clone();
        bad_key[1] = 255;
        let mut broken = image.clone();
        remove(&mut broken, key);
        put(&mut broken, bad_key.clone(), value(&image, key));
        let error = f.reject(&broken);
        assert!(matches!(
            error,
            SnapshotSessionError::State(SnapshotStateError::InvalidKey { .. })
        ));
        assert_eq!(state_row(error), row(&broken, &bad_key));
        let mut broken = image.clone();
        put(&mut broken, key.clone(), vec![255]);
        let error = f.reject(&broken);
        assert!(matches!(
            error,
            SnapshotSessionError::State(SnapshotStateError::InvalidValue { .. })
        ));
        assert_eq!(state_row(error), row(&broken, key));
    }
    let mut trailing = image.clone();
    let mut bytes = value(&image, &primary);
    bytes.push(0);
    put(&mut trailing, primary.clone(), bytes);
    assert_eq!(state_row(f.reject(&trailing)), row(&trailing, &primary));
    let mut short = image.clone();
    remove(&mut short, &primary);
    let shortened = primary[..primary.len() - 1].to_vec();
    put(&mut short, shortened.clone(), value(&image, &primary));
    assert_eq!(state_row(f.reject(&short)), row(&short, &shortened));
    let mut empty_lock_id = image.clone();
    remove(&mut empty_lock_id, &lock);
    let shortened = lock[..lock.len() - session.as_str().len()].to_vec();
    put(&mut empty_lock_id, shortened.clone(), vec![]);
    let error = f.reject(&empty_lock_id);
    assert!(matches!(
        error,
        SnapshotSessionError::State(SnapshotStateError::InvalidKey { .. })
    ));
    assert_eq!(state_row(error), row(&empty_lock_id, &shortened));
    let mut bad_clock = image.clone();
    put(&mut bad_clock, keys::clock(), vec![255]);
    remove(&mut bad_clock, &lock);
    assert!(matches!(
        f.reject(&bad_clock),
        SnapshotSessionError::State(SnapshotStateError::Catalog(
            SnapshotCatalogError::InvalidValue { row: 0, .. }
        ))
    ));
    let mut canonical_before_graph = image.clone();
    remove(&mut canonical_before_graph, &ready);
    put(&mut canonical_before_graph, lock.clone(), vec![1]);
    assert!(matches!(
        f.reject(&canonical_before_graph),
        SnapshotSessionError::State(SnapshotStateError::InvalidValue { .. })
    ));
    assert_eq!(
        state_row(f.reject(&canonical_before_graph)),
        row(&canonical_before_graph, &lock)
    );
    let mut message_before_session = image.clone();
    remove(&mut message_before_session, &ready);
    remove(&mut message_before_session, &lock);
    let old_error = validate_message_rows(&message_before_session).unwrap_err();
    let wrapped = f.reject(&message_before_session);
    assert_eq!(wrapped, SnapshotSessionError::State(old_error));
    assert_eq!(wrapped.to_string(), old_error.to_string());
    let message = keys::message(&f.namespace, &f.queue, seq);
    assert_eq!(
        state_row(f.reject(&message_before_session)),
        row(&message_before_session, &message)
    );
    let mut wrong_case = image.clone();
    remove(&mut wrong_case, &ready);
    let lower = keys::session_ready(&f.namespace, &f.queue, &id("case"), seq);
    put(&mut wrong_case, lower, vec![]);
    assert_eq!(state_row(f.reject(&wrong_case)), row(&wrong_case, &message));
    for orphan in [
        keys::session_ready(&f.namespace, &f.queue, &session, SequenceNumber::new(999)),
        keys::session_ready(
            &NamespaceName::new("other").unwrap(),
            &f.queue,
            &session,
            seq,
        ),
        keys::session_ready(&f.namespace, &f.plain, &session, seq),
    ] {
        let mut broken = image.clone();
        put(&mut broken, orphan.clone(), vec![]);
        let error = f.reject(&broken);
        assert!(matches!(
            error,
            SnapshotSessionError::State(SnapshotStateError::InconsistentMessage { .. })
        ));
        assert_eq!(state_row(error), row(&broken, &orphan));
    }
    f.reopen_and_compare(&image);
}

#[test]
fn memory_canonical_session_forms_and_original_phase_ordinals() {
    canonical_and_priority(MemoryProvider::new());
}
#[test]
fn fjall_canonical_session_forms_and_original_phase_ordinals() {
    canonical_and_priority(DurableProvider::temporary().unwrap());
}

fn owner_roles_and_partial_report<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    let session = id("state-only");
    let accepted = f.accept(3, Some(session.clone()), None);
    f.at(
        &f.queue,
        4,
        CommandKind::ReleaseSession {
            session: accepted.hold(),
        },
    );
    let topic = EntityPath::new("events").unwrap();
    f.at(
        &topic,
        5,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    );
    let subscription = match f.at(
        &topic,
        6,
        CommandKind::CreateSubscription {
            name: SubscriptionName::new("first").unwrap(),
            config: SubscriptionConfig::default(),
        },
    ) {
        CommandOutcome::SubscriptionCreated { entity } => entity,
        outcome => panic!("expected subscription: {outcome:?}"),
    };
    f.send(7, "still-implicit", &session, None);
    let image = f.image();
    let report = f.validate(&image);
    assert_eq!(report.session_rows(), 1);
    assert_eq!(report.session_lock_rows(), 0);
    let queue_shadow = f.queue.dead_letter_queue().unwrap();
    let subscription_shadow = subscription.dead_letter_queue().unwrap();
    for endpoint in [
        &f.plain,
        &topic,
        &subscription,
        &queue_shadow,
        &subscription_shadow,
    ] {
        let key = keys::session(&f.namespace, endpoint, &session);
        let mut broken = image.clone();
        put(
            &mut broken,
            key.clone(),
            codec::encode(&SessionRecord::default()).unwrap(),
        );
        relation(
            f.reject(&broken),
            row(&broken, &key),
            "session endpoint is not a session queue",
        );
        let mut lock_only = image.clone();
        let lock = keys::session_lock(&f.namespace, endpoint, Timestamp::from_millis(10), &session);
        put(&mut lock_only, lock.clone(), vec![]);
        relation(
            f.reject(&lock_only),
            row(&lock_only, &lock),
            "session endpoint is not a session queue",
        );
    }
    let ghost = keys::session(&f.namespace, &EntityPath::new("missing").unwrap(), &session);
    let mut absent_owner = image.clone();
    put(
        &mut absent_owner,
        ghost.clone(),
        codec::encode(&SessionRecord::default()).unwrap(),
    );
    relation(
        f.reject(&absent_owner),
        row(&absent_owner, &ghost),
        "session has no catalog endpoint",
    );
    // Canonical but unmatched history/allocation/external shapes are explicitly
    // injected pending facets, not supported histories or full-health evidence.
    let mut pending = image.clone();
    put(
        &mut pending,
        keys::duplicate_id(&f.namespace, &f.queue, "Unicode-\u{e9}\0tail"),
        codec::encode(&Timestamp::from_millis(99)).unwrap(),
    );
    let counter_key = keys::queue_counters(&f.namespace, &f.queue);
    let mut counters: QueueCounters = codec::decode(&value(&image, &counter_key)).unwrap();
    assert!(counters.next_sequence > 1);
    counters.next_sequence = 1; // Canonical/nonzero, but behind the retained message.
    put(&mut pending, counter_key, codec::encode(&counters).unwrap());
    put(&mut pending, vec![0xF0], vec![255]);
    put(&mut pending, vec![0xF1], vec![0, 255]);
    let old_report = validate_message_rows(&pending).unwrap();
    let partial = f.validate(&pending);
    assert_eq!(partial.pending_duplicate_rows(), 1);
    assert_eq!(
        partial.pending_counter_rows(),
        old_report.pending_counter_rows()
    );
    assert_eq!(partial.pending_external_rows(), 2);
    assert_eq!(partial.message_rows(), old_report.message_rows());
    assert_eq!(
        partial.message_index_rows(),
        old_report.message_index_rows()
    );
    assert_eq!(validate_message_rows(&pending).unwrap(), old_report);
    assert_eq!(partial.catalog(), old_report.catalog());
    // Zero observed history/external rows still do not certify their relationships.
    assert_eq!(report.pending_duplicate_rows(), 0);
    assert_eq!(report.pending_external_rows(), 0);
    assert!(!report.duplicate_relations_checked());
    assert!(!report.external_relations_checked());
    f.reopen_and_compare(&image);
}

#[test]
fn memory_endpoint_roles_and_explicit_pending_facets() {
    owner_roles_and_partial_report(MemoryProvider::new());
}
#[test]
fn fjall_endpoint_roles_and_explicit_pending_facets() {
    owner_roles_and_partial_report(DurableProvider::temporary().unwrap());
}

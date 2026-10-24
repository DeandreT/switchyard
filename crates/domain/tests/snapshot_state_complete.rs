//! Supported paired traces are distinct from qualified injected allocation
//! images. Neither proves historical uniqueness or natural exhaustion.

use domain::{
    AcceptedSession, Command, CommandKind, CommandOutcome, DeadLetterInfo, DeadLetterReason,
    Delivery, DeliveryOrigin, EntityPath, LockToken, MessageEnvelope, MessageRecord, MessageState,
    NamespaceName, QueueConfig, QueueConfigUpdate, QueueCounters, QueueTimeToLiveUpdate,
    ReceiveMode, SequenceNumber, SessionHold, SessionId, SessionLock, SessionRecord, StateMachine,
    SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig, codec, keys,
    snapshot_validation::{
        CompleteStateValidation, SnapshotCatalogError, SnapshotCompleteStateError,
        SnapshotStateError, validate_complete_state, validate_duplicate_rows,
        validate_message_rows, validate_session_rows,
    },
};
use storage::{Key, StateStore, Value};
use testkit::{DurableProvider, MemoryProvider, StoreProvider};

type Image = Vec<(Key, Value)>;

const SEQUENCE_FRONTIER: &str = "message sequence is not positive and below its owner frontier";
const SEQUENCE_ALIAS: &str = "message sequence aliases another retained allocation claim";
const TOKEN_FRONTIER: &str = "retained lock token is not positive and below its endpoint frontier";
const TOKEN_ALIAS: &str = "lock token aliases another retained endpoint hold";

struct Fixture<P: StoreProvider> {
    // Drop the final machine/store handle before the provider's directory.
    machine: StateMachine<P::Store>,
    provider: P,
    namespace: NamespaceName,
}

impl<P: StoreProvider> Fixture<P> {
    fn new(provider: P) -> Self {
        Self {
            machine: StateMachine::new(provider.open().unwrap()),
            provider,
            namespace: NamespaceName::new("tenant").unwrap(),
        }
    }

    fn at_ns(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
        time: u64,
        kind: CommandKind,
    ) -> CommandOutcome {
        self.machine
            .apply(&Command::new(
                namespace.clone(),
                entity.clone(),
                Timestamp::from_millis(time),
                kind,
            ))
            .unwrap()
    }

    fn at(&self, entity: &EntityPath, time: u64, kind: CommandKind) -> CommandOutcome {
        self.at_ns(&self.namespace, entity, time, kind)
    }

    fn queue(&self, name: &str, config: QueueConfig) -> EntityPath {
        let entity = path(name);
        assert_eq!(
            self.at(&entity, 1, CommandKind::CreateQueue { config }),
            CommandOutcome::QueueCreated
        );
        entity
    }

    fn topic(&self, name: &str) -> (EntityPath, EntityPath, EntityPath) {
        let topic = path(name);
        self.at(
            &topic,
            1,
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        );
        let children = ["a", "b"].map(|name| {
            match self.at(
                &topic,
                1,
                CommandKind::CreateSubscription {
                    name: SubscriptionName::new(name).unwrap(),
                    config: SubscriptionConfig::default(),
                },
            ) {
                CommandOutcome::SubscriptionCreated { entity } => entity,
                other => panic!("subscription: {other:?}"),
            }
        });
        let [a, b] = children;
        (topic, a, b)
    }

    fn send(
        &self,
        entity: &EntityPath,
        time: u64,
        id: &str,
        session: Option<SessionId>,
        scheduled: Option<u64>,
    ) -> SequenceNumber {
        match self.at(
            entity,
            time,
            CommandKind::Send {
                message_id: id.to_owned(),
                body: vec![255, 0, 1],
                time_to_live_millis: None,
                session_id: session,
                scheduled_enqueue_at: scheduled.map(Timestamp::from_millis),
                envelope: Some(MessageEnvelope::new(vec![0, 254, 7])),
            },
        ) {
            CommandOutcome::Sent { sequence } => sequence,
            CommandOutcome::Published { sequences, .. } => sequences[0],
            other => panic!("send: {other:?}"),
        }
    }

    fn receive(&self, entity: &EntityPath, time: u64, session: Option<SessionHold>) -> Delivery {
        match self.at(
            entity,
            time,
            CommandKind::Receive {
                mode: ReceiveMode::PeekLock,
                lock_duration_millis: Some(100),
                session,
            },
        ) {
            CommandOutcome::Received(Some(delivery)) => delivery,
            other => panic!("receive: {other:?}"),
        }
    }

    fn accept(&self, entity: &EntityPath, time: u64, id: &SessionId) -> AcceptedSession {
        match self.at(
            entity,
            time,
            CommandKind::AcceptSession {
                session_id: Some(id.clone()),
                lock_duration_millis: Some(100),
            },
        ) {
            CommandOutcome::SessionAccepted(Some(accepted)) => accepted,
            other => panic!("accept: {other:?}"),
        }
    }

    fn image(&self) -> Image {
        self.machine.store().snapshot().unwrap().entries().to_vec()
    }

    fn check(&self, image: &Image) -> CompleteStateValidation {
        let original = image.clone();
        let stored = self.image();
        let clock = clock_row(image);
        let report = validate_complete_state(image).expect("checked complete domain image");
        assert_eq!(image, &original, "every input byte survives validation");
        assert_eq!(self.image(), stored, "validation never writes to storage");
        assert_eq!(clock_row(image), clock);
        assert_eq!(
            report.catalog().catalog_rows()
                + report.message_rows()
                + report.message_index_rows()
                + report.session_rows()
                + report.session_lock_rows()
                + report.lookup_rows()
                + report.expiry_rows()
                + report.pending_external_rows(),
            image.len(),
            "09 overlaps message indexes; counters are already catalog rows"
        );
        assert_eq!(
            report.counter_rows(),
            image.iter().filter(|(key, _)| key[0] == 2).count()
        );
        assert!(report.session_relations_checked());
        assert!(report.duplicate_relations_checked());
        assert!(report.allocation_relations_checked());
        assert!(!report.external_relations_checked());
        assert!(!report.storage_provenance_checked());
        assert!(!report.retained_authority_checked());
        assert!(!report.installation_checked());
        report
    }

    fn reject(&self, image: &Image) -> SnapshotCompleteStateError {
        let original = image.clone();
        let stored = self.image();
        let clock = clock_row(image);
        let error = validate_complete_state(image).expect_err("refuse injected corruption");
        assert_eq!(image, &original, "every input byte survives refusal");
        assert_eq!(self.image(), stored, "refusal never writes to storage");
        assert_eq!(clock_row(image), clock);
        error
    }

    fn allocation(&self, image: &Image, key: &[u8], detail: &'static str) {
        // The old APIs continue to accept these canonical allocation-pending
        // images and return their unchanged observations.
        let messages = validate_message_rows(image).unwrap();
        let sessions = validate_session_rows(image).unwrap();
        let duplicates = validate_duplicate_rows(image).unwrap();
        assert!(!messages.allocation_relations_checked());
        assert!(!sessions.allocation_relations_checked());
        assert!(!duplicates.allocation_relations_checked());
        assert_eq!(sessions.catalog(), messages.catalog());
        assert_eq!(duplicates.catalog(), messages.catalog());
        assert_eq!(sessions.message_rows(), messages.message_rows());
        assert_eq!(
            duplicates.message_index_rows(),
            messages.message_index_rows()
        );
        assert_eq!(validate_message_rows(image).unwrap(), messages);
        assert_eq!(validate_session_rows(image).unwrap(), sessions);
        assert_eq!(validate_duplicate_rows(image).unwrap(), duplicates);
        assert_eq!(
            self.reject(image),
            SnapshotCompleteStateError::InconsistentAllocation {
                row: row(image, key),
                detail,
            }
        );
    }

    fn reopen(self, image: &Image) {
        let Self {
            machine,
            provider,
            namespace,
        } = self;
        drop(machine);
        // DurableProvider opens only after every prior machine/store handle is
        // dropped. MemoryProvider provides the equivalent logical reopen.
        let reopened = Self {
            machine: StateMachine::new(provider.open().unwrap()),
            provider,
            namespace,
        };
        assert_eq!(&reopened.image(), image);
        reopened.check(image);
    }
}

fn path(name: &str) -> EntityPath {
    EntityPath::new(name).unwrap()
}
fn session(name: &str) -> SessionId {
    SessionId::new(name).unwrap()
}
fn clock_row(image: &Image) -> Option<(Key, Value)> {
    image.iter().find(|(key, _)| key == &keys::clock()).cloned()
}
fn value(image: &Image, key: &[u8]) -> Value {
    image.iter().find(|(k, _)| k == key).unwrap().1.clone()
}
fn row(image: &Image, key: &[u8]) -> usize {
    image.iter().position(|(k, _)| k == key).unwrap()
}
fn put(image: &mut Image, key: Key, raw: Value) {
    image.retain(|(k, _)| k != &key);
    image.push((key, raw));
    image.sort_by(|a, b| a.0.cmp(&b.0));
}
fn remove(image: &mut Image, key: &[u8]) {
    let before = image.len();
    image.retain(|(k, _)| k != key);
    assert_eq!(image.len() + 1, before);
}
fn counters(image: &mut Image, ns: &NamespaceName, entity: &EntityPath, seq: u64, token: u64) {
    put(
        image,
        keys::queue_counters(ns, entity),
        codec::encode(&QueueCounters {
            next_sequence: seq,
            next_lock_token: token,
        })
        .unwrap(),
    );
}

// Build qualified, internally consistent message/index graphs without claiming
// that arbitrary values, aliases or opaque payload differences are reachable.
fn record(entity: &EntityPath, sequence: u64) -> MessageRecord {
    MessageRecord {
        sequence: SequenceNumber::new(sequence),
        message_id: "injected".to_owned(),
        body: vec![0, 255],
        enqueued_at: Timestamp::from_millis(1),
        scheduled_enqueue_at: None,
        expires_at: None,
        delivery_count: 0,
        state: MessageState::Ready,
        session_id: None,
        dead_letter: entity.is_dead_letter_queue().then(|| DeadLetterInfo {
            reason: DeadLetterReason::Application("qualified fixture".to_owned()),
            description: String::new(),
            dead_lettered_at: Timestamp::from_millis(1),
        }),
        envelope: Some(MessageEnvelope::new(vec![255, 1])),
    }
}
fn indexes(ns: &NamespaceName, entity: &EntityPath, record: &MessageRecord) -> Vec<Key> {
    let seq = record.sequence;
    match record.state {
        MessageState::Ready => {
            let mut result = vec![match &record.session_id {
                Some(session) => keys::session_ready(ns, entity, session, seq),
                None => keys::ready(ns, entity, seq),
            }];
            if let Some(deadline) = record.expires_at {
                result.push(keys::expiry(ns, entity, deadline, seq));
            }
            result
        }
        MessageState::Locked { locked_until, .. } => {
            vec![keys::lock(ns, entity, locked_until, seq)]
        }
        MessageState::Deferred => vec![keys::deferred(ns, entity, seq)],
        MessageState::Scheduled => vec![keys::scheduled(
            ns,
            entity,
            record.scheduled_enqueue_at.unwrap(),
            seq,
        )],
    }
}
fn put_message(image: &mut Image, ns: &NamespaceName, entity: &EntityPath, record: &MessageRecord) {
    put(
        image,
        keys::message(ns, entity, record.sequence),
        codec::encode(record).unwrap(),
    );
    for key in indexes(ns, entity, record) {
        put(image, key, vec![]);
    }
}
fn put_session(
    image: &mut Image,
    ns: &NamespaceName,
    entity: &EntityPath,
    name: &str,
    token: u64,
) -> Key {
    let id = session(name);
    let lock = SessionLock {
        token: LockToken::new(token),
        locked_until: Timestamp::from_millis(1),
    };
    let key = keys::session(ns, entity, &id);
    put(
        image,
        key.clone(),
        codec::encode(&SessionRecord {
            lock: Some(lock),
            state: vec![0, 255, 3],
        })
        .unwrap(),
    );
    put(
        image,
        keys::session_lock(ns, entity, lock.locked_until, &id),
        vec![],
    );
    key
}
fn locked(mut record: MessageRecord, token: u64) -> MessageRecord {
    record.state = MessageState::Locked {
        token: LockToken::new(token),
        locked_until: Timestamp::from_millis(1),
        origin: DeliveryOrigin::Ready,
    };
    record
}

macro_rules! paired {
    ($memory:ident, $fjall:ident, $case:ident) => {
        #[test]
        fn $memory() {
            $case(MemoryProvider::new());
        }
        #[test]
        fn $fjall() {
            $case(DurableProvider::temporary().unwrap());
        }
    };
}

fn supported_mixed<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    let q = f.queue(
        "queue",
        QueueConfig {
            requires_duplicate_detection: true,
            default_time_to_live_millis: Some(100),
            ..QueueConfig::default()
        },
    );
    let s = f.queue(
        "sessions",
        QueueConfig {
            requires_session: true,
            ..QueueConfig::default()
        },
    );
    let clock = f.queue("clock", QueueConfig::default());
    let (topic, a, b) = f.topic("events");
    let first = f.send(&q, 2, "first", None, None);
    assert!(matches!(
        f.at(
            &q,
            3,
            CommandKind::Send {
                message_id: "first".to_owned(),
                body: vec![],
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: None,
                envelope: None,
            }
        ),
        CommandOutcome::DuplicateSuppressed { .. }
    ));
    let schedule = f.send(&q, 4, "scheduled", None, Some(50));
    let delivery = f.receive(&q, 5, None);
    assert_eq!(delivery.sequence, first);
    f.at(
        &q,
        6,
        CommandKind::DeadLetter {
            sequence: first,
            lock_token: delivery.lock.unwrap().token,
            reason: "application".to_owned(),
            description: String::new(),
            replacement_envelope: Some(MessageEnvelope::new(vec![254, 0])),
        },
    );
    let dlq = q.dead_letter_queue().unwrap();
    let dlq_delivery = f.receive(&dlq, 7, None);
    f.at(
        &dlq,
        8,
        CommandKind::Abandon {
            sequence: first,
            lock_token: dlq_delivery.lock.unwrap().token,
            replacement_envelope: None,
        },
    );
    f.check(&f.image()); // DLQ abandon does not cascade or allocate a new sequence.
    let deferred = f.send(&q, 9, "deferred", None, None);
    let delivery = f.receive(&q, 10, None);
    f.at(
        &q,
        11,
        CommandKind::Defer {
            sequence: deferred,
            lock_token: delivery.lock.unwrap().token,
            replacement_envelope: None,
        },
    );
    let id = session("CaSe");
    f.send(&s, 12, "session-lock", Some(id.clone()), None);
    f.send(&s, 13, "session-ready", Some(id.clone()), None);
    let accepted = f.accept(&s, 14, &id);
    f.receive(&s, 15, Some(accepted.hold()));
    f.at(
        &s,
        16,
        CommandKind::SetSessionState {
            session: accepted.hold(),
            state: vec![255, 0, 7],
        },
    );
    f.at(
        &s,
        17,
        CommandKind::RenewSessionLock {
            session: accepted.hold(),
            lock_duration_millis: Some(100),
        },
    );
    f.at(
        &s,
        18,
        CommandKind::ReleaseSession {
            session: accepted.hold(),
        },
    );
    f.check(&f.image()); // Releasing the session retains its independent message hold.
    let reacquired = f.accept(&s, 19, &id);
    assert_ne!(accepted.lock.token, reacquired.lock.token);
    assert_eq!(reacquired.state, vec![255, 0, 7]);
    let publication = f.send(&topic, 20, "fanout", None, None);
    let a_delivery = f.receive(&a, 21, None);
    assert_eq!(a_delivery.sequence, publication);
    f.at(
        &a,
        22,
        CommandKind::DeadLetter {
            sequence: publication,
            lock_token: a_delivery.lock.unwrap().token,
            reason: "copy evolves".to_owned(),
            description: String::new(),
            replacement_envelope: Some(MessageEnvelope::new(vec![255, 9, 0])),
        },
    );
    let b_delivery = f.receive(&b, 23, None);
    f.at(
        &b,
        24,
        CommandKind::Defer {
            sequence: publication,
            lock_token: b_delivery.lock.unwrap().token,
            replacement_envelope: Some(MessageEnvelope::new(vec![128, 8])),
        },
    );
    let placeholder = f.send(&topic, 25, "later", None, Some(50));
    f.send(&q, 26, "ready", None, None);
    let all_forms = f.image();
    f.check(&all_forms);
    for tag in 0x03..=0x0D {
        assert!(all_forms.iter().any(|(key, _)| key[0] == tag));
    }
    f.at(&q, 50, CommandKind::ActivateScheduled);
    f.at(&topic, 50, CommandKind::ActivateScheduled);
    let active = f.image();
    assert!(!active.iter().any(|(key, _)| {
        key == &keys::message(&f.namespace, &q, schedule)
            || key == &keys::message(&f.namespace, &topic, placeholder)
    }));
    f.at(
        &q,
        51,
        CommandKind::UpdateQueue {
            update: QueueConfigUpdate {
                max_message_bytes: Some(1),
                default_time_to_live_millis: Some(QueueTimeToLiveUpdate::Finite { millis: 1 }),
                ..QueueConfigUpdate::default()
            },
        },
    );
    // Unrelated work advances Clock, leaving all elapsed holds and retained
    // duplicate history unswept and old payloads larger than the new profile.
    f.send(&clock, 1_000_000, "clock", None, None);
    let image = f.image();
    let report = f.check(&image);
    assert_eq!(report.session_rows(), 1);
    assert_eq!(report.session_lock_rows(), 1);
    assert_eq!(report.session_ready_rows(), 1);
    assert_eq!(report.lookup_rows(), 4);
    assert_eq!(report.stale_compatibility_rows(), 0);
    // Qualified extra elapsed compatibility expiry, not an ordinary trace.
    let mut compatible = image.clone();
    put(
        &mut compatible,
        keys::duplicate_expiry(&f.namespace, &q, Timestamp::from_millis(1), "first"),
        vec![],
    );
    assert_eq!(f.check(&compatible).stale_compatibility_rows(), 1);
    f.reopen(&image);
}

paired!(
    memory_supported_mixed,
    fjall_supported_mixed,
    supported_mixed
);

fn sequence_frontiers<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    let q = f.queue("queue", QueueConfig::default());
    let (topic, a, _) = f.topic("events");
    let base = f.image();
    // Each role has the same original owner, even though child-local counters
    // may be absent or arbitrarily further ahead.
    for (endpoint, owner, topic_placeholder) in [
        (q.clone(), q.clone(), false),
        (q.dead_letter_queue().unwrap(), q.clone(), false),
        (topic.clone(), topic.clone(), true),
        (a.clone(), topic.clone(), false),
        (a.dead_letter_queue().unwrap(), topic.clone(), false),
    ] {
        for (sequence, next, accepted) in [
            (0, 8, false),
            (1, 8, true),
            (7, 8, true),
            (8, 8, false),
            (9, 8, false),
            (u64::MAX - 1, u64::MAX, true),
            (u64::MAX, u64::MAX, false),
        ] {
            let mut image = base.clone();
            counters(&mut image, &f.namespace, &owner, next, 1);
            if endpoint != owner {
                counters(&mut image, &f.namespace, &endpoint, u64::MAX, 1);
            }
            let mut message = record(&endpoint, sequence);
            if topic_placeholder {
                message.state = MessageState::Scheduled;
                message.scheduled_enqueue_at = Some(Timestamp::from_millis(99));
            }
            put_message(&mut image, &f.namespace, &endpoint, &message);
            if accepted {
                f.check(&image);
            } else {
                f.allocation(
                    &image,
                    &keys::message(&f.namespace, &endpoint, message.sequence),
                    SEQUENCE_FRONTIER,
                );
            }
        }
        let mut missing = base.clone();
        let key = keys::queue_counters(&f.namespace, &owner);
        if missing.iter().any(|(k, _)| k == &key) {
            remove(&mut missing, &key);
        }
        if endpoint != owner {
            counters(&mut missing, &f.namespace, &endpoint, u64::MAX, 1);
        }
        let mut message = record(&endpoint, 1);
        if topic_placeholder {
            message.state = MessageState::Scheduled;
            message.scheduled_enqueue_at = Some(Timestamp::from_millis(99));
        }
        put_message(&mut missing, &f.namespace, &endpoint, &message);
        f.allocation(
            &missing,
            &keys::message(&f.namespace, &endpoint, message.sequence),
            SEQUENCE_FRONTIER,
        );
        // No retained identifier means a lazy or unused non-one frontier is
        // fine: neither contiguity nor next == max(retained) + 1 is required.
        let mut gaps = base.clone();
        counters(&mut gaps, &f.namespace, &owner, u64::MAX, u64::MAX);
        f.check(&gaps);
        if endpoint != owner {
            let mut no_local = base.clone();
            counters(&mut no_local, &f.namespace, &owner, 8, 1);
            let local = keys::queue_counters(&f.namespace, &endpoint);
            if no_local.iter().any(|(k, _)| k == &local) {
                remove(&mut no_local, &local);
            }
            put_message(&mut no_local, &f.namespace, &endpoint, &message);
            f.check(&no_local);
        }
    }
    f.reopen(&base);
}

paired!(
    memory_sequence_frontiers,
    fjall_sequence_frontiers,
    sequence_frontiers
);

fn sequence_alias_matrix<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    let q = f.queue("queue", QueueConfig::default());
    let other_q = f.queue("other", QueueConfig::default());
    let (topic, a, b) = f.topic("events");
    let (other_topic, other_a, _) = f.topic("other-events");
    let other_ns = NamespaceName::new("other-tenant").unwrap();
    f.at_ns(
        &other_ns,
        &q,
        1,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    );
    // namespace, endpoint, original owner, subscription family (if any).
    let endpoints = [
        (f.namespace.clone(), q.clone(), q.clone(), None),
        (
            f.namespace.clone(),
            q.dead_letter_queue().unwrap(),
            q.clone(),
            None,
        ),
        (f.namespace.clone(), other_q.clone(), other_q, None),
        (f.namespace.clone(), topic.clone(), topic.clone(), None),
        (
            f.namespace.clone(),
            a.clone(),
            topic.clone(),
            Some(a.clone()),
        ),
        (
            f.namespace.clone(),
            a.dead_letter_queue().unwrap(),
            topic.clone(),
            Some(a),
        ),
        (
            f.namespace.clone(),
            b.clone(),
            topic.clone(),
            Some(b.clone()),
        ),
        (
            f.namespace.clone(),
            b.dead_letter_queue().unwrap(),
            topic,
            Some(b),
        ),
        (
            f.namespace.clone(),
            other_a.clone(),
            other_topic,
            Some(other_a),
        ),
        (other_ns, q.clone(), q, None),
    ];
    // Every pair, including both canonical DLQs, different namespaces and
    // owners. Exhaustion does not waive aliases. Copies deliberately carry
    // different opaque bodies/envelopes; equality is not an allocation fact.
    for (sequence, next) in [(7, 8), (u64::MAX - 1, u64::MAX)] {
        let mut base = f.image();
        for (ns, _, owner, _) in &endpoints {
            counters(&mut base, ns, owner, next, 1);
        }
        for (i, left) in endpoints.iter().enumerate() {
            for right in &endpoints[i + 1..] {
                let mut image = base.clone();
                let mut primaries = Vec::new();
                for (index, (ns, entity, owner, _)) in [left, right].into_iter().enumerate() {
                    let mut message = record(entity, sequence);
                    message.body = vec![index as u8, 255];
                    message.envelope = Some(MessageEnvelope::new(vec![255, index as u8, 0]));
                    if entity == owner && entity != &path("queue") && entity != &path("other") {
                        message.state = MessageState::Scheduled;
                        message.scheduled_enqueue_at = Some(Timestamp::from_millis(99));
                    }
                    put_message(&mut image, ns, entity, &message);
                    primaries.push(keys::message(ns, entity, message.sequence));
                }
                let distinct_owner = left.0 != right.0 || left.2 != right.2;
                let distinct_family = matches!((&left.3, &right.3), (Some(a), Some(b)) if a != b);
                if distinct_owner || distinct_family {
                    f.check(&image);
                } else {
                    primaries.sort();
                    f.allocation(&image, &primaries[1], SEQUENCE_ALIAS);
                }
            }
        }
        // Remember every accepted family, not just the last copy. Exercise
        // topic exclusivity and a repeated family after another was accepted.
        for (indices, first_conflict) in [
            (&[4, 6, 7][..], 2),
            (&[5, 6, 7][..], 2),
            (&[3, 4, 6][..], 1),
            (&[4, 5, 6][..], 1),
            (&[4, 5, 6, 7][..], 1),
        ] {
            let mut image = base.clone();
            let mut primaries = Vec::new();
            for &index in indices {
                let (ns, entity, owner, _) = &endpoints[index];
                let mut message = record(entity, sequence);
                if entity == owner {
                    message.state = MessageState::Scheduled;
                    message.scheduled_enqueue_at = Some(Timestamp::from_millis(99));
                }
                put_message(&mut image, ns, entity, &message);
                primaries.push(keys::message(ns, entity, message.sequence));
            }
            primaries.sort();
            f.allocation(&image, &primaries[first_conflict], SEQUENCE_ALIAS);
        }
    }
    let stored = f.image();
    f.reopen(&stored);
}

paired!(
    memory_sequence_alias_matrix,
    fjall_sequence_alias_matrix,
    sequence_alias_matrix
);

fn token_frontiers<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    let q = f.queue("queue", QueueConfig::default());
    let s = f.queue(
        "sessions",
        QueueConfig {
            requires_session: true,
            ..QueueConfig::default()
        },
    );
    let (topic, a, _) = f.topic("events");
    let base = f.image();
    for (endpoint, owner, is_session) in [
        (q.clone(), q.clone(), false),
        (q.dead_letter_queue().unwrap(), q.clone(), false),
        (a.clone(), topic.clone(), false),
        (a.dead_letter_queue().unwrap(), topic.clone(), false),
        (s.clone(), s.clone(), false),
        (s.clone(), s.clone(), true),
    ] {
        for (token, next, accepted) in [
            (0, 8, false),
            (1, 8, true),
            (7, 8, true),
            (8, 8, false),
            (9, 8, false),
            (u64::MAX - 1, u64::MAX, true),
            (u64::MAX, u64::MAX, false),
        ] {
            let mut image = base.clone();
            counters(&mut image, &f.namespace, &owner, 10, u64::MAX);
            counters(&mut image, &f.namespace, &endpoint, 10, next);
            let primary = if is_session {
                put_session(&mut image, &f.namespace, &endpoint, "CaSe", token)
            } else {
                let mut message = locked(record(&endpoint, 7), token);
                if endpoint == s {
                    message.session_id = Some(session("CaSe"));
                }
                put_message(&mut image, &f.namespace, &endpoint, &message);
                keys::message(&f.namespace, &endpoint, message.sequence)
            };
            if accepted {
                f.check(&image); // Deadlines equal Clock are still retained claims.
            } else {
                f.allocation(&image, &primary, TOKEN_FRONTIER);
            }
            if token == 1 {
                let local = keys::queue_counters(&f.namespace, &endpoint);
                remove(&mut image, &local);
                // For a message at its original owner, retain a valid sequence
                // frontier but exercise the missing token default separately
                // with session-only rows. Child messages need no local sequence.
                if is_session || endpoint != owner {
                    f.allocation(&image, &primary, TOKEN_FRONTIER);
                } else {
                    counters(&mut image, &f.namespace, &endpoint, 10, 1);
                    f.allocation(&image, &primary, TOKEN_FRONTIER);
                }
            }
        }
    }
    f.reopen(&base);
}

paired!(
    memory_token_frontiers,
    fjall_token_frontiers,
    token_frontiers
);

fn token_aliases<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    let q = f.queue("queue", QueueConfig::default());
    let other_q = f.queue("other", QueueConfig::default());
    let s = f.queue(
        "sessions",
        QueueConfig {
            requires_session: true,
            ..QueueConfig::default()
        },
    );
    let other_s = f.queue(
        "other-sessions",
        QueueConfig {
            requires_session: true,
            ..QueueConfig::default()
        },
    );
    let (topic, a, b) = f.topic("events");
    let ns2 = NamespaceName::new("other-tenant").unwrap();
    f.at_ns(
        &ns2,
        &q,
        1,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    );
    let base = f.image();
    // message/message, session/session, and message/session all share a token
    // namespace. Paired companion indexes never claim the token a second time.
    for mode in 0..3 {
        for token in [7, u64::MAX - 1] {
            let mut image = base.clone();
            put(
                &mut image,
                keys::clock(),
                codec::encode(&Timestamp::from_millis(2)).unwrap(),
            );
            counters(&mut image, &f.namespace, &s, 10, u64::MAX);
            let mut primaries = Vec::new();
            for i in 0..2 {
                if mode == 1 || (mode == 2 && i == 1) {
                    primaries.push(put_session(
                        &mut image,
                        &f.namespace,
                        &s,
                        if i == 0 { "CaSe" } else { "case" },
                        token,
                    ));
                } else {
                    let mut message = locked(record(&s, 7 + i), token);
                    message.session_id = Some(session("CaSe"));
                    primaries.push(keys::message(&f.namespace, &s, message.sequence));
                    put_message(&mut image, &f.namespace, &s, &message);
                }
            }
            primaries.sort();
            f.allocation(&image, &primaries[1], TOKEN_ALIAS);
        }
    }
    // Every pair of distinct token endpoints is legal, including an ordinary
    // queue vs its DLQ, a subscription vs its DLQ, and identical endpoint names
    // in different namespaces. A high original-owner frontier cannot rescue
    // any child's invalid local token (covered above).
    let endpoints = [
        (f.namespace.clone(), q.clone(), q.clone(), false),
        (
            f.namespace.clone(),
            q.dead_letter_queue().unwrap(),
            q,
            false,
        ),
        (f.namespace.clone(), other_q.clone(), other_q, false),
        (f.namespace.clone(), a.clone(), topic.clone(), false),
        (
            f.namespace.clone(),
            a.dead_letter_queue().unwrap(),
            topic.clone(),
            false,
        ),
        (f.namespace.clone(), b, topic, false),
        (f.namespace.clone(), s.clone(), s, true),
        (f.namespace.clone(), other_s.clone(), other_s, true),
        (ns2, path("queue"), path("queue"), false),
    ];
    for (i, left) in endpoints.iter().enumerate() {
        for right in &endpoints[i + 1..] {
            let mut image = base.clone();
            for (ns, entity, owner, is_session) in [left, right] {
                counters(&mut image, ns, owner, 10, 8);
                counters(&mut image, ns, entity, 10, 8);
                if *is_session {
                    put_session(&mut image, ns, entity, "hold", 7);
                } else {
                    // Distinct sequence slots avoid confusing a same-family
                    // sequence alias with this independent token assertion.
                    let seq = if entity.is_dead_letter_queue() { 6 } else { 7 };
                    put_message(&mut image, ns, entity, &locked(record(entity, seq), 7));
                }
            }
            f.check(&image);
        }
    }
    f.reopen(&base);
}

paired!(memory_token_aliases, fjall_token_aliases, token_aliases);

fn phase_priority<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    let s = f.queue(
        "sessions",
        QueueConfig {
            requires_session: true,
            requires_duplicate_detection: true,
            ..QueueConfig::default()
        },
    );
    let id = session("CaSe");
    let seq = f.send(&s, 2, "history", Some(id.clone()), None);
    let accepted = f.accept(&s, 3, &id);
    let delivery = f.receive(&s, 4, Some(accepted.hold()));
    let base = f.image();
    let primary = keys::message(&f.namespace, &s, seq);
    let message_lock = keys::lock(&f.namespace, &s, delivery.lock.unwrap().locked_until, seq);
    let session_primary = keys::session(&f.namespace, &s, &id);
    let session_lock = keys::session_lock(&f.namespace, &s, accepted.lock.locked_until, &id);
    let lookup = keys::duplicate_id(&f.namespace, &s, "history");
    let deadline: Timestamp = codec::decode(&value(&base, &lookup)).unwrap();
    let expiry = keys::duplicate_expiry(&f.namespace, &s, deadline, "history");
    let mut broken = base.clone();
    counters(&mut broken, &f.namespace, &s, 1, 1);
    remove(&mut broken, &message_lock);
    remove(&mut broken, &session_lock);
    remove(&mut broken, &expiry);
    put(&mut broken, lookup.clone(), vec![255]);
    put(&mut broken, keys::clock(), vec![255]);
    assert_eq!(
        f.reject(&broken),
        SnapshotCompleteStateError::State(SnapshotStateError::Catalog(
            SnapshotCatalogError::InvalidValue {
                row: 0,
                detail: "catalog value cannot be decoded"
            }
        ))
    );
    put(&mut broken, keys::clock(), value(&base, &keys::clock()));
    assert!(
        matches!(f.reject(&broken), SnapshotCompleteStateError::State(SnapshotStateError::InvalidValue { row: ordinal, .. }) if ordinal == row(&broken, &lookup))
    );
    put(&mut broken, lookup.clone(), value(&base, &lookup));
    let old = validate_message_rows(&broken).unwrap_err();
    assert_eq!(f.reject(&broken), SnapshotCompleteStateError::State(old));
    assert!(
        matches!(old, SnapshotStateError::InconsistentMessage { row: ordinal, .. } if ordinal == row(&broken, &primary))
    );
    assert_eq!(f.reject(&broken).to_string(), old.to_string());
    put(
        &mut broken,
        message_lock.clone(),
        value(&base, &message_lock),
    );
    let old = validate_session_rows(&broken).unwrap_err();
    assert_eq!(f.reject(&broken), SnapshotCompleteStateError::Sessions(old));
    assert!(
        matches!(old, domain::snapshot_validation::SnapshotSessionError::InconsistentSession { row: ordinal, .. } if ordinal == row(&broken, &session_primary))
    );
    assert_eq!(f.reject(&broken).to_string(), old.to_string());
    put(
        &mut broken,
        session_lock.clone(),
        value(&base, &session_lock),
    );
    let old = validate_duplicate_rows(&broken).unwrap_err();
    assert_eq!(
        f.reject(&broken),
        SnapshotCompleteStateError::Duplicate(old)
    );
    assert!(
        matches!(old, domain::snapshot_validation::SnapshotDuplicateError::InconsistentDuplicate { row: ordinal, .. } if ordinal == row(&broken, &lookup))
    );
    assert_eq!(f.reject(&broken).to_string(), old.to_string());
    put(&mut broken, expiry.clone(), value(&base, &expiry));
    f.allocation(&broken, &primary, SEQUENCE_FRONTIER);
    // On the same primary, sequence frontier precedes token frontier.
    counters(&mut broken, &f.namespace, &s, 2, 1);
    f.allocation(&broken, &primary, TOKEN_FRONTIER);
    // A later sequence failure must not displace an earlier token failure.
    let extra = locked(
        {
            let mut m = record(&s, 7);
            m.session_id = Some(id.clone());
            m
        },
        7,
    );
    put_message(&mut broken, &f.namespace, &s, &extra);
    f.allocation(&broken, &primary, TOKEN_FRONTIER);
    // Once earlier claims are valid, the later original primary is reported.
    counters(&mut broken, &f.namespace, &s, 2, 10);
    f.allocation(
        &broken,
        &keys::message(&f.namespace, &s, extra.sequence),
        SEQUENCE_FRONTIER,
    );
    // The second primary's sequence claim precedes its invalid local token.
    let mut alias = base.clone();
    let shadow = s.dead_letter_queue().unwrap();
    let copy = locked(record(&shadow, seq.as_u64()), u64::MAX);
    put_message(&mut alias, &f.namespace, &shadow, &copy);
    f.allocation(
        &alias,
        &keys::message(&f.namespace, &shadow, seq),
        SEQUENCE_ALIAS,
    );
    f.reopen(&base);
}

paired!(memory_phase_priority, fjall_phase_priority, phase_priority);

fn counter_only_zero_frontiers<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    let queue = f.queue("empty", QueueConfig::default());
    let base = f.image();
    assert!(clock_row(&base).is_some());
    assert!(!base.iter().any(|(key, _)| matches!(key[0], 0x03..=0x0D)));
    f.check(&base);
    let key = keys::queue_counters(&f.namespace, &queue);
    for (sequence, token) in [(0, 1), (1, 0)] {
        let mut image = base.clone();
        counters(&mut image, &f.namespace, &queue, sequence, token);
        // Catalog rejects present zero frontiers even without retained claims.
        // The shared refusal helper checks every input/store byte and Clock.
        assert_eq!(
            f.reject(&image),
            SnapshotCompleteStateError::State(SnapshotStateError::Catalog(
                SnapshotCatalogError::InvalidValue {
                    row: row(&image, &key),
                    detail: "allocation counters must be nonzero",
                }
            ))
        );
    }
    f.reopen(&base);
}

paired!(
    memory_counter_only_zero_frontiers,
    fjall_counter_only_zero_frontiers,
    counter_only_zero_frontiers
);

fn empty_and_external<P: StoreProvider>(provider: P) {
    let f = Fixture::new(provider);
    let empty = f.image();
    assert!(empty.is_empty());
    assert_eq!(f.check(&empty).catalog().clock(), None);
    let mut clock = vec![(
        keys::clock(),
        codec::encode(&Timestamp::from_millis(u64::MAX)).unwrap(),
    )];
    let report = f.check(&clock);
    assert_eq!(report.message_rows(), 0);
    assert_eq!(report.counter_rows(), 0);
    assert_eq!(report.pending_external_rows(), 0);
    put(&mut clock, vec![0xF0], vec![255]);
    put(&mut clock, vec![0xF1], vec![0, 255]);
    assert_eq!(f.check(&clock).pending_external_rows(), 2);
    // F0/F1 remain opaque even without a domain Clock.
    remove(&mut clock, &keys::clock());
    assert_eq!(f.check(&clock).pending_external_rows(), 2);
    f.reopen(&empty);
}

paired!(
    memory_empty_and_external,
    fjall_empty_and_external,
    empty_and_external
);

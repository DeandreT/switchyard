use domain::{
    Command, CommandKind, CommandOutcome, Delivery, EntityPath, MessageRecord, NamespaceName,
    QueueConfig, ReceiveMode, SequenceNumber, StateMachine, Timestamp, codec, keys,
    snapshot_validation::{MessageRowsValidation, SnapshotStateError, validate_message_rows},
};
use storage::{Key, StateStore, Value};
use testkit::StoreProvider;

pub(super) type Image = Vec<(Key, Value)>;

pub(super) struct Fixture<P: StoreProvider> {
    // The final store handle must drop before a temporary Fjall directory.
    pub(super) machine: StateMachine<P::Store>,
    provider: P,
    pub(super) namespace: NamespaceName,
    pub(super) queue: EntityPath,
}

impl<P: StoreProvider> Fixture<P> {
    pub(super) fn new(provider: P) -> Self {
        let machine = StateMachine::new(provider.open().unwrap());
        let f = Self {
            machine,
            provider,
            namespace: NamespaceName::new("tenant").unwrap(),
            queue: EntityPath::new("orders").unwrap(),
        };
        f.create(
            &f.queue,
            1,
            QueueConfig {
                lock_duration_millis: 10,
                default_time_to_live_millis: Some(30),
                ..QueueConfig::default()
            },
        );
        f
    }

    pub(super) fn at(&self, entity: &EntityPath, time: u64, kind: CommandKind) -> CommandOutcome {
        self.machine
            .apply(&Command::new(
                self.namespace.clone(),
                entity.clone(),
                Timestamp::from_millis(time),
                kind,
            ))
            .unwrap()
    }

    pub(super) fn create(&self, entity: &EntityPath, time: u64, config: QueueConfig) {
        assert_eq!(
            self.at(entity, time, CommandKind::CreateQueue { config }),
            CommandOutcome::QueueCreated
        );
    }

    pub(super) fn send(
        &self,
        entity: &EntityPath,
        time: u64,
        id: &str,
        session: Option<domain::SessionId>,
        scheduled: Option<u64>,
    ) -> SequenceNumber {
        match self.at(
            entity,
            time,
            CommandKind::Send {
                message_id: id.to_owned(),
                body: vec![1, 2, 3],
                time_to_live_millis: None,
                session_id: session,
                scheduled_enqueue_at: scheduled.map(Timestamp::from_millis),
                envelope: None,
            },
        ) {
            CommandOutcome::Sent { sequence } => sequence,
            CommandOutcome::Published { sequences, .. } => sequences[0],
            outcome => panic!("unexpected send: {outcome:?}"),
        }
    }

    pub(super) fn receive(
        &self,
        entity: &EntityPath,
        time: u64,
        mode: ReceiveMode,
        session: Option<domain::SessionHold>,
    ) -> Delivery {
        match self.at(
            entity,
            time,
            CommandKind::Receive {
                mode,
                lock_duration_millis: None,
                session,
            },
        ) {
            CommandOutcome::Received(Some(delivery)) => delivery,
            outcome => panic!("unexpected receive: {outcome:?}"),
        }
    }

    pub(super) fn image(&self) -> Image {
        self.machine.store().snapshot().unwrap().entries().to_vec()
    }

    pub(super) fn accept(&self, image: &Image) -> MessageRowsValidation {
        let original = image.clone();
        let stored = self.image();
        let report = validate_message_rows(image).expect("complete message graph");
        assert_eq!(image, &original, "validation preserves every input byte");
        assert_eq!(
            self.image(),
            stored,
            "validation never writes to the backend"
        );
        assert_eq!(
            report.catalog().catalog_rows()
                + report.catalog().unvalidated_external_rows()
                + report.message_rows()
                + report.message_index_rows()
                + report.pending_session_rows()
                + report.pending_duplicate_rows(),
            image.len()
        );
        assert!(!report.allocation_relations_checked());
        report
    }

    pub(super) fn reject(&self, image: &Image) -> SnapshotStateError {
        let original = image.clone();
        let stored = self.image();
        let error = validate_message_rows(image).expect_err("refuse broken message graph");
        assert_eq!(image, &original, "refusal preserves every input byte");
        assert_eq!(self.image(), stored, "refusal never writes to the backend");
        error
    }

    pub(super) fn restart(self) -> Self {
        // Memory reopens logically; DurableProvider reopens after the final store handle drops.
        let Self {
            machine,
            provider,
            namespace,
            queue,
        } = self;
        drop(machine);
        Self {
            machine: StateMachine::new(provider.open().unwrap()),
            provider,
            namespace,
            queue,
        }
    }

    pub(super) fn record(
        &self,
        image: &Image,
        entity: &EntityPath,
        sequence: SequenceNumber,
    ) -> MessageRecord {
        MessageRecord::decode(&value(
            image,
            &keys::message(&self.namespace, entity, sequence),
        ))
        .unwrap()
    }

    pub(super) fn put_record(
        &self,
        image: &mut Image,
        entity: &EntityPath,
        record: &MessageRecord,
    ) {
        put(
            image,
            keys::message(&self.namespace, entity, record.sequence),
            codec::encode(record).unwrap(),
        );
    }
}

pub(super) fn forms<P: StoreProvider>(provider: P) -> Fixture<P> {
    let f = Fixture::new(provider);
    f.send(&f.queue, 2, "ready", None, None);
    let locked = EntityPath::new("locked").unwrap();
    f.create(&locked, 3, QueueConfig::default());
    f.send(&locked, 4, "lock", None, None);
    f.receive(&locked, 5, ReceiveMode::PeekLock, None);
    let deferred = EntityPath::new("deferred").unwrap();
    f.create(&deferred, 6, QueueConfig::default());
    let seq = f.send(&deferred, 7, "defer", None, None);
    let delivery = f.receive(&deferred, 8, ReceiveMode::PeekLock, None);
    f.at(
        &deferred,
        9,
        CommandKind::Defer {
            sequence: seq,
            lock_token: delivery.lock.unwrap().token,
            replacement_envelope: None,
        },
    );
    let sessions = EntityPath::new("sessions").unwrap();
    f.create(
        &sessions,
        10,
        QueueConfig {
            requires_session: true,
            requires_duplicate_detection: true,
            ..QueueConfig::default()
        },
    );
    let id = domain::SessionId::new("CaSe").unwrap();
    f.send(&sessions, 11, "duplicate\0exact", Some(id.clone()), None);
    f.at(
        &sessions,
        12,
        CommandKind::AcceptSession {
            session_id: Some(id),
            lock_duration_millis: None,
        },
    );
    f.send(&f.queue, 13, "schedule", None, Some(40));
    f
}

pub(super) fn canonical<P: StoreProvider>(provider: P) {
    use domain::snapshot_validation::SnapshotCatalogError;
    let f = forms(provider);
    let image = f.image();
    f.accept(&image);
    for tag in 3..=13 {
        let (key, raw) = image.iter().find(|(key, _)| key[0] == tag).unwrap();
        let mut broken = image.clone();
        let mut key_bad = key.clone();
        key_bad[1] = 255;
        remove(&mut broken, key);
        put(&mut broken, key_bad.clone(), raw.clone());
        let error = f.reject(&broken);
        assert!(matches!(error, SnapshotStateError::InvalidKey { .. }));
        assert_eq!(error_row(error), row(&broken, &key_bad));
        let mut broken = image.clone();
        put(
            &mut broken,
            key.clone(),
            if matches!(tag, 3 | 8 | 12) {
                vec![255]
            } else {
                vec![1]
            },
        );
        let error = f.reject(&broken);
        assert!(matches!(error, SnapshotStateError::InvalidValue { .. }));
        assert_eq!(error_row(error), row(&broken, key));
        let mut upper = image.clone();
        let mut upper_key = key.clone();
        upper_key[1] = b'T';
        remove(&mut upper, key);
        put(&mut upper, upper_key.clone(), raw.clone());
        assert_eq!(error_row(f.reject(&upper)), row(&upper, &upper_key));
        if matches!(tag, 3 | 8 | 12) {
            let mut trailing = image.clone();
            let mut bytes = raw.clone();
            bytes.push(0);
            put(&mut trailing, key.clone(), bytes);
            assert!(matches!(
                f.reject(&trailing),
                SnapshotStateError::InvalidValue { .. }
            ));
        }
        if matches!(tag, 3 | 4 | 5 | 6 | 7 | 9 | 11) {
            for suffix in [key[..key.len() - 1].to_vec(), {
                let mut k = key.clone();
                k.push(0);
                k
            }] {
                let mut broken = image.clone();
                remove(&mut broken, key);
                put(&mut broken, suffix.clone(), raw.clone());
                assert_eq!(error_row(f.reject(&broken)), row(&broken, &suffix));
                assert!(matches!(
                    f.reject(&broken),
                    SnapshotStateError::InvalidKey { .. }
                ));
            }
        }
        if matches!(tag, 8 | 10 | 12 | 13) {
            let suffix = match tag {
                8 => key[..key.len() - 1].to_vec(), // Missing session terminator.
                10 => key[..key.len() - "CaSe".len()].to_vec(), // Empty session after deadline.
                _ => {
                    let mut bytes = key.clone();
                    *bytes.last_mut().unwrap() = 255;
                    bytes
                }
            };
            let mut broken = image.clone();
            remove(&mut broken, key);
            put(&mut broken, suffix.clone(), raw.clone());
            let error = f.reject(&broken);
            assert!(matches!(error, SnapshotStateError::InvalidKey { .. }));
            assert_eq!(error_row(error), row(&broken, &suffix));
        }
    }
    let mut no_clock = image.clone();
    remove(&mut no_clock, &keys::clock());
    assert_eq!(
        f.reject(&no_clock),
        SnapshotStateError::Catalog(SnapshotCatalogError::MissingClock)
    );
}

pub(super) fn value(image: &Image, key: &[u8]) -> Value {
    image
        .iter()
        .find(|(candidate, _)| candidate == key)
        .expect("fixture row")
        .1
        .clone()
}

pub(super) fn put(image: &mut Image, key: Key, value: Value) {
    image.retain(|(candidate, _)| candidate != &key);
    image.push((key, value));
    image.sort_by(|a, b| a.0.cmp(&b.0));
}

pub(super) fn remove(image: &mut Image, key: &[u8]) {
    let before = image.len();
    image.retain(|(candidate, _)| candidate != key);
    assert_eq!(before, image.len() + 1, "remove one exact fixture row");
}

pub(super) fn row(image: &Image, key: &[u8]) -> usize {
    image
        .iter()
        .position(|(candidate, _)| candidate == key)
        .unwrap()
}

pub(super) fn error_row(error: SnapshotStateError) -> usize {
    use domain::snapshot_validation::SnapshotCatalogError;
    match error {
        SnapshotStateError::InvalidKey { row, .. }
        | SnapshotStateError::InvalidValue { row, .. }
        | SnapshotStateError::InconsistentMessage { row, .. } => row,
        SnapshotStateError::Catalog(
            SnapshotCatalogError::EmptyKey { row }
            | SnapshotCatalogError::InputOrder { row }
            | SnapshotCatalogError::UnsupportedTag { row, .. }
            | SnapshotCatalogError::InvalidKey { row, .. }
            | SnapshotCatalogError::InvalidValue { row, .. }
            | SnapshotCatalogError::InconsistentCatalog { row, .. },
        ) => row,
        SnapshotStateError::Catalog(SnapshotCatalogError::MissingClock) => {
            panic!("expected an original ordinal")
        }
    }
}

pub(super) fn owner_roles<P: StoreProvider>(f: &Fixture<P>, original: &Image) {
    use domain::{
        DeadLetterInfo, DeadLetterReason, MessageState, SessionId, SubscriptionConfig,
        SubscriptionName, TopicConfig,
    };

    let one = SequenceNumber::new(1);
    let ordinary = f.record(original, &f.queue, one);
    let sessions = EntityPath::new("sessions").unwrap();
    let mut session_missing = f.record(original, &sessions, one);
    session_missing.session_id = None;
    let mut image = original.clone();
    f.put_record(&mut image, &sessions, &session_missing);
    assert_eq!(
        error_row(f.reject(&image)),
        row(&image, &keys::message(&f.namespace, &sessions, one))
    );
    let info = DeadLetterInfo {
        reason: DeadLetterReason::Application("injected".to_owned()),
        description: "qualified DTO, not a transition".to_owned(),
        dead_lettered_at: Timestamp::from_millis(0),
    };
    let mut session_extra = ordinary.clone();
    session_extra.session_id = Some(SessionId::new("unexpected").unwrap());
    let mut image = original.clone();
    f.put_record(&mut image, &f.queue, &session_extra);
    assert_eq!(
        error_row(f.reject(&image)),
        row(&image, &keys::message(&f.namespace, &f.queue, one))
    );
    let mut provenance = ordinary.clone();
    provenance.dead_letter = Some(info.clone());
    let mut image = original.clone();
    f.put_record(&mut image, &f.queue, &provenance);
    assert_eq!(
        error_row(f.reject(&image)),
        row(&image, &keys::message(&f.namespace, &f.queue, one))
    );
    let shadow = f.queue.dead_letter_queue().unwrap();
    let mut dlq = ordinary.clone();
    dlq.dead_letter = Some(info);
    dlq.expires_at = None;
    let mut compatible = original.clone();
    f.put_record(&mut compatible, &shadow, &dlq);
    put(
        &mut compatible,
        keys::ready(&f.namespace, &shadow, one),
        vec![],
    );
    f.accept(&compatible); // Qualified provenance DTO; actual moves are a separate family.
    for wrong in [
        {
            let mut r = dlq.clone();
            r.dead_letter = None;
            r
        },
        {
            let mut r = dlq.clone();
            r.expires_at = Some(Timestamp::from_millis(32));
            r
        },
        {
            let mut r = dlq.clone();
            r.state = MessageState::Scheduled;
            r.scheduled_enqueue_at = Some(Timestamp::from_millis(40));
            r
        },
    ] {
        let mut image = compatible.clone();
        f.put_record(&mut image, &shadow, &wrong);
        assert_eq!(
            error_row(f.reject(&image)),
            row(&image, &keys::message(&f.namespace, &shadow, one))
        );
    }
    let absent = EntityPath::new("absent").unwrap();
    let mut image = original.clone();
    f.put_record(&mut image, &absent, &ordinary);
    put(&mut image, keys::ready(&f.namespace, &absent, one), vec![]);
    assert_eq!(
        error_row(f.reject(&image)),
        row(&image, &keys::message(&f.namespace, &absent, one))
    );
    let topic = EntityPath::new("roles-topic").unwrap();
    f.at(
        &topic,
        14,
        CommandKind::CreateTopic {
            config: TopicConfig::default(),
        },
    );
    let name = SubscriptionName::new("child").unwrap();
    f.at(
        &topic,
        15,
        CommandKind::CreateSubscription {
            name: name.clone(),
            config: SubscriptionConfig::default(),
        },
    );
    let stored = f.image();
    let child = topic.subscription(&name).unwrap();
    let mut image = stored.clone();
    f.put_record(&mut image, &topic, &ordinary);
    put(&mut image, keys::ready(&f.namespace, &topic, one), vec![]);
    assert_eq!(
        error_row(f.reject(&image)),
        row(&image, &keys::message(&f.namespace, &topic, one))
    );
    let mut scheduled = ordinary;
    scheduled.state = MessageState::Scheduled;
    scheduled.scheduled_enqueue_at = Some(Timestamp::from_millis(40));
    scheduled.expires_at = None;
    let mut image = stored;
    f.put_record(&mut image, &child, &scheduled);
    put(
        &mut image,
        keys::scheduled(&f.namespace, &child, Timestamp::from_millis(40), one),
        vec![],
    );
    assert_eq!(
        error_row(f.reject(&image)),
        row(&image, &keys::message(&f.namespace, &child, one))
    );
}

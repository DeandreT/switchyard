#![cfg(feature = "capacity-conservation-test-support")]

#[path = "support/capacity_oracle.rs"]
mod oracle;

use domain::{
    BoundCommand, BrokerError, Command, CommandKind as C, CommandOutcome as O, Delivery,
    DurableProposal, EntityPath, IndexedWriter, MessageContent, MessageEnvelope, MessageInput,
    MessageRecordFormat, MessageState, NamespaceName, OverlayProperty, QueueCapacity, QueueConfig,
    QueueCounters, RawMessage, ReceiveMode, SequenceNumber as Seq, StateMachine, Timestamp,
    capacity_conservation::{CapacityHarness, CapacityIndexedHarness, TestGenesis, TestOriginal},
    capacity_fence::{self, CapacityIntent},
    codec, keys,
    retained_accounting::{keys::usage, usage_codec},
};
use std::{
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::{DurableProvider, MemoryProvider, StoreProvider};

type Result<T = ()> = std::result::Result<T, Box<dyn Error>>;

macro_rules! paired {
    ($memory:ident,$fjall:ident,$body:ident) => {
        #[test]
        fn $memory() -> Result {
            $body(MemoryProvider::new())
        }
        #[test]
        fn $fjall() -> Result {
            $body(DurableProvider::temporary()?)
        }
    };
}

fn config() -> QueueConfig {
    QueueConfig {
        capacity: QueueCapacity::Finite {
            max_size_bytes: 1024 * 1024,
        },
        max_message_bytes: 2 * 1024 * 1024,
        ..QueueConfig::default()
    }
}
fn raw(size: usize) -> C {
    C::SendContent {
        content: MessageContent::Raw(RawMessage {
            message_id: "fit".into(),
            body: vec![7; size],
            ..RawMessage::default()
        }),
    }
}
fn receive(mode: ReceiveMode) -> C {
    C::Receive {
        mode,
        lock_duration_millis: Some(60_000),
        session: None,
    }
}
fn envelope(size: usize) -> Vec<u8> {
    // One real AMQP data section. Sparse patches rederive its content normally.
    let mut value = vec![0, 0x53, 0x75, 0xb0];
    value.extend_from_slice(&(size as u32).to_be_bytes());
    value.extend(vec![3; size]);
    value
}

struct Fixture<P: StoreProvider> {
    actor: Option<CapacityHarness<P::Store>>,
    store: Option<P::Store>,
    genesis: TestGenesis,
    time: u64,
    provider: P,
}

impl<P: StoreProvider> Fixture<P> {
    fn new(provider: P, config: QueueConfig) -> Result<Self> {
        let genesis = TestGenesis::new(
            NamespaceName::new("tenant")?,
            EntityPath::new("orders")?,
            config,
        )?;
        let store = provider.open()?;
        genesis.initialize(&store)?;
        let actor = CapacityHarness::open(store.clone(), genesis.clone())?;
        let result = Self {
            provider,
            store: Some(store),
            actor: Some(actor),
            genesis,
            time: 0,
        };
        result.observation();
        Ok(result)
    }
    fn store(&self) -> &P::Store {
        self.store.as_ref().unwrap()
    }
    fn image(&self) -> Vec<(Key, Value)> {
        self.store().snapshot().unwrap().entries().to_vec()
    }
    fn observation(&self) -> oracle::Observation {
        oracle::observe(&self.image(), self.genesis.owner())
    }
    fn intent(&self, target: &EntityPath, kind: C) -> Result<CapacityIntent> {
        let binding = self.actor.as_ref().unwrap().bind(target)?;
        let command = Command::new(
            self.genesis.owner().namespace().clone(),
            target.clone(),
            Timestamp::from_millis(self.time),
            kind,
        );
        Ok(CapacityIntent::capture(
            self.genesis.owner().clone(),
            DurableProposal::bound(BoundCommand::new(binding, command))?,
        )?)
    }
    fn at(&mut self, target: &EntityPath, kind: C) -> std::result::Result<O, BrokerError> {
        self.time += 1;
        let intent = self.intent(target, kind).unwrap();
        let result = self.actor.as_mut().unwrap().apply(&intent);
        self.observation();
        result
    }
    fn parent(&mut self, kind: C) -> std::result::Result<O, BrokerError> {
        self.at(&self.genesis.owner().parent().clone(), kind)
    }
    fn shadow(&mut self, kind: C) -> std::result::Result<O, BrokerError> {
        self.at(&self.genesis.owner().shadow().clone(), kind)
    }
    fn locked(&mut self) -> Result<Delivery> {
        match self.parent(receive(ReceiveMode::PeekLock))? {
            O::Received(Some(d)) => Ok(d),
            other => panic!("{other:?}"),
        }
    }
    fn reopen(&mut self) -> Result {
        let before = self.image();
        drop(self.actor.take());
        drop(self.store.take());
        let store = self.provider.open()?;
        self.actor = Some(CapacityHarness::open(store.clone(), self.genesis.clone())?);
        self.store = Some(store);
        assert_eq!(self.image(), before);
        self.observation();
        Ok(())
    }
    fn fill(&mut self) -> Result<usize> {
        for _ in 0..3 {
            self.parent(raw(250_000))?;
        }
        let observed = self.observation();
        let mut template = observed
            .messages
            .values()
            .find(|(_, r)| {
                r.format == MessageRecordFormat::V2
                    && r.envelope.is_none()
                    && r.scheduled_enqueue_at.is_none()
            })
            .unwrap()
            .1
            .clone();
        template.message_id = "fit".into();
        let remaining = 1024 * 1024 - observed.usage.committed_bytes()?;
        let (mut low, mut high) = (0usize, 400_000usize);
        while low < high {
            let mid = low + (high - low).div_ceil(2);
            template.body = vec![7; mid];
            if oracle::ceiling(
                self.genesis.owner(),
                self.genesis.owner().parent(),
                &template,
            ) <= remaining
            {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        template.body = vec![7; low];
        assert_eq!(
            oracle::ceiling(
                self.genesis.owner(),
                self.genesis.owner().parent(),
                &template
            ),
            remaining
        );
        let before = self.image();
        assert!(matches!(
            self.parent(raw(low + 1)),
            Err(BrokerError::RetainedCapacityExceeded {
                committed: 1048577,
                limit: 1048576
            })
        ));
        assert_eq!(self.image(), before);
        self.parent(raw(low))?;
        assert_eq!(self.observation().usage.committed_bytes()?, 1024 * 1024);
        Ok(low)
    }
}

fn lifecycle<P: StoreProvider>(provider: P) -> Result {
    let mut f = Fixture::new(provider, config())?;
    for format in [1, 2, 3] {
        let content = envelope(4);
        let send = match format {
            1 => C::Send {
                message_id: "v1".into(),
                body: vec![2; 3],
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: None,
                envelope: Some(MessageEnvelope::new(content)),
            },
            2 => C::SendContent {
                content: MessageContent::Amqp(content),
            },
            _ => C::SendContent {
                content: MessageContent::AmqpTextIds(content),
            },
        };
        f.parent(send)?;
        let initial = f.observation();
        let delivery = f.locked()?;
        let lock = delivery.lock.unwrap();
        let s = delivery.sequence;
        assert!(f.observation().ceilings[&s] <= initial.ceilings[&s]);
        f.parent(C::RenewLock {
            sequence: s,
            lock_token: lock.token,
            lock_duration_millis: Some(16384),
        })?;
        f.parent(C::DeferWithPatch {
            sequence: s,
            lock_token: lock.token,
            patch: vec![OverlayProperty {
                name: "p".into(),
                value: vec![0x40],
            }],
        })?;
        assert_eq!(f.observation().messages[&s].1.state, MessageState::Deferred);
        let O::DeferredReceived(deliveries) = f.parent(C::ReceiveDeferred {
            sequences: vec![s],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(128),
            session: None,
        })?
        else {
            panic!()
        };
        let lock = deliveries[0].lock.as_ref().unwrap();
        f.parent(C::Abandon {
            sequence: s,
            lock_token: lock.token,
            replacement_envelope: None,
        })?;
        assert_eq!(f.observation().messages[&s].1.state, MessageState::Deferred);
        let O::DeferredReceived(deliveries) = f.parent(C::ReceiveDeferred {
            sequences: vec![s],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(128),
            session: None,
        })?
        else {
            panic!()
        };
        let token = deliveries[0].lock.as_ref().unwrap().token;
        f.parent(C::DeadLetterWithPatch {
            sequence: s,
            lock_token: token,
            reason: "application".into(),
            description: "arbitrary metadata".into(),
            patch: vec![],
        })?;
        f.reopen()?;
        let O::Received(Some(shadow)) = f.shadow(receive(ReceiveMode::PeekLock))? else {
            panic!()
        };
        assert_eq!(shadow.sequence, s);
        let token = shadow.lock.unwrap().token;
        f.shadow(C::RenewLock {
            sequence: s,
            lock_token: token,
            lock_duration_millis: Some(127),
        })?;
        f.shadow(C::Defer {
            sequence: s,
            lock_token: token,
            replacement_envelope: None,
        })?;
        f.shadow(C::ReceiveDeferred {
            sequences: vec![s],
            mode: ReceiveMode::ReceiveAndDelete,
            lock_duration_millis: None,
            session: None,
        })?;
        assert_eq!(f.observation().usage.message_count(), 0);
    }
    f.parent(C::SendBatch {
        messages: vec![
            MessageInput {
                body: vec![1],
                ..MessageInput::default()
            };
            3
        ],
    })?;
    let d = f.locked()?;
    f.parent(C::Complete {
        sequence: d.sequence,
        lock_token: d.lock.unwrap().token,
    })?;
    f.parent(receive(ReceiveMode::ReceiveAndDelete))?;
    let d = f.locked()?;
    f.parent(C::Abandon {
        sequence: d.sequence,
        lock_token: d.lock.unwrap().token,
        replacement_envelope: None,
    })?;
    f.parent(receive(ReceiveMode::ReceiveAndDelete))?;
    f.parent(C::SendContentBatch {
        messages: vec![MessageContent::Raw(RawMessage::default()); 2],
    })?;
    f.parent(C::Peek {
        from_sequence: Seq::new(0),
        max_messages: 5,
        session: None,
    })?;
    f.parent(receive(ReceiveMode::ReceiveAndDelete))?;
    f.parent(receive(ReceiveMode::ReceiveAndDelete))?;
    let before = f.image();
    f.parent(receive(ReceiveMode::PeekLock))?;
    f.parent(C::ExpireMessages)?;
    f.parent(C::ExpireLocks)?;
    f.parent(C::ActivateScheduled)?;
    assert_eq!(f.image(), before);
    f.reopen()?;
    Ok(())
}
paired!(memory_lifecycle, fjall_lifecycle, lifecycle);

fn scheduling<P: StoreProvider>(provider: P) -> Result {
    let mut f = Fixture::new(provider, config())?;
    let inputs = (0..3)
        .map(|i| MessageInput {
            body: vec![i],
            scheduled_enqueue_at: Some(Timestamp::from_millis(100)),
            time_to_live_millis: Some(20),
            ..MessageInput::default()
        })
        .collect();
    let O::BatchSent { sequences, .. } = f.parent(C::SendBatch { messages: inputs })? else {
        panic!()
    };
    let before = f.image();
    assert!(
        f.parent(C::CancelScheduled {
            sequences: vec![sequences[0], Seq::new(1000)]
        })
        .is_err()
    );
    assert_eq!(before, f.image());
    f.parent(C::CancelScheduled {
        sequences: vec![sequences[0]],
    })?;
    let original = f.observation();
    f.time = 127;
    f.parent(C::ActivateScheduled)?;
    let active = f.observation();
    assert_eq!(active.messages.len(), 2);
    assert!(active.messages.keys().all(|s| !sequences.contains(s)));
    assert!(active.usage.committed_bytes()? <= original.usage.committed_bytes()?);
    assert!(
        active
            .messages
            .values()
            .all(|(_, r)| r.expires_at == Some(Timestamp::from_millis(148)))
    );
    f.reopen()?;
    f.time = 200;
    f.parent(C::ExpireMessages)?;
    assert!(
        f.observation()
            .messages
            .values()
            .all(|(e, _)| e == f.genesis.owner().shadow())
    );
    f.shadow(receive(ReceiveMode::ReceiveAndDelete))?;
    f.shadow(receive(ReceiveMode::ReceiveAndDelete))?;
    f.reopen()?;
    Ok(())
}
paired!(memory_scheduling, fjall_scheduling, scheduling);

fn ttl<P: StoreProvider>(provider: P) -> Result {
    // One store, several independent fixture providers are not needed: the
    // genesis policy is chosen once and each command's actual result is checked.
    let mut f = Fixture::new(
        provider,
        QueueConfig {
            dead_lettering_on_message_expiration: false,
            max_delivery_count: 2,
            ..config()
        },
    )?;
    for path in 0..5 {
        let mut input = RawMessage {
            time_to_live_millis: Some(10),
            ..RawMessage::default()
        };
        if path == 4 {
            input.time_to_live_millis = None;
        }
        f.parent(C::SendContent {
            content: MessageContent::Raw(input),
        })?;
        let mut held = None;
        if path != 0 {
            held = Some(f.locked()?);
        }
        if path == 3 {
            let d = held.take().unwrap();
            f.parent(C::Defer {
                sequence: d.sequence,
                lock_token: d.lock.as_ref().unwrap().token,
                replacement_envelope: None,
            })?;
            held = Some(d);
        }
        f.time += 20;
        match path {
            0 => {
                f.parent(receive(ReceiveMode::PeekLock))?;
            }
            1 => {
                let d = held.unwrap();
                f.parent(C::Abandon {
                    sequence: d.sequence,
                    lock_token: d.lock.unwrap().token,
                    replacement_envelope: None,
                })?;
            }
            2 => {
                f.time += 60_000;
                f.parent(C::ExpireLocks)?;
            }
            3 => {
                let d = held.unwrap();
                f.parent(C::ReceiveDeferred {
                    sequences: vec![d.sequence],
                    mode: ReceiveMode::ReceiveAndDelete,
                    lock_duration_millis: None,
                    session: None,
                })?;
            }
            _ => {
                let d = held.unwrap();
                f.parent(C::Abandon {
                    sequence: d.sequence,
                    lock_token: d.lock.unwrap().token,
                    replacement_envelope: None,
                })?;
                let d = f.locked()?;
                f.parent(C::Abandon {
                    sequence: d.sequence,
                    lock_token: d.lock.unwrap().token,
                    replacement_envelope: None,
                })?;
                assert!(
                    f.observation()
                        .messages
                        .values()
                        .all(|(e, _)| e == f.genesis.owner().shadow())
                );
                f.shadow(receive(ReceiveMode::ReceiveAndDelete))?;
            }
        }
        assert_eq!(f.observation().usage.message_count(), 0);
        f.reopen()?;
    }
    Ok(())
}
paired!(
    memory_ttl_delete_and_delivery_limit,
    fjall_ttl_delete_and_delivery_limit,
    ttl
);

fn bound<P: StoreProvider>(provider: P) -> Result {
    let mut f = Fixture::new(provider, config())?;
    f.parent(raw(1))?;
    f.fill()?;
    let before = f.observation();
    let d = f.locked()?;
    let token = d.lock.unwrap().token;
    f.time = 16383;
    f.parent(C::RenewLock {
        sequence: d.sequence,
        lock_token: token,
        lock_duration_millis: Some(16384),
    })?;
    assert!(f.observation().ceilings[&d.sequence] <= before.ceilings[&d.sequence]);
    f.parent(C::Defer {
        sequence: d.sequence,
        lock_token: token,
        replacement_envelope: None,
    })?;
    f.parent(C::ReceiveDeferred {
        sequences: vec![d.sequence],
        mode: ReceiveMode::ReceiveAndDelete,
        lock_duration_millis: None,
        session: None,
    })?;
    assert!(f.observation().usage.committed_bytes()? < 1024 * 1024);
    f.reopen()?;
    Ok(())
}
paired!(memory_exact_limit, fjall_exact_limit, bound);

fn rewrite<P: StoreProvider>(provider: P) -> Result {
    let mut f = Fixture::new(
        provider,
        QueueConfig {
            default_time_to_live_millis: Some(100),
            max_delivery_count: 2,
            dead_lettering_on_message_expiration: false,
            ..config()
        },
    )?;
    f.parent(C::SendContent {
        content: MessageContent::Amqp(envelope(1)),
    })?;
    let d = f.locked()?;
    let s = d.sequence;
    let mut token = d.lock.unwrap().token;
    f.fill()?;
    let unchanged = f.image();
    assert!(matches!(
        f.parent(C::Abandon {
            sequence: s,
            lock_token: token,
            replacement_envelope: Some(MessageEnvelope::new(envelope(256)))
        }),
        Err(BrokerError::RetainedCapacityExceeded { .. })
    ));
    assert_eq!(
        f.image(),
        unchanged,
        "normal ready-return branch cannot bypass rewrite proof"
    );
    f.parent(C::Abandon {
        sequence: s,
        lock_token: token,
        replacement_envelope: None,
    })?;
    let d = f.locked()?;
    assert_eq!(d.sequence, s);
    token = d.lock.unwrap().token;
    let unchanged = f.image();
    assert!(matches!(
        f.parent(C::Abandon {
            sequence: s,
            lock_token: token,
            replacement_envelope: Some(MessageEnvelope::new(envelope(256)))
        }),
        Err(BrokerError::RetainedCapacityExceeded { .. })
    ));
    assert_eq!(
        f.image(),
        unchanged,
        "maximum-delivery branch cannot bypass rewrite proof"
    );
    let before = f.image();
    f.time = 200;
    let patch = vec![OverlayProperty {
        name: "oversized".into(),
        value: vec![0xa1, 128].into_iter().chain(vec![b'a'; 128]).collect(),
    }];
    for kind in [
        C::AbandonWithPatch {
            sequence: s,
            lock_token: token,
            patch: patch.clone(),
        },
        C::DeferWithPatch {
            sequence: s,
            lock_token: token,
            patch: patch.clone(),
        },
        C::DeadLetterWithPatch {
            sequence: s,
            lock_token: token,
            patch,
            reason: "r".into(),
            description: "d".into(),
        },
        C::Abandon {
            sequence: s,
            lock_token: token,
            replacement_envelope: Some(MessageEnvelope::new(envelope(256))),
        },
        C::Defer {
            sequence: s,
            lock_token: token,
            replacement_envelope: Some(MessageEnvelope::new(envelope(256))),
        },
        C::DeadLetter {
            sequence: s,
            lock_token: token,
            replacement_envelope: Some(MessageEnvelope::new(envelope(256))),
            reason: "r".into(),
            description: "d".into(),
        },
        C::DeadLetter {
            sequence: s,
            lock_token: token,
            replacement_envelope: None,
            reason: "arbitrary".into(),
            description: "x".repeat(1024),
        },
    ] {
        assert!(matches!(
            f.parent(kind),
            Err(BrokerError::RetainedCapacityExceeded { .. })
        ));
        assert_eq!(
            f.image(),
            before,
            "rewrite cannot escape through TTL or max-delivery"
        );
    }
    let other = EntityPath::new("unrelated")?;
    let command = Command::new(
        f.genesis.owner().namespace().clone(),
        other,
        Timestamp::from_millis(200),
        C::CreateQueue {
            config: QueueConfig::default(),
        },
    );
    f.actor
        .as_mut()
        .unwrap()
        .apply_ordinary(&DurableProposal::unbound(command))?;
    f.observation();
    f.parent(C::Abandon {
        sequence: s,
        lock_token: token,
        replacement_envelope: None,
    })?;
    assert!(!f.observation().messages.contains_key(&s));
    f.reopen()?;
    Ok(())
}
paired!(memory_rewrite_barriers, fjall_rewrite_barriers, rewrite);

fn rewrite_ttl_dlq<P: StoreProvider>(provider: P) -> Result {
    let mut f = Fixture::new(
        provider,
        QueueConfig {
            default_time_to_live_millis: Some(100),
            ..config()
        },
    )?;
    f.parent(C::SendContent {
        content: MessageContent::Amqp(envelope(1)),
    })?;
    let d = f.locked()?;
    let token = d.lock.unwrap().token;
    f.fill()?;
    let before = f.image();
    f.time = 200;
    assert!(matches!(
        f.parent(C::Abandon {
            sequence: d.sequence,
            lock_token: token,
            replacement_envelope: Some(MessageEnvelope::new(envelope(256)))
        }),
        Err(BrokerError::RetainedCapacityExceeded { .. })
    ));
    assert_eq!(
        f.image(),
        before,
        "TTL DLQ is not permission to skip external admission"
    );
    f.parent(C::Abandon {
        sequence: d.sequence,
        lock_token: token,
        replacement_envelope: None,
    })?;
    assert_eq!(
        &f.observation().messages[&d.sequence].0,
        f.genesis.owner().shadow()
    );
    f.reopen()?;
    Ok(())
}
paired!(
    memory_rewrite_before_ttl_dlq,
    fjall_rewrite_before_ttl_dlq,
    rewrite_ttl_dlq
);

fn replacement_success<P: StoreProvider>(provider: P) -> Result {
    let mut f = Fixture::new(provider, config())?;
    for format in [1, 2, 3] {
        let content = envelope(1);
        let send = match format {
            1 => C::Send {
                message_id: "id".into(),
                body: vec![1],
                envelope: Some(MessageEnvelope::new(content)),
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: None,
            },
            2 => C::SendContent {
                content: MessageContent::Amqp(content),
            },
            _ => C::SendContent {
                content: MessageContent::AmqpTextIds(content),
            },
        };
        f.parent(send)?;
        let d = f.locked()?;
        f.parent(C::Abandon {
            sequence: d.sequence,
            lock_token: d.lock.unwrap().token,
            replacement_envelope: Some(MessageEnvelope::new(envelope(2))),
        })?;
        let d = f.locked()?;
        f.parent(C::Defer {
            sequence: d.sequence,
            lock_token: d.lock.unwrap().token,
            replacement_envelope: Some(MessageEnvelope::new(envelope(3))),
        })?;
        let O::DeferredReceived(deliveries) = f.parent(C::ReceiveDeferred {
            sequences: vec![d.sequence],
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        })?
        else {
            panic!()
        };
        let token = deliveries[0].lock.as_ref().unwrap().token;
        f.parent(C::DeadLetter {
            sequence: d.sequence,
            lock_token: token,
            replacement_envelope: Some(MessageEnvelope::new(envelope(4))),
            reason: "application reason".into(),
            description: "arbitrary description".into(),
        })?;
        let O::Received(Some(delivery)) = f.shadow(receive(ReceiveMode::PeekLock))? else {
            panic!()
        };
        let token = delivery.lock.unwrap().token;
        f.shadow(C::AbandonWithPatch {
            sequence: d.sequence,
            lock_token: token,
            patch: vec![OverlayProperty {
                name: "p".into(),
                value: vec![0x40],
            }],
        })?;
        f.shadow(receive(ReceiveMode::ReceiveAndDelete))?;
        f.reopen()?;
    }
    Ok(())
}
paired!(
    memory_external_replacement_success,
    fjall_external_replacement_success,
    replacement_success
);

fn corrupt_and_versions<P: StoreProvider>(provider: P) -> Result {
    let mut f = Fixture::new(provider, config())?;
    f.parent(C::SendContent {
        content: MessageContent::Amqp(envelope(1)),
    })?;
    let d = f.locked()?;
    let token = d.lock.unwrap().token;
    let good = f.image();
    drop(f.actor.take());
    for text_ids in [false, true] {
        for removals in [vec![], vec!["removed".to_string()]] {
            let mut record = oracle::observe(&good, f.genesis.owner()).messages[&d.sequence]
                .1
                .clone();
            record.format = MessageRecordFormat::V4 { text_ids };
            record.envelope = Some(record.envelope.unwrap().with_removals(removals));
            let key = capacity_fence::data_key(&keys::message(
                f.genesis.owner().namespace(),
                f.genesis.owner().parent(),
                d.sequence,
            ))?;
            f.store()
                .apply(WriteBatch::default().put(key, codec::message::encode(&record)?))?;
            let unsupported = f.image();
            assert!(matches!(
                CapacityHarness::open(f.store().clone(), f.genesis.clone()),
                Err(BrokerError::CapacityMessageUnsupported { version: 4 })
            ));
            // All operations must fail before an owner can execute/refund any V4.
            for kind in [
                receive(ReceiveMode::ReceiveAndDelete),
                receive(ReceiveMode::PeekLock),
                C::ExpireLocks,
                C::ExpireMessages,
                C::ActivateScheduled,
                C::Complete {
                    sequence: d.sequence,
                    lock_token: token,
                },
                C::AbandonWithPatch {
                    sequence: d.sequence,
                    lock_token: token,
                    patch: vec![],
                },
                C::DeferWithPatch {
                    sequence: d.sequence,
                    lock_token: token,
                    patch: vec![],
                },
                C::DeadLetterWithPatch {
                    sequence: d.sequence,
                    lock_token: token,
                    patch: vec![],
                    reason: "r".into(),
                    description: "d".into(),
                },
                C::CancelScheduled {
                    sequences: vec![d.sequence],
                },
            ] {
                let original = TestOriginal::Owned(CapacityIntent::capture(
                    f.genesis.owner().clone(),
                    DurableProposal::unbound(Command::new(
                        f.genesis.owner().namespace().clone(),
                        f.genesis.owner().parent().clone(),
                        Timestamp::from_millis(5),
                        kind,
                    )),
                )?);
                assert!(
                    CapacityIndexedHarness::open(f.store().clone(), f.genesis.clone()).is_err()
                );
                assert!(matches!(original, TestOriginal::Owned(_)));
                assert_eq!(f.image(), unsupported);
            }
            restore(f.store(), &good)?;
        }
    }
    for mutation in [
        WriteBatch::default().delete(usage(f.genesis.owner())),
        WriteBatch::default().put(usage(f.genesis.owner()), vec![0; 72]),
        WriteBatch::default().put(
            keys::ready(
                f.genesis.owner().namespace(),
                f.genesis.owner().parent(),
                d.sequence,
            ),
            vec![],
        ),
        WriteBatch::default().put(
            capacity_fence::data_key(&keys::ready(
                f.genesis.owner().namespace(),
                f.genesis.owner().parent(),
                d.sequence,
            ))?,
            vec![],
        ),
    ] {
        f.store().apply(mutation)?;
        let bad = f.image();
        assert!(CapacityHarness::open(f.store().clone(), f.genesis.clone()).is_err());
        assert_eq!(f.image(), bad);
        restore(f.store(), &good)?;
    }
    f.actor = Some(CapacityHarness::open(f.store().clone(), f.genesis.clone())?);
    let command = Command::new(
        f.genesis.owner().namespace().clone(),
        f.genesis.owner().parent().clone(),
        Timestamp::from_millis(5),
        C::Complete {
            sequence: d.sequence,
            lock_token: token,
        },
    );
    assert_eq!(
        StateMachine::new(f.store().clone()).apply(&command),
        Err(BrokerError::CapacityStateUnsupported)
    );
    assert!(IndexedWriter::open(f.store().clone()).is_err());
    assert_eq!(f.image(), good);
    f.parent(C::Complete {
        sequence: d.sequence,
        lock_token: token,
    })?;
    f.reopen()?;
    Ok(())
}
fn restore<S: StateStore>(store: &S, rows: &[(Key, Value)]) -> Result {
    let mut batch = WriteBatch::default();
    for (key, _) in store.snapshot()?.entries() {
        batch.push_delete(key.clone());
    }
    for (key, value) in rows {
        batch.push_put(key.clone(), value.clone());
    }
    store.apply(batch)?;
    Ok(())
}
paired!(
    memory_versions_corruption_and_default_fences,
    fjall_versions_corruption_and_default_fences,
    corrupt_and_versions
);

#[derive(Clone, Debug)]
struct FaultStore<S> {
    inner: S,
    fail: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
    log: Arc<Mutex<Vec<Event>>>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
enum Event {
    Get(Key),
    Scan(Key, Key, usize),
    Apply(WriteBatch),
}
impl<S: StateStore> StateStore for FaultStore<S> {
    fn get(&self, key: &[u8]) -> std::result::Result<Option<Value>, StorageError> {
        self.log.lock().unwrap().push(Event::Get(key.to_vec()));
        self.read()?;
        self.inner.get(key)
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> std::result::Result<Vec<(Key, Value)>, StorageError> {
        self.log
            .lock()
            .unwrap()
            .push(Event::Scan(prefix.to_vec(), start.to_vec(), limit));
        self.read()?;
        self.inner.scan_from(prefix, start, limit)
    }
    fn snapshot(&self) -> std::result::Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }
    fn apply(&self, batch: WriteBatch) -> std::result::Result<(), StorageError> {
        self.log.lock().unwrap().push(Event::Apply(batch.clone()));
        match self.fail.load(Ordering::SeqCst) {
            usize::MAX => Err(fault()),
            x if x == usize::MAX - 1 => {
                self.inner.apply(batch)?;
                Err(fault())
            }
            x if x == usize::MAX - 2 => {
                self.inner.apply(batch)?;
                panic!("postcommit test panic")
            }
            _ => self.inner.apply(batch),
        }
    }
}
fn fault() -> StorageError {
    StorageError::Backend {
        operation: "injected",
        detail: "owned fixture".into(),
    }
}
impl<S> FaultStore<S> {
    fn read(&self) -> std::result::Result<(), StorageError> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if n == self.fail.load(Ordering::SeqCst) {
            Err(fault())
        } else {
            Ok(())
        }
    }
}

fn faults<P: StoreProvider>(provider: P) -> Result {
    let store = provider.open()?;
    let genesis = TestGenesis::new(
        NamespaceName::new("tenant")?,
        EntityPath::new("orders")?,
        config(),
    )?;
    genesis.initialize(&store)?;
    let faulted = FaultStore {
        inner: store.clone(),
        fail: Arc::new(AtomicUsize::new(0)),
        calls: Arc::new(AtomicUsize::new(0)),
        log: Arc::new(Mutex::new(Vec::new())),
    };
    let mut actor = CapacityHarness::open(faulted.clone(), genesis.clone())?;
    let intent = CapacityIntent::capture(
        genesis.owner().clone(),
        DurableProposal::unbound(Command::new(
            genesis.owner().namespace().clone(),
            genesis.owner().parent().clone(),
            Timestamp::from_millis(1),
            raw(16),
        )),
    )?;
    faulted.calls.store(0, Ordering::SeqCst);
    faulted.log.lock().unwrap().clear();
    actor.apply(&intent)?;
    let calls = faulted.calls.load(Ordering::SeqCst);
    let after = store.snapshot()?;
    oracle::observe(after.entries(), genesis.owner());
    let n = genesis.owner().namespace();
    let p = genesis.owner().parent();
    let s = genesis.owner().shadow();
    let mut expected = vec![
        Event::Get(keys::entity_metadata(n, p)),
        Event::Get(keys::entity_metadata(n, s)),
        Event::Get(keys::topic_config(n, p)),
        Event::Get(keys::topic_config(n, s)),
    ];
    for endpoint in [p, s] {
        expected.push(Event::Get(keys::queue_config(n, endpoint)));
        for tag in 2..=16 {
            let mut prefix = keys::queue_config(n, endpoint);
            prefix[0] = tag;
            expected.push(Event::Scan(prefix.clone(), prefix.clone(), 1));
            prefix.insert(0, 0x13);
            expected.push(Event::Scan(prefix.clone(), prefix, 32));
        }
    }
    let mut shadow_usage = keys::queue_config(n, s);
    shadow_usage[0] = 0x12;
    expected.push(Event::Get(shadow_usage));
    expected.push(Event::Get(usage(genesis.owner())));
    expected.push(Event::Get(keys::clock()));
    let trace = faulted.log.lock().unwrap().clone();
    let (last, prefix) = trace.split_last().unwrap();
    assert_eq!(
        prefix,
        expected.as_slice(),
        "exact selected-owner health before stored Clock, no foreign probes"
    );
    let Event::Apply(batch) = last else {
        panic!("missing actual atomic apply")
    };
    let expected_keys = [
        capacity_fence::data_key(&keys::message(n, p, Seq::new(1)))?,
        capacity_fence::data_key(&keys::ready(n, p, Seq::new(1)))?,
        capacity_fence::data_key(&keys::queue_counters(n, p))?,
        usage(genesis.owner()),
        keys::clock(),
    ];
    let actual = after
        .entries()
        .iter()
        .cloned()
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(batch.mutations().len(), expected_keys.len());
    for (mutation, key) in batch.mutations().iter().zip(expected_keys) {
        let storage::Mutation::Put {
            key: written,
            value,
        } = mutation
        else {
            panic!("unexpected deletion")
        };
        assert_eq!(*written, key);
        assert_eq!(*value, actual[&key]);
    }
    drop(actor);
    // Each external reader failure before apply leaves the full original image.
    let empty_store = storage::MemoryStore::default();
    genesis.initialize(&empty_store)?;
    let initial = empty_store.snapshot()?;
    for nth in 1..=calls {
        restore(&store, initial.entries())?;
        faulted.fail.store(0, Ordering::SeqCst);
        let mut actor = CapacityHarness::open(faulted.clone(), genesis.clone())?;
        faulted.calls.store(0, Ordering::SeqCst);
        faulted.fail.store(nth, Ordering::SeqCst);
        assert!(actor.apply(&intent).is_err());
        assert_eq!(store.snapshot()?, initial);
        oracle::observe(initial.entries(), genesis.owner());
    }
    for mode in [usize::MAX, usize::MAX - 1, usize::MAX - 2] {
        restore(&store, initial.entries())?;
        faulted.fail.store(0, Ordering::SeqCst);
        let mut actor = CapacityHarness::open(faulted.clone(), genesis.clone())?;
        faulted.fail.store(mode, Ordering::SeqCst);
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| actor.apply(&intent)));
        if mode == usize::MAX - 2 {
            assert!(result.is_err());
        } else {
            assert!(result.unwrap().is_err());
        }
        assert_eq!(
            actor.apply(&intent),
            Err(BrokerError::CapacityWriterUnusable)
        );
        drop(actor);
        let image = store.snapshot()?;
        assert_eq!(
            image,
            if mode == usize::MAX {
                initial.clone()
            } else {
                after.clone()
            }
        );
        oracle::observe(image.entries(), genesis.owner());
        faulted.fail.store(0, Ordering::SeqCst);
        drop(CapacityHarness::open(faulted.clone(), genesis.clone())?);
    }
    let final_image = store.snapshot()?;
    drop(faulted);
    drop(store);
    let reopened = provider.open()?;
    assert_eq!(reopened.snapshot()?, final_image);
    oracle::observe(final_image.entries(), genesis.owner());
    Ok(())
}
paired!(
    memory_faults_and_ambiguous_retirement,
    fjall_faults_and_ambiguous_retirement,
    faults
);

fn varints<P: StoreProvider>(provider: P) -> Result {
    let mut f = Fixture::new(provider, config())?;
    f.parent(raw(1))?;
    let good = f.observation();
    drop(f.actor.take());
    for boundary in [127, 128, 16383, 16384, u64::MAX - 1] {
        let original = &good.messages.values().next().unwrap().1;
        let mut record = original.clone();
        record.sequence = Seq::new(boundary);
        record.enqueued_at = Timestamp::from_millis(boundary);
        record.delivery_count = u32::try_from(boundary).unwrap_or(u32::MAX);
        let mut batch = WriteBatch::default();
        for (key, _) in f.image() {
            if key.first() == Some(&0x13) {
                batch.push_delete(key);
            }
        }
        let n = f.genesis.owner().namespace();
        let e = f.genesis.owner().parent();
        for (key, value) in [
            (
                keys::message(n, e, record.sequence),
                codec::message::encode(&record)?,
            ),
            (keys::ready(n, e, record.sequence), vec![]),
            (
                keys::queue_counters(n, e),
                codec::encode(&QueueCounters {
                    next_sequence: boundary + 1,
                    next_lock_token: boundary,
                })?,
            ),
        ] {
            batch.push_put(capacity_fence::data_key(&key)?, value);
        }
        f.store().apply(batch)?;
        let recomputed = oracle::recompute(&f.image(), f.genesis.owner());
        f.store().apply(WriteBatch::default().put(
            usage(f.genesis.owner()),
            usage_codec::encode(f.genesis.owner(), recomputed.usage)?,
        ))?;
        f.actor = Some(CapacityHarness::open(f.store().clone(), f.genesis.clone())?);
        f.time = boundary;
        if boundary < u64::MAX - 1 {
            let d = f.locked()?;
            f.parent(C::RenewLock {
                sequence: d.sequence,
                lock_token: d.lock.unwrap().token,
                lock_duration_millis: Some(boundary),
            })?;
        } else {
            f.parent(receive(ReceiveMode::ReceiveAndDelete))?;
        }
        f.reopen()?;
        drop(f.actor.take());
    }
    Ok(())
}
paired!(memory_varint_edges, fjall_varint_edges, varints);

fn terminal_allocations<P: StoreProvider>(provider: P) -> Result {
    let mut f = Fixture::new(provider, config())?;
    f.parent(raw(1))?;
    // Synthetic canonical domain frontiers, not a claim of 2^64 sends/receives
    // or AMQP traffic. No actor is alive while this exact fixture is seeded.
    drop(f.actor.take());
    let key = capacity_fence::data_key(&keys::queue_counters(
        f.genesis.owner().namespace(),
        f.genesis.owner().parent(),
    ))?;
    f.store().apply(WriteBatch::default().put(
        key.clone(),
        codec::encode(&QueueCounters {
            next_sequence: u64::MAX - 1,
            next_lock_token: u64::MAX - 1,
        })?,
    ))?;
    f.observation();
    f.actor = Some(CapacityHarness::open(f.store().clone(), f.genesis.clone())?);
    let O::Sent {
        sequence: terminal_sequence,
    } = f.parent(raw(1))?
    else {
        panic!()
    };
    assert_eq!(terminal_sequence.as_u64(), u64::MAX - 1);
    let counters: QueueCounters = codec::decode(&f.store().get(&key)?.unwrap())?;
    assert_eq!(counters.next_sequence, u64::MAX);
    let before = f.image();
    assert_eq!(f.parent(raw(1)), Err(BrokerError::SequenceNumberExhausted));
    assert_eq!(f.image(), before);
    f.observation();
    let commitment = f.observation().ceilings[&Seq::new(1)];
    let first = f.locked()?;
    let token = first.lock.unwrap().token;
    assert_eq!(token.as_u64(), u64::MAX - 1);
    assert!(f.observation().ceilings[&first.sequence] <= commitment);
    let counters: QueueCounters = codec::decode(&f.store().get(&key)?.unwrap())?;
    assert_eq!(
        (counters.next_sequence, counters.next_lock_token),
        (u64::MAX, u64::MAX)
    );
    let before = f.image();
    let usage = f.observation().usage;
    assert_eq!(
        f.parent(receive(ReceiveMode::PeekLock)),
        Err(BrokerError::LockTokenExhausted)
    );
    assert_eq!(f.image(), before);
    assert_eq!(f.observation().usage, usage);
    assert_eq!(
        f.observation().messages[&terminal_sequence].1.state,
        MessageState::Ready
    );
    f.reopen()?;
    f.parent(C::Complete {
        sequence: first.sequence,
        lock_token: token,
    })?;
    let before = f.image();
    assert_eq!(
        f.parent(receive(ReceiveMode::PeekLock)),
        Err(BrokerError::LockTokenExhausted)
    );
    assert_eq!(
        f.image(),
        before,
        "deleting an old lock does not reset its token frontier"
    );
    f.parent(receive(ReceiveMode::ReceiveAndDelete))?;
    assert_eq!(f.observation().usage.message_count(), 0);
    f.reopen()?;
    Ok(())
}
paired!(
    memory_terminal_owned_allocations,
    fjall_terminal_owned_allocations,
    terminal_allocations
);

fn full_deferred_terminal_shadow<P: StoreProvider>(provider: P) -> Result {
    let mut f = Fixture::new(
        provider,
        QueueConfig {
            default_time_to_live_millis: Some(500),
            ..config()
        },
    )?;
    f.time = u64::MAX - 1001;
    let O::Sent { sequence: old } = f.parent(C::SendContent {
        content: MessageContent::AmqpTextIds(envelope(3)),
    })?
    else {
        panic!()
    };
    let mut record = f.observation().messages[&old].1.clone();
    assert_eq!(record.format, MessageRecordFormat::V3);
    assert_eq!(
        record.expires_at,
        Some(Timestamp::from_millis(u64::MAX - 500))
    );
    // This is a canonical stopped-owner domain allocation fixture, not a
    // forged actor request, native-flow identity or production migration.
    drop(f.actor.take());
    let sequence = Seq::new(u64::MAX - 9);
    record.sequence = sequence;
    record.delivery_count = u32::MAX - 1;
    let n = f.genesis.owner().namespace();
    let parent = f.genesis.owner().parent();
    let shadow = f.genesis.owner().shadow();
    let mut seed = WriteBatch::default();
    for (k, _) in f.image() {
        if k.first() == Some(&0x13) {
            seed.push_delete(k);
        }
    }
    for (k, v) in [
        (
            keys::message(n, parent, sequence),
            codec::message::encode(&record)?,
        ),
        (keys::ready(n, parent, sequence), vec![]),
        (
            keys::expiry(n, parent, record.expires_at.unwrap(), sequence),
            vec![],
        ),
        (
            keys::queue_counters(n, parent),
            codec::encode(&QueueCounters {
                next_sequence: u64::MAX - 8,
                next_lock_token: u64::MAX - 1,
            })?,
        ),
        (
            keys::queue_counters(n, shadow),
            codec::encode(&QueueCounters {
                next_sequence: 1,
                next_lock_token: u64::MAX - 1,
            })?,
        ),
    ] {
        seed.push_put(capacity_fence::data_key(&k)?, v);
    }
    f.store().apply(seed)?;
    let computed = oracle::recompute(&f.image(), f.genesis.owner());
    f.store().apply(WriteBatch::default().put(
        usage(f.genesis.owner()),
        usage_codec::encode(f.genesis.owner(), computed.usage)?,
    ))?;
    f.observation();
    f.actor = Some(CapacityHarness::open(f.store().clone(), f.genesis.clone())?);
    f.fill()?;
    let full = f.observation();
    assert_eq!(full.usage.committed_bytes()?, 1048576);
    let peak = full.ceilings[&sequence];
    let d = f.locked()?;
    assert_eq!(d.sequence, sequence);
    let token = d.lock.unwrap().token;
    assert_eq!(token.as_u64(), u64::MAX - 1);
    assert_eq!(f.observation().usage.committed_bytes()?, 1048576);
    f.parent(C::Defer {
        sequence,
        lock_token: token,
        replacement_envelope: None,
    })?;
    assert_eq!(
        f.observation().messages[&sequence].1.state,
        MessageState::Deferred
    );
    assert_eq!(f.observation().usage.committed_bytes()?, 1048576);
    f.time = u64::MAX - 501;
    let O::DeferredReceived(deliveries) = f.parent(C::ReceiveDeferred {
        sequences: vec![sequence],
        mode: ReceiveMode::PeekLock,
        lock_duration_millis: None,
        session: None,
    })?
    else {
        panic!()
    };
    assert!(deliveries.is_empty());
    let moved = f.observation();
    let (endpoint, record) = &moved.messages[&sequence];
    assert_eq!(endpoint, f.genesis.owner().shadow());
    assert_eq!(
        record.dead_letter.as_ref().unwrap().description,
        "the deferred message exceeded its time to live"
    );
    assert!(moved.ceilings[&sequence] <= peak);
    assert_eq!(
        moved.usage.committed_bytes()?,
        1048576,
        "fixed longest DLQ metadata was already prepaid"
    );
    let O::Received(Some(d)) = f.shadow(receive(ReceiveMode::PeekLock))? else {
        panic!()
    };
    assert_eq!(d.sequence, sequence);
    let shadow_token = d.lock.unwrap().token;
    assert_eq!(shadow_token.as_u64(), u64::MAX - 1);
    let locked = f.observation();
    assert!(locked.ceilings[&sequence] <= moved.ceilings[&sequence]);
    assert_eq!(locked.usage.committed_bytes()?, 1048576);
    let rows = f.image();
    let group = rows
        .iter()
        .filter(|(k, _)| {
            k.first() == Some(&0x13)
                && matches!(k.get(1), Some(3..=7 | 11))
                && k.ends_with(&sequence.as_u64().to_be_bytes())
        })
        .collect::<Vec<_>>();
    assert_eq!(group.len(), 2);
    assert!(
        group.iter().any(|(k, _)| k.starts_with(&[0x13, 5])),
        "actual lock is tag05, not expiry06"
    );
    let actual = group
        .iter()
        .map(|(k, v)| (k.len() + v.len()) as u64)
        .sum::<u64>();
    let unframed = group
        .iter()
        .map(|(k, v)| (k.len() - 1 + v.len()) as u64)
        .sum::<u64>();
    assert_eq!(
        actual,
        unframed + 2,
        "one extra physical byte on EACH of the two keys"
    );
    assert_eq!(
        actual, locked.ceilings[&sequence],
        "max-width shadow lock has consumed this message's remaining reserve"
    );
    assert_eq!(
        rows.iter()
            .filter(|(k, _)| k.first() == Some(&0x12))
            .count(),
        1
    );
    let before = f.image();
    assert!(matches!(
        f.parent(raw(1)),
        Err(BrokerError::RetainedCapacityExceeded { .. })
    ));
    assert_eq!(f.image(), before);
    let parent_counters: QueueCounters = codec::decode(
        &f.store()
            .get(&capacity_fence::data_key(&keys::queue_counters(
                f.genesis.owner().namespace(),
                f.genesis.owner().parent(),
            ))?)?
            .unwrap(),
    )?;
    let shadow_counters: QueueCounters = codec::decode(
        &f.store()
            .get(&capacity_fence::data_key(&keys::queue_counters(
                f.genesis.owner().namespace(),
                f.genesis.owner().shadow(),
            ))?)?
            .unwrap(),
    )?;
    assert_eq!(parent_counters.next_lock_token, u64::MAX);
    assert_eq!(shadow_counters.next_lock_token, u64::MAX);
    assert_eq!(parent_counters.next_sequence, u64::MAX - 4);
    f.reopen()?;
    f.shadow(C::Complete {
        sequence,
        lock_token: shadow_token,
    })?;
    assert_eq!(
        f.observation().usage.committed_bytes()?,
        1048576 - locked.ceilings[&sequence]
    );
    f.reopen()?;
    Ok(())
}
paired!(
    memory_full_deferred_expiry_terminal_shadow_lock,
    fjall_full_deferred_expiry_terminal_shadow_lock,
    full_deferred_terminal_shadow
);

#[test]
fn manual_v3_peak_and_reserve_anchor() -> Result {
    use domain::{
        DeadLetterInfo, DeadLetterReason, DeliveryOrigin, LockToken, MessageRecord,
        retained_accounting::{CapacityFramingBudget, CapacityOwner, reserve_message},
    };
    let owner = CapacityOwner::new(NamespaceName::new("n")?, EntityPath::new("q")?, 1)?;
    let mut record = MessageRecord {
        sequence: Seq::new(u64::MAX - 1),
        message_id: String::new(),
        body: vec![],
        enqueued_at: Timestamp::from_millis(u64::MAX),
        scheduled_enqueue_at: None,
        expires_at: Some(Timestamp::from_millis(u64::MAX)),
        delivery_count: u32::MAX,
        state: MessageState::Ready,
        session_id: None,
        dead_letter: None,
        envelope: Some(MessageEnvelope::new(vec![0, 0x53, 0x75, 0xa0, 2, 1, 2])),
        format: MessageRecordFormat::V3,
    };
    // HUMAN ANCHOR, not a candidate enumerator or codec-derived expectation:
    // V3 prefix1; sequence10; empty string1; enqueued10; scheduled None1;
    // expiry Some(max)11; delivery u32max5; Ready1; session None1; DLQ None1;
    // Amqp content = variant1 + (length1 + original7) + empty overlay1 +
    // projection9 (eight absent option tags + empty map length) = 19.
    // Ready value = 61. Physical parent primary/ready = 14 each:
    // outer1+tag1+n1+NUL1+q1+NUL1+sequence8. Expiry adds deadline8 =>22.
    // Thus actual Ready = 61+14+14+22 = 111.
    const READY_VALUE: u64 = 61;
    const READY_ACTUAL: u64 = 111;
    let value = codec::message::encode(&record)?;
    assert_eq!(value.len() as u64, READY_VALUE);
    let rows = vec![
        (
            capacity_fence::data_key(&keys::message(
                owner.namespace(),
                owner.parent(),
                record.sequence,
            ))?,
            value,
        ),
        (
            capacity_fence::data_key(&keys::ready(
                owner.namespace(),
                owner.parent(),
                record.sequence,
            ))?,
            vec![],
        ),
        (
            capacity_fence::data_key(&keys::expiry(
                owner.namespace(),
                owner.parent(),
                record.expires_at.unwrap(),
                record.sequence,
            ))?,
            vec![],
        ),
    ];
    assert_eq!(
        rows.iter().map(|(k, _)| k.len()).collect::<Vec<_>>(),
        vec![14, 14, 22]
    );
    assert_eq!(
        rows.iter()
            .map(|(k, v)| (k.len() + v.len()) as u64)
            .sum::<u64>(),
        READY_ACTUAL
    );
    // FUTURE PEAK, also calculated by hand:
    // parent Locked adds21 value bytes, swaps ready+expiry(36) for lock22:
    // 82+14+22 =118. Parent Deferred =61+14+14=89. No Scheduled is possible
    // without a supplied schedule. DLQ clears expiry (-10), adds metadata58
    // (reason1 + string-length1 + longest fixed description46 + timestamp10).
    // Shadow path is18 bytes (q + /$deadletterqueue17): primary/ready31,
    // lock39. DLQ Ready/Deferred=109+31+31=171; Locked=130+31+39=200.
    // Automatic reasons are one-byte tags and their descriptions are <=46.
    // Therefore peak C=200, R for current Ready =200-111=89. These literals
    // do not come from reserve_message, the finalizer, or oracle::ceiling.
    const PEAK: u64 = 200;
    const READY_RESERVE: u64 = 89;
    let actual = reserve_message(&owner, &rows, CapacityFramingBudget::IsolatedKeyspaceV1)?;
    assert_eq!(
        (
            actual.retained_bytes,
            actual.reserved_growth_bytes,
            actual.ceiling_bytes
        ),
        (READY_ACTUAL, READY_RESERVE, PEAK)
    );
    assert_eq!(oracle::ceiling(&owner, owner.parent(), &record), PEAK);
    record.state = MessageState::Locked {
        token: LockToken::new(u64::MAX),
        locked_until: Timestamp::from_millis(u64::MAX),
        origin: DeliveryOrigin::Deferred,
    };
    assert_eq!(codec::message::encode(&record)?.len(), 82);
    record.expires_at = None;
    record.dead_letter = Some(DeadLetterInfo {
        reason: DeadLetterReason::TimeToLiveExpired,
        description: "the deferred message exceeded its time to live".into(),
        dead_lettered_at: Timestamp::from_millis(u64::MAX),
    });
    assert_eq!(record.dead_letter.as_ref().unwrap().description.len(), 46);
    assert_eq!(codec::message::encode(&record)?.len(), 130);
    let primary = capacity_fence::data_key(&keys::message(
        owner.namespace(),
        owner.shadow(),
        record.sequence,
    ))?;
    let lock = capacity_fence::data_key(&keys::lock(
        owner.namespace(),
        owner.shadow(),
        Timestamp::from_millis(u64::MAX),
        record.sequence,
    ))?;
    assert_eq!((primary.len(), lock.len()), (31, 39));
    assert_eq!(130 + primary.len() + lock.len(), 200);
    Ok(())
}

fn ttl_dlq<P: StoreProvider>(provider: P) -> Result {
    let mut f = Fixture::new(provider, config())?;
    for path in 0..6 {
        f.parent(C::SendContent {
            content: MessageContent::Raw(RawMessage {
                time_to_live_millis: Some(10),
                ..RawMessage::default()
            }),
        })?;
        let mut held = None;
        if (1..=4).contains(&path) {
            held = Some(f.locked()?);
        }
        if path == 3 || path == 4 {
            let d = held.as_ref().unwrap();
            f.parent(C::Defer {
                sequence: d.sequence,
                lock_token: d.lock.as_ref().unwrap().token,
                replacement_envelope: None,
            })?;
        }
        f.time += 20;
        match path {
            0 => {
                f.parent(receive(ReceiveMode::ReceiveAndDelete))?;
            }
            1 => {
                let d = held.unwrap();
                f.parent(C::Abandon {
                    sequence: d.sequence,
                    lock_token: d.lock.unwrap().token,
                    replacement_envelope: None,
                })?;
            }
            2 => {
                f.time += 60_000;
                f.parent(C::ExpireLocks)?;
            }
            3 | 4 => {
                f.parent(C::ReceiveDeferred {
                    sequences: vec![held.unwrap().sequence],
                    mode: if path == 3 {
                        ReceiveMode::PeekLock
                    } else {
                        ReceiveMode::ReceiveAndDelete
                    },
                    lock_duration_millis: None,
                    session: None,
                })?;
            }
            _ => {
                f.parent(C::ExpireMessages)?;
            }
        }
        let observation = f.observation();
        assert_eq!(observation.messages.len(), 1);
        let (endpoint, record) = observation.messages.values().next().unwrap();
        assert_eq!(endpoint, f.genesis.owner().shadow());
        assert_eq!(
            record.dead_letter.as_ref().unwrap().reason,
            domain::DeadLetterReason::TimeToLiveExpired
        );
        if path == 3 || path == 4 {
            assert_eq!(
                record.dead_letter.as_ref().unwrap().description,
                "the deferred message exceeded its time to live"
            );
        }
        f.reopen()?;
        f.shadow(receive(ReceiveMode::ReceiveAndDelete))?;
        assert_eq!(f.observation().usage.message_count(), 0);
    }
    Ok(())
}
paired!(memory_all_ttl_dlq_paths, fjall_all_ttl_dlq_paths, ttl_dlq);

fn scheduled_full<P: StoreProvider>(provider: P) -> Result {
    let mut f = Fixture::new(provider, config())?;
    let O::Sent { sequence } = f.parent(C::SendContent {
        content: MessageContent::Raw(RawMessage {
            scheduled_enqueue_at: Some(Timestamp::from_millis(100)),
            time_to_live_millis: Some(200),
            ..RawMessage::default()
        }),
    })?
    else {
        panic!()
    };
    f.fill()?;
    let before = f.observation();
    f.time = 127;
    f.parent(C::ActivateScheduled)?;
    let after = f.observation();
    assert_eq!(after.messages.len(), before.messages.len());
    assert!(!after.messages.contains_key(&sequence));
    assert!(after.usage.committed_bytes()? <= before.usage.committed_bytes()?);
    f.time = 500;
    f.parent(C::ExpireMessages)?;
    f.reopen()?;
    Ok(())
}
paired!(
    memory_full_scheduled_automatic_growth,
    fjall_full_scheduled_automatic_growth,
    scheduled_full
);

fn batch_refusal<P: StoreProvider>(provider: P) -> Result {
    let mut f = Fixture::new(provider, config())?;
    for index in 0..3 {
        let mut messages = vec![MessageContent::Raw(RawMessage::default()); 3];
        messages[index] = MessageContent::Amqp(vec![0xff]);
        let before = f.image();
        assert!(f.parent(C::SendContentBatch { messages }).is_err());
        assert_eq!(f.image(), before);
    }
    f.parent(C::SendBatch {
        messages: vec![MessageInput::default(); 3],
    })?;
    let mut sequences = Vec::new();
    for _ in 0..3 {
        let d = f.locked()?;
        sequences.push(d.sequence);
        f.parent(C::Defer {
            sequence: d.sequence,
            lock_token: d.lock.unwrap().token,
            replacement_envelope: None,
        })?;
    }
    for index in 0..3 {
        let mut request = sequences.clone();
        request[index] = Seq::new(999);
        let before = f.image();
        assert!(
            f.parent(C::ReceiveDeferred {
                sequences: request,
                mode: ReceiveMode::ReceiveAndDelete,
                lock_duration_millis: None,
                session: None
            })
            .is_err()
        );
        assert_eq!(f.image(), before);
    }
    f.parent(C::ReceiveDeferred {
        sequences,
        mode: ReceiveMode::ReceiveAndDelete,
        lock_duration_millis: None,
        session: None,
    })?;
    f.reopen()?;
    Ok(())
}
paired!(
    memory_owned_batch_error_atomicity,
    fjall_owned_batch_error_atomicity,
    batch_refusal
);

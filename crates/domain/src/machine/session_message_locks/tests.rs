use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use storage::{Key, MemoryStore, StorageError, StoreSnapshot, Value};

use super::*;

type ScanCalls = Arc<Mutex<Vec<(Vec<u8>, usize)>>>;

#[derive(Clone, Default)]
struct ProbeStore {
    inner: MemoryStore,
    scans: ScanCalls,
    applies: Arc<AtomicUsize>,
}

impl StateStore for ProbeStore {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.inner.get(key)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.applies.fetch_add(1, Ordering::SeqCst);
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
        self.scans
            .lock()
            .expect("probe lock")
            .push((prefix.to_vec(), limit));
        self.inner.scan_from(prefix, start, limit)
    }
}

fn scope() -> (Command, QueueConfig, SessionHold) {
    let hold = SessionHold::new(SessionId::new("A").expect("session"), LockToken::new(3));
    let command = Command::new(
        NamespaceName::new("test").expect("namespace"),
        EntityPath::new("queue").expect("entity"),
        Timestamp::from_millis(10),
        CommandKind::ExpireLocks,
    );
    let config = QueueConfig {
        requires_session: true,
        ..QueueConfig::default()
    };
    (command, config, hold)
}

fn ready(sequence: u64, session: &SessionId) -> MessageRecord {
    MessageRecord {
        sequence: SequenceNumber::new(sequence),
        message_id: format!("m{sequence}"),
        body: Vec::new(),
        enqueued_at: Timestamp::from_millis(1),
        expires_at: None,
        delivery_count: 0,
        state: MessageState::Ready,
        session_id: Some(session.clone()),
        dead_letter: None,
        scheduled_enqueue_time: None,
        envelope: None,
    }
}

fn locked(sequence: u64, session: &SessionId) -> MessageRecord {
    let mut record = ready(sequence, session);
    record.state = MessageState::Locked {
        token: LockToken::new(100 + sequence),
        locked_until: Timestamp::from_millis(100),
    };
    record
}

fn install(
    store: &ProbeStore,
    command: &Command,
    config: &QueueConfig,
    count: u64,
    hold: Option<&SessionHold>,
) -> Result<(), BrokerError> {
    let mut batch = WriteBatch::default();
    let session = SessionId::new("A").expect("session");
    let mut tracker = SessionMessageLocks::new(store, command, config, &mut batch);
    for sequence in 1..=count {
        tracker.install_locked(
            &ready(sequence, &session),
            LockToken::new(100 + sequence),
            Timestamp::from_millis(100),
            hold,
        )?;
    }
    store.apply(batch)?;
    Ok(())
}

#[test]
fn scoped_owner_keys_roundtrip_without_prefix_aliases() {
    let (command, _, hold) = scope();
    let sequence = SequenceNumber::new(19);
    let reverse_prefix =
        keys::session_message_lock_reverse_prefix(&command.namespace, &command.entity);
    let reverse = keys::session_message_lock_reverse(&command.namespace, &command.entity, sequence);
    assert_eq!(
        keys::session_message_lock_reverse_parts(&reverse_prefix, &reverse),
        Some(sequence)
    );
    let prefix = keys::session_message_lock_forward_prefix(
        &command.namespace,
        &command.entity,
        &hold.session_id,
    );
    for generation in [None, Some(hold.token)] {
        let key = keys::session_message_lock_forward(
            &command.namespace,
            &command.entity,
            &hold.session_id,
            generation,
            sequence,
        );
        assert_eq!(
            keys::session_message_lock_forward_parts(&prefix, &key),
            Some((generation, sequence))
        );
        for malformed in [
            key[..key.len() - 1].to_vec(),
            [key.as_slice(), &[0]].concat(),
        ] {
            assert_eq!(
                keys::session_message_lock_forward_parts(&prefix, &malformed),
                None
            );
        }
        let alias = keys::session_message_lock_forward_prefix(
            &command.namespace,
            &command.entity,
            &SessionId::new("AA").expect("session"),
        );
        assert_eq!(keys::session_message_lock_forward_parts(&alias, &key), None);
        let mut malformed = key.clone();
        malformed[prefix.len()] = 2;
        assert_eq!(
            keys::session_message_lock_forward_parts(&prefix, &malformed),
            None
        );
    }
    let invalid_owned = keys::session_message_lock_forward(
        &command.namespace,
        &command.entity,
        &hold.session_id,
        Some(LockToken::new(0)),
        sequence,
    );
    assert_eq!(
        keys::session_message_lock_forward_parts(&prefix, &invalid_owned),
        None
    );
    let summary_prefix =
        keys::session_message_lock_summary_prefix(&command.namespace, &command.entity);
    let summary =
        keys::session_message_lock_summary(&command.namespace, &command.entity, &hold.session_id);
    assert_eq!(
        keys::session_message_lock_summary_parts(&summary_prefix, &summary),
        Some("A")
    );
    assert_eq!(
        keys::session_message_lock_summary_parts(
            &summary_prefix,
            &[summary.as_slice(), &[0]].concat()
        ),
        None
    );
    assert_eq!(
        keys::session_message_lock_reverse_parts(
            &reverse_prefix,
            &[reverse.as_slice(), &[0]].concat()
        ),
        None
    );
    let mut wrong_tag = reverse_prefix.clone();
    wrong_tag[0] = 0x14;
    assert_eq!(
        keys::session_message_lock_reverse_parts(&wrong_tag, &reverse),
        None
    );
    let malformed_prefix = b"\x13test\0\0";
    let malformed_key = [malformed_prefix.as_slice(), &19_u64.to_be_bytes()].concat();
    assert_eq!(
        keys::session_message_lock_reverse_parts(malformed_prefix, &malformed_key),
        None
    );
    let malformed_prefix = b"\x14test\0queue\0A\0extra\0";
    let malformed_key = [malformed_prefix.as_slice(), &[0; 17]].concat();
    assert_eq!(
        keys::session_message_lock_forward_parts(malformed_prefix, &malformed_key),
        None
    );
}

#[test]
fn reverse_forward_identity_must_match_locked_record() -> Result<(), BrokerError> {
    let (command, config, hold) = scope();
    for mismatch in 0..9 {
        let store = ProbeStore::default();
        install(&store, &command, &config, 1, Some(&hold))?;
        let reverse = keys::session_message_lock_reverse(
            &command.namespace,
            &command.entity,
            SequenceNumber::new(1),
        );
        let forward = keys::session_message_lock_forward(
            &command.namespace,
            &command.entity,
            &hold.session_id,
            Some(hold.token),
            SequenceNumber::new(1),
        );
        let mut row: Row = codec::decode(&store.get(&reverse)?.expect("row"))?;
        match mismatch {
            0 => row.namespace = NamespaceName::new("other").expect("namespace"),
            1 => row.entity = EntityPath::new("other").expect("entity"),
            2 => row.session_id = SessionId::new("other").expect("session"),
            3 => row.sequence = SequenceNumber::new(2),
            4 => row.message_token = LockToken::new(0),
            5 => row.message_token = LockToken::new(102),
            6 => row.locked_until = Timestamp::from_millis(99),
            7 => row.owner = Owner::HeldGeneration(LockToken::new(0)),
            _ => {}
        }
        let mut damage = WriteBatch::default().put(reverse.clone(), codec::encode(&row)?);
        if mismatch == 8 {
            damage.push_delete(forward);
        }
        store.apply(damage)?;
        let before = store.snapshot()?;
        let mut batch = WriteBatch::default();
        let mut tracker = SessionMessageLocks::new(&store, &command, &config, &mut batch);
        assert_eq!(
            tracker.validate_locked(&locked(1, &hold.session_id), None),
            Err(BrokerError::MalformedIndexKey)
        );
        assert_eq!(
            tracker.leave_locked(&locked(1, &hold.session_id)),
            Err(BrokerError::MalformedIndexKey)
        );
        assert!(batch.is_empty());
        assert_eq!(store.snapshot()?, before);
    }
    for version in [10, 12] {
        let store = ProbeStore::default();
        install(&store, &command, &config, 1, Some(&hold))?;
        let reverse = keys::session_message_lock_reverse(
            &command.namespace,
            &command.entity,
            SequenceNumber::new(1),
        );
        let mut raw = store.get(&reverse)?.expect("row");
        raw[0] = version;
        store.apply(WriteBatch::default().put(reverse, raw))?;
        let mut batch = WriteBatch::default();
        assert_eq!(
            SessionMessageLocks::new(&store, &command, &config, &mut batch)
                .validate_locked(&locked(1, &hold.session_id), None),
            Err(BrokerError::Codec(crate::CodecError::UnsupportedVersion {
                version
            }))
        );
        assert!(batch.is_empty());
    }
    let store = ProbeStore::default();
    install(&store, &command, &config, 1, Some(&hold))?;
    let reverse = keys::session_message_lock_reverse(
        &command.namespace,
        &command.entity,
        SequenceNumber::new(1),
    );
    let mut raw = store.get(&reverse)?.expect("row");
    raw.push(0);
    store.apply(WriteBatch::default().put(reverse, raw))?;
    let mut batch = WriteBatch::default();
    assert_eq!(
        SessionMessageLocks::new(&store, &command, &config, &mut batch)
            .validate_locked(&locked(1, &hold.session_id), None),
        Err(BrokerError::Codec(crate::CodecError::Decode))
    );
    assert!(batch.is_empty());
    for mode in [ReceiveMode::PeekLock, ReceiveMode::ReceiveAndDelete] {
        let store = ProbeStore::default();
        let other = SessionId::new("B").expect("session");
        let record = ready(1, &other);
        let session = SessionRecord {
            lock: Some(SessionLock {
                token: hold.token,
                locked_until: Timestamp::from_millis(100),
            }),
            state: Vec::new(),
        };
        store.apply(
            WriteBatch::default()
                .put(
                    keys::queue_config(&command.namespace, &command.entity),
                    codec::encode(&config)?,
                )
                .put(
                    keys::message(&command.namespace, &command.entity, record.sequence),
                    codec::encode(&record)?,
                )
                .put(
                    keys::session(&command.namespace, &command.entity, &hold.session_id),
                    codec::encode(&session)?,
                )
                .put(
                    keys::session_ready(
                        &command.namespace,
                        &command.entity,
                        &hold.session_id,
                        record.sequence,
                    ),
                    Vec::new(),
                ),
        )?;
        let before = store.snapshot()?;
        let applies = store.applies.load(Ordering::SeqCst);
        let mut receive = command.clone();
        receive.kind = CommandKind::Receive {
            mode,
            lock_duration_millis: None,
            session: Some(hold.clone()),
        };
        let machine = StateMachine::new(store.clone());
        assert_eq!(machine.apply(&receive), Err(BrokerError::MalformedIndexKey));
        assert_eq!(store.snapshot()?, before);
        assert_eq!(store.applies.load(Ordering::SeqCst), applies);
        assert_eq!(machine.last_applied_time()?, Timestamp::UNIX_EPOCH);
    }
    Ok(())
}

#[test]
fn summary_counts_and_generation_are_canonical_and_checked() -> Result<(), BrokerError> {
    let (command, config, hold) = scope();
    let template = Summary {
        namespace: command.namespace.clone(),
        entity: command.entity.clone(),
        session_id: hold.session_id.clone(),
        owned_generation: Some(hold.token),
        owned_count: 1,
        unowned_count: 0,
    };
    for (generation, owned, unowned) in [
        (None, 0, 0),
        (Some(hold.token), 0, 1),
        (None, 1, 0),
        (Some(LockToken::new(0)), 1, 0),
        (Some(hold.token), u64::MAX, 1),
    ] {
        let store = ProbeStore::default();
        let summary = Summary {
            owned_generation: generation,
            owned_count: owned,
            unowned_count: unowned,
            ..template.clone()
        };
        store.apply(WriteBatch::default().put(
            keys::session_message_lock_summary(
                &command.namespace,
                &command.entity,
                &hold.session_id,
            ),
            codec::encode(&summary)?,
        ))?;
        let mut batch = WriteBatch::default();
        assert_eq!(
            SessionMessageLocks::new(&store, &command, &config, &mut batch)
                .takeover_pending(&hold.session_id),
            Err(BrokerError::MalformedIndexKey)
        );
        assert!(batch.is_empty());
    }
    let mut overflow = Summary {
        owned_count: u64::MAX,
        ..template.clone()
    };
    assert_eq!(
        overflow.add(Owner::HeldGeneration(hold.token)),
        Err(BrokerError::MalformedIndexKey)
    );
    let mut overflow = Summary {
        owned_generation: None,
        owned_count: 0,
        unowned_count: u64::MAX,
        ..template.clone()
    };
    assert_eq!(
        overflow.add(Owner::TrustedUnowned),
        Err(BrokerError::MalformedIndexKey)
    );
    let mut mixed = template.clone();
    mixed.add(Owner::TrustedUnowned)?;
    assert_eq!(mixed.total()?, 2);
    assert_eq!(
        mixed.remove(Owner::HeldGeneration(LockToken::new(4))),
        Err(BrokerError::MalformedIndexKey)
    );
    assert_eq!(
        mixed.add(Owner::HeldGeneration(LockToken::new(4))),
        Err(BrokerError::MalformedIndexKey)
    );
    assert!(!mixed.remove(Owner::HeldGeneration(hold.token))?);
    assert_eq!(mixed.owned_generation, None);
    assert!(mixed.remove(Owner::TrustedUnowned)?);
    assert_eq!(
        mixed.remove(Owner::TrustedUnowned),
        Err(BrokerError::MalformedIndexKey)
    );
    Ok(())
}

#[test]
fn staged_multiple_lock_entries_read_the_previous_summary_update() -> Result<(), BrokerError> {
    let (command, config, hold) = scope();
    let store = ProbeStore::default();
    let mut batch = WriteBatch::default();
    for sequence in 1..=256 {
        SessionMessageLocks::new(&store, &command, &config, &mut batch).install_locked(
            &ready(sequence, &hold.session_id),
            LockToken::new(sequence + 100),
            Timestamp::from_millis(100),
            if sequence % 2 == 0 { Some(&hold) } else { None },
        )?;
    }
    let staged = batch.clone();
    let wrong = SessionHold::new(hold.session_id.clone(), LockToken::new(4));
    assert_eq!(
        SessionMessageLocks::new(&store, &command, &config, &mut batch).install_locked(
            &ready(257, &hold.session_id),
            LockToken::new(357),
            Timestamp::from_millis(100),
            Some(&wrong)
        ),
        Err(BrokerError::MalformedIndexKey)
    );
    assert_eq!(batch, staged);
    let summary = SessionMessageLocks::new(&store, &command, &config, &mut batch)
        .summary(&hold.session_id)?
        .expect("staged summary");
    assert_eq!(
        (
            summary.owned_count,
            summary.unowned_count,
            summary.owned_generation
        ),
        (128, 128, Some(hold.token))
    );
    assert!(store.snapshot()?.entries().is_empty());
    store.apply(batch)?;
    assert_eq!(store.snapshot()?.entries().len(), 513);
    let mut batch = WriteBatch::default();
    let key =
        keys::session_message_lock_summary(&command.namespace, &command.entity, &hold.session_id);
    batch.push_delete(key.clone());
    assert_eq!(
        SessionMessageLocks::new(&store, &command, &config, &mut batch).read_staged(&key)?,
        None
    );
    batch.push_put(key.clone(), codec::encode(&summary)?);
    assert_eq!(
        SessionMessageLocks::new(&store, &command, &config, &mut batch)
            .summary(&hold.session_id)?,
        Some(summary)
    );
    Ok(())
}

#[test]
fn staged_multiple_exits_and_renewal_preserve_exact_original_owner() -> Result<(), BrokerError> {
    let (command, config, hold) = scope();
    let store = ProbeStore::default();
    install(&store, &command, &config, 256, Some(&hold))?;
    let mut batch = WriteBatch::default();
    let mut first = locked(1, &hold.session_id);
    SessionMessageLocks::new(&store, &command, &config, &mut batch).renew_locked(
        &first,
        Timestamp::from_millis(200),
        Some(&hold),
    )?;
    first.state = MessageState::Locked {
        token: LockToken::new(101),
        locked_until: Timestamp::from_millis(200),
    };
    SessionMessageLocks::new(&store, &command, &config, &mut batch).leave_locked(&first)?;
    for sequence in 2..=256 {
        SessionMessageLocks::new(&store, &command, &config, &mut batch)
            .leave_locked(&locked(sequence, &hold.session_id))?;
    }
    assert_eq!(
        SessionMessageLocks::new(&store, &command, &config, &mut batch)
            .summary(&hold.session_id)?,
        None
    );
    assert_eq!(
        SessionMessageLocks::new(&store, &command, &config, &mut batch).read_staged(
            &keys::session_message_lock_reverse(
                &command.namespace,
                &command.entity,
                SequenceNumber::new(1)
            )
        )?,
        None
    );
    assert!(store.scans.lock().expect("probe lock").is_empty());
    store.apply(batch)?;
    assert!(store.snapshot()?.entries().is_empty());
    let mut batch = WriteBatch::default();
    assert!(
        !SessionMessageLocks::new(&store, &command, &config, &mut batch)
            .takeover_pending(&hold.session_id)?
    );
    assert_eq!(store.scans.lock().expect("probe lock").len(), 1);
    install(&store, &command, &config, 2, Some(&hold))?;
    let summary_key =
        keys::session_message_lock_summary(&command.namespace, &command.entity, &hold.session_id);
    let mut forged: Summary = codec::decode(&store.get(&summary_key)?.expect("summary"))?;
    forged.owned_count = 1;
    store.apply(WriteBatch::default().put(summary_key, codec::encode(&forged)?))?;
    let mut batch = WriteBatch::default();
    SessionMessageLocks::new(&store, &command, &config, &mut batch)
        .leave_locked(&locked(1, &hold.session_id))?;
    let staged = batch.clone();
    assert_eq!(
        SessionMessageLocks::new(&store, &command, &config, &mut batch)
            .leave_locked(&locked(2, &hold.session_id)),
        Err(BrokerError::MalformedIndexKey)
    );
    assert_eq!(batch, staged);
    // Deliberately commit this private forged-count setup to expose its orphan.
    // Ordinary command preparation discards the entire batch on the later error.
    store.apply(batch)?;
    let mut next = WriteBatch::default();
    assert_eq!(
        SessionMessageLocks::new(&store, &command, &config, &mut next)
            .takeover_pending(&hold.session_id),
        Err(BrokerError::MalformedIndexKey)
    );
    assert!(next.is_empty());
    Ok(())
}

#[test]
fn absent_summary_probes_only_the_first_forward_entry() -> Result<(), BrokerError> {
    let (command, config, hold) = scope();
    let store = ProbeStore::default();
    let prefix = keys::session_message_lock_forward_prefix(
        &command.namespace,
        &command.entity,
        &hold.session_id,
    );
    for count in [0, 2] {
        if count > 0 {
            install(&store, &command, &config, count, Some(&hold))?;
            store.apply(
                WriteBatch::default().delete(keys::session_message_lock_summary(
                    &command.namespace,
                    &command.entity,
                    &hold.session_id,
                )),
            )?;
        }
        let mut batch = WriteBatch::default();
        let result = SessionMessageLocks::new(&store, &command, &config, &mut batch)
            .takeover_pending(&hold.session_id);
        assert_eq!(
            result,
            if count == 0 {
                Ok(false)
            } else {
                Err(BrokerError::MalformedIndexKey)
            }
        );
        assert_eq!(
            store.scans.lock().expect("probe lock").last(),
            Some(&(prefix.clone(), 1))
        );
        assert!(batch.is_empty());
    }
    let store = ProbeStore::default();
    install(&store, &command, &config, 1, Some(&hold))?;
    let mut batch = WriteBatch::default();
    assert!(
        SessionMessageLocks::new(&store, &command, &config, &mut batch)
            .takeover_pending(&hold.session_id)?
    );
    assert!(store.scans.lock().expect("probe lock").is_empty());
    let other = SessionId::new("AA").expect("session");
    assert!(
        !SessionMessageLocks::new(&store, &command, &config, &mut batch)
            .takeover_pending(&other)?
    );
    assert_eq!(store.scans.lock().expect("probe lock").len(), 1);
    Ok(())
}

#[test]
fn held_call_requires_owned_generation_but_never_converts_unowned() -> Result<(), BrokerError> {
    let (command, config, hold) = scope();
    for owner in [None, Some(&hold)] {
        let store = ProbeStore::default();
        install(&store, &command, &config, 1, owner)?;
        let mut batch = WriteBatch::default();
        let record = locked(1, &hold.session_id);
        let wrong = SessionHold::new(hold.session_id.clone(), LockToken::new(4));
        let mut tracker = SessionMessageLocks::new(&store, &command, &config, &mut batch);
        tracker.validate_locked(&record, Some(&hold))?;
        let zero = SessionHold::new(hold.session_id.clone(), LockToken::new(0));
        assert_eq!(
            tracker.validate_locked(&record, Some(&zero)),
            Err(BrokerError::SessionLockNotHeld {
                session_id: hold.session_id.clone()
            })
        );
        assert_eq!(
            tracker.validate_locked(&record, Some(&wrong)),
            if owner.is_some() {
                Err(BrokerError::SessionLockNotHeld {
                    session_id: hold.session_id.clone(),
                })
            } else {
                Ok(())
            }
        );
        let other = SessionHold::new(SessionId::new("B").expect("session"), hold.token);
        assert_eq!(
            tracker.validate_locked(&record, Some(&other)),
            Err(BrokerError::SessionLockNotHeld {
                session_id: other.session_id.clone()
            })
        );
        tracker.renew_locked(&record, Timestamp::from_millis(200), Some(&hold))?;
        let row: Row = tracker
            .read(&tracker.reverse_key(record.sequence))?
            .expect("row");
        assert_eq!(row.owner.generation(), owner.map(|hold| hold.token));
        assert_eq!(
            (row.message_token, row.locked_until),
            (LockToken::new(101), Timestamp::from_millis(200))
        );
        assert_eq!(
            tracker
                .summary(&hold.session_id)?
                .expect("summary")
                .owned_generation,
            owner.map(|hold| hold.token)
        );
        assert!(!batch.is_empty());
    }
    let store = ProbeStore::default();
    let mut batch = WriteBatch::default();
    let zero = SessionHold::new(hold.session_id.clone(), LockToken::new(0));
    assert_eq!(
        SessionMessageLocks::new(&store, &command, &config, &mut batch).install_locked(
            &ready(1, &hold.session_id),
            LockToken::new(101),
            Timestamp::from_millis(100),
            Some(&zero)
        ),
        Err(BrokerError::MalformedIndexKey)
    );
    assert!(batch.is_empty());
    Ok(())
}

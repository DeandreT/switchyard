use storage::MemoryStore;

use super::*;

type ScanCalls = Arc<Mutex<Vec<(Key, Key, usize)>>>;

#[derive(Clone, Default)]
struct ProbeStore {
    inner: MemoryStore,
    scans: ScanCalls,
    gets: Arc<Mutex<Vec<Key>>>,
    failure: Arc<Mutex<Option<StorageError>>>,
}

impl StateStore for ProbeStore {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.gets.lock().expect("probe lock").push(key.to_vec());
        if let Some(error) = self.failure.lock().expect("probe lock").clone() {
            return Err(error);
        }
        self.inner.get(key)
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
            .push((prefix.to_vec(), start.to_vec(), limit));
        if let Some(error) = self.failure.lock().expect("probe lock").clone() {
            return Err(error);
        }
        self.inner.scan_from(prefix, start, limit)
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.inner.apply(batch)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }
}

fn scope() -> (Command, QueueConfig, SessionHold) {
    let command = Command::new(
        NamespaceName::new("test").expect("namespace"),
        EntityPath::new("queue").expect("entity"),
        Timestamp::from_millis(10),
        CommandKind::RetireSessionGenerationPage { after: None },
    );
    let config = QueueConfig {
        requires_session: true,
        ..QueueConfig::default()
    };
    let hold = SessionHold::new(SessionId::new("A").expect("session"), LockToken::new(3));
    (command, config, hold)
}

fn record(sequence: u64, session: &SessionId) -> MessageRecord {
    MessageRecord {
        sequence: SequenceNumber::new(sequence),
        message_id: format!("m{sequence}"),
        body: vec![sequence as u8],
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

fn install(
    store: &ProbeStore,
    command: &Command,
    config: &QueueConfig,
    hold: &SessionHold,
    owners: &[Option<LockToken>],
) -> Result<Vec<MessageRecord>, BrokerError> {
    let mut batch = WriteBatch::default()
        .put(
            keys::queue_config(&command.namespace, &command.entity),
            codec::encode(config)?,
        )
        .put(
            keys::session(&command.namespace, &command.entity, &hold.session_id),
            codec::encode(&SessionRecord {
                lock: None,
                state: vec![0x80, 0],
            })?,
        );
    let mut records = Vec::new();
    for (index, generation) in owners.iter().enumerate() {
        let mut original = record(index as u64 + 1, &hold.session_id);
        let token = LockToken::new(100 + original.sequence.as_u64());
        let deadline = Timestamp::from_millis(100);
        let owner = generation.map(|token| SessionHold::new(hold.session_id.clone(), token));
        SessionMessageLocks::new(store, command, config, &mut batch).install_locked(
            &original,
            token,
            deadline,
            owner.as_ref(),
        )?;
        original.state = MessageState::Locked {
            token,
            locked_until: deadline,
        };
        batch.push_put(
            keys::message(&command.namespace, &command.entity, original.sequence),
            codec::encode(&original)?,
        );
        batch.push_put(
            keys::lock(
                &command.namespace,
                &command.entity,
                deadline,
                original.sequence,
            ),
            Vec::new(),
        );
        records.push(original);
    }
    store.apply(batch)?;
    Ok(records)
}

fn summary_key(command: &Command, hold: &SessionHold) -> Key {
    keys::session_message_lock_summary(&command.namespace, &command.entity, &hold.session_id)
}

#[test]
fn exact_selected_generation_never_scans_unowned_rows() -> Result<(), BrokerError> {
    let (command, config, hold) = scope();
    let store = ProbeStore::default();
    let records = install(
        &store,
        &command,
        &config,
        &hold,
        &[None, Some(hold.token), None],
    )?;
    let forward = keys::session_message_lock_forward(
        &command.namespace,
        &command.entity,
        &hold.session_id,
        Some(hold.token),
        records[1].sequence,
    );
    let original_row = store.get(&forward)?.expect("original row");
    let outcome = StateMachine::new(store.clone()).apply(&command)?;
    assert_eq!(
        outcome,
        CommandOutcome::SessionRetired(SessionRetirementOutcome {
            returned_to_ready: 1,
            dead_lettered: 0,
            dropped: 0,
            page: SessionRetirementPage::End,
        })
    );
    let owned_prefix = keys::session_message_lock_generation_prefix(
        &command.namespace,
        &command.entity,
        &hold.session_id,
        hold.token,
    );
    let summary_prefix =
        keys::session_message_lock_summary_prefix(&command.namespace, &command.entity);
    let scans = store.scans.lock().expect("probe lock");
    assert_eq!(
        scans
            .iter()
            .filter(|(prefix, _, _)| prefix == &owned_prefix)
            .count(),
        2
    );
    assert!(
        scans.iter().all(|(prefix, _, limit)| *limit == 1
            && (prefix == &owned_prefix || prefix == &summary_prefix))
    );
    drop(scans);
    for index in [0, 2] {
        let key = keys::message(&command.namespace, &command.entity, records[index].sequence);
        assert_eq!(store.get(&key)?, Some(codec::encode(&records[index])?));
        assert!(
            store
                .get(&keys::session_message_lock_reverse(
                    &command.namespace,
                    &command.entity,
                    records[index].sequence,
                ))?
                .is_some()
        );
    }
    let owned = &records[1];
    assert!(
        store
            .get(&keys::session_message_lock_reverse(
                &command.namespace,
                &command.entity,
                owned.sequence,
            ))?
            .is_none()
    );
    assert!(store.get(&summary_key(&command, &hold))?.is_some());
    let mut batch = WriteBatch::default();
    let wrong = keys::session_message_lock_exclusive_start(&forward);
    assert_eq!(
        SessionMessageLocks::new(&store, &command, &config, &mut batch)
            .retirement_entry(&hold.session_id, hold.token, (&wrong, &original_row))
            .map(|_| ()),
        Err(BrokerError::MalformedIndexKey)
    );
    assert!(batch.is_empty());
    Ok(())
}

#[test]
fn staged_owned_counts_and_terminal_tail_lookahead_are_atomic() -> Result<(), BrokerError> {
    let (command, config, hold) = scope();
    let store = ProbeStore::default();
    let records = install(
        &store,
        &command,
        &config,
        &hold,
        &[Some(hold.token), Some(hold.token)],
    )?;
    let mut batch = WriteBatch::default();
    for (index, record) in records.iter().enumerate() {
        let key = keys::session_message_lock_forward(
            &command.namespace,
            &command.entity,
            &hold.session_id,
            Some(hold.token),
            record.sequence,
        );
        let raw = store.get(&key)?.expect("forward");
        let (entry, loaded) = SessionMessageLocks::new(&store, &command, &config, &mut batch)
            .retirement_entry(&hold.session_id, hold.token, (&key, &raw))?;
        assert_eq!(entry.owned_count, 2 - index as u64);
        assert_eq!(loaded, *record);
        SessionMessageLocks::new(&store, &command, &config, &mut batch).leave_locked(&loaded)?;
    }
    assert_eq!(store.scans.lock().expect("probe lock").len(), 1);
    assert!(batch.mutations().iter().any(|mutation| matches!(mutation,
        Mutation::Delete { key } if key == &summary_key(&command, &hold))));
    assert!(store.get(&summary_key(&command, &hold))?.is_some());

    // The test-only tuple has the private summary's unchanged positional wire shape.
    let summary = (
        command.namespace.clone(),
        command.entity.clone(),
        hold.session_id.clone(),
        Some(hold.token),
        1_u64,
        0_u64,
    );
    store
        .apply(WriteBatch::default().put(summary_key(&command, &hold), codec::encode(&summary)?))?;
    let before = store.snapshot()?;
    let machine = StateMachine::new(store.clone());
    assert_eq!(machine.apply(&command), Err(BrokerError::MalformedIndexKey));
    assert_eq!(store.snapshot()?, before);
    assert_eq!(machine.last_applied_time()?, Timestamp::UNIX_EPOCH);

    let store = ProbeStore::default();
    let records = install(
        &store,
        &command,
        &config,
        &hold,
        &[Some(hold.token), Some(hold.token)],
    )?;
    let last = keys::session_message_lock_forward(
        &command.namespace,
        &command.entity,
        &hold.session_id,
        Some(hold.token),
        records[1].sequence,
    );
    store.apply(WriteBatch::default().put(
        keys::session_message_lock_exclusive_start(&last),
        b"bad".to_vec(),
    ))?;
    let before = store.snapshot()?;
    assert_eq!(
        StateMachine::new(store.clone()).apply(&command),
        Err(BrokerError::MalformedIndexKey)
    );
    assert_eq!(store.snapshot()?, before);
    Ok(())
}

#[test]
fn exclusive_starts_include_malformed_descendants_at_max_sequence() -> Result<(), BrokerError> {
    let (command, _, hold) = scope();
    let store = ProbeStore::default();
    for key in [
        keys::session_message_lock_forward(
            &command.namespace,
            &command.entity,
            &hold.session_id,
            Some(hold.token),
            SequenceNumber::new(u64::MAX),
        ),
        summary_key(&command, &hold),
    ] {
        let start = keys::session_message_lock_exclusive_start(&key);
        assert_eq!(&start[..key.len()], key.as_slice());
        assert_eq!(start.last(), Some(&0));
        let descendant = [key.as_slice(), &[0, 0]].concat();
        store.apply(
            WriteBatch::default()
                .put(key.clone(), Vec::new())
                .put(descendant.clone(), vec![1]),
        )?;
        assert_eq!(
            store.scan_from(&key, &start, 1)?,
            vec![(descendant, vec![1])]
        );
    }
    Ok(())
}

#[test]
fn read_only_budget_store_counts_original_reads_and_denies_writes() -> Result<(), BrokerError> {
    let store = ProbeStore::default();
    store.apply(WriteBatch::default().put(b"a".to_vec(), vec![1, 2, 3]))?;
    let reader = RetirementReadStore::with_maxima(store.clone(), [1, 1, 3]);
    assert_eq!(reader.clone().get(b"a")?, Some(vec![1, 2, 3]));
    assert_eq!(reader.get(b"a"), Err(budget_error()));
    assert_eq!(store.gets.lock().expect("probe lock").len(), 1);
    assert_eq!(
        reader.map_error(BrokerError::Storage(budget_error())),
        SessionRetirementLimit::ReadOperations.exceeded(1)
    );
    for (maxima, limit) in [
        ([2, 0, 3], SessionRetirementLimit::ReadKeyBytes),
        ([2, 1, 2], SessionRetirementLimit::ReadValueBytes),
    ] {
        let reader = RetirementReadStore::with_maxima(store.clone(), maxima);
        assert_eq!(reader.get(b"a"), Err(budget_error()));
        let index = if limit == SessionRetirementLimit::ReadKeyBytes {
            1
        } else {
            2
        };
        assert_eq!(
            reader.map_error(BrokerError::Storage(budget_error())),
            limit.exceeded(maxima[index])
        );
    }
    let reader = RetirementReadStore::with_maxima(store.clone(), [1, 3, 3]);
    assert_eq!(
        reader.scan_from(b"a", b"a", 1)?,
        vec![(b"a".to_vec(), vec![1, 2, 3])]
    );
    assert_eq!(reader.lock()?.used, [1, 3, 3]);
    for maxima in [[1, 2, 3], [1, 3, 2]] {
        assert_eq!(
            RetirementReadStore::with_maxima(store.clone(), maxima).scan_from(b"a", b"a", 1),
            Err(budget_error())
        );
    }
    assert_eq!(reader.apply(WriteBatch::default()), Err(forbidden_error()));
    assert_eq!(reader.snapshot(), Err(forbidden_error()));
    assert_eq!(reader.scan_from(b"a", b"a", 2), Err(forbidden_error()));
    let original = StorageError::Backend {
        operation: "original read",
        detail: String::from("same cause"),
    };
    *store.failure.lock().expect("probe lock") = Some(original.clone());
    let reader = RetirementReadStore::new(store);
    assert_eq!(reader.get(b"a"), Err(original.clone()));
    assert_eq!(
        reader.map_error(BrokerError::Storage(original.clone())),
        BrokerError::Storage(original)
    );
    let mut overflow = ReadBudget {
        used: [usize::MAX, 0, 0],
        maxima: [usize::MAX; 3],
        failure: None,
    };
    assert_eq!(overflow.charge(0, 1), Err(budget_error()));
    assert_eq!(
        overflow.failure,
        Some((SessionRetirementLimit::ReadOperations, usize::MAX))
    );
    Ok(())
}

#[test]
fn mutation_budget_counts_repeated_entries_and_exact_clock_reserve() -> Result<(), BrokerError> {
    let (command, _, _) = scope();
    let batch = WriteBatch::default()
        .put(b"a".to_vec(), vec![1, 2])
        .put(b"a".to_vec(), vec![3])
        .delete(b"a".to_vec());
    let mut budget = MutationBudget {
        used: [0; 3],
        maxima: [3, 3, 3],
        charged: 0,
    };
    budget.charge_new(&batch)?;
    assert_eq!(budget.used, [3, 3, 3]);
    budget.charge_new(&batch)?;
    assert_eq!(budget.used, [3, 3, 3]);
    for (maxima, limit, maximum) in [
        ([2, 3, 3], SessionRetirementLimit::MutationEntries, 2),
        ([3, 2, 3], SessionRetirementLimit::MutationKeyBytes, 2),
        ([3, 3, 2], SessionRetirementLimit::MutationValueBytes, 2),
    ] {
        let mut budget = MutationBudget {
            used: [0; 3],
            maxima,
            charged: 0,
        };
        assert_eq!(budget.charge_new(&batch), Err(limit.exceeded(maximum)));
    }
    let clock_bytes = codec::encode(&command.issued_at)?.len();
    let mut budget = MutationBudget {
        used: [0; 3],
        maxima: [4, 3 + keys::clock().len(), 3 + clock_bytes],
        charged: 0,
    };
    budget.charge_new(&batch)?;
    budget.check_clock(&command, &batch)?;
    assert_eq!(budget.used, [3, 3, 3]);
    budget.reserve_clock(&command, &batch)?;
    assert_eq!(budget.used, budget.maxima);
    for (maxima, limit, maximum) in [
        (
            [3, 3 + keys::clock().len(), 3 + clock_bytes],
            SessionRetirementLimit::MutationEntries,
            3,
        ),
        (
            [4, 2 + keys::clock().len(), 3 + clock_bytes],
            SessionRetirementLimit::MutationKeyBytes,
            2 + keys::clock().len(),
        ),
        (
            [4, 3 + keys::clock().len(), 2 + clock_bytes],
            SessionRetirementLimit::MutationValueBytes,
            2 + clock_bytes,
        ),
    ] {
        let mut budget = MutationBudget {
            used: [0; 3],
            maxima,
            charged: 0,
        };
        budget.charge_new(&batch)?;
        assert_eq!(
            budget.check_clock(&command, &batch),
            Err(limit.exceeded(maximum))
        );
        assert_eq!(budget.used, [3, 3, 3]);
    }
    let mut empty = MutationBudget {
        used: [0; 3],
        maxima: [0; 3],
        charged: 0,
    };
    empty.reserve_clock(&command, &WriteBatch::default())?;
    assert_eq!(empty.used, [0; 3]);
    let mut overflow = MutationBudget {
        used: [usize::MAX, 0, 0],
        maxima: [usize::MAX; 3],
        charged: 0,
    };
    assert_eq!(
        overflow.charge(1, 0, 0),
        Err(SessionRetirementLimit::MutationEntries.exceeded(usize::MAX))
    );
    Ok(())
}

#[test]
fn private_owner_liveness_uses_ordinary_session_codec() -> Result<(), BrokerError> {
    let (command, config, hold) = scope();
    for version in 1..=codec::VALUE_FORMAT_V11 {
        let store = ProbeStore::default();
        let records = install(&store, &command, &config, &hold, &[Some(hold.token)])?;
        let key = keys::session(&command.namespace, &command.entity, &hold.session_id);
        let mut raw = codec::encode(&SessionRecord {
            lock: Some(SessionLock {
                token: hold.token,
                locked_until: Timestamp::from_millis(20),
            }),
            state: vec![0x80, 0],
        })?;
        raw[0] = version;
        store.apply(WriteBatch::default().put(key, raw))?;
        let mut batch = WriteBatch::default();
        let mut tracker = SessionMessageLocks::new(&store, &command, &config, &mut batch);
        assert!(
            !tracker
                .retirement_group(&hold.session_id, None)?
                .expect("group")
                .eligible
        );
        tracker.renew_locked(&records[0], Timestamp::from_millis(200), None)?;
        assert!(!batch.is_empty());
    }
    for lock in [
        None,
        Some(SessionLock {
            token: LockToken::new(0),
            locked_until: Timestamp::from_millis(20),
        }),
        Some(SessionLock {
            token: LockToken::new(4),
            locked_until: Timestamp::from_millis(20),
        }),
        Some(SessionLock {
            token: hold.token,
            locked_until: command.issued_at,
        }),
    ] {
        let store = ProbeStore::default();
        let records = install(&store, &command, &config, &hold, &[Some(hold.token)])?;
        store.apply(WriteBatch::default().put(
            keys::session(&command.namespace, &command.entity, &hold.session_id),
            codec::encode(&SessionRecord {
                lock,
                state: vec![0x80, 0],
            })?,
        ))?;
        let mut batch = WriteBatch::default();
        let mut tracker = SessionMessageLocks::new(&store, &command, &config, &mut batch);
        assert_eq!(
            tracker.renew_locked(&records[0], Timestamp::from_millis(200), None),
            if lock.as_ref().is_some_and(|lock| lock.token == hold.token) {
                Err(BrokerError::SessionLockExpired {
                    session_id: hold.session_id.clone(),
                    locked_until: command.issued_at,
                })
            } else {
                Err(BrokerError::SessionLockNotHeld {
                    session_id: hold.session_id.clone(),
                })
            }
        );
        let group = tracker.retirement_group(&hold.session_id, None);
        if lock.as_ref().is_some_and(|lock| lock.token != hold.token) {
            assert_eq!(group.map(|_| ()), Err(BrokerError::MalformedIndexKey));
        } else {
            assert!(group?.expect("group").eligible);
        }
        assert!(batch.is_empty());
    }
    let store = ProbeStore::default();
    let records = install(&store, &command, &config, &hold, &[Some(hold.token)])?;
    store.apply(WriteBatch::default().delete(keys::session(
        &command.namespace,
        &command.entity,
        &hold.session_id,
    )))?;
    let mut batch = WriteBatch::default();
    let mut tracker = SessionMessageLocks::new(&store, &command, &config, &mut batch);
    assert_eq!(
        tracker.renew_locked(&records[0], Timestamp::from_millis(200), None),
        Err(BrokerError::SessionLockNotHeld {
            session_id: hold.session_id.clone()
        })
    );
    assert_eq!(
        tracker.retirement_group(&hold.session_id, None).map(|_| ()),
        Err(BrokerError::MalformedIndexKey)
    );
    assert!(batch.is_empty());
    for version in [10, 12] {
        let store = ProbeStore::default();
        install(&store, &command, &config, &hold, &[Some(hold.token)])?;
        let key = summary_key(&command, &hold);
        let mut raw = store.get(&key)?.expect("summary");
        raw[0] = version;
        store.apply(WriteBatch::default().put(key, raw))?;
        let mut batch = WriteBatch::default();
        assert_eq!(
            SessionMessageLocks::new(&store, &command, &config, &mut batch)
                .retirement_group(&hold.session_id, None)
                .map(|_| ()),
            Err(BrokerError::Codec(crate::CodecError::UnsupportedVersion {
                version
            }))
        );
        assert!(batch.is_empty());
    }
    Ok(())
}

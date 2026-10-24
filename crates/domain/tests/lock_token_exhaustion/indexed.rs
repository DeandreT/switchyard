use super::*;
use domain::{
    DurableProposal, IndexedApplyError, IndexedApplyOutcome, IndexedWriter, NamespaceName,
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use storage::{StorageError, StoreSnapshot};

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
                operation: "latest duplicate read",
                detail: "token exhaustion read-free witness".to_owned(),
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

pub(super) fn indexed<P: StoreProvider>(provider: P) -> TestResult {
    let store = provider.open()?;
    let probe = Arc::new(ReadProbe::default());
    let mut writer = IndexedWriter::open(Observed {
        inner: store.clone(),
        probe: probe.clone(),
    })?;
    let namespace = NamespaceName::new("t")?;
    let entity = EntityPath::new("indexed-tokens")?;
    let session_entity = EntityPath::new("indexed-sessions")?;
    let command = |at, target: &EntityPath, kind| {
        Command::new(
            namespace.clone(),
            target.clone(),
            Timestamp::from_millis(at),
            kind,
        )
    };
    let kinds = [
        (
            1,
            10,
            &entity,
            CommandKind::CreateQueue {
                config: config(false),
            },
        ),
        (
            2,
            11,
            &entity,
            CommandKind::SendBatch {
                messages: vec![
                    MessageInput {
                        message_id: "deferred".to_owned(),
                        ..MessageInput::default()
                    },
                    MessageInput {
                        message_id: "ready".to_owned(),
                        ..MessageInput::default()
                    },
                ],
            },
        ),
        (3, 12, &entity, take(ReceiveMode::PeekLock, None)),
    ];
    let mut original = None;
    for (index, at, target, kind) in kinds {
        let IndexedApplyOutcome::Applied(outcome) =
            writer.apply(index, &DurableProposal::unbound(command(at, target, kind)))?
        else {
            panic!("expected clean bootstrap effect")
        };
        if index == 3 {
            original = Some(delivery(outcome));
        }
    }
    let original = original.unwrap();
    assert_eq!(
        writer.apply(
            4,
            &DurableProposal::unbound(command(
                13,
                &entity,
                CommandKind::Defer {
                    sequence: original.sequence,
                    lock_token: original.lock.unwrap().token,
                    replacement_envelope: None
                }
            ))
        )?,
        IndexedApplyOutcome::Applied(CommandOutcome::Deferred)
    );
    assert_eq!(
        writer.apply(
            5,
            &DurableProposal::unbound(command(
                14,
                &session_entity,
                CommandKind::CreateQueue {
                    config: config(true)
                }
            ))
        )?,
        IndexedApplyOutcome::Applied(CommandOutcome::QueueCreated)
    );
    assert!(matches!(
        writer.apply(
            6,
            &DurableProposal::unbound(command(
                15,
                &session_entity,
                send_kind("session", None, Some(id()))
            ))
        )?,
        IndexedApplyOutcome::Applied(CommandOutcome::Sent { .. })
    ));
    let IndexedApplyOutcome::Applied(outcome) = writer.apply(
        7,
        &DurableProposal::unbound(command(16, &session_entity, accept(Some(id())))),
    )?
    else {
        panic!("expected session bootstrap hold")
    };
    let held = accepted(outcome);
    let machine = StateMachine::new(store.clone());
    let binding = machine.bind_entity(&namespace, &entity)?;
    let session_binding = machine.bind_entity(&namespace, &session_entity)?;
    drop(machine);
    // Bootstrap finishes before raw seeding. ALL raw/indexed writers are serial.
    for target in [&entity, &session_entity] {
        let key = keys::queue_counters(&namespace, target);
        let mut value: QueueCounters = codec::decode(&store.get(&key)?.unwrap())?;
        value.next_lock_token = u64::MAX;
        store.apply(WriteBatch::default().put(key, codec::encode(&value)?))?;
    }
    let before = store.snapshot()?;
    let original_metadata: Vec<_> = before
        .entries()
        .iter()
        .filter(|(key, _)| key.first() == Some(&0xF1))
        .cloned()
        .collect();
    assert_eq!(original_metadata.len(), 2);
    let mut latest = None;
    for (index, target, retained, kind) in [
        (8, &entity, &binding, take(ReceiveMode::PeekLock, None)),
        (
            9,
            &entity,
            &binding,
            retrieve(vec![original.sequence], ReceiveMode::PeekLock, None),
        ),
        (
            10,
            &session_entity,
            &session_binding,
            accept(Some(SessionId::new("empty")?)),
        ),
    ] {
        let proposal = DurableProposal::bound(BoundCommand::new(
            (*retained).clone(),
            command(20, target, kind),
        ))?;
        let result = writer.apply(index, &proposal);
        assert!(
            matches!(&result, Ok(IndexedApplyOutcome::Refused(_))),
            "expected checkpointed token refusal, got {result:?}"
        );
        let IndexedApplyOutcome::Refused(error) = result? else {
            unreachable!()
        };
        assert_eq!(format!("{error:?}"), "LockTokenExhausted");
        // Later checkpoint/byte/read-free/reopen witnesses are unreached on RED.
        assert_eq!(writer.applied_index()?, index);
        let after = store.snapshot()?;
        assert_eq!(domain_rows(&after), domain_rows(&before));
        let metadata: Vec<_> = after
            .entries()
            .iter()
            .filter(|(key, _)| key.first() == Some(&0xF1))
            .cloned()
            .collect();
        assert_eq!(metadata.len(), 2);
        assert_eq!(metadata[0], original_metadata[0], "owner stays unchanged");
        assert_ne!(metadata[1], original_metadata[1], "checkpoint advances");
        assert_ne!(
            after, before,
            "F1 advances; this is not whole-store conservation"
        );
        let applies = probe.applies.load(Ordering::SeqCst);
        probe.deny.store(true, Ordering::SeqCst);
        assert_eq!(
            writer.apply(index, &proposal)?,
            IndexedApplyOutcome::AlreadyApplied
        );
        assert_eq!(
            writer.apply(
                index,
                &DurableProposal::unbound(command(
                    21,
                    target,
                    take(ReceiveMode::ReceiveAndDelete, None)
                ))
            ),
            Err(IndexedApplyError::ConflictingProposal { index })
        );
        assert_eq!(probe.applies.load(Ordering::SeqCst), applies);
        probe.deny.store(false, Ordering::SeqCst);
        assert_eq!(store.snapshot()?, after);
        latest = Some(proposal);
    }
    let after = store.snapshot()?;
    // Both clones drop before Fjall directory reopen; probe owns no store.
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
        writer.apply(10, &latest.unwrap())?,
        IndexedApplyOutcome::AlreadyApplied
    );
    probe.deny.store(false, Ordering::SeqCst);
    let key = keys::message(&namespace, &entity, SequenceNumber::new(2));
    let original_raw = store.get(&key)?.unwrap();
    let bad = vec![0];
    let error = MessageRecord::decode(&bad).unwrap_err();
    store.apply(WriteBatch::default().put(key.clone(), bad))?;
    let corrupt = store.snapshot()?;
    assert_eq!(
        writer.apply(
            11,
            &DurableProposal::unbound(command(21, &entity, take(ReceiveMode::PeekLock, None)))
        ),
        Err(IndexedApplyError::Domain(BrokerError::Codec(error)))
    );
    assert_eq!(writer.applied_index()?, 10);
    assert_eq!(store.snapshot()?, corrupt);
    store.apply(WriteBatch::default().put(key, original_raw))?;
    assert_eq!(store.snapshot()?, after);
    let IndexedApplyOutcome::Applied(outcome) = writer.apply(
        11,
        &DurableProposal::unbound(command(
            22,
            &entity,
            take(ReceiveMode::ReceiveAndDelete, None),
        )),
    )?
    else {
        panic!("expected no-token cleanup")
    };
    assert_eq!(delivery(outcome).sequence, SequenceNumber::new(2));
    assert!(matches!(
        writer.apply(
            12,
            &DurableProposal::unbound(command(
                23,
                &entity,
                retrieve(vec![original.sequence], ReceiveMode::ReceiveAndDelete, None)
            ))
        )?,
        IndexedApplyOutcome::Applied(CommandOutcome::DeferredReceived(_))
    ));
    assert_eq!(
        writer.apply(
            13,
            &DurableProposal::unbound(command(
                24,
                &session_entity,
                CommandKind::ReleaseSession {
                    session: held.hold()
                }
            ))
        )?,
        IndexedApplyOutcome::Applied(CommandOutcome::SessionReleased)
    );
    let final_image = store.snapshot()?;
    drop(writer);
    drop(store);
    let store = provider.open()?;
    assert_eq!(store.snapshot()?, final_image);
    drop(store);
    Ok(())
}

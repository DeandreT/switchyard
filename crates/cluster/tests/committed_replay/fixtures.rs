use super::*;

pub(super) type TestResult = Result<(), Box<dyn Error>>;
pub(super) const F0: &[u8] = b"\xF0switchyard/journal\0";
pub(super) const F1: &[u8] = b"\xF1switchyard/replay\0";

#[derive(Clone, Debug)]
pub(super) enum Backend {
    Memory(MemoryStore),
    Fjall(FjallStore),
}

impl StateStore for Backend {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        match self {
            Self::Memory(s) => s.get(key),
            Self::Fjall(s) => s.get(key),
        }
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        match self {
            Self::Memory(s) => s.apply(batch),
            Self::Fjall(s) => s.apply(batch),
        }
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        match self {
            Self::Memory(s) => s.snapshot(),
            Self::Fjall(s) => s.snapshot(),
        }
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        match self {
            Self::Memory(s) => s.scan_from(prefix, start, limit),
            Self::Fjall(s) => s.scan_from(prefix, start, limit),
        }
    }
    fn scan_prefix(&self, prefix: &[u8], limit: usize) -> Result<Vec<(Key, Value)>, StorageError> {
        match self {
            Self::Memory(s) => s.scan_prefix(prefix, limit),
            Self::Fjall(s) => s.scan_prefix(prefix, limit),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct Trace {
    pub(super) gets: Vec<Key>,
    pub(super) scans: Vec<(Key, Key, usize)>,
    pub(super) prefixes: Vec<(Key, usize)>,
    snapshots: usize,
    pub(super) batches: Vec<WriteBatch>,
}

#[derive(Debug)]
pub(super) struct Control {
    trace: Mutex<Trace>,
    pub(super) clone_calls: AtomicUsize,
    pub(super) apply_fault: AtomicUsize,
    pub(super) read_fault: AtomicUsize,
    pub(super) prefix_fault: AtomicBool,
    pub(super) payload: Arc<str>,
}

#[derive(Debug)]
pub(super) struct Observed {
    inner: Backend,
    control: Arc<Control>,
}

impl Clone for Observed {
    fn clone(&self) -> Self {
        self.control.clone_calls.fetch_add(1, Ordering::SeqCst);
        panic!("the replay owner must move this original store, never clone it")
    }
}

pub(super) fn injected_error() -> StorageError {
    StorageError::Backend {
        operation: "controlled replay boundary",
        detail: "original logical error".into(),
    }
}

impl Observed {
    fn read_boundary(&self, domain: bool, scan: bool) -> Result<(), StorageError> {
        let armed = self.control.read_fault.load(Ordering::SeqCst);
        if (scan && matches!(armed, 1 | 2)) || (domain && matches!(armed, 3 | 4)) {
            assert_eq!(self.control.read_fault.swap(0, Ordering::SeqCst), armed);
            if matches!(armed, 2 | 4) {
                panic_any(Arc::clone(&self.control.payload));
            }
            return Err(injected_error());
        }
        Ok(())
    }
}

impl StateStore for Observed {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.control.trace.lock().unwrap().gets.push(key.to_vec());
        self.read_boundary(key.first().is_some_and(|tag| *tag <= 0x11), false)?;
        self.inner.get(key)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.control
            .trace
            .lock()
            .unwrap()
            .batches
            .push(batch.clone());
        let fault = self.control.apply_fault.swap(0, Ordering::SeqCst);
        if fault == 1 {
            return Err(injected_error());
        }
        if fault == 3 {
            panic_any(Arc::clone(&self.control.payload));
        }
        self.inner.apply(batch)?;
        if fault == 2 {
            return Err(injected_error());
        }
        if fault == 4 {
            panic_any(Arc::clone(&self.control.payload));
        }
        Ok(())
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.control.trace.lock().unwrap().snapshots += 1;
        self.inner.snapshot()
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.control
            .trace
            .lock()
            .unwrap()
            .scans
            .push((prefix.to_vec(), start.to_vec(), limit));
        self.read_boundary(prefix.first().is_some_and(|tag| *tag <= 0x11), true)?;
        let mut rows = self.inner.scan_from(prefix, start, limit)?;
        let fault = self.control.read_fault.load(Ordering::SeqCst);
        if matches!(fault, 5..=7) {
            assert_eq!(self.control.read_fault.swap(0, Ordering::SeqCst), fault);
            // Deliberate backend-response violations after open, not live raw writes.
            if fault == 5 {
                rows.clear();
            } else if let Some((key, value)) = rows.first_mut() {
                let index = u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap());
                if fault == 6 {
                    *key = entry_key(index + 1);
                    *value = entry_value(index + 1, &noop(10).encode().unwrap());
                } else {
                    *value = entry_value(index, &[0xAA]);
                }
            }
        }
        Ok(rows)
    }
    fn scan_prefix(&self, prefix: &[u8], limit: usize) -> Result<Vec<(Key, Value)>, StorageError> {
        self.control
            .trace
            .lock()
            .unwrap()
            .prefixes
            .push((prefix.to_vec(), limit));
        if prefix == [0xF1] && self.control.prefix_fault.load(Ordering::SeqCst) {
            return Err(injected_error());
        }
        self.read_boundary(prefix.first().is_some_and(|tag| *tag <= 0x11), false)?;
        self.inner.scan_prefix(prefix, limit)
    }
}

pub(super) struct Fixture {
    backend: Option<Backend>,
    directory: Option<TempDir>,
    pub(super) control: Arc<Control>,
}

impl Fixture {
    pub(super) fn new(durable: bool) -> Result<Self, Box<dyn Error>> {
        let directory = if durable { Some(TempDir::new()?) } else { None };
        let backend = match &directory {
            Some(directory) => Backend::Fjall(FjallStore::open(directory.path())?),
            None => Backend::Memory(MemoryStore::default()),
        };
        Ok(Self {
            backend: Some(backend),
            directory,
            control: Arc::new(Control {
                trace: Mutex::new(Trace::default()),
                clone_calls: AtomicUsize::new(0),
                apply_fault: AtomicUsize::new(0),
                read_fault: AtomicUsize::new(0),
                prefix_fault: AtomicBool::new(false),
                payload: Arc::from("original raw replay unwind"),
            }),
        })
    }
    pub(super) fn store(&self) -> &Backend {
        self.backend.as_ref().unwrap()
    }
    pub(super) fn observed(&self) -> Observed {
        Observed {
            inner: self.store().clone(),
            control: Arc::clone(&self.control),
        }
    }
    pub(super) fn open(&self) -> Result<CommittedReplay<Observed>, ReplayError> {
        CommittedReplay::open(self.observed())
    }
    pub(super) fn snapshot(&self) -> Vec<(Key, Value)> {
        self.store().snapshot().unwrap().entries().to_vec()
    }
    pub(super) fn raw(&self, batch: WriteBatch) {
        self.store().apply(batch).unwrap();
    }
    pub(super) fn reset(&self) {
        *self.control.trace.lock().unwrap() = Trace::default();
    }
    pub(super) fn trace(&self) -> Trace {
        self.control.trace.lock().unwrap().clone()
    }
    pub(super) fn log(&self, payloads: &[Vec<u8>], committed: u64) {
        let mut journal = Journal::open(self.store().clone()).unwrap();
        for (offset, payload) in payloads.iter().enumerate() {
            journal.append(offset as u64 + 1, payload).unwrap();
        }
        journal.commit(committed).unwrap();
    }
    pub(super) fn proposals(&self, proposals: &[DurableProposal], committed: u64) {
        self.log(
            &proposals
                .iter()
                .map(|p| p.encode().unwrap())
                .collect::<Vec<_>>(),
            committed,
        );
    }
    pub(super) fn seed(&self, proposals: &[DurableProposal]) {
        let mut writer = IndexedWriter::open(self.store().clone()).unwrap();
        for (offset, proposal) in proposals.iter().enumerate() {
            writer.apply(offset as u64 + 1, proposal).unwrap();
        }
    }
    pub(super) fn reopen(&mut self) -> TestResult {
        // Callers must first drop every owner, wrapper and original backend clone.
        let old = self.backend.take().unwrap();
        self.backend = Some(match old {
            Backend::Memory(store) => Backend::Memory(store),
            Backend::Fjall(store) => {
                drop(store);
                Backend::Fjall(FjallStore::open(self.directory.as_ref().unwrap().path())?)
            }
        });
        self.reset();
        Ok(())
    }
}

pub(super) fn ns() -> NamespaceName {
    NamespaceName::new("tenant").unwrap()
}
pub(super) fn queue() -> EntityPath {
    EntityPath::new("orders").unwrap()
}
pub(super) fn topic() -> EntityPath {
    EntityPath::new("events").unwrap()
}
pub(super) fn subscription() -> EntityPath {
    topic()
        .subscription(&SubscriptionName::new("worker").unwrap())
        .unwrap()
}
pub(super) fn proposal(entity: &EntityPath, at: u64, kind: CommandKind) -> DurableProposal {
    DurableProposal::unbound(Command::new(
        ns(),
        entity.clone(),
        Timestamp::from_millis(at),
        kind,
    ))
}
pub(super) fn create(at: u64) -> DurableProposal {
    proposal(
        &queue(),
        at,
        CommandKind::CreateQueue {
            config: QueueConfig::default(),
        },
    )
}
pub(super) fn noop(at: u64) -> DurableProposal {
    proposal(&queue(), at, CommandKind::ExpireSessionLocks)
}
pub(super) fn receive(at: u64) -> DurableProposal {
    proposal(
        &queue(),
        at,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: Some(100),
            session: None,
        },
    )
}
pub(super) fn send(at: u64) -> DurableProposal {
    proposal(
        &queue(),
        at,
        CommandKind::Send {
            message_id: "original".into(),
            body: b"original bytes".to_vec(),
            time_to_live_millis: None,
            session_id: None,
            scheduled_enqueue_at: None,
            envelope: None,
        },
    )
}
pub(super) fn bound(fixture: &Fixture, original: DurableProposal) -> DurableProposal {
    let binding = StateMachine::new(fixture.store().clone())
        .bind_entity(&ns(), &original.command().entity)
        .unwrap();
    DurableProposal::bound(BoundCommand::new(binding, original.command().clone())).unwrap()
}
pub(super) fn key(prefix: &[u8], tag: u8) -> Key {
    let mut result = prefix.to_vec();
    result.push(tag);
    result
}
pub(super) fn entry_key(index: u64) -> Key {
    let mut result = key(F0, 2);
    result.extend_from_slice(&index.to_be_bytes());
    result
}
fn hash(scope: &[u8], bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(scope);
    hash.update(bytes);
    hash.finalize().into()
}
pub(super) fn checkpoint(index: u64, original: &DurableProposal) -> Value {
    let mut value = index.to_be_bytes().to_vec();
    value.extend_from_slice(&hash(
        b"switchyard indexed proposal v1\0",
        &original.encode().unwrap(),
    ));
    value.extend_from_slice(&hash(b"switchyard indexed checkpoint v1\0", &value));
    value
}
pub(super) fn owner_record(version: u32) -> Value {
    let mut value = b"SWIA".to_vec();
    value.extend_from_slice(&version.to_be_bytes());
    value.extend_from_slice(&hash(b"switchyard indexed owner v1\0", &value));
    value
}
pub(super) fn entry_value(index: u64, payload: &[u8]) -> Value {
    let mut value = 1_u32.to_be_bytes().to_vec();
    value.extend_from_slice(&index.to_be_bytes());
    value.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
    let mut digest = Sha256::new();
    digest.update(b"switchyard journal entry v1\0");
    digest.update(&value);
    digest.update(payload);
    value.extend_from_slice(&digest.finalize());
    value.extend_from_slice(payload);
    value
}
pub(super) fn apply_rows(before: &[(Key, Value)], batch: &WriteBatch) -> Vec<(Key, Value)> {
    let mut rows: BTreeMap<_, _> = before.iter().cloned().collect();
    for mutation in batch.mutations() {
        match mutation {
            Mutation::Put { key, value } => {
                rows.insert(key.clone(), value.clone());
            }
            Mutation::Delete { key } => {
                rows.remove(key);
            }
        }
    }
    rows.into_iter().collect()
}
pub(super) fn reserved(rows: &[(Key, Value)], prefix: &[u8]) -> Vec<(Key, Value)> {
    rows.iter()
        .filter(|(key, _)| key.starts_with(prefix))
        .cloned()
        .collect()
}
pub(super) fn no_io(fixture: &Fixture) {
    assert_eq!(fixture.trace(), Trace::default());
    assert_eq!(fixture.control.clone_calls.load(Ordering::SeqCst), 0);
}
pub(super) fn frozen_f0(fixture: &Fixture, before: &[(Key, Value)]) {
    assert_eq!(reserved(&fixture.snapshot(), F0), reserved(before, F0));
    for batch in fixture.trace().batches {
        assert!(batch.mutations().iter().all(|m| match m {
            Mutation::Put { key, .. } | Mutation::Delete { key } => !key.starts_with(F0),
        }));
    }
}
pub(super) fn unusable(owner: &mut CommittedReplay<Observed>, fixture: &Fixture) {
    fixture.reset();
    assert_eq!(owner.applied_index(), Err(ReplayError::Unusable));
    assert_eq!(owner.committed_index(), Err(ReplayError::Unusable));
    assert_eq!(owner.last_appended_index(), Err(ReplayError::Unusable));
    for limit in [
        0,
        1,
        MAX_JOURNAL_READ_ENTRIES,
        MAX_JOURNAL_READ_ENTRIES + 1,
        usize::MAX,
    ] {
        assert_eq!(owner.replay_batch(limit), Err(ReplayError::Unusable));
    }
    no_io(fixture);
}
pub(super) fn progress(applied: u64, committed: u64, processed: usize) -> ReplayProgress {
    ReplayProgress {
        applied_index: applied,
        committed_index: committed,
        processed,
    }
}
pub(super) fn caught_up(
    owner: &mut CommittedReplay<Observed>,
    fixture: &Fixture,
    at: u64,
    last: u64,
) {
    fixture.reset();
    assert_eq!(owner.applied_index(), Ok(at));
    assert_eq!(owner.committed_index(), Ok(at));
    assert_eq!(owner.last_appended_index(), Ok(last));
    assert_eq!(owner.replay_batch(64), Ok(progress(at, at, 0)));
    no_io(fixture);
}

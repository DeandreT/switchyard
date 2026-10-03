use std::sync::{Arc, Mutex};

use domain::{
    CommittedApplication, CommittedApplyResult, CommittedCheckpointUpdate, CommittedEntryId,
    CommittedEntryMark, CommittedQueueCommand, CommittedQueueWork, CommittedSend,
    CommittedStateMachine, CommittedStreamId, EntityPath, MessageRecord, NamespaceName,
    QueueConfig, QueueCounters, SequenceNumber, StateMachine, Timestamp, codec, keys,
};
use storage::{CommittedStore, Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};

use super::TestResult;

pub(super) fn stream() -> TestResult<CommittedStreamId> {
    Ok(CommittedStreamId::new([7; 16])?)
}

pub(super) fn namespace() -> TestResult<NamespaceName> {
    Ok(NamespaceName::new("tenant")?)
}

pub(super) fn entity() -> TestResult<EntityPath> {
    Ok(EntityPath::new("orders")?)
}

pub(super) fn create(time: u64, config: QueueConfig) -> TestResult<CommittedQueueWork> {
    Ok(CommittedQueueWork::Queue(
        CommittedQueueCommand::create_queue(
            namespace()?,
            entity()?,
            Timestamp::from_millis(time),
            config,
        ),
    ))
}

pub(super) fn send(time: u64, id: &str, body: &[u8]) -> TestResult<CommittedQueueWork> {
    Ok(CommittedQueueWork::Queue(CommittedQueueCommand::send(
        namespace()?,
        entity()?,
        Timestamp::from_millis(time),
        CommittedSend {
            message_id: id.into(),
            body: body.to_vec(),
            time_to_live_millis: None,
            session_id: None,
        },
    )))
}

pub(super) fn update<W: CommittedStore>(
    machine: &CommittedStateMachine<W>,
    index: u64,
) -> TestResult<CommittedCheckpointUpdate> {
    Ok(CommittedCheckpointUpdate {
        stream: stream()?,
        expected_previous: machine.checkpoint()?.last(),
        entry: CommittedEntryId {
            term: 1,
            node_id: 9,
            index,
        },
    })
}

pub(super) fn apply<W: CommittedStore>(
    machine: &mut CommittedStateMachine<W>,
    index: u64,
    work: &CommittedQueueWork,
) -> TestResult<CommittedApplyResult> {
    let update = update(machine, index)?;
    Ok(machine.apply_committed(&update, work)?)
}

pub(super) fn applied(
    result: CommittedApplyResult,
) -> TestResult<(CommittedEntryMark, CommittedApplication)> {
    match result {
        CommittedApplyResult::Applied {
            position,
            application,
        } => Ok((position, application)),
        CommittedApplyResult::AlreadyApplied { .. } => {
            Err("expected a new committed application".into())
        }
    }
}

pub(super) fn record<R: StateStore>(
    reader: &R,
    sequence: u64,
) -> TestResult<Option<MessageRecord>> {
    Ok(StateMachine::new(reader.clone()).message(
        &namespace()?,
        &entity()?,
        SequenceNumber::new(sequence),
    )?)
}

pub(super) fn counters<R: StateStore>(reader: &R) -> TestResult<Option<QueueCounters>> {
    reader
        .get(&keys::queue_counters(&namespace()?, &entity()?))?
        .map(|value| Ok(codec::decode(&value)?))
        .transpose()
}

pub(super) fn checkpoint_key<R: StateStore>(reader: &R) -> TestResult<Key> {
    let snapshot = reader.snapshot()?;
    if snapshot.entries().len() != 1 {
        return Err("the baseline should contain only its checkpoint".into());
    }
    Ok(snapshot.entries()[0].0.clone())
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct Counts {
    pub gets: usize,
    pub scans: usize,
    pub snapshots: usize,
    pub commits: usize,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) enum CommitFault {
    #[default]
    None,
    Before,
    After,
    ExitBefore,
    ExitAfter,
}

struct WriterState<W> {
    writer: W,
    fault: CommitFault,
    batches: Vec<WriteBatch>,
}

pub(super) struct ObservedWriter<W> {
    state: Arc<Mutex<WriterState<W>>>,
    counts: Arc<Mutex<Counts>>,
}

pub(super) struct Control<W> {
    state: Arc<Mutex<WriterState<W>>>,
    counts: Arc<Mutex<Counts>>,
}

pub(super) fn observed<W: CommittedStore>(writer: W) -> (ObservedWriter<W>, Control<W>) {
    let state = Arc::new(Mutex::new(WriterState {
        writer,
        fault: CommitFault::None,
        batches: Vec::new(),
    }));
    let counts = Arc::new(Mutex::new(Counts::default()));
    (
        ObservedWriter {
            state: state.clone(),
            counts: counts.clone(),
        },
        Control { state, counts },
    )
}

impl<W: CommittedStore> Control<W> {
    pub(super) fn counts(&self) -> Counts {
        *self.counts.lock().expect("test observation lock")
    }

    pub(super) fn batches(&self) -> Vec<WriteBatch> {
        self.state.lock().expect("test writer lock").batches.clone()
    }

    pub(super) fn fault(&self, fault: CommitFault) {
        self.state.lock().expect("test writer lock").fault = fault;
    }

    pub(super) fn reader(&self) -> W::Reader {
        self.state.lock().expect("test writer lock").writer.reader()
    }

    pub(super) fn inject(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.state
            .lock()
            .expect("test writer lock")
            .writer
            .commit(batch)
    }

    pub(super) fn recover_writer(&self) -> ObservedWriter<W> {
        ObservedWriter {
            state: self.state.clone(),
            counts: self.counts.clone(),
        }
    }
}

impl<W: CommittedStore> CommittedStore for ObservedWriter<W> {
    type Reader = ObservedReader<W::Reader>;

    fn reader(&self) -> Self::Reader {
        ObservedReader {
            inner: self.state.lock().expect("test writer lock").writer.reader(),
            counts: self.counts.clone(),
        }
    }

    fn is_initialized(&self) -> Result<bool, StorageError> {
        self.state
            .lock()
            .expect("test writer lock")
            .writer
            .is_initialized()
    }

    fn commit(&mut self, batch: WriteBatch) -> Result<(), StorageError> {
        self.counts.lock().expect("test observation lock").commits += 1;
        let mut state = self.state.lock().expect("test writer lock");
        state.batches.push(batch.clone());
        let fault = std::mem::take(&mut state.fault);
        match fault {
            CommitFault::Before => return Err(injected_error()),
            CommitFault::ExitBefore => std::process::exit(71),
            _ => {}
        }
        state.writer.commit(batch)?;
        match fault {
            CommitFault::After => Err(injected_error()),
            CommitFault::ExitAfter => std::process::exit(72),
            _ => Ok(()),
        }
    }
}

fn injected_error() -> StorageError {
    StorageError::CorruptMetadata {
        detail: "injected committed-apply write failure".into(),
    }
}

#[derive(Clone)]
pub(super) struct ObservedReader<R> {
    inner: R,
    counts: Arc<Mutex<Counts>>,
}

impl<R: StateStore> StateStore for ObservedReader<R> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.counts.lock().expect("test observation lock").gets += 1;
        self.inner.get(key)
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.inner.apply(batch)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.counts.lock().expect("test observation lock").snapshots += 1;
        self.inner.snapshot()
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.counts.lock().expect("test observation lock").scans += 1;
        self.inner.scan_from(prefix, start, limit)
    }
}

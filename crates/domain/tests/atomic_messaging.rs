//! One admitted ordinary queue, ordered preparation, and one final store commit.

use std::{
    collections::BTreeMap,
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use domain::{
    AtomicMessagingCommand, AtomicMessagingLimit, BrokerError, Command, CommandKind,
    CommandOutcome, DeleteEntityTarget, EntityBinding, EntityIncarnationKind, EntityPath,
    IngressEnvelope, LockToken, MAX_ATOMIC_MESSAGING_ACTIONS, MAX_ATOMIC_MESSAGING_CONTENT_BYTES,
    MAX_ATOMIC_MESSAGING_MESSAGES, MAX_ATOMIC_MESSAGING_MUTATION_VALUE_BYTES,
    MAX_ATOMIC_MESSAGING_READ_KEY_BYTES, MAX_ATOMIC_MESSAGING_READ_OPERATIONS,
    MAX_ATOMIC_MESSAGING_READ_VALUE_BYTES, MAX_ATOMIC_MESSAGING_VALUE_ITEMS, MAX_SEQUENCE_NUMBER,
    MessageBody, MessageEnvelope, MessageIdentifier, MessageProperties, MessageRecord,
    MessageState, MessageValue, NamespaceName, QueueConfig, QueueConfigUpdate, QueueCounterKind,
    QueueCounters, ReceiveMode, SequenceNumber, SessionId, SettlementDisposition,
    SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig, codec, keys,
    validate_atomic_messaging_kinds,
};
use storage::{Key, Mutation, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::{QueueFixture, StoreProvider};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[derive(Clone, Debug, Default)]
struct Observations {
    reads: Vec<Key>,
    read_value_bytes: usize,
    scans: Vec<(Key, Key, usize)>,
    scan_key_bytes: usize,
    scan_value_bytes: usize,
    snapshots: usize,
    commits: usize,
    mutations: Vec<Mutation>,
    expected_before_commit: Option<StoreSnapshot>,
}

#[derive(Clone, Debug)]
struct ObservedStore<S> {
    inner: S,
    observations: Arc<Mutex<Observations>>,
    fail_next: Arc<AtomicBool>,
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        let value = self.inner.get(key)?;
        let mut observations = self.observations.lock().expect("observations");
        observations.reads.push(key.to_vec());
        observations.read_value_bytes += value.as_ref().map_or(0, Vec::len);
        Ok(value)
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        let rows = self.inner.scan_from(prefix, start, limit)?;
        let mut observations = self.observations.lock().expect("observations");
        observations
            .scans
            .push((prefix.to_vec(), start.to_vec(), limit));
        observations.scan_key_bytes +=
            prefix.len() + start.len() + rows.iter().map(|(key, _)| key.len()).sum::<usize>();
        observations.scan_value_bytes += rows.iter().map(|(_, value)| value.len()).sum::<usize>();
        Ok(rows)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.observations.lock().expect("observations").snapshots += 1;
        self.inner.snapshot()
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        let expected = {
            let mut observations = self.observations.lock().expect("observations");
            observations.commits += 1;
            observations.mutations = batch.mutations().to_vec();
            observations.expected_before_commit.clone()
        };
        if let Some(expected) = expected {
            assert_eq!(
                self.inner.snapshot()?,
                expected,
                "staged changes escaped before commit"
            );
        }
        if self.fail_next.swap(false, Ordering::Relaxed) {
            return Err(StorageError::Backend {
                operation: "commit",
                detail: "injected atomic messaging failure".into(),
            });
        }
        self.inner.apply(batch)
    }
}

struct ObservedProvider<P> {
    inner: P,
    observations: Arc<Mutex<Observations>>,
    fail_next: Arc<AtomicBool>,
}

impl<P: StoreProvider> StoreProvider for ObservedProvider<P> {
    type Store = ObservedStore<P::Store>;

    fn open(&self) -> Result<Self::Store, StorageError> {
        Ok(ObservedStore {
            inner: self.inner.open()?,
            observations: Arc::clone(&self.observations),
            fail_next: Arc::clone(&self.fail_next),
        })
    }
}

fn fixture<P: StoreProvider>(
    provider: P,
    config: QueueConfig,
) -> TestResult<QueueFixture<ObservedProvider<P>>> {
    Ok(QueueFixture::new(
        ObservedProvider {
            inner: provider,
            observations: Arc::new(Mutex::new(Observations::default())),
            fail_next: Arc::new(AtomicBool::new(false)),
        },
        "tenant",
        "orders",
        config,
    )?)
}

fn reset<P: StoreProvider>(fixture: &QueueFixture<ObservedProvider<P>>) {
    *fixture
        .machine
        .store()
        .observations
        .lock()
        .expect("observations") = Observations::default();
}

fn observed<P: StoreProvider>(fixture: &QueueFixture<ObservedProvider<P>>) -> Observations {
    fixture
        .machine
        .store()
        .observations
        .lock()
        .expect("observations")
        .clone()
}

fn bind<P: StoreProvider>(fixture: &QueueFixture<P>) -> TestResult<EntityBinding> {
    Ok(fixture
        .machine
        .bind_entity(
            &fixture.namespace,
            &fixture.entity,
            &fixture.entity,
            EntityIncarnationKind::Queue,
        )?
        .expect("ordinary queue binding"))
}

fn atomic<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
    kinds: Vec<CommandKind>,
) -> TestResult<AtomicMessagingCommand> {
    Ok(AtomicMessagingCommand {
        binding: bind(fixture)?,
        issued_at: Timestamp::from_millis(millis),
        commands: kinds
            .into_iter()
            .map(|kind| fixture.command(millis, kind))
            .collect(),
    })
}

fn send(id: &str) -> CommandKind {
    CommandKind::Send {
        message_id: id.into(),
        body: b"payload".to_vec(),
        time_to_live_millis: None,
        session_id: None,
    }
}

fn member(id: &str) -> IngressEnvelope {
    IngressEnvelope {
        message_id: id.into(),
        body: b"payload".to_vec(),
        time_to_live_millis: None,
        session_id: None,
        envelope: MessageEnvelope {
            properties: MessageProperties {
                message_id: Some(MessageIdentifier::String(id.into())),
                ..MessageProperties::default()
            },
            body: MessageBody::Data(vec![b"payload".to_vec()]),
            ..MessageEnvelope::default()
        },
        scheduled_enqueue_time: None,
    }
}

fn hold<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    millis: u64,
) -> TestResult<(SequenceNumber, LockToken)> {
    let CommandOutcome::Received(Some(delivery)) = fixture.at(
        millis,
        CommandKind::Receive {
            mode: ReceiveMode::PeekLock,
            lock_duration_millis: None,
            session: None,
        },
    )?
    else {
        panic!("held message expected")
    };
    Ok((delivery.sequence, delivery.lock.expect("peek-lock").token))
}

fn record<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    entity: &EntityPath,
    sequence: SequenceNumber,
) -> TestResult<Option<MessageRecord>> {
    fixture
        .machine
        .store()
        .get(&keys::message(&fixture.namespace, entity, sequence))?
        .map(|bytes| codec::decode(&bytes).map_err(Into::into))
        .transpose()
}

fn counters<P: StoreProvider>(fixture: &QueueFixture<P>) -> TestResult<QueueCounters> {
    Ok(codec::decode(
        &fixture
            .machine
            .store()
            .get(&keys::queue_counters(&fixture.namespace, &fixture.entity))?
            .expect("allocated counters"),
    )?)
}

fn assert_health_probes(binding: &EntityBinding, observations: &Observations) {
    let mut prefix = keys::topic_mode(binding.namespace(), binding.owner());
    assert_eq!(prefix.pop(), Some(0));
    prefix.extend_from_slice(b"/subscriptions/");
    assert!(
        !observations.scans.is_empty(),
        "live queue profile must probe owned metadata"
    );
    assert!(
        observations
            .scans
            .iter()
            .all(|(query, start, limit)| query == &prefix && start == query && *limit == 1)
    );
    assert_eq!(observations.snapshots, 0);
}

fn expect_refusal<P: StoreProvider>(
    fixture: &QueueFixture<ObservedProvider<P>>,
    envelope: &AtomicMessagingCommand,
    error: BrokerError,
) -> TestResult {
    expect_refusal_with_probes(fixture, envelope, error, false)
}

fn expect_health_refusal<P: StoreProvider>(
    fixture: &QueueFixture<ObservedProvider<P>>,
    envelope: &AtomicMessagingCommand,
    error: BrokerError,
) -> TestResult {
    expect_refusal_with_probes(fixture, envelope, error, true)
}

fn expect_refusal_with_probes<P: StoreProvider>(
    fixture: &QueueFixture<ObservedProvider<P>>,
    envelope: &AtomicMessagingCommand,
    error: BrokerError,
    health_probes: bool,
) -> TestResult {
    let before = fixture.machine.store().snapshot()?;
    let clock = fixture.machine.last_applied_time()?;
    reset(fixture);
    assert_eq!(fixture.machine.apply_atomic_messaging(envelope), Err(error));
    let observations = observed(fixture);
    assert_eq!(observations.commits, 0);
    if health_probes {
        assert_health_probes(&envelope.binding, &observations);
    } else {
        assert!(observations.scans.is_empty());
        assert_eq!(observations.snapshots, 0);
    }
    assert_eq!(fixture.machine.store().snapshot()?, before);
    assert_eq!(fixture.machine.last_applied_time()?, clock);
    *fixture
        .machine
        .store()
        .observations
        .lock()
        .expect("observations") = observations;
    Ok(())
}

fn limit(limit: AtomicMessagingLimit, maximum: usize) -> BrokerError {
    BrokerError::AtomicMessagingTooLarge { limit, maximum }
}

#[path = "atomic_messaging/atomicity.rs"]
mod atomicity;
#[path = "atomic_messaging/lifecycle.rs"]
mod lifecycle;
#[path = "atomic_messaging/limits.rs"]
mod limits;

macro_rules! backend_cases {
    ($module:ident, $provider:expr) => {
        mod $module {
            use super::*;

            #[test]
            fn ordered_sends_share_dedup_and_counters() -> TestResult {
                lifecycle::ordered_sends_share_dedup_and_counters($provider)
            }
            #[test]
            fn settlements_preserve_order_and_final_effects() -> TestResult {
                lifecycle::settlements_preserve_order_and_final_effects($provider)
            }
            #[test]
            fn empty_sections_and_settlement_only_have_exact_effects() -> TestResult {
                lifecycle::empty_sections_and_settlement_only_have_exact_effects($provider)
            }
            #[test]
            fn late_settlement_failures_rollback_every_staged_change() -> TestResult {
                atomicity::late_settlement_failures_rollback_every_staged_change($provider)
            }
            #[test]
            fn late_content_counter_and_stored_record_failures_are_atomic() -> TestResult {
                atomicity::late_content_counter_and_stored_record_failures_are_atomic($provider)
            }
            #[test]
            fn failed_commit_reopens_and_retries_the_same_serialized_envelope() -> TestResult {
                atomicity::failed_commit_reopens_and_retries_the_same_serialized_envelope($provider)
            }
            #[test]
            fn scope_stamp_and_stale_guards_precede_clock() -> TestResult {
                atomicity::scope_stamp_and_stale_guards_precede_clock($provider)
            }
            #[test]
            fn empty_envelope_validates_identity_without_clock_or_commit() -> TestResult {
                atomicity::empty_envelope_validates_identity_without_clock_or_commit($provider)
            }
            #[test]
            fn aggregate_actions_and_messages_include_duplicates() -> TestResult {
                limits::aggregate_actions_and_messages_include_duplicates($provider)
            }
            #[test]
            fn aggregate_content_and_value_boundaries() -> TestResult {
                limits::aggregate_content_and_value_boundaries($provider)
            }
            #[test]
            fn borrowed_patch_detail_and_empty_section_budgets() -> TestResult {
                limits::borrowed_patch_detail_and_empty_section_budgets($provider)
            }
            #[test]
            fn repeated_reads_charge_values_before_decode() -> TestResult {
                limits::repeated_reads_charge_values_before_decode($provider)
            }
            #[test]
            fn generated_put_bytes_are_cumulative_not_final_size() -> TestResult {
                limits::generated_put_bytes_are_cumulative_not_final_size($provider)
            }
        }
    };
}

backend_cases!(memory, testkit::MemoryProvider::new());
backend_cases!(durable, testkit::DurableProvider::temporary()?);

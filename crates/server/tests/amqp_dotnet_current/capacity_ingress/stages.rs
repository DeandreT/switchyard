use std::{collections::BTreeSet, path::Path, time::Duration};

use domain::{
    EntityPath, FiniteQueueCapacity, MessageRecord, MessageState, MessageValue, NamespaceName,
    QueueCapacityStatus, QueueCapacityView, QueueConfig, StateMachine, keys,
};
use storage::{Mutation, StateStore, StoreSnapshot, WriteBatch};
use testkit::StoreProvider;
use tokio::time::timeout;

use super::{
    TestResult,
    fixture::Fixture,
    process::{self, CapacityConstructor, CapacityStage},
};

const CONSTRUCTORS: [CapacityConstructor; 2] =
    [CapacityConstructor::Named, CapacityConstructor::Connection];
const CREDIT_BYTES: usize = 8 * 1024;
const RETAINED_BYTES: usize = 20 * 1024;
const SMALL_BYTES: usize = 512;

fn queue(kind: &str, constructor: CapacityConstructor) -> TestResult<EntityPath> {
    Ok(EntityPath::new(format!(
        "sdk-capacity-{kind}-{}",
        constructor.name()
    ))?)
}

fn capacity(view: &QueueCapacityView) -> (FiniteQueueCapacity, u64, u64) {
    match &view.capacity {
        QueueCapacityStatus::FiniteV1 {
            limit,
            reserved_bytes,
            message_count,
        } => (*limit, *reserved_bytes, *message_count),
        QueueCapacityStatus::NonFinite => panic!("SDK finite owner became non-finite"),
    }
}

async fn child<P: StoreProvider>(
    fixture: &mut Fixture<P>,
    dll: &Path,
    stage: CapacityStage,
    constructor: CapacityConstructor,
    entity: &EntityPath,
) -> TestResult<QueueCapacityView> {
    let endpoint = fixture.begin_stage().await?;
    let outcome = process::run_capacity_client(
        dll,
        stage,
        constructor,
        &endpoint,
        entity.as_str(),
        &fixture.ca_file,
        &fixture.ca_directory,
    )
    .await;
    let cleanup = fixture.end_stage().await;
    match (outcome, cleanup) {
        (Err(error), Err(cleanup)) => {
            return Err(std::io::Error::other(format!(
                "Capacity SDK child failed: {error}; original socket cleanup also failed: {cleanup}"
            ))
            .into());
        }
        (Err(error), Ok(())) => return Err(error),
        (Ok(_), Err(error)) => return Err(error),
        (Ok(_), Ok(())) => {}
    }
    // Facade leases have actually disappeared; serialize already-admitted owner work.
    let view = fixture.view(entity).await?;
    eprintln!(
        "capacity-sdk stage-finish stage={} constructor={}",
        stage.name(),
        constructor.name()
    );
    Ok(view)
}

fn records(
    snapshot: &StoreSnapshot,
    namespace: &NamespaceName,
    entity: &EntityPath,
) -> TestResult<Vec<MessageRecord>> {
    let prefix = keys::message_prefix(namespace, entity);
    snapshot
        .entries()
        .iter()
        .filter(|(key, _)| key.starts_with(&prefix))
        .map(|(key, value)| {
            let record = MessageRecord::decode(value)?;
            assert_eq!(*key, keys::message(namespace, entity, record.sequence));
            Ok(record)
        })
        .collect()
}

fn known(record: &MessageRecord, id: &str, bytes: usize) {
    assert_eq!(record.message_id, id);
    assert_eq!(
        record.body,
        (0..bytes)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>()
    );
    assert!(
        record.session_id.is_none()
            && record.dead_letter.is_none()
            && record.scheduled_enqueue_time.is_none()
    );
    let envelope = record
        .envelope
        .as_ref()
        .expect("SDK retained producer envelope");
    assert_eq!(envelope.properties.subject.as_deref(), Some(id));
    assert_eq!(
        envelope.properties.content_type.as_deref(),
        Some("application/octet-stream")
    );
    assert_eq!(
        envelope.application_properties.get("capacity-source"),
        Some(&MessageValue::String("official-sdk".into()))
    );
}

fn ledger<P: StoreProvider>(
    fixture: &Fixture<P>,
    entity: &EntityPath,
    record: &MessageRecord,
) -> TestResult<Vec<(Vec<u8>, Vec<u8>)>> {
    [
        keys::queue_capacity_usage(&fixture.namespace, entity),
        keys::message_charge(&fixture.namespace, entity, record.sequence),
    ]
    .into_iter()
    .map(|key| {
        let value = fixture.raw(&key)?.expect("SDK actual reservation ledger");
        Ok((key, value))
    })
    .collect()
}

async fn refusal<P: StoreProvider>(
    fixture: &mut Fixture<P>,
    dll: &Path,
    stage: CapacityStage,
    constructor: CapacityConstructor,
    entity: &EntityPath,
    clocks: usize,
) -> TestResult {
    fixture.view(entity).await?;
    let before = fixture.snapshot()?;
    let effects = fixture.effects();
    child(fixture, dll, stage, constructor, entity).await?;
    let after = fixture.effects();
    assert_eq!(
        after.clock - effects.clock,
        clocks,
        "wrong isolated owner-stamp path: {stage:?}"
    );
    assert_eq!(
        after.attempts, effects.attempts,
        "refused SDK send attempted a batch"
    );
    assert_eq!(
        after.commits, effects.commits,
        "refused SDK send committed a batch"
    );
    assert_eq!(
        fixture.snapshot()?,
        before,
        "refused SDK send changed whole persisted image"
    );
    Ok(())
}

fn abandon_journal<P: StoreProvider>(
    fixture: &Fixture<P>,
    entity: &EntityPath,
    original: &MessageRecord,
    ledger: &[(Vec<u8>, Vec<u8>)],
    batches: &[WriteBatch],
) -> TestResult {
    let key = keys::message(&fixture.namespace, entity, original.sequence);
    let mut states = Vec::new();
    for batch in batches {
        for mutation in batch.mutations() {
            match mutation {
                Mutation::Put {
                    key: changed,
                    value,
                } if changed == &key => {
                    let record = MessageRecord::decode(value)?;
                    let mut expected = original.clone();
                    expected.delivery_count += 1;
                    expected.state = record.state.clone();
                    assert_eq!(
                        record, expected,
                        "lock/abandon rewrote retained producer content or deadlines"
                    );
                    if let MessageState::Locked { locked_until, .. } = record.state {
                        assert!(
                            locked_until.as_millis() > 1_600_000_000_000,
                            "SDK lock was not stamped with current time"
                        );
                    }
                    states.push(record.state);
                }
                Mutation::Delete { key: changed } if changed == &key => {
                    panic!("abandon deleted its retained message")
                }
                _ => {}
            }
            for (ledger_key, original_value) in ledger {
                match mutation {
                    Mutation::Put {
                        key: changed,
                        value,
                    } if changed == ledger_key => assert_eq!(
                        value, original_value,
                        "lock/abandon changed reservation bytes"
                    ),
                    Mutation::Delete { key: changed } if changed == ledger_key => {
                        panic!("lock/abandon refunded its reservation")
                    }
                    _ => {}
                }
            }
        }
    }
    assert_eq!(
        states.len(),
        2,
        "must observe both original committed lock and abandon mutations"
    );
    assert!(matches!(states[0], MessageState::Locked { .. }));
    assert_eq!(states[1], MessageState::Ready);
    for (key, value) in ledger {
        assert_eq!(fixture.raw(key)?.as_ref(), Some(value));
    }
    Ok(())
}

async fn zero<P: StoreProvider>(fixture: &Fixture<P>, entity: &EntityPath) -> TestResult {
    timeout(Duration::from_secs(5), async {
        loop {
            let (_, reserved, count) = capacity(&fixture.view(entity).await?);
            if reserved == 0 && count == 0 {
                let snapshot = fixture.snapshot()?;
                for target in [entity.clone(), entity.dead_letter_queue()?] {
                    assert!(records(&snapshot, &fixture.namespace, &target)?.is_empty());
                    assert!(!snapshot.entries().iter().any(|(key, _)| {
                        key.starts_with(&keys::message_charge_prefix(&fixture.namespace, &target))
                    }));
                }
                return Ok::<_, Box<dyn std::error::Error>>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| "SDK Complete did not reach zero reservation before retry")??;
    Ok(())
}

pub(super) async fn workflows<P: StoreProvider>(
    fixture: &mut Fixture<P>,
    dll: &Path,
) -> TestResult {
    for constructor in CONSTRUCTORS {
        let credit = queue("credit", constructor)?;
        fixture
            .handle()
            .create_finite_queue(
                fixture.namespace.clone(),
                credit.clone(),
                QueueConfig {
                    lock_duration_millis: 60_000,
                    ..QueueConfig::default()
                },
                FiniteQueueCapacity::new(4 * 1024 * 1024)?,
            )
            .await?;
        let seeded = child(fixture, dll, CapacityStage::Seed, constructor, &credit).await?;
        let (_, reserved, count) = capacity(&seeded);
        assert_eq!(count, 1);
        assert!(reserved > 0);
        let stored = records(&fixture.snapshot()?, &fixture.namespace, &credit)?;
        assert_eq!(stored.len(), 1);
        known(&stored[0], "capacity-credit", CREDIT_BYTES);
        assert_eq!(stored[0].state, MessageState::Ready);
        let original_ledger = ledger(fixture, &credit, &stored[0])?;
        // Exact observed logical reservation; not an Azure quota or Atom MiB size.
        let full = fixture
            .handle()
            .set_queue_capacity_limit_fenced(seeded.binding, FiniteQueueCapacity::new(reserved)?)
            .await?;
        assert_eq!(
            capacity(&full),
            (FiniteQueueCapacity::new(reserved)?, reserved, 1)
        );
        refusal(fixture, dll, CapacityStage::Quota, constructor, &credit, 1).await?;
        let effects = fixture.effects();
        child(fixture, dll, CapacityStage::Abandon, constructor, &credit).await?;
        abandon_journal(
            fixture,
            &credit,
            &stored[0],
            &original_ledger,
            &fixture.batches_since(effects),
        )?;
        assert_eq!(capacity(&fixture.view(&credit).await?), capacity(&full));
        refusal(fixture, dll, CapacityStage::Quota, constructor, &credit, 1).await?;
        child(fixture, dll, CapacityStage::Complete, constructor, &credit).await?;
        // This observed owner barrier is required BEFORE the next sending child starts.
        zero(fixture, &credit).await?;
        let retried = child(fixture, dll, CapacityStage::Retry, constructor, &credit).await?;
        let (limit, reserved, count) = capacity(&retried);
        assert_eq!(count, 1);
        assert!(reserved > 0 && reserved <= limit.bytes());
        let retried_records = records(&fixture.snapshot()?, &fixture.namespace, &credit)?;
        assert_eq!(retried_records.len(), 1);
        known(&retried_records[0], "capacity-credit", CREDIT_BYTES);
        child(fixture, dll, CapacityStage::Complete, constructor, &credit).await?;
        zero(fixture, &credit).await?;

        let size = queue("size", constructor)?;
        fixture
            .handle()
            .create_finite_queue(
                fixture.namespace.clone(),
                size.clone(),
                QueueConfig {
                    lock_duration_millis: 60_000,
                    ..QueueConfig::default()
                },
                FiniteQueueCapacity::new(4 * 1024 * 1024)?,
            )
            .await?;
        let high = child(fixture, dll, CapacityStage::SizeSeed, constructor, &size).await?;
        let retained = records(&fixture.snapshot()?, &fixture.namespace, &size)?;
        assert_eq!(retained.len(), 1);
        known(&retained[0], "capacity-retained", RETAINED_BYTES);
        let before = fixture.snapshot()?;
        let effects = fixture.effects();
        let high_capacity = capacity(&high);
        let mut config = high.config;
        config.max_message_bytes = 4 * 1024;
        let low = fixture
            .handle()
            .set_finite_queue_definition_fenced(high.binding, config, high_capacity.0)
            .await?;
        assert_eq!(low.config, config);
        assert_eq!(capacity(&low), high_capacity);
        future_limit_only(
            &before,
            &fixture.snapshot()?,
            &fixture.namespace,
            &size,
            &fixture.batches_since(effects),
        );
        assert_eq!(
            records(&fixture.snapshot()?, &fixture.namespace, &size)?,
            retained
        );
        refusal(
            fixture,
            dll,
            CapacityStage::BrokerSize,
            constructor,
            &size,
            1,
        )
        .await?;
        refusal(
            fixture,
            dll,
            CapacityStage::NegotiatedSize,
            constructor,
            &size,
            0,
        )
        .await?;
        let smaller = child(fixture, dll, CapacityStage::Small, constructor, &size).await?;
        assert_eq!(capacity(&smaller).2, 2);
        let actual = records(&fixture.snapshot()?, &fixture.namespace, &size)?;
        assert_eq!(actual.len(), 2);
        let small = actual
            .iter()
            .find(|record| record.message_id == "capacity-small")
            .expect("accepted below-limit SDK message");
        known(small, "capacity-small", SMALL_BYTES);
        assert!(
            actual.iter().any(|record| record == &retained[0]),
            "future maximum rewrote prior large message"
        );
        child(fixture, dll, CapacityStage::DrainSize, constructor, &size).await?;
        zero(fixture, &size).await?;
    }
    Ok(())
}

fn future_limit_only(
    before: &StoreSnapshot,
    after: &StoreSnapshot,
    namespace: &NamespaceName,
    entity: &EntityPath,
    batches: &[WriteBatch],
) {
    let allowed = BTreeSet::from([
        keys::queue_config(namespace, entity),
        keys::queue_config(namespace, &entity.dead_letter_queue().expect("size DLQ")),
        keys::clock(),
    ]);
    let unchanged = |snapshot: &StoreSnapshot| {
        snapshot
            .entries()
            .iter()
            .filter(|(key, _)| !allowed.contains(key))
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(
        unchanged(before),
        unchanged(after),
        "future message limit changed retained records/Usage/Charge"
    );
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].mutations().len(), 3);
    let actual = batches[0]
        .mutations()
        .iter()
        .map(|mutation| match mutation {
            Mutation::Put { key, .. } => key.clone(),
            Mutation::Delete { .. } => panic!("future maximum deleted runtime state"),
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        actual, allowed,
        "config-only update did not use its single prepared batch"
    );
}

pub(super) fn clean<S: StateStore>(store: &S, namespace: &NamespaceName) -> TestResult {
    let machine = StateMachine::new(store.clone());
    for constructor in CONSTRUCTORS {
        for kind in ["credit", "size"] {
            let entity = queue(kind, constructor)?;
            let view = machine
                .describe_queue_capacity(namespace, &entity)?
                .expect("reopened finite SDK owner");
            let (_, reserved, count) = capacity(&view);
            assert_eq!((reserved, count), (0, 0));
            assert_eq!(
                view.config.max_message_bytes,
                if kind == "size" {
                    4 * 1024
                } else {
                    domain::DEFAULT_MAX_MESSAGE_BYTES
                }
            );
            for target in [entity.clone(), entity.dead_letter_queue()?] {
                for prefix in [
                    keys::message_prefix(namespace, &target),
                    keys::message_charge_prefix(namespace, &target),
                    keys::ready_prefix(namespace, &target),
                    keys::lock_prefix(namespace, &target),
                    keys::expiry_prefix(namespace, &target),
                    keys::scheduled_prefix(namespace, &target),
                    keys::session_lock_prefix(namespace, &target),
                    keys::entity_session_prefix(namespace, &target),
                ] {
                    assert!(
                        store.scan_prefix(&prefix, 1)?.is_empty(),
                        "drained SDK queue retained a live message/index"
                    );
                }
            }
        }
    }
    Ok(())
}

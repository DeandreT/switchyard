//! Queue discovery pages are bounded, exclusive, and namespace confined.

use std::{
    error::Error,
    sync::{Arc, Mutex},
};

use domain::{
    BrokerError, Command, CommandKind, CommandOutcome, EntityPath, MAX_QUEUE_PAGE_SIZE,
    NamespaceName, QueueConfig, QueueCursor, QueuePage, Timestamp, codec, keys,
};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::{QueueFixture, StoreProvider};

fn create<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    namespace: &str,
    entity: &str,
) -> Result<(), Box<dyn Error>> {
    assert_eq!(
        fixture.machine.apply(&Command::new(
            NamespaceName::new(namespace)?,
            EntityPath::new(entity)?,
            Timestamp::UNIX_EPOCH,
            CommandKind::CreateQueue {
                config: QueueConfig::default()
            },
        ))?,
        CommandOutcome::QueueCreated
    );
    Ok(())
}

fn collect_pages<P: StoreProvider>(
    fixture: &QueueFixture<P>,
    namespace: Option<&NamespaceName>,
    mut after: Option<QueueCursor>,
    limit: usize,
) -> Result<Vec<(NamespaceName, EntityPath)>, BrokerError> {
    let mut all = Vec::new();
    loop {
        let page = fixture
            .machine
            .queues_page(namespace, after.as_ref(), limit)?;
        assert!(page.queues.len() <= limit);
        if let Some(cursor) = &page.continuation {
            assert_eq!(
                page.queues.last(),
                Some(&(cursor.namespace.clone(), cursor.entity.clone()))
            );
            assert_ne!(after.as_ref(), Some(cursor));
        }
        all.extend(page.queues);
        after = page.continuation;
        if after.is_none() {
            return Ok(all);
        }
    }
}

fn global_and_namespace_pages_preserve_key_order_and_shadow_entries<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "beta", "root")?;
    for namespace in ["alpha", "beta", "beta-two"] {
        for entity in ["q", "q/nested"] {
            create(&fixture, namespace, entity)?;
        }
    }
    let expected = fixture.machine.queues(usize::MAX)?;
    assert_eq!(expected.len(), 14);
    assert_eq!(
        expected
            .iter()
            .filter(|(_, entity)| entity.is_dead_letter_queue())
            .count(),
        7
    );
    let snapshot = fixture.machine.store().snapshot()?;
    assert_eq!(collect_pages(&fixture, None, None, 3)?, expected);
    let first = fixture.machine.queues_page(None, None, 3)?;
    let fixture = fixture.restart()?;
    let mut resumed = first.queues;
    resumed.extend(collect_pages(&fixture, None, first.continuation, 3)?);
    assert_eq!(resumed, expected);
    for name in ["alpha", "beta", "beta-two", "absent"] {
        let namespace = NamespaceName::new(name)?;
        let scoped: Vec<_> = expected
            .iter()
            .filter(|(stored, _)| stored == &namespace)
            .cloned()
            .collect();
        assert_eq!(collect_pages(&fixture, Some(&namespace), None, 2)?, scoped);
    }
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ScanCall {
    prefix: Vec<u8>,
    start: Vec<u8>,
    limit: usize,
}

#[derive(Clone, Debug)]
struct ObservedStore<S> {
    inner: S,
    calls: Arc<Mutex<Vec<ScanCall>>>,
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.inner.get(key)
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
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
        if limit > MAX_QUEUE_PAGE_SIZE + 1 {
            return Err(StorageError::Backend {
                operation: "scan a bounded queue page",
                detail: String::from("page scan exceeded its bound"),
            });
        }
        self.calls.lock().expect("scan recorder").push(ScanCall {
            prefix: prefix.to_vec(),
            start: start.to_vec(),
            limit,
        });
        self.inner.scan_from(prefix, start, limit)
    }
}

struct ObservedProvider<P> {
    inner: P,
    calls: Arc<Mutex<Vec<ScanCall>>>,
}

impl<P: StoreProvider> StoreProvider for ObservedProvider<P> {
    type Store = ObservedStore<P::Store>;
    fn open(&self) -> Result<Self::Store, StorageError> {
        Ok(ObservedStore {
            inner: self.inner.open()?,
            calls: self.calls.clone(),
        })
    }
}

fn limits_and_scope_reject_before_scans_and_pages_use_only_one_lookahead<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let fixture = QueueFixture::with_defaults(
        ObservedProvider {
            inner: provider,
            calls: calls.clone(),
        },
        "tenant",
        "root",
    )?;
    let mut batch = WriteBatch::default();
    for index in 0..=MAX_QUEUE_PAGE_SIZE {
        batch.push_put(
            keys::queue_config(
                &fixture.namespace,
                &EntityPath::new(format!("queue-{index:04}"))?,
            ),
            codec::encode(&QueueConfig::default())?,
        );
    }
    fixture.machine.store().apply(batch)?;
    let snapshot = fixture.machine.store().snapshot()?;
    calls.lock().expect("scan recorder").clear();
    for limit in [MAX_QUEUE_PAGE_SIZE + 1, usize::MAX] {
        assert_eq!(
            fixture.machine.queues_page(None, None, limit),
            Err(BrokerError::QueuePageLimitExceeded {
                limit,
                maximum: MAX_QUEUE_PAGE_SIZE
            })
        );
    }
    let wrong = QueueCursor {
        namespace: NamespaceName::new("other")?,
        entity: EntityPath::new("root")?,
    };
    for limit in [0, 1] {
        assert_eq!(
            fixture
                .machine
                .queues_page(Some(&fixture.namespace), Some(&wrong), limit),
            Err(BrokerError::QueueCursorNamespaceMismatch {
                namespace: fixture.namespace.clone(),
                cursor_namespace: wrong.namespace.clone()
            })
        );
    }
    assert_eq!(
        fixture.machine.queues_page(None, None, 0)?,
        QueuePage {
            queues: Vec::new(),
            continuation: None
        }
    );
    assert!(calls.lock().expect("scan recorder").is_empty());
    let first = fixture
        .machine
        .queues_page(None, None, MAX_QUEUE_PAGE_SIZE)?;
    assert_eq!(first.queues.len(), MAX_QUEUE_PAGE_SIZE);
    let cursor = first
        .continuation
        .expect("lookahead discovers remaining configs");
    let second = fixture
        .machine
        .queues_page(None, Some(&cursor), MAX_QUEUE_PAGE_SIZE)?;
    assert_eq!(second.queues.len(), 3);
    assert_eq!(second.continuation, None);
    let mut expected_start = keys::queue_config(&cursor.namespace, &cursor.entity);
    expected_start.push(0);
    assert_eq!(
        *calls.lock().expect("scan recorder"),
        vec![
            ScanCall {
                prefix: keys::queue_config_prefix(),
                start: keys::queue_config_prefix(),
                limit: MAX_QUEUE_PAGE_SIZE + 1
            },
            ScanCall {
                prefix: keys::queue_config_prefix(),
                start: expected_start,
                limit: MAX_QUEUE_PAGE_SIZE + 1
            },
        ]
    );
    calls.lock().expect("scan recorder").clear();
    fixture
        .machine
        .queues_page(Some(&fixture.namespace), None, 1)?;
    let prefix = keys::namespace_queue_config_prefix(&fixture.namespace);
    assert_eq!(
        *calls.lock().expect("scan recorder"),
        vec![ScanCall {
            prefix: prefix.clone(),
            start: prefix,
            limit: 2
        }]
    );
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    let fixture = fixture.restart()?;
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    Ok(())
}

fn deleted_cursors_and_insertions_resume_without_repeating_earlier_keys<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "middle")?;
    create(&fixture, "tenant", "a")?;
    create(&fixture, "tenant", "z")?;
    let first = fixture
        .machine
        .queues_page(Some(&fixture.namespace), None, 1)?;
    assert_eq!(
        first.queues,
        vec![(fixture.namespace.clone(), EntityPath::new("a")?)]
    );
    let cursor = first.continuation.expect("more queues");
    let mut batch = WriteBatch::default();
    batch.push_delete(keys::queue_config(&cursor.namespace, &cursor.entity));
    batch.push_delete(keys::queue_config(
        &cursor.namespace,
        &cursor.entity.dead_letter_queue()?,
    ));
    fixture.machine.store().apply(batch)?;
    create(&fixture, "tenant", "0-before")?;
    create(&fixture, "tenant", "b-after")?;
    let expected = fixture.machine.queues(usize::MAX)?;
    let remaining: Vec<_> = expected
        .iter()
        .filter(|(namespace, entity)| (namespace, entity) > (&cursor.namespace, &cursor.entity))
        .cloned()
        .collect();
    let snapshot = fixture.machine.store().snapshot()?;
    let fixture = fixture.restart()?;
    let resumed = collect_pages(&fixture, Some(&fixture.namespace), Some(cursor), 2)?;
    assert_eq!(resumed, remaining);
    assert!(
        !resumed
            .iter()
            .any(|(_, entity)| entity.as_str().starts_with("0-before"))
    );
    assert!(
        resumed
            .iter()
            .any(|(_, entity)| entity.as_str() == "b-after")
    );
    assert_eq!(
        collect_pages(&fixture, Some(&fixture.namespace), None, 2)?,
        expected
    );
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    Ok(())
}

fn exact_page_end_and_empty_ranges_do_not_invent_continuations<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "root")?;
    let expected = fixture.machine.queues(usize::MAX)?;
    assert_eq!(
        fixture.machine.queues_page(None, None, 2)?,
        QueuePage {
            queues: expected.clone(),
            continuation: None
        }
    );
    let first = fixture.machine.queues_page(None, None, 1)?;
    let cursor = first.continuation.expect("shadow remains");
    let second = fixture.machine.queues_page(None, Some(&cursor), 1)?;
    assert_eq!(second.queues, expected[1..]);
    assert_eq!(second.continuation, None);
    let last = QueueCursor {
        namespace: expected[1].0.clone(),
        entity: expected[1].1.clone(),
    };
    assert_eq!(
        fixture.machine.queues_page(None, Some(&last), 1)?,
        QueuePage {
            queues: Vec::new(),
            continuation: None
        }
    );
    let absent = NamespaceName::new("absent")?;
    assert_eq!(
        fixture.machine.queues_page(Some(&absent), None, 1)?,
        QueuePage {
            queues: Vec::new(),
            continuation: None
        }
    );
    let beyond = QueueCursor {
        namespace: NamespaceName::new("zzzz")?,
        entity: EntityPath::new("zzzz")?,
    };
    assert_eq!(
        fixture.machine.queues_page(None, Some(&beyond), 1)?,
        QueuePage {
            queues: Vec::new(),
            continuation: None
        }
    );
    assert_eq!(fixture.machine.queues(usize::MAX)?, expected);
    Ok(())
}

fn malformed_config_keys_are_reported_without_mutation<P: StoreProvider>(
    provider: P,
) -> Result<(), Box<dyn Error>> {
    let fixture = QueueFixture::with_defaults(provider, "tenant", "root")?;
    let mut malformed = keys::queue_config(&fixture.namespace, &EntityPath::new("bad")?);
    malformed.push(1);
    let mut batch = WriteBatch::default();
    batch.push_put(malformed, codec::encode(&QueueConfig::default())?);
    fixture.machine.store().apply(batch)?;
    let snapshot = fixture.machine.store().snapshot()?;
    assert_eq!(
        fixture.machine.queues_page(None, None, 2),
        Err(BrokerError::MalformedIndexKey)
    );
    let fixture = fixture.restart()?;
    assert_eq!(
        fixture
            .machine
            .queues_page(Some(&fixture.namespace), None, 2),
        Err(BrokerError::MalformedIndexKey)
    );
    assert_eq!(fixture.machine.store().snapshot()?, snapshot);
    Ok(())
}

#[test]
fn namespace_config_prefix_terminates_the_name_before_paths() -> Result<(), Box<dyn Error>> {
    let namespace = NamespaceName::new("tenant")?;
    let neighboring = NamespaceName::new("tenant-two")?;
    let entity = EntityPath::new("orders/nested")?;
    let prefix = keys::namespace_queue_config_prefix(&namespace);
    let mut expected = keys::queue_config_prefix();
    expected.extend_from_slice(b"tenant\0");
    assert_eq!(prefix, expected);
    assert!(keys::queue_config(&namespace, &entity).starts_with(&prefix));
    assert!(!keys::queue_config(&neighboring, &entity).starts_with(&prefix));
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[test] fn $case() -> Result<(), Box<dyn std::error::Error>> { super::$case(::testkit::MemoryProvider::new()) })+ }
        mod durable { $(#[test] fn $case() -> Result<(), Box<dyn std::error::Error>> { super::$case(::testkit::DurableProvider::temporary()?) })+ }
    };
}

for_each_backend! {
    global_and_namespace_pages_preserve_key_order_and_shadow_entries,
    limits_and_scope_reject_before_scans_and_pages_use_only_one_lookahead,
    deleted_cursors_and_insertions_resume_without_repeating_earlier_keys,
    exact_page_end_and_empty_ranges_do_not_invent_continuations,
    malformed_config_keys_are_reported_without_mutation,
}

//! Queue discovery bounds hidden topology work and retains every unread lookahead.

use std::{
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use admin_api::v1::{
    CreateEntityRequest, EntityKind, GetEntityRequest, ListEntitiesRequest, ListEntitiesResponse,
    entity_service_server::EntityService,
};
use base64::{
    Engine,
    engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD},
};
use domain::{EntityPath, NamespaceName, QueueConfig, StateMachine, codec, keys};
use prost::Message;
use server::{
    Broker, LocalProposer, MAX_NATIVE_QUEUE_SCAN_ROUNDS, MAX_NATIVE_QUEUE_SCAN_ROWS, ManualClock,
    NativeAdminService,
};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::StoreProvider;
use tonic::{Code, Request};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const DEADLINE: Duration = Duration::from_secs(5);
const RAW_PREFIX: &str = "queue.scan.v1.";

#[derive(Clone, Debug)]
struct Scan {
    limit: usize,
    rows: Vec<Key>,
}

#[derive(Default)]
struct Observations {
    reads: AtomicUsize,
    writes: AtomicUsize,
    scans: Mutex<Vec<Scan>>,
}

#[derive(Clone)]
struct ObservedStore<S> {
    inner: S,
    observations: Arc<Observations>,
}

impl<S: StateStore> StateStore for ObservedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.observations.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.get(key)
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.observations.writes.fetch_add(1, Ordering::SeqCst);
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
        assert!(limit <= domain::MAX_QUEUE_PAGE_SIZE + 1);
        self.observations.reads.fetch_add(1, Ordering::SeqCst);
        let rows = self.inner.scan_from(prefix, start, limit)?;
        self.observations.scans.lock().expect("scans").push(Scan {
            limit,
            rows: rows.iter().map(|(key, _)| key.clone()).collect(),
        });
        Ok(rows)
    }
}

struct Node<P: StoreProvider> {
    broker: Broker,
    service: NativeAdminService,
    store: ObservedStore<P::Store>,
    clock: ManualClock,
    provider: P,
}

impl<P: StoreProvider> Node<P> {
    fn start(provider: P) -> TestResult<Self> {
        let store = ObservedStore {
            inner: provider.open()?,
            observations: Arc::new(Observations::default()),
        };
        let clock = ManualClock::at(1_000);
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            clock.clone(),
        ));
        let service = NativeAdminService::new(broker.handle(), NamespaceName::new("tenant")?);
        Ok(Self {
            broker,
            service,
            store,
            clock,
            provider,
        })
    }

    fn reopen(self) -> TestResult<Self> {
        let Self {
            broker,
            service,
            store,
            clock: _,
            provider,
        } = self;
        drop(service);
        drop(broker);
        drop(store);
        Self::start(provider)
    }

    async fn create_queue(&self, path: &str) -> TestResult {
        tokio::time::timeout(
            DEADLINE,
            self.service
                .create_entity(Request::new(CreateEntityRequest {
                    namespace: "tenant".into(),
                    path: path.into(),
                    kind: EntityKind::Queue as i32,
                    ..CreateEntityRequest::default()
                })),
        )
        .await??;
        Ok(())
    }

    fn seed_hidden(&self, count: usize) -> TestResult {
        let namespace = NamespaceName::new("tenant")?;
        let mut batch = WriteBatch::default();
        for index in 0..count {
            batch.push_put(
                keys::queue_config(&namespace, &EntityPath::new(hidden(index))?),
                codec::encode(&QueueConfig::default())?,
            );
        }
        self.store.apply(batch)?;
        Ok(())
    }

    fn scans(&self) -> Vec<Scan> {
        std::mem::take(&mut *self.store.observations.scans.lock().expect("scans"))
    }

    fn reads(&self) -> usize {
        self.store.observations.reads.load(Ordering::SeqCst)
    }
    fn writes(&self) -> usize {
        self.store.observations.writes.load(Ordering::SeqCst)
    }
    fn snapshot(&self) -> TestResult<StoreSnapshot> {
        Ok(self.store.snapshot()?)
    }

    fn unchanged(&self, before: &StoreSnapshot, writes: usize) -> TestResult {
        assert_eq!(&self.snapshot()?, before);
        assert_eq!(self.writes(), writes);
        Ok(())
    }

    async fn page(&self, size: u32, token: &str) -> Result<ListEntitiesResponse, tonic::Status> {
        Ok(tokio::time::timeout(
            DEADLINE,
            self.service.list_entities(Request::new(list(size, token))),
        )
        .await
        .expect("bounded list")?
        .into_inner())
    }
}

fn hidden(index: usize) -> String {
    let path = format!("a/subscriptions/Member{:04}", index / 2);
    if index.is_multiple_of(2) {
        path
    } else {
        format!("{path}/$deadletterqueue")
    }
}

fn list(size: u32, token: &str) -> ListEntitiesRequest {
    ListEntitiesRequest {
        namespace: "tenant".into(),
        page_size: size,
        page_token: token.into(),
        ..ListEntitiesRequest::default()
    }
}

fn raw_cursor(token: &str) -> TestResult<GetEntityRequest> {
    Ok(GetEntityRequest::decode(
        URL_SAFE_NO_PAD
            .decode(token.strip_prefix(RAW_PREFIX).expect("raw queue cursor"))?
            .as_slice(),
    )?)
}

fn assert_bounds(scans: &[Scan]) {
    assert!(scans.len() <= MAX_NATIVE_QUEUE_SCAN_ROUNDS);
    assert!(scans.iter().map(|scan| scan.rows.len()).sum::<usize>() <= MAX_NATIVE_QUEUE_SCAN_ROWS);
    assert!(scans.iter().map(|scan| scan.limit).sum::<usize>() <= MAX_NATIVE_QUEUE_SCAN_ROWS);
}

fn paths(page: &ListEntitiesResponse) -> Vec<&str> {
    for entity in &page.entities {
        assert_eq!(entity.kind, EntityKind::Queue as i32);
        assert!(entity.queue_config.is_some());
        assert!(
            !EntityPath::new(&entity.path)
                .expect("entity path")
                .is_subscription_path()
        );
        assert!(
            !EntityPath::new(&entity.path)
                .expect("entity path")
                .is_dead_letter_queue()
        );
    }
    page.entities
        .iter()
        .map(|entity| entity.path.as_str())
        .collect()
}

async fn hidden_prefix_returns_empty_changing_progress_without_stamps<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.seed_hidden(80)?;
    node.create_queue("z-visible").await?;
    let before = node.snapshot()?;
    let writes = node.writes();
    let applied = node.broker.handle().last_applied_blocking()?;
    node.clock.set(0);
    node.scans();
    let mut token = String::new();
    for last_consumed in [31, 63] {
        let result = node.page(1, &token).await?;
        assert!(paths(&result).is_empty());
        assert_ne!(result.next_page_token, token);
        assert_eq!(
            raw_cursor(&result.next_page_token)?.path,
            hidden(last_consumed)
        );
        let scans = node.scans();
        assert_eq!(scans.len(), MAX_NATIVE_QUEUE_SCAN_ROUNDS);
        assert_eq!(scans.iter().map(|scan| scan.rows.len()).sum::<usize>(), 48);
        assert_bounds(&scans);
        token = result.next_page_token;
        node.unchanged(&before, writes)?;
    }
    let result = node.page(1, &token).await?;
    assert_eq!(paths(&result), ["z-visible"]);
    assert!(result.next_page_token.is_empty());
    assert_bounds(&node.scans());
    node.unchanged(&before, writes)?;
    assert_eq!(node.broker.handle().last_applied_blocking()?, applied);
    Ok(())
}

async fn partial_progress_resumes_without_repeating_the_visible_prefix<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.create_queue("0-first").await?;
    node.seed_hidden(40)?;
    node.create_queue("z-visible").await?;
    let before = node.snapshot()?;
    let writes = node.writes();
    let applied = node.broker.handle().last_applied_blocking()?;
    node.clock.set(0);
    node.scans();

    let first = node.page(2, "").await?;
    assert_eq!(paths(&first), ["0-first"]);
    assert_eq!(raw_cursor(&first.next_page_token)?.path, hidden(30));
    let scans = node.scans();
    assert_eq!(scans.len(), MAX_NATIVE_QUEUE_SCAN_ROUNDS);
    assert_eq!(scans[0].limit, 4);
    assert!(scans[1..].iter().all(|scan| scan.limit == 3));
    assert_eq!(scans.iter().map(|scan| scan.rows.len()).sum::<usize>(), 49);
    assert_bounds(&scans);

    let resumed = node.page(2, &first.next_page_token).await?;
    assert_eq!(paths(&resumed), ["z-visible"]);
    assert!(resumed.next_page_token.is_empty());
    assert_bounds(&node.scans());
    assert_eq!(node.page(2, "").await?, first);
    assert_bounds(&node.scans());
    node.unchanged(&before, writes)?;
    assert_eq!(node.broker.handle().last_applied_blocking()?, applied);
    Ok(())
}

async fn physical_row_budget_preserves_the_final_backend_lookahead<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.seed_hidden(4_092)?;
    node.create_queue("z-visible").await?;
    let before = node.snapshot()?;
    let writes = node.writes();
    let applied = node.broker.handle().last_applied_blocking()?;
    node.clock.set(0);
    node.scans();
    let first = node.page(1_024, "").await?;
    assert!(paths(&first).is_empty());
    assert_eq!(raw_cursor(&first.next_page_token)?.path, hidden(4_091));
    let scans = node.scans();
    assert_eq!(scans.len(), 4);
    assert_eq!(
        scans.iter().map(|scan| scan.rows.len()).sum::<usize>(),
        MAX_NATIVE_QUEUE_SCAN_ROWS
    );
    assert_eq!(
        scans.iter().map(|scan| scan.limit).collect::<Vec<_>>(),
        [1_025, 1_025, 1_025, 1_021]
    );
    assert_eq!(
        scans.last().expect("last scan").rows.last(),
        Some(&keys::queue_config(
            &NamespaceName::new("tenant")?,
            &EntityPath::new("z-visible")?
        ))
    );
    assert_bounds(&scans);
    let resumed = node.page(1_024, &first.next_page_token).await?;
    assert_eq!(paths(&resumed), ["z-visible"]);
    assert!(resumed.next_page_token.is_empty());
    assert_bounds(&node.scans());
    node.unchanged(&before, writes)?;
    assert_eq!(node.broker.handle().last_applied_blocking()?, applied);
    Ok(())
}

async fn deleted_raw_marker_resumes_after_reopen<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::start(provider)?;
    node.seed_hidden(40)?;
    node.create_queue("z-visible").await?;
    let first = node.page(1, "").await?;
    assert!(first.entities.is_empty());
    let marker = raw_cursor(&first.next_page_token)?;
    let mut batch = WriteBatch::default();
    batch.push_delete(keys::queue_config(
        &NamespaceName::new(&marker.namespace)?,
        &EntityPath::new(&marker.path)?,
    ));
    node.store.apply(batch)?;
    let before = node.snapshot()?;
    let node = node.reopen()?;
    assert_eq!(node.snapshot()?, before);
    let writes = node.writes();
    let applied = node.broker.handle().last_applied_blocking()?;
    node.clock.set(0);
    let result = node.page(1, &first.next_page_token).await?;
    assert_eq!(paths(&result), ["z-visible"]);
    assert!(result.next_page_token.is_empty());
    assert_bounds(&node.scans());
    node.unchanged(&before, writes)?;
    assert_eq!(node.broker.handle().last_applied_blocking()?, applied);
    Ok(())
}

async fn exact_backend_exhaustion_has_no_false_progress_token<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    for (size, hidden_count, rounds, rows) in [(1, 32, 16, 47), (1_024, 4_092, 4, 4_095)] {
        node.seed_hidden(hidden_count)?;
        let before = node.snapshot()?;
        let writes = node.writes();
        let applied = node.broker.handle().last_applied_blocking()?;
        node.clock.set(0);
        node.scans();
        let result = node.page(size, "").await?;
        assert!(paths(&result).is_empty());
        assert!(result.next_page_token.is_empty());
        let scans = node.scans();
        assert_eq!(scans.len(), rounds);
        assert_eq!(
            scans.iter().map(|scan| scan.rows.len()).sum::<usize>(),
            rows
        );
        assert_bounds(&scans);
        node.unchanged(&before, writes)?;
        assert_eq!(node.broker.handle().last_applied_blocking()?, applied);
    }
    Ok(())
}

async fn dense_queue_pages_keep_legacy_tokens_and_ignore_hidden_shadows<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    for path in ["alpha", "omega"] {
        node.create_queue(path).await?;
    }
    let before = node.snapshot()?;
    let writes = node.writes();
    let applied = node.broker.handle().last_applied_blocking()?;
    node.clock.set(0);
    node.scans();
    let first = node.page(1, "").await?;
    assert_eq!(paths(&first), ["alpha"]);
    assert!(first.next_page_token.starts_with("v1."));
    let legacy = GetEntityRequest {
        namespace: "tenant".into(),
        path: "alpha".into(),
    };
    assert_eq!(
        first.next_page_token,
        format!("v1.{}", URL_SAFE_NO_PAD.encode(legacy.encode_to_vec()))
    );
    assert_bounds(&node.scans());
    let last = node.page(1, &first.next_page_token).await?;
    assert_eq!(paths(&last), ["omega"]);
    assert!(last.next_page_token.is_empty());
    assert_bounds(&node.scans());
    node.unchanged(&before, writes)?;
    assert_eq!(node.broker.handle().last_applied_blocking()?, applied);
    Ok(())
}

#[derive(Clone, PartialEq, Message)]
struct UnknownCursorField {
    #[prost(bool, tag = "3")]
    ignored: bool,
}

async fn raw_cursor_context_and_noncanonical_encodings_fail_before_owner_io<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    node.seed_hidden(40)?;
    node.create_queue("z-visible").await?;
    let first = node.page(1, "").await?;
    let cursor = raw_cursor(&first.next_page_token)?;
    let canonical = cursor.encode_to_vec();
    let mut duplicate = canonical.clone();
    duplicate.extend(
        GetEntityRequest {
            namespace: cursor.namespace.clone(),
            path: String::new(),
        }
        .encode_to_vec(),
    );
    let mut unknown = canonical.clone();
    unknown.extend(UnknownCursorField { ignored: true }.encode_to_vec());
    let mut reordered = GetEntityRequest {
        namespace: String::new(),
        path: cursor.path.clone(),
    }
    .encode_to_vec();
    reordered.extend(
        GetEntityRequest {
            namespace: cursor.namespace.clone(),
            path: String::new(),
        }
        .encode_to_vec(),
    );
    let mut tokens = Vec::new();
    for bytes in [duplicate, unknown, reordered] {
        assert_eq!(GetEntityRequest::decode(bytes.as_slice())?, cursor);
        assert_ne!(bytes, canonical);
        tokens.push(format!("{RAW_PREFIX}{}", URL_SAFE_NO_PAD.encode(bytes)));
    }
    let padded = URL_SAFE.encode(&canonical);
    assert_ne!(
        padded,
        URL_SAFE_NO_PAD.encode(&canonical),
        "fixture requires padding"
    );
    assert_eq!(URL_SAFE.decode(&padded)?, canonical);
    tokens.extend([
        format!("{RAW_PREFIX}{padded}"),
        "queue.scan.v1.!".into(),
        "queue.scan.v2.AA".into(),
        format!("{RAW_PREFIX}{}", URL_SAFE_NO_PAD.encode([255])),
        format!(
            "{RAW_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(
                GetEntityRequest {
                    namespace: "neighbor".into(),
                    path: cursor.path.clone()
                }
                .encode_to_vec()
            )
        ),
        format!(
            "{RAW_PREFIX}{}",
            URL_SAFE_NO_PAD.encode(
                GetEntityRequest {
                    namespace: "tenant".into(),
                    path: String::new()
                }
                .encode_to_vec()
            )
        ),
        "x".repeat(513),
        format!("v1.{}", URL_SAFE_NO_PAD.encode(&canonical)),
        format!("topic.v1.{}", URL_SAFE_NO_PAD.encode(&canonical)),
        format!("subscription.v1.{}", URL_SAFE_NO_PAD.encode(&canonical)),
    ]);
    let before = node.snapshot()?;
    let writes = node.writes();
    let applied = node.broker.handle().last_applied_blocking()?;
    node.clock.set(0);
    node.scans();
    for token in tokens {
        let reads = node.reads();
        assert_eq!(
            node.page(1, &token)
                .await
                .expect_err("invalid cursor")
                .code(),
            Code::InvalidArgument
        );
        assert_eq!(node.reads(), reads, "bad cursor must not reach owner I/O");
        assert!(node.scans().is_empty());
        node.unchanged(&before, writes)?;
        assert_eq!(node.broker.handle().last_applied_blocking()?, applied);
    }
    for kind in [EntityKind::Topic, EntityKind::Subscription] {
        let reads = node.reads();
        let mut request = list(1, &first.next_page_token);
        request.kind = kind as i32;
        if kind == EntityKind::Subscription {
            request.parent_topic = "a".into();
        }
        let result =
            tokio::time::timeout(DEADLINE, node.service.list_entities(Request::new(request)))
                .await?;
        assert_eq!(
            result.expect_err("crossed cursor family").code(),
            Code::InvalidArgument
        );
        assert_eq!(node.reads(), reads);
        assert!(node.scans().is_empty());
    }
    let last = node.page(1, &first.next_page_token).await?;
    assert_eq!(paths(&last), ["z-visible"]);
    assert!(last.next_page_token.is_empty());
    assert_bounds(&node.scans());
    node.unchanged(&before, writes)?;
    assert_eq!(node.broker.handle().last_applied_blocking()?, applied);
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case() -> super::TestResult { tokio::time::timeout(super::DEADLINE * 12, super::$case(::testkit::MemoryProvider::new())).await? })+ }
        mod durable { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)] async fn $case() -> super::TestResult { tokio::time::timeout(super::DEADLINE * 12, super::$case(::testkit::DurableProvider::temporary()?)).await? })+ }
    };
}

for_each_backend! {
    hidden_prefix_returns_empty_changing_progress_without_stamps,
    partial_progress_resumes_without_repeating_the_visible_prefix,
    physical_row_budget_preserves_the_final_backend_lookahead,
    deleted_raw_marker_resumes_after_reopen,
    exact_backend_exhaustion_has_no_false_progress_token,
    dense_queue_pages_keep_legacy_tokens_and_ignore_hidden_shadows,
    raw_cursor_context_and_noncanonical_encodings_fail_before_owner_io,
}

//! Hidden scan positions never widen native queue-list authorization.

use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use admin_api::v1::{
    EntityKind, GetEntityRequest, ListEntitiesRequest, entity_service_server::EntityService,
};
use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use domain::{
    CommandKind, EntityPath, NamespaceName, QueueConfig, StateMachine, SubscriptionConfig,
    SubscriptionName, Timestamp, TopicConfig,
};
use hmac::{Hmac, Mac};
use prost::Message;
use server::{Broker, Clock, LocalProposer, ManualClock, NativeAdminService};
use sha2::Sha256;
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::StoreProvider;
use tonic::{Code, Request};
use url::form_urlencoded::byte_serialize;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const HOST: &str = "tenant.servicebus.windows.net";
const KEY: &str = "queue-scan-authorization-secret";
const EXPIRY: u64 = 4_102_444_800;
const DEADLINE: Duration = Duration::from_secs(5);
const SCAN_PREFIX: &str = "queue.scan.v1.";

#[derive(Default)]
struct Observations {
    reads: AtomicUsize,
    writes: AtomicUsize,
    clock_reads: AtomicUsize,
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
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.observations.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.scan_from(prefix, start, limit)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.inner.snapshot()
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.observations.writes.fetch_add(1, Ordering::SeqCst);
        self.inner.apply(batch)
    }
}

#[derive(Clone)]
struct CountedClock {
    value: ManualClock,
    observations: Arc<Observations>,
}

impl Clock for CountedClock {
    fn now(&self) -> Timestamp {
        self.observations.clock_reads.fetch_add(1, Ordering::SeqCst);
        self.value.now()
    }
}

struct Guard {
    reads: usize,
    writes: usize,
    clock_reads: usize,
    snapshot: StoreSnapshot,
    applied: Timestamp,
}

struct Node<P: StoreProvider> {
    broker: Broker,
    store: ObservedStore<P::Store>,
    service: NativeAdminService,
    observations: Arc<Observations>,
    clock: ManualClock,
    _provider: P,
}

impl<P: StoreProvider> Node<P> {
    fn start(provider: P) -> TestResult<Self> {
        let observations = Arc::new(Observations::default());
        let store = ObservedStore {
            inner: provider.open()?,
            observations: observations.clone(),
        };
        let clock = ManualClock::at(10_000);
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            CountedClock {
                value: clock.clone(),
                observations: observations.clone(),
            },
        ));
        let namespace = NamespaceName::new("tenant")?;
        let topic = EntityPath::new("a-hidden")?;
        broker.handle().submit_blocking(
            namespace.clone(),
            topic.clone(),
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        )?;
        for index in 0..domain::MAX_TOPIC_SUBSCRIPTIONS {
            broker.handle().submit_blocking(
                namespace.clone(),
                topic.clone(),
                CommandKind::CreateSubscription {
                    name: SubscriptionName::new(format!("Member-{index:02}"))?,
                    config: SubscriptionConfig::default(),
                },
            )?;
        }
        for path in ["z-visible", "zz-last"] {
            broker.handle().submit_blocking(
                namespace.clone(),
                EntityPath::new(path)?,
                CommandKind::CreateQueue {
                    config: QueueConfig::default(),
                },
            )?;
        }
        let policy = SharedAccessPolicy::new([
            SharedAccessRule::new(
                "manage",
                ResourceScope::namespace(HOST)?,
                SharedAccessKey::new(KEY)?,
                None,
                PermissionSet::MANAGE,
            )?,
            SharedAccessRule::new(
                "send",
                ResourceScope::namespace(HOST)?,
                SharedAccessKey::new(KEY)?,
                None,
                PermissionSet::SEND,
            )?,
            SharedAccessRule::new(
                "listen",
                ResourceScope::namespace(HOST)?,
                SharedAccessKey::new(KEY)?,
                None,
                PermissionSet::LISTEN,
            )?,
        ])?;
        let service = NativeAdminService::new(broker.handle(), namespace)
            .with_shared_access_policy(policy, HOST)?;
        Ok(Self {
            broker,
            store,
            service,
            observations,
            clock,
            _provider: provider,
        })
    }

    fn guard(&self) -> TestResult<Guard> {
        let applied = self.broker.handle().last_applied_blocking()?;
        Ok(Guard {
            reads: self.observations.reads.load(Ordering::SeqCst),
            writes: self.observations.writes.load(Ordering::SeqCst),
            clock_reads: self.observations.clock_reads.load(Ordering::SeqCst),
            snapshot: self.store.snapshot()?,
            applied,
        })
    }

    fn unchanged(&self, before: &Guard, allow_reads: bool) -> TestResult {
        if !allow_reads {
            assert_eq!(
                self.observations.reads.load(Ordering::SeqCst),
                before.reads,
                "refused scan cursor never reaches the owner"
            );
        }
        assert_eq!(
            self.observations.writes.load(Ordering::SeqCst),
            before.writes
        );
        assert_eq!(
            self.observations.clock_reads.load(Ordering::SeqCst),
            before.clock_reads
        );
        assert_eq!(self.store.snapshot()?, before.snapshot);
        assert_eq!(
            self.broker.handle().last_applied_blocking()?,
            before.applied
        );
        Ok(())
    }

    async fn progress(&self) -> TestResult<String> {
        let page = self
            .service
            .list_entities(authorized(list(""), &sas("", "manage", EXPIRY)))
            .await?
            .into_inner();
        assert!(page.entities.is_empty());
        assert!(page.next_page_token.starts_with(SCAN_PREFIX));
        Ok(page.next_page_token)
    }
}

fn signed_resource(resource: &str, rule: &str, expiry: u64) -> String {
    let resource = byte_serialize(resource.as_bytes()).collect::<String>();
    let mut mac = Hmac::<Sha256>::new_from_slice(KEY.as_bytes()).expect("HMAC key");
    mac.update(format!("{resource}\n{expiry}").as_bytes());
    let signature =
        byte_serialize(STANDARD.encode(mac.finalize().into_bytes()).as_bytes()).collect::<String>();
    format!("SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn={rule}")
}

fn sas(path: &str, rule: &str, expiry: u64) -> String {
    signed_resource(
        &if path.is_empty() {
            format!("amqps://{HOST}")
        } else {
            format!("amqps://{HOST}/{path}")
        },
        rule,
        expiry,
    )
}

fn authorized<T>(input: T, token: &str) -> Request<T> {
    let mut request = Request::new(input);
    request.set_timeout(DEADLINE);
    request
        .metadata_mut()
        .insert("authorization", token.parse().expect("ASCII SAS token"));
    request
}

fn list(cursor: &str) -> ListEntitiesRequest {
    ListEntitiesRequest {
        namespace: "tenant".into(),
        page_token: cursor.into(),
        page_size: 1,
        ..Default::default()
    }
}

fn raw_cursor(namespace: &str, path: &str) -> String {
    format!(
        "{SCAN_PREFIX}{}",
        URL_SAFE_NO_PAD.encode(
            GetEntityRequest {
                namespace: namespace.into(),
                path: path.into()
            }
            .encode_to_vec()
        )
    )
}

fn code<T>(result: Result<T, tonic::Status>, expected: Code) {
    match result {
        Err(error) => assert_eq!(error.code(), expected, "{error}"),
        Ok(_) => panic!("request unexpectedly succeeded; wanted {expected:?}"),
    }
}

async fn valid_scan_tokens_still_require_namespace_manage<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    let cursor = node.progress().await?;
    node.clock.set(0);
    for token in [
        sas("a-hidden", "manage", EXPIRY),
        sas("a-hidden/subscriptions/Member-00", "manage", EXPIRY),
        sas(
            "a-hidden/subscriptions/Member-00/$management",
            "manage",
            EXPIRY,
        ),
        sas("z-visible", "manage", EXPIRY),
        sas("", "send", EXPIRY),
        sas("", "listen", EXPIRY),
    ] {
        let before = node.guard()?;
        code(
            node.service
                .list_entities(authorized(list(&cursor), &token))
                .await,
            Code::PermissionDenied,
        );
        node.unchanged(&before, false)?;
    }
    let before = node.guard()?;
    let page = node
        .service
        .list_entities(authorized(list(&cursor), &sas("", "manage", EXPIRY)))
        .await?
        .into_inner();
    assert_ne!(page.next_page_token, cursor);
    node.unchanged(&before, true)?;
    Ok(())
}

async fn expired_forged_and_foreign_tokens_refuse_before_cursor_io<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    let cursor = node.progress().await?;
    node.clock.set(0);
    for token in [
        sas("", "manage", 1),
        sas("", "manage", EXPIRY).replace("sig=", "sig=bad"),
        signed_resource("amqps://other.servicebus.windows.net", "manage", EXPIRY),
    ] {
        let before = node.guard()?;
        code(
            node.service
                .list_entities(authorized(list(&cursor), &token))
                .await,
            Code::Unauthenticated,
        );
        node.unchanged(&before, false)?;
    }
    let before = node.guard()?;
    code(
        node.service
            .list_entities(Request::new(list(&cursor)))
            .await,
        Code::Unauthenticated,
    );
    node.unchanged(&before, false)?;
    Ok(())
}

async fn wrong_namespace_and_noncanonical_scan_tokens_refuse_before_owner_io<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    let cursor = node.progress().await?;
    let payload = cursor.strip_prefix(SCAN_PREFIX).expect("scan payload");
    let mut unknown_field = URL_SAFE_NO_PAD.decode(payload)?;
    unknown_field.extend_from_slice(&[0x18, 1]);
    let token = sas("", "manage", EXPIRY);
    node.clock.set(0);
    for malformed in [
        raw_cursor("foreign", "a-hidden/subscriptions/Member-00"),
        "queue.scan.v1.%".into(),
        format!("{cursor}="),
        format!("topic.v1.{payload}"),
        format!("subscription.v1.{payload}"),
        format!("{SCAN_PREFIX}{}", URL_SAFE_NO_PAD.encode(unknown_field)),
        raw_cursor("tenant", ""),
        raw_cursor("tenant", "a\0forged"),
        raw_cursor("tenant", &"a".repeat(domain::MAX_ENTITY_PATH_BYTES + 1)),
        format!("v1.{payload}"),
        format!("{SCAN_PREFIX}{}", "a".repeat(513)),
    ] {
        let before = node.guard()?;
        code(
            node.service
                .list_entities(authorized(list(&malformed), &token))
                .await,
            Code::InvalidArgument,
        );
        node.unchanged(&before, false)?;
    }
    let before = node.guard()?;
    let mut foreign = list(&cursor);
    foreign.namespace = "foreign".into();
    code(
        node.service
            .list_entities(authorized(foreign, &token))
            .await,
        Code::PermissionDenied,
    );
    node.unchanged(&before, false)?;
    let before = node.guard()?;
    let mut wrong_family = list(&cursor);
    wrong_family.kind = EntityKind::Topic as i32;
    code(
        node.service
            .list_entities(authorized(wrong_family, &token))
            .await,
        Code::InvalidArgument,
    );
    node.unchanged(&before, false)?;
    let before = node.guard()?;
    let mut wrong_family = list(&cursor);
    wrong_family.kind = EntityKind::Subscription as i32;
    wrong_family.parent_topic = "a-hidden".into();
    code(
        node.service
            .list_entities(authorized(wrong_family, &token))
            .await,
        Code::InvalidArgument,
    );
    node.unchanged(&before, false)?;
    Ok(())
}

async fn raw_positions_keep_literal_user_case_and_never_stamp_clock<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider)?;
    let token = sas("", "manage", EXPIRY);
    node.clock.set(0);
    let before = node.guard()?;
    let upper = node
        .service
        .list_entities(authorized(
            list(&raw_cursor(
                "tenant",
                "a-hidden/subscriptions/Member-00/$deadletterqueue",
            )),
            &token,
        ))
        .await?
        .into_inner();
    assert!(upper.entities.is_empty());
    assert!(upper.next_page_token.starts_with(SCAN_PREFIX));
    let lower = node
        .service
        .list_entities(authorized(
            list(&raw_cursor(
                "tenant",
                "a-hidden/subscriptions/member-00/$deadletterqueue",
            )),
            &token,
        ))
        .await?
        .into_inner();
    assert_eq!(lower.entities.len(), 1);
    assert_eq!(lower.entities[0].path, "z-visible");
    assert_eq!(lower.entities[0].kind, EntityKind::Queue as i32);
    assert!(lower.next_page_token.starts_with("v1."));
    node.unchanged(&before, true)?;
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident,)+) => {
        mod memory { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $case() -> super::TestResult {
                tokio::time::timeout(super::DEADLINE * 6,
                    super::$case(::testkit::MemoryProvider::new())).await?
            })+ }
        mod durable { $(#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
            async fn $case() -> super::TestResult {
                tokio::time::timeout(super::DEADLINE * 6,
                    super::$case(::testkit::DurableProvider::temporary()?)).await?
            })+ }
    };
}

for_each_backend! {
    valid_scan_tokens_still_require_namespace_manage,
    expired_forged_and_foreign_tokens_refuse_before_cursor_io,
    wrong_namespace_and_noncanonical_scan_tokens_refuse_before_owner_io,
    raw_positions_keep_literal_user_case_and_never_stamp_clock,
}

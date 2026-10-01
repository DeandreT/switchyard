//! Native topic administration preserves literal SAS scopes before owner access.

use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use admin_api::v1::{
    CreateEntityRequest, EntityKind, GetEntityRequest, ListEntitiesRequest,
    entity_service_server::EntityService,
};
use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use base64::{Engine, engine::general_purpose::STANDARD};
use domain::{
    CommandKind, EntityPath, NamespaceName, StateMachine, SubscriptionConfig, SubscriptionName,
    Timestamp, TopicConfig,
};
use hmac::{Hmac, Mac};
use server::{Broker, Clock, LocalProposer, ManualClock, NativeAdminService};
use sha2::Sha256;
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::StoreProvider;
use tonic::{Code, Request};
use url::form_urlencoded::byte_serialize;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const HOST: &str = "tenant.servicebus.windows.net";
const KEY: &str = "native-topic-scope-secret";
const EXPIRY: u64 = 4_102_444_800;
const DEADLINE: Duration = Duration::from_secs(5);

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

struct Node<P: StoreProvider> {
    broker: Broker,
    store: ObservedStore<P::Store>,
    clock: ManualClock,
    observations: Arc<Observations>,
    service: NativeAdminService,
    _provider: P,
}

struct Guard {
    reads: usize,
    writes: usize,
    clock_reads: usize,
    snapshot: StoreSnapshot,
    applied: Timestamp,
}

impl<P: StoreProvider> Node<P> {
    fn start(provider: P, scope_path: Option<&str>) -> TestResult<Self> {
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
        for topic in [
            "Orders",
            "orders",
            "Orders-old",
            "Events/$Management",
            "Events/$management",
        ] {
            broker.handle().submit_blocking(
                namespace.clone(),
                EntityPath::new(topic)?,
                CommandKind::CreateTopic {
                    config: TopicConfig::default(),
                },
            )?;
            for name in ["Accounting", "accounting", "Billing"] {
                broker.handle().submit_blocking(
                    namespace.clone(),
                    EntityPath::new(topic)?,
                    CommandKind::CreateSubscription {
                        name: SubscriptionName::new(name)?,
                        config: SubscriptionConfig::default(),
                    },
                )?;
            }
        }
        broker.handle().submit_blocking(
            namespace.clone(),
            EntityPath::new("Ordinary")?,
            CommandKind::CreateQueue {
                config: domain::QueueConfig::default(),
            },
        )?;
        let scope = match scope_path {
            Some(path) => ResourceScope::entity(HOST, path)?,
            None => ResourceScope::namespace(HOST)?,
        };
        let policy = SharedAccessPolicy::new([
            SharedAccessRule::new(
                "manage",
                scope.clone(),
                SharedAccessKey::new(KEY)?,
                None,
                PermissionSet::MANAGE,
            )?,
            SharedAccessRule::new(
                "send",
                scope.clone(),
                SharedAccessKey::new(KEY)?,
                None,
                PermissionSet::SEND,
            )?,
            SharedAccessRule::new(
                "listen",
                scope,
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
            clock,
            observations,
            service,
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
                "refused administration must not reach the owner"
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

fn signed_resource(audience: &str, rule: &str, expiry: u64) -> String {
    let resource = byte_serialize(audience.as_bytes()).collect::<String>();
    let mut mac = Hmac::<Sha256>::new_from_slice(KEY.as_bytes()).expect("HMAC key");
    mac.update(format!("{resource}\n{expiry}").as_bytes());
    let signature =
        byte_serialize(STANDARD.encode(mac.finalize().into_bytes()).as_bytes()).collect::<String>();
    format!("SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn={rule}")
}

fn authorized<T>(body: T, token: &str) -> Request<T> {
    let mut request = Request::new(body);
    request.set_timeout(DEADLINE);
    request
        .metadata_mut()
        .insert("authorization", token.parse().expect("ASCII token"));
    request
}

fn create(path: &str, kind: EntityKind) -> CreateEntityRequest {
    CreateEntityRequest {
        namespace: "tenant".into(),
        path: path.into(),
        kind: kind as i32,
        ..Default::default()
    }
}

fn get(path: &str) -> GetEntityRequest {
    GetEntityRequest {
        namespace: "tenant".into(),
        path: path.into(),
    }
}

fn list(kind: EntityKind, parent: &str) -> ListEntitiesRequest {
    ListEntitiesRequest {
        namespace: "tenant".into(),
        kind: kind as i32,
        parent_topic: parent.into(),
        ..Default::default()
    }
}

fn code<T>(result: Result<T, tonic::Status>, expected: Code) {
    match result {
        Err(error) => assert_eq!(error.code(), expected, "{error}"),
        Ok(_) => panic!("request unexpectedly succeeded; wanted {expected:?}"),
    }
}

async fn exact_child_manage_can_create_and_get_but_cannot_list_siblings<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let path = "Orders/subscriptions/Private";
    let node = Node::start(provider, Some(path))?;
    let token = sas(path, "manage", EXPIRY);
    let created = node
        .service
        .create_entity(authorized(
            create("Orders/SUBSCRIPTIONS/Private", EntityKind::Subscription),
            &token,
        ))
        .await?
        .into_inner();
    assert_eq!(created.kind, EntityKind::Subscription as i32);
    assert_eq!(created.path, path);
    assert!(created.subscription_config.is_some());
    node.clock.set(0);
    let before = node.guard()?;
    let found = node
        .service
        .get_entity(authorized(get("Orders/Subscriptions/Private"), &token))
        .await?
        .into_inner();
    assert_eq!(found, created);
    node.unchanged(&before, true)?;
    for denied in [
        "Orders",
        "Orders/subscriptions/Billing",
        "Orders/subscriptions/Missing",
        "orders/subscriptions/Private",
        "Orders/subscriptions/private",
        "Orders-old",
        "Ordinary",
    ] {
        let before = node.guard()?;
        code(
            node.service
                .get_entity(authorized(get(denied), &token))
                .await,
            Code::PermissionDenied,
        );
        node.unchanged(&before, false)?;
    }
    for denied in [
        create("Orders", EntityKind::Topic),
        create("Orders/subscriptions/Other", EntityKind::Subscription),
    ] {
        let before = node.guard()?;
        code(
            node.service.create_entity(authorized(denied, &token)).await,
            Code::PermissionDenied,
        );
        node.unchanged(&before, false)?;
    }
    for denied in [
        list(EntityKind::Subscription, "Orders"),
        list(EntityKind::Topic, ""),
        list(EntityKind::Unspecified, ""),
    ] {
        let before = node.guard()?;
        code(
            node.service.list_entities(authorized(denied, &token)).await,
            Code::PermissionDenied,
        );
        node.unchanged(&before, false)?;
    }
    Ok(())
}

async fn topic_manage_inherits_children_without_neighbor_or_user_case_aliases<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, Some("Orders"))?;
    let token = sas("Orders", "manage", EXPIRY);
    let created = node
        .service
        .create_entity(authorized(
            create("Orders/Subscriptions/NewMember", EntityKind::Subscription),
            &token,
        ))
        .await?
        .into_inner();
    assert_eq!(created.path, "Orders/subscriptions/NewMember");
    node.clock.set(0);
    let before = node.guard()?;
    for path in [
        "Orders",
        "Orders/Subscriptions/Accounting",
        "Orders/subscriptions/Billing",
    ] {
        node.service
            .get_entity(authorized(get(path), &token))
            .await?;
    }
    let members = node
        .service
        .list_entities(authorized(list(EntityKind::Subscription, "Orders"), &token))
        .await?
        .into_inner();
    assert_eq!(members.entities.len(), 4);
    assert!(members.next_page_token.is_empty());
    assert!(
        members
            .entities
            .iter()
            .all(|entity| entity.path.starts_with("Orders/subscriptions/"))
    );
    node.unchanged(&before, true)?;
    for path in [
        "orders",
        "orders/subscriptions/Accounting",
        "Orders-old",
        "Orders-old/subscriptions/Accounting",
        "Ordinary",
    ] {
        let before = node.guard()?;
        code(
            node.service.get_entity(authorized(get(path), &token)).await,
            Code::PermissionDenied,
        );
        node.unchanged(&before, false)?;
    }
    for parent in ["orders", "Orders-old"] {
        let before = node.guard()?;
        code(
            node.service
                .list_entities(authorized(list(EntityKind::Subscription, parent), &token))
                .await,
            Code::PermissionDenied,
        );
        node.unchanged(&before, false)?;
    }
    let before = node.guard()?;
    code(
        node.service
            .list_entities(authorized(list(EntityKind::Topic, ""), &token))
            .await,
        Code::PermissionDenied,
    );
    node.unchanged(&before, false)?;
    Ok(())
}

async fn credentials_permissions_and_namespaces_refuse_before_owner_access<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, None)?;
    node.clock.set(0);
    for (token, expected) in [
        (sas("", "send", EXPIRY), Code::PermissionDenied),
        (sas("", "listen", EXPIRY), Code::PermissionDenied),
        (sas("", "manage", 1), Code::Unauthenticated),
        (
            sas("", "manage", EXPIRY).replace("sig=", "sig=bad"),
            Code::Unauthenticated,
        ),
        (sas("", "unknown", EXPIRY), Code::Unauthenticated),
        (
            signed_resource("amqps://foreign.servicebus.windows.net", "manage", EXPIRY),
            Code::Unauthenticated,
        ),
    ] {
        let before = node.guard()?;
        code(
            node.service
                .get_entity(authorized(get("Orders"), &token))
                .await,
            expected,
        );
        code(
            node.service
                .create_entity(authorized(create("NewTopic", EntityKind::Topic), &token))
                .await,
            expected,
        );
        code(
            node.service
                .create_entity(authorized(
                    create("Orders/subscriptions/NewMember", EntityKind::Subscription),
                    &token,
                ))
                .await,
            expected,
        );
        code(
            node.service
                .list_entities(authorized(list(EntityKind::Topic, ""), &token))
                .await,
            expected,
        );
        code(
            node.service
                .list_entities(authorized(list(EntityKind::Subscription, "Orders"), &token))
                .await,
            expected,
        );
        node.unchanged(&before, false)?;
    }
    let namespace_token = sas("", "manage", EXPIRY);
    let before = node.guard()?;
    let mut foreign = get("Orders");
    foreign.namespace = "foreign".into();
    code(
        node.service
            .get_entity(authorized(foreign, &namespace_token))
            .await,
        Code::PermissionDenied,
    );
    let mut foreign = create("Orders/subscriptions/Foreign", EntityKind::Subscription);
    foreign.namespace = "foreign".into();
    code(
        node.service
            .create_entity(authorized(foreign, &namespace_token))
            .await,
        Code::PermissionDenied,
    );
    let mut foreign = list(EntityKind::Subscription, "Orders");
    foreign.namespace = "foreign".into();
    code(
        node.service
            .list_entities(authorized(foreign, &namespace_token))
            .await,
        Code::PermissionDenied,
    );
    code(
        node.service
            .create_entity(Request::new(create("", EntityKind::Subscription)))
            .await,
        Code::Unauthenticated,
    );
    code(
        node.service.get_entity(Request::new(get(""))).await,
        Code::Unauthenticated,
    );
    code(
        node.service
            .list_entities(Request::new(list(EntityKind::Subscription, "")))
            .await,
        Code::Unauthenticated,
    );
    node.unchanged(&before, false)?;
    Ok(())
}

async fn control_endpoint_tokens_do_not_administer_base_entities<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, None)?;
    node.clock.set(0);
    for endpoint in [
        "Orders/$management",
        "Orders/subscriptions/Accounting/$management",
        "Orders/subscriptions/Accounting/$deadletterqueue",
        "Orders/subscriptions/Accounting/$deadletterqueue/$management",
    ] {
        let token = sas(endpoint, "manage", EXPIRY);
        let before = node.guard()?;
        code(
            node.service
                .get_entity(authorized(get("Orders"), &token))
                .await,
            Code::PermissionDenied,
        );
        code(
            node.service
                .get_entity(authorized(get("Orders/subscriptions/Accounting"), &token))
                .await,
            Code::PermissionDenied,
        );
        code(
            node.service
                .create_entity(authorized(
                    create("Orders/subscriptions/Accounting", EntityKind::Subscription),
                    &token,
                ))
                .await,
            Code::PermissionDenied,
        );
        code(
            node.service
                .list_entities(authorized(list(EntityKind::Subscription, "Orders"), &token))
                .await,
            Code::PermissionDenied,
        );
        code(
            node.service
                .list_entities(authorized(list(EntityKind::Topic, ""), &token))
                .await,
            Code::PermissionDenied,
        );
        node.unchanged(&before, false)?;
    }
    Ok(())
}

async fn literal_control_parent_names_and_signed_separators_remain_exact<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let parent = "Events/$Management";
    let node = Node::start(provider, Some(parent))?;
    let token = sas(parent, "manage", EXPIRY);
    node.clock.set(0);
    let before = node.guard()?;
    assert_eq!(
        node.service
            .get_entity(authorized(
                get("Events/$Management/SUBSCRIPTIONS/Accounting"),
                &token
            ))
            .await?
            .into_inner()
            .path,
        "Events/$Management/subscriptions/Accounting"
    );
    assert_eq!(
        node.service
            .list_entities(authorized(list(EntityKind::Subscription, parent), &token))
            .await?
            .into_inner()
            .entities
            .len(),
        3
    );
    node.unchanged(&before, true)?;
    for path in [
        "Events/$management",
        "Events/$management/subscriptions/Accounting",
    ] {
        let before = node.guard()?;
        code(
            node.service.get_entity(authorized(get(path), &token)).await,
            Code::PermissionDenied,
        );
        node.unchanged(&before, false)?;
    }
    let before = node.guard()?;
    code(
        node.service
            .list_entities(authorized(
                list(EntityKind::Subscription, "Events/$management"),
                &token,
            ))
            .await,
        Code::PermissionDenied,
    );
    node.unchanged(&before, false)?;

    let exact_policy = SharedAccessPolicy::new([SharedAccessRule::new(
        "manage",
        ResourceScope::entity(HOST, "Orders/subscriptions/Accounting")?,
        SharedAccessKey::new(KEY)?,
        None,
        PermissionSet::MANAGE,
    )?])?;
    let exact_service =
        NativeAdminService::new(node.broker.handle(), NamespaceName::new("tenant")?)
            .with_shared_access_policy(exact_policy, HOST)?;
    let before = node.guard()?;
    let signed_alias = sas("Orders/Subscriptions/Accounting", "manage", EXPIRY);
    code(
        exact_service
            .get_entity(authorized(
                get("Orders/Subscriptions/Accounting"),
                &signed_alias,
            ))
            .await,
        Code::Unauthenticated,
    );
    node.unchanged(&before, false)?;
    let canonical = sas("Orders/subscriptions/Accounting", "manage", EXPIRY);
    let before = node.guard()?;
    assert_eq!(
        exact_service
            .get_entity(authorized(
                get("Orders/SUBSCRIPTIONS/Accounting"),
                &canonical
            ))
            .await?
            .into_inner()
            .path,
        "Orders/subscriptions/Accounting"
    );
    node.unchanged(&before, true)?;
    Ok(())
}

async fn namespace_reads_ignore_regressed_clock_without_widening_scope<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::start(provider, None)?;
    let token = sas("", "manage", EXPIRY);
    node.clock.set(0);
    let before = node.guard()?;
    for path in [
        "Orders",
        "orders",
        "Orders/subscriptions/Accounting",
        "Ordinary",
    ] {
        node.service
            .get_entity(authorized(get(path), &token))
            .await?;
    }
    let topics = node
        .service
        .list_entities(authorized(list(EntityKind::Topic, ""), &token))
        .await?
        .into_inner();
    assert_eq!(topics.entities.len(), 5);
    assert!(
        topics
            .entities
            .iter()
            .all(|entity| entity.kind == EntityKind::Topic as i32)
    );
    let queues = node
        .service
        .list_entities(authorized(list(EntityKind::Unspecified, ""), &token))
        .await?
        .into_inner();
    assert_eq!(queues.entities.len(), 1);
    assert_eq!(queues.entities[0].path, "Ordinary");
    assert_eq!(
        node.service
            .list_entities(authorized(list(EntityKind::Subscription, "Orders"), &token))
            .await?
            .into_inner()
            .entities
            .len(),
        3
    );
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
    exact_child_manage_can_create_and_get_but_cannot_list_siblings,
    topic_manage_inherits_children_without_neighbor_or_user_case_aliases,
    credentials_permissions_and_namespaces_refuse_before_owner_access,
    control_endpoint_tokens_do_not_administer_base_entities,
    literal_control_parent_names_and_signed_separators_remain_exact,
    namespace_reads_ignore_regressed_clock_without_widening_scope,
}

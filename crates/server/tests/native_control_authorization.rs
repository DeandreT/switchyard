//! Native resource names keep literal control-segment case in SAS authorization.

use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use admin_api::v1::{
    CreateEntityRequest, EntityKind, GetEntityRequest, entity_service_server::EntityService,
};
use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use base64::{Engine, engine::general_purpose::STANDARD};
use domain::{EntityPath, NamespaceName, StateMachine};
use hmac::{Hmac, Mac};
use server::{Broker, LocalProposer, ManualClock, NativeAdminService};
use sha2::Sha256;
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::StoreProvider;
use tonic::{Code, Request};
use url::form_urlencoded::byte_serialize;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const HOST: &str = "tenant.servicebus.windows.net";
const KEY: &str = "native-literal-control-test-key";
const DEADLINE: Duration = Duration::from_secs(5);

#[derive(Default)]
struct Observations {
    reads: AtomicUsize,
    writes: AtomicUsize,
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

fn sas(audience: &str) -> String {
    let resource = byte_serialize(audience.as_bytes()).collect::<String>();
    let expiry = 4_102_444_800_u64;
    let mut mac = Hmac::<Sha256>::new_from_slice(KEY.as_bytes()).expect("valid HMAC key");
    mac.update(format!("{resource}\n{expiry}").as_bytes());
    let signature = STANDARD.encode(mac.finalize().into_bytes());
    let signature = byte_serialize(signature.as_bytes()).collect::<String>();
    format!("SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn=manage")
}

fn authorized<T>(body: T, token: &str) -> Request<T> {
    let mut request = Request::new(body);
    request
        .metadata_mut()
        .insert("authorization", token.parse().expect("ASCII SAS token"));
    request
}

fn create(path: &str) -> CreateEntityRequest {
    CreateEntityRequest {
        namespace: "tenant".into(),
        path: path.into(),
        kind: EntityKind::Queue as i32,
        ..CreateEntityRequest::default()
    }
}

fn get(path: &str) -> GetEntityRequest {
    GetEntityRequest {
        namespace: "tenant".into(),
        path: path.into(),
    }
}

async fn native_control_segments_use_exact_raw_manage_scopes<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let observations = Arc::new(Observations::default());
    let store = ObservedStore {
        inner: provider.open()?,
        observations: observations.clone(),
    };
    let clock = ManualClock::at(1_000);
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(store.clone()),
        clock.clone(),
    ));
    let namespace = NamespaceName::new("tenant")?;
    let policy = SharedAccessPolicy::new([SharedAccessRule::new(
        "manage",
        ResourceScope::namespace(HOST)?,
        SharedAccessKey::new(KEY)?,
        None,
        PermissionSet::MANAGE,
    )?])?;
    let service = NativeAdminService::new(broker.handle(), namespace.clone())
        .with_shared_access_policy(policy, HOST)?;
    for (exact, alternatives) in [
        ("orders/$Management", vec!["orders/$management"]),
        ("orders/$management", vec!["orders/$Management"]),
        (
            "orders/$deadletterqueue/$Management",
            vec![
                "orders/$deadletterqueue/$management",
                "orders/$DeadLetterQueue/$Management",
                "orders/$DeadLetterQueue/$management",
            ],
        ),
    ] {
        clock.set(1_000);
        let token = sas(&format!("amqps://{HOST}/{exact}"));
        if broker
            .handle()
            .queue_config_blocking(namespace.clone(), EntityPath::new(exact)?)?
            .is_none()
        {
            let created = tokio::time::timeout(
                DEADLINE,
                service.create_entity(authorized(create(exact), &token)),
            )
            .await??
            .into_inner();
            assert_eq!(created.path, exact);
            assert_eq!(created.kind, EntityKind::Queue as i32);
        }
        for alternative in &alternatives {
            if broker
                .handle()
                .queue_config_blocking(namespace.clone(), EntityPath::new(*alternative)?)?
                .is_none()
            {
                let alternative_token = sas(&format!("amqps://{HOST}/{alternative}"));
                let created = tokio::time::timeout(
                    DEADLINE,
                    service.create_entity(authorized(create(alternative), &alternative_token)),
                )
                .await??
                .into_inner();
                assert_eq!(created.path, *alternative);
            }
        }
        let before = store.snapshot()?;
        let applied = broker.handle().last_applied_blocking()?;
        let writes = observations.writes.load(Ordering::SeqCst);
        clock.set(0);
        let found =
            tokio::time::timeout(DEADLINE, service.get_entity(authorized(get(exact), &token)))
                .await??
                .into_inner();
        assert_eq!(found.path, exact);
        assert_eq!(store.snapshot()?, before);
        assert_eq!(observations.writes.load(Ordering::SeqCst), writes);
        for alternative in alternatives {
            let reads = observations.reads.load(Ordering::SeqCst);
            assert_eq!(
                tokio::time::timeout(
                    DEADLINE,
                    service.create_entity(authorized(create(alternative), &token))
                )
                .await?
                .expect_err("case-distinct creation must be denied")
                .code(),
                Code::PermissionDenied
            );
            assert_eq!(
                tokio::time::timeout(
                    DEADLINE,
                    service.get_entity(authorized(get(alternative), &token))
                )
                .await?
                .expect_err("case-distinct lookup must be denied")
                .code(),
                Code::PermissionDenied
            );
            assert_eq!(
                observations.reads.load(Ordering::SeqCst),
                reads,
                "denied literal names must not reach storage"
            );
            assert_eq!(observations.writes.load(Ordering::SeqCst), writes);
            assert_eq!(store.snapshot()?, before);
        }
        assert_eq!(broker.handle().last_applied_blocking()?, applied);
    }
    Ok(())
}

mod memory {
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_control_segments_use_exact_raw_manage_scopes() -> super::TestResult {
        tokio::time::timeout(
            super::DEADLINE * 8,
            super::native_control_segments_use_exact_raw_manage_scopes(
                ::testkit::MemoryProvider::new(),
            ),
        )
        .await?
    }
}

mod durable {
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_control_segments_use_exact_raw_manage_scopes() -> super::TestResult {
        tokio::time::timeout(
            super::DEADLINE * 8,
            super::native_control_segments_use_exact_raw_manage_scopes(
                ::testkit::DurableProvider::temporary()?,
            ),
        )
        .await?
    }
}

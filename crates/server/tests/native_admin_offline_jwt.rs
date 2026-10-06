//! Offline JWT Manage over the actual Tonic TLS transport; no synthetic TLS extensions.
use std::{
    error::Error,
    panic::{AssertUnwindSafe, resume_unwind},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use admin_api::v1::{
    CreateEntityRequest, CreateRuleRequest, CreateRuleWithActionRequest, DeleteEntityRequest,
    DeleteRuleRequest, EntityKind, GetClockReadinessRequest, GetEntityRequest, GetRuleRequest,
    ListEntitiesRequest, ListRulesRequest, MaintenanceClockState, QueueConfiguration, RuleFilter,
    SqlRuleAction, TrueRuleFilter, UpdateEntityRequest, entity_service_client::EntityServiceClient,
    entity_service_server::EntityService, maintenance_service_client::MaintenanceServiceClient,
    rule_filter::Filter, rule_service_client::RuleServiceClient,
};
use auth::{
    JwtPolicy, PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule,
};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use domain::{NamespaceName, StateMachine};
use futures_util::FutureExt;
use hmac::{Hmac, Mac};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustls::{
    SignatureScheme,
    crypto::ring,
    pki_types::{PrivateKeyDer, PrivatePkcs1KeyDer},
};
use server::{
    Broker, Clock, LocalProposer, ManualClock, NativeAdminError, NativeAdminListener,
    NativeAdminService,
};
use sha2::Sha256;
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::StoreProvider;
use tokio::{
    net::TcpListener,
    task::{JoinError, JoinHandle},
    time::timeout,
};
use tonic::{
    Code, Request,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};
use url::form_urlencoded::byte_serialize;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
type Observed =
    Result<Result<TestResult, tokio::time::error::Elapsed>, Box<dyn std::any::Any + Send>>;
const DEADLINE: Duration = Duration::from_secs(5);
const HOST: &str = "tenant.servicebus.windows.net";
const ISSUER: &str = "https://issuer.example/";
const RESOURCE_AUDIENCE: &str = "urn:switchyard:native-admin";
const SAS_KEY: &str = "native-offline-jwt-test-secret";

const MODULUS: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";
// Public RSA PKCS1 fixture shared with the pure auth tests, never live credentials.
const PRIVATE_DER: &str = concat!(
    "MIIEpAIBAAKCAQEAyRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTL",
    "UTv4l4sggh5/CYYi/cvI+SXVT9kPWSKXxJXBXd/4LkvcPuUakBoAkfh+eiFVMh2V",
    "rUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8H",
    "oGfG/AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBI",
    "Mc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi+yUod+j8MtvIj812dkS4QMiRVN/",
    "by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQIDAQABAoIBAHREk0I0O9DvECKd",
    "WUpAmF3mY7oY9PNQiu44Yaf+AoSuyRpRUGTMIgc3u3eivOE8ALX0BmYUO5JtuRNZ",
    "Dpvt4SAwqCnVUinIf6C+eH/wSurCpapSM0BAHp4aOA7igptyOMgMPYBHNA1e9A7j",
    "E0dCxKWMl3DSWNyjQTk4zeRGEAEfbNjHrq6YCtjHSZSLmWiG80hnfnYos9hOr5Jn",
    "LnyS7ZmFE/5P3XVrxLc/tQ5zum0R4cbrgzHiQP5RgfxGJaEi7XcgherCCOgurJSS",
    "bYH29Gz8u5fFbS+Yg8s+OiCss3cs1rSgJ9/eHZuzGEdUZVARH6hVMjSuwvqVTFaE",
    "8AgtleECgYEA+uLMn4kNqHlJS2A5uAnCkj90ZxEtNm3E8hAxUrhssktY5XSOAPBl",
    "xyf5RuRGIImGtUVIr4HuJSa5TX48n3Vdt9MYCprO/iYl6moNRSPt5qowIIOJmIjY",
    "2mqPDfDt/zw+fcDD3lmCJrFlzcnh0uea1CohxEbQnL3cypeLt+WbU6kCgYEAzSp1",
    "9m1ajieFkqgoB0YTpt/OroDx38vvI5unInJlEeOjQ+oIAQdN2wpxBvTrRorMU6P0",
    "7mFUbt1j+Co6CbNiw+X8HcCaqYLR5clbJOOWNR36PuzOpQLkfK8woupBxzW9B8gZ",
    "mY8rB1mbJ+/WTPrEJy6YGmIEBkWylQ2VpW8O4O0CgYEApdbvvfFBlwD9YxbrcGz7",
    "MeNCFbMz+MucqQntIKoKJ91ImPxvtc0y6e/Rhnv0oyNlaUOwJVu0yNgNG117w0g4",
    "t/+Q38mvVC5xV7/cn7x9UMFk6MkqVir3dYGEqIl/OP1grY2Tq9HtB5iyG9L8NIam",
    "QOLMyUqqMUILxdthHyFmiGkCgYEAn9+PjpjGMPHxL0gj8Q8VbzsFtou6b1deIRRA",
    "2CHmSltltR1gYVTMwXxQeUhPMmgkMqUXzs4/WijgpthY44hK1TaZEKIuoxrS70nJ",
    "4WQLf5a9k1065fDsFZD6yGjdGxvwEmlGMZgTwqV7t1I4X0Ilqhav5hcs5apYL7gn",
    "PYPeRz0CgYALHCj/Ji8XSsDoF/MhVhnGdIs2P99NNdmo3R2Pv0CuZbDKMU559LJH",
    "UvrKS8WkuWRDuKrz1W/EQKApFjDGpdqToZqriUFQzwy7mR3ayIiogzNtHcvbDHx8",
    "oFnGY0OFksX/ye0/XGpy2SFxYRwGU98HPYeBvAQQrVjdkzfy7BmXQQ==",
);

fn policy() -> TestResult<JwtPolicy> {
    Ok(JwtPolicy::from_json(&format!(
        r#"{{"version":1,"issuer":"{ISSUER}","audience":"{RESOURCE_AUDIENCE}","keys":[{{"kid":"key-1","kty":"RSA","alg":"RS256","use":"sig","n":"{MODULUS}","e":"AQAB"}}],"bindings":[{{"subject":"administrator","scope":"amqps://{HOST}","permissions":["manage"]}},{{"subject":"orders-manager","scope":"amqps://{HOST}/orders","permissions":["manage"]}},{{"subject":"subscription-manager","scope":"amqps://{HOST}/events/subscriptions/sub","permissions":["manage"]}},{{"subject":"sender","scope":"amqps://{HOST}","permissions":["send"]}},{{"subject":"literal-manager","scope":"amqps://{HOST}/orders/$Management","permissions":["manage"]}}]}}"#
    ))?)
}

fn epoch() -> TestResult<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}

fn signed(
    subject: &str,
    issuer: &str,
    audience: &str,
    issued: u64,
    expires: u64,
) -> TestResult<String> {
    let input = format!("{}.{}",
        URL_SAFE_NO_PAD.encode(r#"{"alg":"RS256","kid":"key-1","typ":"at+jwt"}"#),
        URL_SAFE_NO_PAD.encode(format!(
            r#"{{"iss":"{issuer}","sub":"{subject}","aud":"{audience}","iat":{issued},"exp":{expires}}}"#
        )));
    let provider = ring::default_provider();
    let key = provider
        .key_provider
        .load_private_key(PrivateKeyDer::Pkcs1(PrivatePkcs1KeyDer::from(
            STANDARD.decode(PRIVATE_DER)?,
        )))?;
    let signer = key
        .choose_scheme(&[SignatureScheme::RSA_PKCS1_SHA256])
        .ok_or("public fixture RS256 signing unavailable")?;
    Ok(format!(
        "{input}.{}",
        URL_SAFE_NO_PAD.encode(signer.sign(input.as_bytes())?)
    ))
}

fn jwt(subject: &str) -> TestResult<String> {
    let now = epoch()?;
    signed(
        subject,
        ISSUER,
        RESOURCE_AUDIENCE,
        now,
        now.checked_add(120).ok_or("fixture epoch overflow")?,
    )
}

fn authorized<T>(body: T, token: &str) -> TestResult<Request<T>> {
    let mut request = Request::new(body);
    let mut value = format!("Bearer {token}").parse::<tonic::metadata::MetadataValue<_>>()?;
    value.set_sensitive(true);
    request.metadata_mut().insert("authorization", value);
    Ok(request)
}

fn sas_policy() -> TestResult<SharedAccessPolicy> {
    Ok(SharedAccessPolicy::new([SharedAccessRule::new(
        "administrator",
        ResourceScope::namespace(HOST)?,
        SharedAccessKey::new(SAS_KEY)?,
        None,
        PermissionSet::MANAGE,
    )?])?)
}

fn sas<T>(body: T) -> TestResult<Request<T>> {
    let resource = byte_serialize(format!("amqps://{HOST}").as_bytes()).collect::<String>();
    let expires = epoch()?.checked_add(120).ok_or("fixture epoch overflow")?;
    let mut mac = Hmac::<Sha256>::new_from_slice(SAS_KEY.as_bytes())?;
    mac.update(format!("{resource}\n{expires}").as_bytes());
    let signature =
        byte_serialize(STANDARD.encode(mac.finalize().into_bytes()).as_bytes()).collect::<String>();
    let token = format!(
        "SharedAccessSignature sr={resource}&sig={signature}&se={expires}&skn=administrator"
    );
    let mut request = Request::new(body);
    let mut value = token.parse::<tonic::metadata::MetadataValue<_>>()?;
    value.set_sensitive(true);
    request.metadata_mut().insert("authorization", value);
    Ok(request)
}

fn create(path: &str, kind: EntityKind) -> CreateEntityRequest {
    CreateEntityRequest {
        namespace: "tenant".into(),
        path: path.into(),
        kind: kind as i32,
        ..Default::default()
    }
}

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

struct Node<P: StoreProvider> {
    broker: Option<Broker>,
    service: NativeAdminService,
    store: ObservedStore<P::Store>,
    clock: ManualClock,
    endpoint: String,
    certificate: String,
    channel: Option<Channel>,
    listener: Option<JoinHandle<Result<(), NativeAdminError>>>,
    _provider: P,
}

impl<P: StoreProvider> Node<P> {
    async fn start(provider: P, with_sas: bool) -> TestResult<Self> {
        let store = ObservedStore {
            inner: provider.open()?,
            observations: Arc::default(),
        };
        let clock = ManualClock::at(1_000);
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            clock.clone(),
        ));
        let service = NativeAdminService::new(broker.handle(), NamespaceName::new("tenant")?);
        let service = if with_sas {
            service.with_shared_access_policy(sas_policy()?, HOST)?
        } else {
            service
        }
        .with_offline_jwt_policy(policy()?, HOST)?
        .with_development_maintenance_readiness();
        let CertifiedKey { cert, key_pair } =
            generate_simple_self_signed(vec!["localhost".into()])?;
        let certificate = cert.pem();
        let admin = NativeAdminListener::new(service.clone())
            .with_tls(certificate.as_bytes(), key_pair.serialize_pem().as_bytes())?;
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("https://{}", socket.local_addr()?);
        let listener = Some(tokio::spawn(admin.serve(socket)));
        Ok(Self {
            broker: Some(broker),
            service,
            store,
            clock,
            endpoint,
            certificate,
            channel: None,
            listener,
            _provider: provider,
        })
    }

    async fn connect(&mut self) -> TestResult<Channel> {
        let channel = Endpoint::from_shared(self.endpoint.clone())?
            .connect_timeout(DEADLINE)
            .timeout(DEADLINE)
            .tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(Certificate::from_pem(&self.certificate))
                    .domain_name("localhost"),
            )?
            .connect()
            .await?;
        self.channel = Some(channel.clone());
        Ok(channel)
    }

    fn reset(&self) -> TestResult<StoreSnapshot> {
        let before = self.store.snapshot()?;
        self.store.observations.reads.store(0, Ordering::SeqCst);
        self.store.observations.writes.store(0, Ordering::SeqCst);
        Ok(before)
    }

    fn unchanged(&self, before: StoreSnapshot) -> TestResult {
        assert_eq!(self.store.observations.reads.load(Ordering::SeqCst), 0);
        assert_eq!(self.store.observations.writes.load(Ordering::SeqCst), 0);
        assert_eq!(self.store.snapshot()?, before);
        assert_eq!(self.clock.now(), domain::Timestamp::from_millis(1_000));
        Ok(())
    }

    async fn finish(mut self, observed: Observed) -> TestResult {
        drop(self.channel.take());
        let listener = self.listener.take().expect("original listener retained");
        listener.abort();
        let joined: Result<Result<(), NativeAdminError>, JoinError> = listener.await;
        // Broker Drop requests Stop and joins its original owner; it does not expose that result.
        drop(self.broker.take());
        match observed {
            Err(payload) => resume_unwind(payload),
            Ok(result) => result??,
        }
        match joined {
            Err(error) if error.is_cancelled() => (),
            Err(error) => return Err(error.into()),
            Ok(result) => result?,
        }
        Ok(())
    }
}

async fn namespace_manage<P: StoreProvider>(provider: P) -> TestResult {
    let mut node = Node::start(provider, false).await?;
    let observed = AssertUnwindSafe(timeout(DEADLINE * 4, async {
        let channel = node.connect().await?;
        let mut entities = EntityServiceClient::new(channel.clone());
        let before = node.reset()?;
        let refused = entities
            .create_entity(sas(create("raw-sas-refused", EntityKind::Queue))?)
            .await
            .unwrap_err();
        assert_eq!(refused.code(), Code::Unauthenticated);
        assert_eq!(
            refused.message(),
            "shared-access authentication is not configured"
        );
        node.unchanged(before)?;
        let token = jwt("administrator")?;
        let created = entities
            .create_entity(authorized(create("orders", EntityKind::Queue), &token)?)
            .await?
            .into_inner();
        assert_eq!(created.path, "orders");
        assert_eq!(
            entities
                .get_entity(authorized(
                    GetEntityRequest {
                        namespace: "tenant".into(),
                        path: "orders".into()
                    },
                    &token
                )?)
                .await?
                .into_inner(),
            created
        );
        let updated = entities
            .update_entity(authorized(
                UpdateEntityRequest {
                    namespace: "tenant".into(),
                    path: "orders".into(),
                    queue_config: Some(QueueConfiguration {
                        lock_duration_millis: Some(40_000),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                &token,
            )?)
            .await?
            .into_inner();
        assert_eq!(
            updated.queue_config.unwrap().lock_duration_millis,
            Some(40_000)
        );
        let listed = entities
            .list_entities(authorized(
                ListEntitiesRequest {
                    namespace: "tenant".into(),
                    page_size: 1,
                    ..Default::default()
                },
                &token,
            )?)
            .await?
            .into_inner();
        assert_eq!(listed.entities.len(), 1);
        assert_eq!(listed.entities[0].path, "orders");
        assert!(listed.next_page_token.is_empty());
        entities
            .create_entity(authorized(create("events", EntityKind::Topic), &token)?)
            .await?;
        entities
            .create_entity(authorized(
                create("events/Subscriptions/sub", EntityKind::Subscription),
                &token,
            )?)
            .await?;
        let mut maintenance = MaintenanceServiceClient::new(channel.clone());
        let assessed = maintenance
            .get_clock_readiness(authorized(
                GetClockReadinessRequest {
                    namespace: "tenant".into(),
                },
                &token,
            )?)
            .await?
            .into_inner();
        assert!(MaintenanceClockState::try_from(assessed.state).is_ok());
        let mut rules = RuleServiceClient::new(channel);
        let filter = Some(RuleFilter {
            filter: Some(Filter::TrueFilter(TrueRuleFilter {})),
        });
        rules
            .create_rule(authorized(
                CreateRuleRequest {
                    namespace: "tenant".into(),
                    subscription_path: "events/Subscriptions/sub".into(),
                    name: "true".into(),
                    filter: filter.clone(),
                },
                &token,
            )?)
            .await?;
        rules
            .create_rule_with_action(authorized(
                CreateRuleWithActionRequest {
                    namespace: "tenant".into(),
                    subscription_path: "events/Subscriptions/sub".into(),
                    name: "action".into(),
                    filter,
                    action: Some(SqlRuleAction {
                        expression: "SET flag = 1".into(),
                        semantic_version: Some(2),
                    }),
                },
                &token,
            )?)
            .await?;
        assert_eq!(
            rules
                .get_rule(authorized(
                    GetRuleRequest {
                        namespace: "tenant".into(),
                        subscription_path: "events/subscriptions/sub".into(),
                        name: "action".into(),
                        include_actions: true
                    },
                    &token
                )?)
                .await?
                .into_inner()
                .name,
            "action"
        );
        let listed = rules
            .list_rules(authorized(
                ListRulesRequest {
                    namespace: "tenant".into(),
                    subscription_path: "events/Subscriptions/sub".into(),
                    include_actions: true,
                },
                &token,
            )?)
            .await?
            .into_inner();
        assert!(listed.rules.iter().any(|rule| rule.name == "true"));
        assert!(listed.rules.iter().any(|rule| rule.name == "action"));
        rules
            .delete_rule(authorized(
                DeleteRuleRequest {
                    namespace: "tenant".into(),
                    subscription_path: "events/Subscriptions/sub".into(),
                    name: "true".into(),
                },
                &token,
            )?)
            .await?;
        assert_eq!(
            rules
                .get_rule(authorized(
                    GetRuleRequest {
                        namespace: "tenant".into(),
                        subscription_path: "events/Subscriptions/sub".into(),
                        name: "true".into(),
                        include_actions: true
                    },
                    &token
                )?)
                .await
                .unwrap_err()
                .code(),
            Code::NotFound
        );
        entities
            .delete_entity(authorized(
                DeleteEntityRequest {
                    namespace: "tenant".into(),
                    path: "orders".into(),
                    kind: EntityKind::Queue as i32,
                },
                &token,
            )?)
            .await?;
        assert_eq!(
            entities
                .get_entity(authorized(
                    GetEntityRequest {
                        namespace: "tenant".into(),
                        path: "orders".into()
                    },
                    &token
                )?)
                .await
                .unwrap_err()
                .code(),
            Code::NotFound
        );
        Ok(())
    }))
    .catch_unwind()
    .await;
    node.finish(observed).await
}

async fn scoped_denials<P: StoreProvider>(provider: P) -> TestResult {
    let mut node = Node::start(provider, true).await?;
    let observed = AssertUnwindSafe(timeout(DEADLINE * 4, async {
        let channel = node.connect().await?;
        let mut entities = EntityServiceClient::new(channel.clone());
        let administrator = jwt("administrator")?;
        entities
            .create_entity(authorized(
                create("events", EntityKind::Topic),
                &administrator,
            )?)
            .await?;
        entities
            .create_entity(authorized(
                create("events/Subscriptions/sub", EntityKind::Subscription),
                &administrator,
            )?)
            .await?;
        let orders = jwt("orders-manager")?;
        entities
            .create_entity(authorized(create("orders", EntityKind::Queue), &orders)?)
            .await?;
        let mut maintenance = MaintenanceServiceClient::new(channel.clone());
        let mut rules = RuleServiceClient::new(channel);
        let sub = jwt("subscription-manager")?;
        rules
            .list_rules(authorized(
                ListRulesRequest {
                    namespace: "tenant".into(),
                    subscription_path: "events/subscriptions/sub".into(),
                    include_actions: true,
                },
                &sub,
            )?)
            .await?;
        let before = node.reset()?;
        assert_eq!(
            entities
                .create_entity(authorized(
                    create("orders-archive", EntityKind::Queue),
                    &orders
                )?)
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        assert_eq!(
            entities
                .list_entities(authorized(
                    ListEntitiesRequest {
                        namespace: "tenant".into(),
                        ..Default::default()
                    },
                    &orders
                )?)
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        assert_eq!(
            maintenance
                .get_clock_readiness(authorized(
                    GetClockReadinessRequest {
                        namespace: "tenant".into()
                    },
                    &orders
                )?)
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        assert_eq!(
            rules
                .list_rules(authorized(
                    ListRulesRequest {
                        namespace: "tenant".into(),
                        subscription_path: "events/Subscriptions/other".into(),
                        include_actions: true
                    },
                    &sub
                )?)
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        let sender = jwt("sender")?;
        let mut invalid = create("orders", EntityKind::Queue);
        invalid.kind = -1;
        invalid.placement_group_id = "unsupported".into();
        assert_eq!(
            entities
                .create_entity(authorized(invalid, &sender)?)
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        assert_eq!(
            rules
                .create_rule(authorized(
                    CreateRuleRequest {
                        namespace: "tenant".into(),
                        subscription_path: "events/Subscriptions/sub".into(),
                        name: "bad".into(),
                        filter: None
                    },
                    &sender
                )?)
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        node.unchanged(before)?;
        Ok(())
    }))
    .catch_unwind()
    .await;
    node.finish(observed).await
}

async fn credential_denials<P: StoreProvider>(provider: P) -> TestResult {
    let mut node = Node::start(provider, true).await?;
    let observed = AssertUnwindSafe(timeout(DEADLINE * 4, async {
        let channel = node.connect().await?;
        let mut client = EntityServiceClient::new(channel);
        let now = epoch()?;
        let valid = jwt("administrator")?;
        let before = node.reset()?;
        assert_eq!(
            client
                .create_entity(create("orders", EntityKind::Queue))
                .await
                .unwrap_err()
                .code(),
            Code::Unauthenticated
        );
        for token in [
            signed("administrator", ISSUER, RESOURCE_AUDIENCE, 0, 1)?,
            signed(
                "administrator",
                "https://other.example/",
                RESOURCE_AUDIENCE,
                now,
                now.checked_add(120).ok_or("fixture epoch overflow")?,
            )?,
            signed(
                "administrator",
                ISSUER,
                "urn:other",
                now,
                now.checked_add(120).ok_or("fixture epoch overflow")?,
            )?,
            signed(
                "unknown-subject",
                ISSUER,
                RESOURCE_AUDIENCE,
                now,
                now.checked_add(120).ok_or("fixture epoch overflow")?,
            )?,
            format!("{valid}x"),
            "x".repeat(8193),
        ] {
            let error = client
                .create_entity(authorized(create("orders", EntityKind::Queue), &token)?)
                .await
                .unwrap_err();
            assert_eq!(error.code(), Code::Unauthenticated);
            assert!(!error.message().contains(&token));
        }
        for credential in [
            "bearer value",
            "Bearer ",
            "Bearer value extra",
            "Basic value",
        ] {
            let mut request = Request::new(create("orders", EntityKind::Queue));
            request
                .metadata_mut()
                .insert("authorization", credential.parse()?);
            assert_eq!(
                client.create_entity(request).await.unwrap_err().code(),
                Code::Unauthenticated
            );
        }
        let mut duplicated = authorized(create("orders", EntityKind::Queue), &valid)?;
        duplicated
            .metadata_mut()
            .append("authorization", format!("Bearer {valid}").parse()?);
        assert_eq!(
            client.create_entity(duplicated).await.unwrap_err().code(),
            Code::Unauthenticated
        );
        node.unchanged(before)?;
        client
            .create_entity(authorized(
                create("healthy-after-denial", EntityKind::Queue),
                &valid,
            )?)
            .await?;
        Ok(())
    }))
    .catch_unwind()
    .await;
    node.finish(observed).await
}

async fn sas_coexists<P: StoreProvider>(provider: P) -> TestResult {
    let mut node = Node::start(provider, true).await?;
    let observed = AssertUnwindSafe(timeout(DEADLINE * 4, async {
        let channel = node.connect().await?;
        let mut client = EntityServiceClient::new(channel);
        let before = node.reset()?;
        let mut duplicate = sas(create("duplicate-sas", EntityKind::Queue))?;
        let original = duplicate
            .metadata()
            .get("authorization")
            .cloned()
            .ok_or("original SAS authorization required")?;
        duplicate.metadata_mut().append("authorization", original);
        let refused = client.create_entity(duplicate).await.unwrap_err();
        assert_eq!(refused.code(), Code::Unauthenticated);
        assert_eq!(refused.message(), "invalid authorization credential");
        let mut oversized = sas(create("oversized-sas", EntityKind::Queue))?;
        oversized.metadata_mut().insert(
            "authorization",
            format!("SharedAccessSignature {}", "x".repeat(8199)).parse()?,
        );
        let refused = client.create_entity(oversized).await.unwrap_err();
        assert_eq!(refused.code(), Code::Unauthenticated);
        assert_eq!(refused.message(), "invalid authorization credential");
        node.unchanged(before)?;
        client
            .create_entity(sas(create("sas-orders", EntityKind::Queue))?)
            .await?;
        let token = jwt("administrator")?;
        client
            .create_entity(authorized(create("jwt-orders", EntityKind::Queue), &token)?)
            .await?;
        let before = node.reset()?;
        let mut foreign = create("foreign", EntityKind::Queue);
        foreign.namespace = "other".into();
        assert_eq!(
            client
                .create_entity(authorized(foreign.clone(), &token)?)
                .await
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
        assert_eq!(
            client.create_entity(foreign).await.unwrap_err().code(),
            Code::Unauthenticated
        );
        node.unchanged(before)?;
        Ok(())
    }))
    .catch_unwind()
    .await;
    node.finish(observed).await
}

async fn literal_control_scope<P: StoreProvider>(provider: P) -> TestResult {
    let mut node = Node::start(provider, false).await?;
    let observed = AssertUnwindSafe(timeout(DEADLINE * 4, async {
        let channel = node.connect().await?;
        let mut client = EntityServiceClient::new(channel);
        let token = jwt("literal-manager")?;
        let before = node.reset()?;
        let denied = client
            .create_entity(authorized(
                create("orders/$management", EntityKind::Queue),
                &token,
            )?)
            .await
            .unwrap_err();
        assert_eq!(denied.code(), Code::PermissionDenied);
        node.unchanged(before)?;
        let created = client
            .create_entity(authorized(
                create("orders/$Management", EntityKind::Queue),
                &token,
            )?)
            .await?
            .into_inner();
        assert_eq!(created.path, "orders/$Management");
        let found = client
            .get_entity(authorized(
                GetEntityRequest {
                    namespace: "tenant".into(),
                    path: "orders/$Management".into(),
                },
                &token,
            )?)
            .await?
            .into_inner();
        assert_eq!(found, created);
        Ok(())
    }))
    .catch_unwind()
    .await;
    node.finish(observed).await
}

#[tokio::test]
async fn configured_jwt_refuses_a_plaintext_listener_before_admission() -> TestResult {
    let broker = Broker::spawn(LocalProposer::new(
        StateMachine::new(storage::MemoryStore::default()),
        ManualClock::at(1_000),
    ));
    let service = NativeAdminService::new(broker.handle(), NamespaceName::new("tenant")?)
        .with_offline_jwt_policy(policy()?, HOST)?;
    let socket = TcpListener::bind("127.0.0.1:0").await?;
    let observed = AssertUnwindSafe(timeout(DEADLINE, async {
        assert!(matches!(
            NativeAdminListener::new(service).serve(socket).await,
            Err(NativeAdminError::AuthenticationRequiresTls)
        ));
        Ok::<(), Box<dyn Error>>(())
    }))
    .catch_unwind()
    .await;
    drop(broker);
    match observed {
        Err(payload) => resume_unwind(payload),
        Ok(result) => result??,
    }
    Ok(())
}

#[tokio::test]
async fn a_direct_bearer_request_cannot_manufacture_transport_proof() -> TestResult {
    let mut node = Node::start(testkit::MemoryProvider::default(), false).await?;
    let observed = AssertUnwindSafe(timeout(DEADLINE, async {
        let token = jwt("administrator")?;
        let before = node.reset()?;
        let mut direct = authorized(create("orders", EntityKind::Queue), &token)?;
        direct
            .metadata_mut()
            .insert("x-forwarded-proto", "https".parse()?);
        let error = node.service.create_entity(direct).await.unwrap_err();
        assert_eq!(error.code(), Code::Unauthenticated);
        assert_eq!(error.message(), "offline JWT requires a TLS transport");
        node.unchanged(before)?;
        // A real trusted connection is accepted by the same configured service.
        let channel = node.connect().await?;
        EntityServiceClient::new(channel)
            .create_entity(authorized(create("orders", EntityKind::Queue), &token)?)
            .await?;
        Ok(())
    }))
    .catch_unwind()
    .await;
    node.finish(observed).await
}

macro_rules! backend_tests {
    ($name:ident, $provider:expr) => {
        mod $name {
            use super::*;
            #[tokio::test]
            async fn namespace_manage_traverses_actual_tls_entities_and_rules() -> TestResult {
                namespace_manage($provider).await
            }
            #[tokio::test]
            async fn scoped_manage_and_send_only_denials_precede_owner_work() -> TestResult {
                scoped_denials($provider).await
            }
            #[tokio::test]
            async fn invalid_bearer_credentials_leave_the_owner_untouched() -> TestResult {
                credential_denials($provider).await
            }
            #[tokio::test]
            async fn sas_and_jwt_keep_namespace_authentication_priority() -> TestResult {
                sas_coexists($provider).await
            }
            #[tokio::test]
            async fn literal_manage_scope_preserves_native_control_names() -> TestResult {
                literal_control_scope($provider).await
            }
        }
    };
}

backend_tests!(memory, testkit::MemoryProvider::default());
backend_tests!(durable, testkit::DurableProvider::temporary()?);

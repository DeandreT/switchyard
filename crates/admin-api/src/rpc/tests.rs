use std::{
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use domain::{
    AdminCatalogCursor, AdminCatalogEntry, AdminCatalogKind, AdminCatalogPage, AdminQueue,
    AdminSubscription, AdminTopic, CommandKind, CommandOutcome, EntityPath, NamespaceName,
    QueueConfig, SubscriptionConfig, SubscriptionName, TopicConfig, keys,
};
use prost::Message;
use ring::{
    rand::SystemRandom,
    signature::{RSA_PKCS1_SHA256, RsaKeyPair},
};
use serde_json::{Value, json};
use tonic::Code;

use super::*;
use crate::{EntityAddress, EntityOptions, EntityProfile, EntityTarget};
use wire::entity_service_server::EntityService as _;

// The same public jsonwebtoken v11.1.0 PKCS1 test material already verified in
// crates/auth/src/jwt/tests.rs. Test-only signing, never a runtime credential API.
const MODULUS: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";
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

fn configuration(scope: &str, rights: &[&str]) -> Value {
    json!({
        "version": 1, "issuer": "https://issuer.example/",
        "audience": "urn:switchyard:tenant",
        "keys": [{"kid":"key-1", "kty":"RSA", "alg":"RS256", "use":"sig", "n":MODULUS, "e":"AQAB"}],
        "bindings": [{"subject":"operator", "scope":scope, "permissions":rights}]
    })
}

fn claims() -> Value {
    json!({"iss":"https://issuer.example/", "sub":"operator", "aud":"urn:switchyard:tenant", "iat":100, "exp":200})
}

fn signed_raw(claims: &str) -> String {
    let header = r#"{"alg":"RS256","kid":"key-1","typ":"switchyard+jwt"}"#;
    let input = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(header),
        URL_SAFE_NO_PAD.encode(claims)
    );
    let key = RsaKeyPair::from_der(&STANDARD.decode(PRIVATE_DER).unwrap()).unwrap();
    let mut signature = vec![0; key.public().modulus_len()];
    key.sign(
        &RSA_PKCS1_SHA256,
        &SystemRandom::new(),
        input.as_bytes(),
        &mut signature,
    )
    .unwrap();
    format!("{input}.{}", URL_SAFE_NO_PAD.encode(signature))
}

fn token() -> String {
    signed_raw(&claims().to_string())
}

fn ns() -> NamespaceName {
    NamespaceName::new("tenant").unwrap()
}
fn path(value: &str) -> EntityPath {
    EntityPath::new(value).unwrap()
}

#[derive(Clone, Debug)]
struct Failure(BackendErrorKind);

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PRIVATE backend path /credential/token secret details")
    }
}
impl std::error::Error for Failure {}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Call {
    Create(NamespaceName, EntityPath, Box<CommandKind>),
    Get(NamespaceName, EntityTarget),
    List(
        NamespaceName,
        AdminCatalogKind,
        Option<AdminCatalogCursor>,
        usize,
    ),
}

#[derive(Default)]
struct State {
    calls: Vec<Call>,
    failure: Option<BackendErrorKind>,
    entry: Option<AdminCatalogEntry>,
    continuation: Option<AdminCatalogCursor>,
    unexpected: bool,
}

#[derive(Clone, Default)]
struct RecordingBackend(Arc<Mutex<State>>);

impl RecordingBackend {
    fn calls(&self) -> Vec<Call> {
        self.0.lock().unwrap().calls.clone()
    }
}

impl EntityBackend for RecordingBackend {
    type Error = Failure;

    async fn create(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        command: CommandKind,
    ) -> Result<CommandOutcome, ServiceError<Failure>> {
        let mut state = self.0.lock().unwrap();
        state.calls.push(Call::Create(
            namespace,
            entity.clone(),
            Box::new(command.clone()),
        ));
        if let Some(kind) = state.failure {
            return Err(ServiceError::Backend(Failure(kind)));
        }
        if state.unexpected {
            return Ok(CommandOutcome::TopicCreated);
        }
        let invalid = || ServiceError::Backend(Failure(BackendErrorKind::InvalidArgument));
        Ok(match command {
            CommandKind::CreateQueue { config } => {
                config.validate().map_err(|_| invalid())?;
                CommandOutcome::QueueCreated
            }
            CommandKind::CreateTopic { config } => {
                config.validate().map_err(|_| invalid())?;
                CommandOutcome::TopicCreated
            }
            CommandKind::CreateSubscription { name, config } => {
                config.validate().map_err(|_| invalid())?;
                CommandOutcome::SubscriptionCreated {
                    entity: entity.subscription(&name).unwrap(),
                }
            }
            _ => panic!("not an entity create"),
        })
    }

    async fn get(
        &self,
        namespace: NamespaceName,
        target: EntityTarget,
    ) -> Result<Option<AdminCatalogEntry>, ServiceError<Failure>> {
        let mut state = self.0.lock().unwrap();
        state.calls.push(Call::Get(namespace, target));
        if let Some(kind) = state.failure {
            return Err(ServiceError::Backend(Failure(kind)));
        }
        Ok(state.entry.clone())
    }

    async fn list(
        &self,
        namespace: NamespaceName,
        kind: AdminCatalogKind,
        after: Option<AdminCatalogCursor>,
        limit: usize,
    ) -> Result<AdminCatalogPage, ServiceError<Failure>> {
        let mut state = self.0.lock().unwrap();
        state.calls.push(Call::List(namespace, kind, after, limit));
        if let Some(kind) = state.failure {
            return Err(ServiceError::Backend(Failure(kind)));
        }
        Ok(AdminCatalogPage {
            entries: state.entry.clone().into_iter().collect(),
            continuation: state.continuation.clone(),
        })
    }
}

fn classify(failure: &Failure) -> BackendErrorKind {
    failure.0
}
type Rpc = EntityRpc<RecordingBackend, fn(&Failure) -> BackendErrorKind>;

fn rpc_with(config: Value) -> (Rpc, RecordingBackend, Arc<AtomicU64>) {
    let backend = RecordingBackend::default();
    let mut rpc = EntityRpc::new(
        EntityService::new(ns(), backend.clone()),
        JwtPolicy::from_json(&config.to_string()).unwrap(),
        ResourceScope::parse("amqps://tenant.example").unwrap(),
        classify as fn(&Failure) -> BackendErrorKind,
    )
    .unwrap();
    let clock = Arc::new(AtomicU64::new(100));
    let read = clock.clone();
    rpc.clock = Some(Arc::new(move || read.load(Ordering::SeqCst)));
    (rpc, backend, clock)
}

fn rpc() -> (Rpc, RecordingBackend, Arc<AtomicU64>) {
    rpc_with(configuration("amqps://tenant.example", &["manage"]))
}

fn credential<T>(message: T, value: Option<&str>) -> Request<T> {
    let mut request = Request::new(message);
    if let Some(value) = value {
        request
            .metadata_mut()
            .insert("authorization", value.parse().unwrap());
    }
    request
}

fn authorized<T>(message: T) -> Request<T> {
    credential(message, Some(&format!("Bearer {}", token())))
}

fn create(kind: wire::EntityKind) -> wire::CreateEntityRequest {
    wire::CreateEntityRequest {
        namespace: "TENANT".into(),
        path: "Orders".into(),
        kind: kind.into(),
        ..Default::default()
    }
}

fn get() -> wire::GetEntityRequest {
    wire::GetEntityRequest {
        namespace: "tenant".into(),
        path: "orders".into(),
        ..Default::default()
    }
}

fn list() -> wire::ListEntitiesRequest {
    wire::ListEntitiesRequest {
        namespace: "tenant".into(),
        page_size: 10,
        ..Default::default()
    }
}

fn safe(status: Status, code: Code) {
    assert_eq!(status.code(), code);
    assert!(status.details().is_empty());
    assert!(!status.message().contains("PRIVATE"));
    assert!(!status.message().contains("secret"));
    assert!(!status.message().contains("tenant.example"));
    assert!(!status.message().contains(&token()));
}

async fn reject_all(rpc: &Rpc, value: Option<&str>, code: Code) {
    safe(
        rpc.create_entity(credential(create(wire::EntityKind::Queue), value))
            .await
            .unwrap_err(),
        code,
    );
    safe(
        rpc.get_entity(credential(get(), value)).await.unwrap_err(),
        code,
    );
    safe(
        rpc.list_entities(credential(list(), value))
            .await
            .unwrap_err(),
        code,
    );
    safe(
        rpc.delete_entity(credential(wire::DeleteEntityRequest::default(), value))
            .await
            .unwrap_err(),
        code,
    );
}

#[tokio::test]
async fn missing_malformed_duplicate_invalid_and_expired_tokens_never_dispatch() {
    let (rpc, backend, _) = rpc();
    for value in [
        None,
        Some(""),
        Some("Basic secret"),
        Some("Bearer"),
        Some("Bearer "),
        Some("Bearer a b"),
        Some("Bearer x.x.x"),
        Some("SharedAccessSignature secret"),
    ] {
        reject_all(&rpc, value, Code::Unauthenticated).await;
    }
    let valid = format!("Bearer {}", token());
    let mut request = credential(get(), Some(&valid));
    request
        .metadata_mut()
        .append("authorization", valid.parse().unwrap());
    safe(
        rpc.get_entity(request).await.unwrap_err(),
        Code::Unauthenticated,
    );
    for (field, value) in [
        ("exp", json!(100)),
        ("iat", json!(101)),
        ("aud", json!("https://tenant.example")),
        ("iss", json!("https://foreign.example/")),
        ("sub", json!("unbound")),
    ] {
        let mut claims = claims();
        claims[field] = value;
        reject_all(
            &rpc,
            Some(&format!("Bearer {}", signed_raw(&claims.to_string()))),
            Code::Unauthenticated,
        )
        .await;
    }
    let duplicate = r#"{"iss":"https://issuer.example/","sub":"operator","aud":"urn:switchyard:tenant","iat":100,"exp":200,"exp":201}"#;
    reject_all(
        &rpc,
        Some(&format!("Bearer {}", signed_raw(duplicate))),
        Code::Unauthenticated,
    )
    .await;
    let valid = token();
    let (input, sig) = valid.rsplit_once('.').unwrap();
    let mut bytes = URL_SAFE_NO_PAD.decode(sig).unwrap();
    bytes[0] ^= 1;
    reject_all(
        &rpc,
        Some(&format!("Bearer {input}.{}", URL_SAFE_NO_PAD.encode(bytes))),
        Code::Unauthenticated,
    )
    .await;
    assert!(backend.calls().is_empty());
}

#[tokio::test]
async fn manage_on_namespace_root_is_required_even_for_get_and_list() {
    for (scope, rights) in [
        ("amqps://tenant.example", vec!["send"]),
        ("amqps://tenant.example", vec!["listen"]),
        ("amqps://tenant.example", vec!["send", "listen"]),
        ("amqps://foreign.example", vec!["manage"]),
        ("amqps://tenant.example/orders", vec!["manage"]),
    ] {
        let (rpc, backend, _) = rpc_with(configuration(scope, &rights));
        reject_all(
            &rpc,
            Some(&format!("Bearer {}", token())),
            Code::PermissionDenied,
        )
        .await;
        assert!(backend.calls().is_empty());
    }
    let (rpc, backend, _) = rpc();
    let mut create = create(wire::EntityKind::Queue);
    create.namespace = "foreign".into();
    let mut get = get();
    get.namespace = "foreign".into();
    let mut list = list();
    list.namespace = "foreign".into();
    safe(
        rpc.create_entity(authorized(create)).await.unwrap_err(),
        Code::PermissionDenied,
    );
    safe(
        rpc.get_entity(authorized(get)).await.unwrap_err(),
        Code::PermissionDenied,
    );
    safe(
        rpc.list_entities(authorized(list)).await.unwrap_err(),
        Code::PermissionDenied,
    );
    assert!(backend.calls().is_empty());
}

#[tokio::test]
async fn every_unary_rechecks_current_epoch_without_connection_grants() {
    let (rpc, backend, clock) = rpc();
    backend.0.lock().unwrap().entry = Some(AdminCatalogEntry::Queue(AdminQueue {
        path: path("orders"),
        config: QueueConfig::default(),
    }));
    // Real generated client/server codecs, in-process and without any listener.
    let mut client = wire::entity_service_client::EntityServiceClient::new(rpc.into_service());
    let bearer = format!("bEaReR {}", token());
    client
        .create_entity(credential(create(wire::EntityKind::Queue), Some(&bearer)))
        .await
        .unwrap();
    client
        .get_entity(credential(get(), Some(&bearer)))
        .await
        .unwrap();
    client
        .list_entities(credential(list(), Some(&bearer)))
        .await
        .unwrap();
    safe(
        client
            .delete_entity(credential(
                wire::DeleteEntityRequest::default(),
                Some(&bearer),
            ))
            .await
            .unwrap_err(),
        Code::Unimplemented,
    );
    assert_eq!(backend.calls().len(), 3);
    clock.store(200, Ordering::SeqCst);
    safe(
        client
            .create_entity(credential(create(wire::EntityKind::Queue), Some(&bearer)))
            .await
            .unwrap_err(),
        Code::Unauthenticated,
    );
    safe(
        client
            .get_entity(credential(get(), Some(&bearer)))
            .await
            .unwrap_err(),
        Code::Unauthenticated,
    );
    safe(
        client
            .list_entities(credential(list(), Some(&bearer)))
            .await
            .unwrap_err(),
        Code::Unauthenticated,
    );
    safe(
        client
            .delete_entity(credential(
                wire::DeleteEntityRequest::default(),
                Some(&bearer),
            ))
            .await
            .unwrap_err(),
        Code::Unauthenticated,
    );
    assert_eq!(backend.calls().len(), 3);
    safe(
        client.get_entity(Request::new(get())).await.unwrap_err(),
        Code::Unauthenticated,
    );
}

#[test]
fn constructor_refuses_entity_scope_and_https_resource_scope() {
    assert!(ResourceScope::parse("https://tenant.example").is_err());
    let (rpc, _, _) = rpc();
    assert!(matches!(
        EntityRpc::new(
            rpc.service,
            rpc.policy,
            ResourceScope::entity("tenant.example", "orders").unwrap(),
            InternalBackendErrors
        ),
        Err(NamespaceRootRequired)
    ));
}

#[tokio::test]
async fn unrelated_services_are_not_registered() {
    let (rpc, backend, _) = rpc();
    let mut service = rpc.into_service();
    for name in [
        "ClusterService/GetCluster",
        "NamespaceService/ListNamespaces",
        "BackupService/StartBackup",
        "AuditService/StreamAudit",
    ] {
        let request = tonic::codegen::http::Request::builder()
            .uri(format!("/switchyard.admin.v1.{name}"))
            .body(tonic::body::Body::empty())
            .unwrap();
        let response = tonic::codegen::Service::call(&mut service, request)
            .await
            .unwrap();
        assert_eq!(response.headers()["grpc-status"], "12");
    }
    assert!(backend.calls().is_empty());
}

#[test]
fn protobuf_presence_numbers_checked_sizes_and_history_translation() {
    let omitted = create(wire::EntityKind::Queue);
    let decoded = wire::CreateEntityRequest::decode(omitted.encode_to_vec().as_slice()).unwrap();
    let typed = crate::CreateEntityRequest::try_from(decoded).unwrap();
    assert_eq!(typed.options, EntityOptions::default());
    // Existing tags: max_size=5, ttl=6, lock=7, deliveries=8, sessions=9,
    // duplicate flag=10, history=11, message-size=12; explicit zeros survive.
    let encoded_zero_fields = [
        0x28, 0, 0x30, 0, 0x38, 0, 0x40, 0, 0x48, 0, 0x50, 0, 0x58, 0, 0x60, 0,
    ];
    let mut bytes = omitted.encode_to_vec();
    bytes.extend(encoded_zero_fields);
    let decoded = wire::CreateEntityRequest::decode(bytes.as_slice()).unwrap();
    let typed = crate::CreateEntityRequest::try_from(decoded).unwrap();
    assert_eq!(typed.options.max_size_bytes, Some(0));
    assert_eq!(typed.options.default_time_to_live_millis, Some(0));
    assert_eq!(typed.options.lock_duration_millis, Some(0));
    assert_eq!(typed.options.max_delivery_count, Some(0));
    assert_eq!(typed.options.max_message_bytes, Some(0));
    assert_eq!(typed.options.requires_session, Some(false));
    assert_eq!(typed.options.requires_duplicate_detection, Some(false));
    assert_eq!(typed.options.duplicate_detection_history_millis, None);
    for kind in [
        wire::EntityKind::Queue,
        wire::EntityKind::Topic,
        wire::EntityKind::Subscription,
    ] {
        let mut value = create(kind);
        value.duplicate_detection_history_millis = 20_000;
        assert_eq!(
            crate::CreateEntityRequest::try_from(value)
                .unwrap()
                .options
                .duplicate_detection_history_millis,
            Some(20_000)
        );
    }
    let mut value = omitted;
    value.max_message_bytes = Some(u64::MAX);
    let converted = crate::CreateEntityRequest::try_from(value);
    if usize::BITS < 64 {
        safe(converted.unwrap_err(), Code::InvalidArgument);
    } else {
        assert_eq!(
            converted.unwrap().options.max_message_bytes,
            Some(usize::MAX)
        );
    }
}

#[tokio::test]
async fn strict_enums_addresses_options_and_page_arguments_refuse_before_dispatch() {
    let (rpc, backend, _) = rpc();
    for kind in [0, -1, 4, i32::MAX] {
        let mut value = create(wire::EntityKind::Queue);
        value.kind = kind;
        safe(
            rpc.create_entity(authorized(value)).await.unwrap_err(),
            Code::InvalidArgument,
        );
    }
    for selection in [-1, 4, i32::MAX] {
        let mut value = list();
        value.selection = selection;
        safe(
            rpc.list_entities(authorized(value)).await.unwrap_err(),
            Code::InvalidArgument,
        );
    }
    for size in [0, 101, u32::MAX] {
        let mut value = list();
        value.page_size = size;
        safe(
            rpc.list_entities(authorized(value)).await.unwrap_err(),
            Code::InvalidArgument,
        );
    }
    let mut value = list();
    #[allow(deprecated)]
    {
        value.page_token = "text-token".into();
    }
    safe(
        rpc.list_entities(authorized(value)).await.unwrap_err(),
        Code::InvalidArgument,
    );
    let mut value = list();
    value.continuation = Some(Vec::new());
    safe(
        rpc.list_entities(authorized(value)).await.unwrap_err(),
        Code::InvalidArgument,
    );
    for (topic, name, path) in [
        (Some("t"), None, ""),
        (None, Some("s"), ""),
        (Some("t"), Some("s"), "q"),
        (Some(""), Some("s"), ""),
    ] {
        let mut value = create(wire::EntityKind::Subscription);
        value.path = path.into();
        value.subscription_topic = topic.map(Into::into);
        value.subscription_name = name.map(Into::into);
        safe(
            rpc.create_entity(authorized(value.clone()))
                .await
                .unwrap_err(),
            Code::InvalidArgument,
        );
        safe(
            rpc.get_entity(authorized(wire::GetEntityRequest {
                namespace: value.namespace,
                path: value.path,
                subscription_topic: value.subscription_topic,
                subscription_name: value.subscription_name,
            }))
            .await
            .unwrap_err(),
            Code::InvalidArgument,
        );
    }
    let mut value = create(wire::EntityKind::Subscription);
    safe(
        rpc.create_entity(authorized(value.clone()))
            .await
            .unwrap_err(),
        Code::InvalidArgument,
    );
    value.kind = wire::EntityKind::Queue.into();
    value.path.clear();
    value.subscription_topic = Some("t".into());
    value.subscription_name = Some("s".into());
    safe(
        rpc.create_entity(authorized(value)).await.unwrap_err(),
        Code::InvalidArgument,
    );
    for path in [
        "",
        "q/$deadletterqueue",
        "t/subscriptions/s",
        "q/$management",
    ] {
        let mut value = get();
        value.path = path.into();
        safe(
            rpc.get_entity(authorized(value)).await.unwrap_err(),
            Code::InvalidArgument,
        );
    }
    let mut options = Vec::new();
    let mut value = create(wire::EntityKind::Queue);
    value.placement_group_id = Some(String::new());
    options.push(value);
    let mut value = create(wire::EntityKind::Queue);
    value.max_size_bytes = Some(0);
    options.push(value);
    let mut value = create(wire::EntityKind::Queue);
    value.used_logical_bytes = Some(0);
    options.push(value);
    let mut value = create(wire::EntityKind::Topic);
    value.lock_duration_millis = Some(0);
    options.push(value);
    let mut value = create(wire::EntityKind::Topic);
    value.max_delivery_count = Some(0);
    options.push(value);
    let mut value = create(wire::EntityKind::Topic);
    value.requires_session = Some(false);
    options.push(value);
    let mut value = create(wire::EntityKind::Topic);
    value.requires_duplicate_detection = Some(false);
    options.push(value);
    let mut value = create(wire::EntityKind::Topic);
    value.duplicate_detection_history_millis = 20_000;
    options.push(value);
    for value in options {
        safe(
            rpc.create_entity(authorized(value)).await.unwrap_err(),
            Code::Unimplemented,
        );
    }
    assert!(backend.calls().is_empty());
}

#[tokio::test]
async fn supported_profiles_defaults_zeros_and_explicit_subscriptions_are_preserved() {
    let (rpc, backend, _) = rpc();
    let queue = rpc
        .create_entity(authorized(create(wire::EntityKind::Queue)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(queue.default_ttl_millis, None);
    assert_eq!(queue.lock_duration_millis, Some(60_000));
    assert_eq!(queue.max_delivery_count, Some(10));
    assert_eq!(queue.max_message_bytes, Some(262_144));
    assert_eq!(queue.requires_session, Some(false));
    assert_eq!(queue.duplicate_detection_history_millis, 600_000);
    assert_eq!(queue.placement_group_id, None);
    assert_eq!(queue.max_size_bytes, None);
    assert_eq!(queue.used_logical_bytes, None);
    let topic = rpc
        .create_entity(authorized(create(wire::EntityKind::Topic)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(topic.lock_duration_millis, None);
    assert_eq!(topic.max_delivery_count, None);
    assert_eq!(topic.requires_session, None);
    assert_eq!(topic.duplicate_detection_history_millis, 0);
    assert!(!topic.requires_duplicate_detection);
    let mut value = create(wire::EntityKind::Queue);
    value.default_ttl_millis = Some(900);
    value.lock_duration_millis = Some(500);
    value.max_delivery_count = Some(2);
    value.max_message_bytes = Some(1000);
    value.requires_session = Some(true);
    value.requires_duplicate_detection = Some(true);
    value.duplicate_detection_history_millis = 20_000;
    let created = rpc
        .create_entity(authorized(value))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(created.default_ttl_millis, Some(900));
    assert_eq!(created.lock_duration_millis, Some(500));
    assert_eq!(created.max_delivery_count, Some(2));
    assert_eq!(created.max_message_bytes, Some(1000));
    assert_eq!(created.requires_session, Some(true));
    assert!(created.requires_duplicate_detection);
    assert_eq!(created.duplicate_detection_history_millis, 20_000);
    for option in 0..4 {
        let mut value = create(wire::EntityKind::Queue);
        match option {
            0 => value.default_ttl_millis = Some(0),
            1 => value.lock_duration_millis = Some(0),
            2 => value.max_delivery_count = Some(0),
            _ => value.max_message_bytes = Some(0),
        }
        safe(
            rpc.create_entity(authorized(value)).await.unwrap_err(),
            Code::InvalidArgument,
        );
        let Call::Create(_, _, command) = backend.calls().last().unwrap().clone() else {
            panic!()
        };
        let CommandKind::CreateQueue { config } = *command else {
            panic!()
        };
        match option {
            0 => assert_eq!(config.default_time_to_live_millis, Some(0)),
            1 => assert_eq!(config.lock_duration_millis, 0),
            2 => assert_eq!(config.max_delivery_count, 0),
            _ => assert_eq!(config.max_message_bytes, 0),
        }
    }
    // Composite path exceeds the ordinary public bound; components stay explicit.
    let topic = "t".repeat(260);
    let name = "s".repeat(50);
    let mut value = create(wire::EntityKind::Subscription);
    value.path.clear();
    value.subscription_topic = Some(topic.clone());
    value.subscription_name = Some(name.clone());
    value.default_ttl_millis = Some(321);
    value.lock_duration_millis = Some(700);
    value.max_delivery_count = Some(4);
    let created = rpc
        .create_entity(authorized(value.clone()))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(created.subscription_topic, Some(topic.clone()));
    assert_eq!(created.subscription_name, Some(name.clone()));
    assert!(created.path.len() > 260);
    assert_eq!(created.max_message_bytes, None);
    assert_eq!(created.requires_session, None);
    assert_eq!(created.default_ttl_millis, Some(321));
    assert_eq!(created.lock_duration_millis, Some(700));
    assert_eq!(created.max_delivery_count, Some(4));
    let topic_path = path(&topic);
    let subscription = SubscriptionName::new(&name).unwrap();
    backend.0.lock().unwrap().entry = Some(AdminCatalogEntry::Subscription(AdminSubscription {
        topic: topic_path.clone(),
        name: subscription.clone(),
        path: topic_path.subscription(&subscription).unwrap(),
        config: SubscriptionConfig {
            default_time_to_live_millis: Some(321),
            lock_duration_millis: 700,
            max_delivery_count: 4,
            ..Default::default()
        },
    }));
    let fetched = rpc
        .get_entity(authorized(wire::GetEntityRequest {
            namespace: "tenant".into(),
            path: String::new(),
            subscription_topic: Some(topic),
            subscription_name: Some(name),
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(created, fetched);
    let before = backend.calls().len();
    value.max_message_bytes = Some(0);
    safe(
        rpc.create_entity(authorized(value)).await.unwrap_err(),
        Code::Unimplemented,
    );
    assert_eq!(backend.calls().len(), before);
}

fn cursor(namespace: &str, kind: u8) -> AdminCatalogCursor {
    // Version-1 local cursor fixture, checked by the authoritative domain parser.
    let mut bytes = vec![1, kind];
    bytes.extend(keys::queue_config(
        &NamespaceName::new(namespace).unwrap(),
        &path("position"),
    ));
    AdminCatalogCursor::from_bytes(&bytes).unwrap()
}

#[tokio::test]
async fn opaque_continuation_and_empty_continuing_page_are_preserved() {
    let (rpc, backend, _) = rpc();
    let cursor = cursor("tenant", 3);
    backend.0.lock().unwrap().continuation = Some(cursor.clone());
    let mut value = list();
    value.continuation = Some(cursor.as_bytes().to_vec());
    let decoded = wire::ListEntitiesRequest::decode(value.encode_to_vec().as_slice()).unwrap();
    let page = rpc
        .list_entities(authorized(decoded))
        .await
        .unwrap()
        .into_inner();
    assert!(page.entities.is_empty());
    assert_eq!(page.continuation, Some(cursor.as_bytes().to_vec()));
    #[allow(deprecated)]
    {
        assert!(page.next_page_token.is_empty());
    }
    assert_eq!(
        backend.calls(),
        vec![Call::List(
            ns(),
            AdminCatalogKind::All,
            Some(cursor.clone()),
            10
        )]
    );
    let mut value = list();
    value.selection = wire::EntitySelection::Queues.into();
    value.continuation = Some(cursor.as_bytes().to_vec());
    safe(
        rpc.list_entities(authorized(value)).await.unwrap_err(),
        Code::InvalidArgument,
    );
    let mut value = list();
    value.continuation = Some(self::cursor("foreign", 3).as_bytes().to_vec());
    safe(
        rpc.list_entities(authorized(value)).await.unwrap_err(),
        Code::InvalidArgument,
    );
    assert_eq!(backend.calls().len(), 1);
    backend.0.lock().unwrap().entry = Some(AdminCatalogEntry::Topic(AdminTopic {
        path: path("events"),
        config: TopicConfig::default(),
    }));
    backend.0.lock().unwrap().continuation = None;
    for (selection, kind) in [
        (wire::EntitySelection::All, AdminCatalogKind::All),
        (wire::EntitySelection::Queues, AdminCatalogKind::Queues),
        (wire::EntitySelection::Topics, AdminCatalogKind::Topics),
        (
            wire::EntitySelection::Subscriptions,
            AdminCatalogKind::Subscriptions,
        ),
    ] {
        let mut value = list();
        value.selection = selection.into();
        let page = rpc
            .list_entities(authorized(value))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(page.entities[0].kind, wire::EntityKind::Topic as i32);
        assert_eq!(page.continuation, None);
        assert_eq!(
            backend.calls().last(),
            Some(&Call::List(ns(), kind, None, 10))
        );
    }
}

#[tokio::test]
async fn errors_are_classified_without_exposing_backend_or_request_details() {
    let (rpc, backend, _) = rpc();
    safe(
        rpc.get_entity(authorized(get())).await.unwrap_err(),
        Code::NotFound,
    );
    for (kind, code) in [
        (BackendErrorKind::InvalidArgument, Code::InvalidArgument),
        (BackendErrorKind::NotFound, Code::NotFound),
        (BackendErrorKind::AlreadyExists, Code::AlreadyExists),
        (
            BackendErrorKind::FailedPrecondition,
            Code::FailedPrecondition,
        ),
        (BackendErrorKind::Unavailable, Code::Unavailable),
        (BackendErrorKind::Internal, Code::Internal),
    ] {
        backend.0.lock().unwrap().failure = Some(kind);
        safe(
            rpc.create_entity(authorized(create(wire::EntityKind::Queue)))
                .await
                .unwrap_err(),
            code,
        );
        safe(rpc.get_entity(authorized(get())).await.unwrap_err(), code);
        safe(
            rpc.list_entities(authorized(list())).await.unwrap_err(),
            code,
        );
    }
    backend.0.lock().unwrap().failure = None;
    backend.0.lock().unwrap().unexpected = true;
    safe(
        rpc.create_entity(authorized(create(wire::EntityKind::Queue)))
            .await
            .unwrap_err(),
        Code::Internal,
    );
    safe(
        rpc.status(ServiceError::Catalog(AdminCatalogError::CorruptCatalog)),
        Code::Internal,
    );
    assert_eq!(
        InternalBackendErrors.classify(&Failure(BackendErrorKind::InvalidArgument)),
        BackendErrorKind::Internal
    );
}

#[tokio::test]
async fn typed_service_stays_transport_and_authorization_independent() {
    let backend = RecordingBackend::default();
    let typed = EntityService::new(ns(), backend.clone());
    let entity = typed
        .create(crate::CreateEntityRequest {
            namespace: "tenant".into(),
            address: EntityAddress::Entity { path: "q".into() },
            kind: crate::EntityKind::Queue,
            options: EntityOptions::default(),
        })
        .await
        .unwrap();
    assert!(matches!(entity.profile, EntityProfile::Queue(_)));
    assert_eq!(backend.calls().len(), 1);
    assert!(matches!(
        typed
            .get(crate::GetEntityRequest {
                namespace: "foreign".into(),
                address: EntityAddress::Entity { path: "q".into() }
            })
            .await,
        Err(ServiceError::ForeignNamespace { .. })
    ));
    assert_eq!(backend.calls().len(), 1);
}

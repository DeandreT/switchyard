//! Real signatures, CBS, trusted TLS and recording broker admission witnesses.

use std::{
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

use amqp::{
    ApplicationProperties, Body, ClientConnection as Connection, ClientReceiver as Receiver,
    ClientSender as Sender, ClientSession as Session, Message, OrderedMap, Outcome, Properties,
    SaslInit, Symbol, Value,
};
use auth::{
    JwtPolicy, PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule,
};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use domain::{
    CommandKind, CommandOutcome, EntityBinding, EntityPath, NamespaceName, QueueConfig,
    StateMachine,
};
use ring::{
    rand::SystemRandom,
    signature::{RSA_PKCS1_SHA256, RsaKeyPair},
};
use serde_json::{Value as Json, json};
use storage::MemoryStore;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use super::{
    test_support::{WAIT, wss_test_client_tls, wss_test_server_tls},
    *,
};
use crate::{
    BrokerRejection, SharedAccessAuthentication,
    cbs::{JWT_TOKEN_TYPE, SAS_TOKEN_TYPE},
    websocket::WebSocketIo,
};

mod fixture;

const HOST: &str = "tenant.servicebus.windows.net";
const RESOURCE: &str = "amqps://tenant.servicebus.windows.net/orders";

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn claims(iat: u64, exp: u64) -> Json {
    json!({"iss":"https://issuer.example/", "sub":"producer", "aud":"urn:switchyard:tenant", "iat":iat, "exp":exp,
        "roles":["manage"], "scope":"send listen manage"})
}

fn signed(header: &Json, claims: &Json) -> String {
    let input = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(header.to_string()),
        URL_SAFE_NO_PAD.encode(claims.to_string())
    );
    let key = RsaKeyPair::from_der(&STANDARD.decode(fixture::PRIVATE_DER).unwrap()).unwrap();
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

fn header() -> Json {
    json!({"alg":"RS256", "kid":"key-1", "typ":"switchyard+jwt"})
}
pub(super) fn token(iat: u64, exp: u64) -> String {
    signed(&header(), &claims(iat, exp))
}

pub(super) fn policy() -> JwtPolicy {
    JwtPolicy::from_json(&json!({"version":1, "issuer":"https://issuer.example/", "audience":"urn:switchyard:tenant",
        "keys":[{"kid":"key-1","kty":"RSA","alg":"RS256","use":"sig","n":fixture::MODULUS,"e":"AQAB"}],
        "bindings":[{"subject":"producer","scope":RESOURCE,"permissions":["send"]}]}).to_string()).unwrap()
}

fn authentication(jwt: bool) -> SharedAccessAuthentication {
    let sas = SharedAccessPolicy::new([SharedAccessRule::new(
        "sas",
        ResourceScope::namespace(HOST).unwrap(),
        SharedAccessKey::new("secret").unwrap(),
        None,
        PermissionSet::SEND,
    )
    .unwrap()])
    .unwrap();
    let config = SharedAccessAuthentication::new(sas, HOST).unwrap();
    if jwt {
        config.with_jwt_policy(policy())
    } else {
        config
    }
}

#[derive(Clone)]
struct RecordingBroker {
    handle: server::BrokerHandle,
    calls: Arc<Mutex<Vec<CommandKind>>>,
    binds: Arc<Mutex<Vec<EntityPath>>>,
}

impl Broker for RecordingBroker {
    async fn bind_entity(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
    ) -> Result<EntityBinding, BrokerRejection> {
        self.binds.lock().unwrap().push(entity.clone());
        self.handle
            .bind_entity(namespace, entity)
            .await
            .map_err(|_| BrokerRejection::Unavailable("fixture bind failed".into()))
    }
    async fn submit_bound(
        &self,
        binding: EntityBinding,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        self.calls.lock().unwrap().push(kind.clone());
        self.handle
            .submit_bound(binding, kind)
            .await
            .map_err(|_| BrokerRejection::Unavailable("fixture command failed".into()))
    }
    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        self.calls.lock().unwrap().push(kind.clone());
        self.handle
            .submit(namespace, entity, kind)
            .await
            .map_err(|_| BrokerRejection::Unavailable("fixture command failed".into()))
    }
    async fn deliverable(&self, _: &NamespaceName, _: &EntityPath) {
        std::future::pending().await
    }
}

struct Node {
    service: AmqpListenerService<RecordingBroker>,
    address: SocketAddr,
    calls: Arc<Mutex<Vec<CommandKind>>>,
    binds: Arc<Mutex<Vec<EntityPath>>>,
    _broker: server::Broker,
}

impl Node {
    async fn start(tls: bool, websocket: bool, jwt: bool) -> Self {
        let broker = server::Broker::spawn(server::LocalProposer::new(
            StateMachine::new(MemoryStore::default()),
            server::ManualClock::at(1000),
        ));
        let namespace = NamespaceName::new("tenant").unwrap();
        broker
            .handle()
            .submit_blocking(
                namespace.clone(),
                EntityPath::new("orders").unwrap(),
                CommandKind::CreateQueue {
                    config: QueueConfig::default(),
                },
            )
            .unwrap();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let binds = Arc::new(Mutex::new(Vec::new()));
        let adapter = RecordingBroker {
            handle: broker.handle(),
            calls: calls.clone(),
            binds: binds.clone(),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut config = AmqpListener::new(adapter, namespace)
            .with_shared_access_authentication(authentication(jwt));
        if tls {
            config = config.with_tls(wss_test_server_tls());
        }
        let service = if websocket {
            config.into_wss_service(listener)
        } else {
            config.into_tcp_service(listener)
        };
        Self {
            service,
            address,
            calls,
            binds,
            _broker: broker,
        }
    }

    async fn run<F: Future>(&mut self, scenario: F) -> F::Output {
        let result = tokio::select! {
            result = scenario => result,
            () = self.service.serve() => panic!("listener ended before scenario"),
        };
        timeout(WAIT, self.service.finish())
            .await
            .expect("original listener family joined");
        assert!(matches!(
            self.service.take_exit(),
            Some(AmqpListenerExit::Complete(Ok(())))
        ));
        result
    }
}

async fn open<S: AsyncRead + AsyncWrite + Send + Unpin + 'static>(
    stream: S,
    address: SocketAddr,
    websocket: bool,
) -> Connection {
    let builder = Connection::builder()
        .container_id("jwt-test")
        .sasl(SaslInit {
            mechanism: Symbol::from("ANONYMOUS"),
            initial_response: None,
            hostname: Some(HOST.into()),
        });
    if websocket {
        let mut request = format!("ws://{address}/$servicebus/websocket")
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert("sec-websocket-protocol", "AMQPWSB10".parse().unwrap());
        let (socket, _) = tokio_tungstenite::client_async(request, stream)
            .await
            .unwrap();
        builder
            .open_with_stream(WebSocketIo::new(socket))
            .await
            .unwrap()
    } else {
        builder.open_with_stream(stream).await.unwrap()
    }
}

async fn connect(address: SocketAddr, tls: bool, websocket: bool) -> Connection {
    let tcp = TcpStream::connect(address).await.unwrap();
    if tls {
        let stream = wss_test_client_tls()
            .connect(
                rustls::pki_types::ServerName::try_from("localhost").unwrap(),
                tcp,
            )
            .await
            .unwrap();
        open(stream, address, websocket).await
    } else {
        open(tcp, address, websocket).await
    }
}

pub(super) async fn put_token(
    session: &mut Session,
    token_type: &str,
    resource: &str,
    token: String,
) -> i32 {
    let mut requests = Sender::attach(session, "cbs-request", crate::CBS_NODE)
        .await
        .unwrap();
    let mut replies = Receiver::builder()
        .name("cbs-reply")
        .source(crate::CBS_NODE)
        .target("jwt-reply")
        .attach(session)
        .await
        .unwrap();
    requests
        .send(
            Message::builder()
                .properties(Properties {
                    message_id: Some("token-1".into()),
                    reply_to: Some("jwt-reply".into()),
                    ..Properties::default()
                })
                .application_properties(
                    ApplicationProperties::builder()
                        .insert("operation", "put-token")
                        .insert("type", token_type)
                        .insert("name", resource)
                        .build(),
                )
                .body(Body::Value(Value::String(token)))
                .build(),
        )
        .await
        .unwrap();
    let reply = replies.recv().await.unwrap();
    assert_eq!(
        reply.message().properties.as_ref().unwrap().correlation_id,
        Some("token-1".into())
    );
    let properties = reply.message().application_properties.as_ref().unwrap();
    let Some(Value::Int(status)) = properties.get("status-code") else {
        panic!("missing status")
    };
    let status = *status;
    assert_eq!(
        properties.get("status-description"),
        Some(&Value::String(
            match status {
                202 => "Accepted",
                401 => "Unauthorized",
                400 => "unsupported CBS operation or token type",
                _ => panic!("unexpected status"),
            }
            .into()
        ))
    );
    replies.accept(&reply).await.unwrap();
    requests.close().await.unwrap();
    replies.close().await.unwrap();
    status
}

async fn refused_send(session: &mut Session, name: &str, entity: &str) {
    let mut sender = Sender::attach(session, name, entity).await.unwrap();
    assert!(
        sender
            .send(
                Message::builder()
                    .body(Body::Value(Value::String("denied".into())))
                    .build()
            )
            .await
            .is_err()
    );
}

pub(super) fn sas_token(expiry: u64) -> String {
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    let resource =
        percent_encoding::utf8_percent_encode(RESOURCE, percent_encoding::NON_ALPHANUMERIC);
    let mut mac = Hmac::<Sha256>::new_from_slice(b"secret").unwrap();
    mac.update(format!("{resource}\n{expiry}").as_bytes());
    let signature = STANDARD.encode(mac.finalize().into_bytes());
    let signature =
        percent_encoding::utf8_percent_encode(&signature, percent_encoding::NON_ALPHANUMERIC);
    format!("SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn=sas")
}

pub(super) async fn management(
    requests: &mut Sender,
    responses: &mut Receiver,
    operation: &str,
    body: OrderedMap<Value, Value>,
) -> i32 {
    let request = Message::builder()
        .properties(Properties {
            message_id: Some("management-1".into()),
            reply_to: Some("management-reply".into()),
            ..Properties::default()
        })
        .application_properties(
            ApplicationProperties::builder()
                .insert(crate::OPERATION_PROPERTY, operation)
                .build(),
        )
        .body(Body::Value(Value::Map(body)))
        .build();
    assert!(matches!(
        requests.send(request).await.unwrap(),
        Outcome::Accepted(_)
    ));
    let reply = responses.recv().await.unwrap();
    let Some(Value::Int(status)) = reply
        .message()
        .application_properties
        .as_ref()
        .unwrap()
        .get(crate::STATUS_CODE_PROPERTY)
    else {
        panic!("missing management status")
    };
    let status = *status;
    responses.accept(&reply).await.unwrap();
    status
}

pub(super) fn cancel_body() -> OrderedMap<Value, Value> {
    let mut body = OrderedMap::new();
    body.insert(
        Value::String("sequence-numbers".into()),
        Value::Array(amqp::Array::from(vec![Value::Long(1)])),
    );
    body
}

pub(super) fn schedule_body() -> OrderedMap<Value, Value> {
    let mut annotations = amqp::MessageAnnotations::default();
    annotations.insert(
        amqp::AnnotationKey::from("x-opt-scheduled-enqueue-time"),
        Value::Timestamp(5000_i64.into()),
    );
    let mut scheduled = Message::builder()
        .properties(Properties {
            message_id: Some("scheduled".into()),
            ..Properties::default()
        })
        .body(Body::Data(vec![b"scheduled".to_vec().into()]))
        .build();
    scheduled.message_annotations = Some(annotations);
    let mut entry = OrderedMap::new();
    entry.insert(
        Value::String("message".into()),
        Value::Binary(amqp::encode_message(&scheduled).unwrap().into()),
    );
    entry.insert(
        Value::String("message-id".into()),
        Value::String("scheduled".into()),
    );
    let mut body = OrderedMap::new();
    body.insert(
        Value::String("messages".into()),
        Value::List(vec![Value::Map(entry)]),
    );
    body
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn jwt_requires_actual_tls_admission_on_tcp_and_websocket_and_sas_stays_compatible() {
    for websocket in [false, true] {
        for (tls, configured) in [(false, true), (true, false), (true, true)] {
            let mut node = Node::start(tls, websocket, configured).await;
            let address = node.address;
            let calls = node.calls.clone();
            let binds = node.binds.clone();
            node.run(async move {
                let mut connection = connect(address, tls, websocket).await;
                let mut session = Session::begin(&mut connection).await.unwrap();
                let current = now();
                let expected = if tls && configured { 202 } else { 401 };
                assert_eq!(
                    put_token(
                        &mut session,
                        JWT_TOKEN_TYPE,
                        RESOURCE,
                        token(current, current + 300)
                    )
                    .await,
                    expected
                );
                assert_eq!(
                    put_token(&mut session, "JWT", RESOURCE, token(current, current + 300)).await,
                    400
                );
                if expected != 202 {
                    refused_send(&mut session, "denied-data", "orders").await;
                    refused_send(&mut session, "denied-management", "orders/$management").await;
                    assert!(calls.lock().unwrap().is_empty());
                    assert!(binds.lock().unwrap().is_empty());
                }
                let sas = sas_token(current + 300);
                assert_eq!(
                    put_token(&mut session, SAS_TOKEN_TYPE, RESOURCE, sas).await,
                    202
                );
                connection.close().await.unwrap();
            })
            .await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_jwt_rejections_never_dispatch_and_claims_cannot_widen_local_scope_or_rights() {
    let mut node = Node::start(true, false, true).await;
    let address = node.address;
    let calls = node.calls.clone();
    let binds = node.binds.clone();
    node.run(async move {
        let mut connection = connect(address, true, false).await;
        let mut session = Session::begin(&mut connection).await.unwrap();
        let current = now();
        for (field, value) in [
            ("iss", json!("https://other.example/")),
            ("aud", json!(RESOURCE)),
            ("sub", json!("unknown")),
            ("exp", json!(current)),
            ("nbf", json!(current + 100)),
            ("iat", json!(current + 100)),
        ] {
            let mut invalid = claims(current, current + 300);
            invalid[field] = value;
            assert_eq!(
                put_token(
                    &mut session,
                    JWT_TOKEN_TYPE,
                    RESOURCE,
                    signed(&header(), &invalid)
                )
                .await,
                401,
                "rejected {field} fixture"
            );
        }
        let mut wrong_algorithm = header();
        wrong_algorithm["alg"] = json!("HS256");
        assert_eq!(
            put_token(
                &mut session,
                JWT_TOKEN_TYPE,
                RESOURCE,
                signed(&wrong_algorithm, &claims(current, current + 300))
            )
            .await,
            401
        );
        let valid = token(current, current + 300);
        let (prefix, signature) = valid.rsplit_once('.').unwrap();
        let mut bytes = URL_SAFE_NO_PAD.decode(signature).unwrap();
        bytes[0] ^= 1;
        assert_eq!(
            put_token(
                &mut session,
                JWT_TOKEN_TYPE,
                RESOURCE,
                format!("{prefix}.{}", URL_SAFE_NO_PAD.encode(bytes))
            )
            .await,
            401
        );
        assert_eq!(
            put_token(
                &mut session,
                JWT_TOKEN_TYPE,
                "amqps://other.example/orders",
                valid.clone()
            )
            .await,
            401
        );
        assert_eq!(
            put_token(
                &mut session,
                JWT_TOKEN_TYPE,
                "amqps://tenant.servicebus.windows.net/other",
                valid.clone()
            )
            .await,
            401
        );
        refused_send(&mut session, "invalid-data", "orders").await;
        refused_send(&mut session, "invalid-management", "orders/$management").await;
        assert!(calls.lock().unwrap().is_empty());
        assert!(binds.lock().unwrap().is_empty());
        assert_eq!(
            put_token(&mut session, JWT_TOKEN_TYPE, RESOURCE, valid).await,
            202
        );
        refused_send(&mut session, "sibling-data", "orders-archive").await;
        refused_send(
            &mut session,
            "sibling-management",
            "orders-archive/$management",
        )
        .await;
        let mut receiver = Receiver::attach(&mut session, "listen-denied", "orders")
            .await
            .unwrap();
        assert!(receiver.recv().await.is_err());
        assert!(calls.lock().unwrap().is_empty());
        assert!(binds.lock().unwrap().is_empty());
        let mut sender = Sender::attach(&mut session, "permitted", "orders")
            .await
            .unwrap();
        assert!(matches!(
            sender
                .send(
                    Message::builder()
                        .body(Body::Value(Value::String("allowed".into())))
                        .build()
                )
                .await
                .unwrap(),
            Outcome::Accepted(_)
        ));
        assert_eq!(calls.lock().unwrap().len(), 1);
        let mut replies = Receiver::builder()
            .name("management-reply")
            .source("orders/$management")
            .target("management-reply")
            .attach(&mut session)
            .await
            .unwrap();
        let mut requests = Sender::attach(&mut session, "management-request", "orders/$management")
            .await
            .unwrap();
        let mut peek = OrderedMap::new();
        peek.insert(Value::String("from-sequence-number".into()), Value::Long(0));
        peek.insert(Value::String("message-count".into()), Value::Int(1));
        assert_eq!(
            management(
                &mut requests,
                &mut replies,
                crate::PEEK_MESSAGE_OPERATION,
                peek
            )
            .await,
            401
        );
        let mut rules = OrderedMap::new();
        rules.insert(Value::String("skip".into()), Value::Int(0));
        rules.insert(Value::String("top".into()), Value::Int(10));
        assert_eq!(
            management(
                &mut requests,
                &mut replies,
                crate::ENUMERATE_RULES_OPERATION,
                rules
            )
            .await,
            401
        );
        assert_eq!(
            calls.lock().unwrap().len(),
            1,
            "denied Listen and Manage operations never dispatch"
        );
        assert_eq!(
            management(
                &mut requests,
                &mut replies,
                crate::SCHEDULE_MESSAGE_OPERATION,
                schedule_body()
            )
            .await,
            200
        );
        assert_eq!(
            calls.lock().unwrap().len(),
            2,
            "Send-authorized management dispatches"
        );
        connection.close().await.unwrap();
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tls_jwt_shorter_refresh_expires_retained_sender_and_management_links() {
    let mut node = Node::start(true, false, true).await;
    let address = node.address;
    let calls = node.calls.clone();
    node.run(async move {
        let mut connection = connect(address, true, false).await;
        let mut session = Session::begin(&mut connection).await.unwrap();
        let current = now();
        assert_eq!(
            put_token(
                &mut session,
                JWT_TOKEN_TYPE,
                RESOURCE,
                token(current, current + 300)
            )
            .await,
            202
        );
        let mut sender = Sender::attach(&mut session, "retained-sender", "orders")
            .await
            .unwrap();
        let mut requests =
            Sender::attach(&mut session, "retained-management", "orders/$management")
                .await
                .unwrap();
        assert!(matches!(
            sender
                .send(
                    Message::builder()
                        .body(Body::Value(Value::String("before".into())))
                        .build()
                )
                .await
                .unwrap(),
            Outcome::Accepted(_)
        ));
        // Refresh the same issuer, subject and scope with a shorter interval.
        // Invalid refresh must not destroy the current grant or widen rights.
        assert_eq!(
            put_token(
                &mut session,
                JWT_TOKEN_TYPE,
                RESOURCE,
                "invalid-refresh".into()
            )
            .await,
            401
        );
        let current = now();
        assert_eq!(
            put_token(
                &mut session,
                JWT_TOKEN_TYPE,
                RESOURCE,
                token(current, current + 3)
            )
            .await,
            202
        );
        timeout(WAIT, sender.on_detach())
            .await
            .expect("retained sender observes refreshed expiry, not original expiry");
        assert!(
            sender
                .send(
                    Message::builder()
                        .body(Body::Value(Value::String("after".into())))
                        .build()
                )
                .await
                .is_err()
        );
        let request = Message::builder()
            .properties(Properties {
                message_id: Some("expired-management".into()),
                reply_to: Some("unused".into()),
                ..Properties::default()
            })
            .application_properties(
                ApplicationProperties::builder()
                    .insert(
                        crate::OPERATION_PROPERTY,
                        crate::CANCEL_SCHEDULED_MESSAGE_OPERATION,
                    )
                    .build(),
            )
            .body(Body::Value(Value::Map(cancel_body())))
            .build();
        // The retained management link may already have detached; either way it
        // cannot submit broker work after its grant expires.
        let _ = timeout(WAIT, requests.send(request))
            .await
            .expect("expired management request completes or refuses");
        connection.close().await.unwrap();
        assert_eq!(calls.lock().unwrap().len(), 1);
    })
    .await;
    assert_eq!(
        node.calls.lock().unwrap().len(),
        1,
        "no late broker work after original connection family joined"
    );
}

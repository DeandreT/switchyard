//! TLS, SASL ANONYMOUS, CBS, and scoped link authorization as one wire path.

use std::{
    error::Error,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use amqp::{
    AnnotationKey, ApplicationProperties, Array, Body, ClientConnection as Connection,
    ClientReceiver as Receiver, ClientSender as Sender, ClientSession as Session, Message,
    MessageAnnotations, OrderedMap, Outcome, Properties, SaslInit, Symbol, Value,
};
use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use base64::{Engine, engine::general_purpose::STANDARD};
use domain::{CommandKind, QueueConfig, StateMachine};
use hmac::{Hmac, Mac};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustls::{
    ClientConfig, RootCertStore,
    crypto::ring,
    pki_types::{CertificateDer, ServerName},
    version::{TLS12, TLS13},
};
use server::{Broker, LocalProposer, ManualClock};
use sha2::Sha256;
use storage::MemoryStore;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsConnector;
use url::form_urlencoded::byte_serialize;

const HOST: &str = "tenant.servicebus.windows.net";
const AUDIENCE: &str = "amqps://tenant.servicebus.windows.net/orders";
const MIXED_CASE_AUDIENCE: &str = "amqps://tenant.servicebus.windows.net/oRdErS";
const RULE: &str = "test-rule";
const KEY: &str = "test-secret";
const REPLY_TO: &str = "cbs-client-reply-to";

struct AuthNode {
    _broker: Broker,
    address: String,
    certificate: CertificateDer<'static>,
}

enum SaslProfile {
    Anonymous,
    Plain { username: String, password: String },
}

impl AuthNode {
    async fn start(
        permissions: PermissionSet,
        authorization_timeout: Duration,
    ) -> Result<Self, Box<dyn Error>> {
        Self::start_with_scope(
            ResourceScope::namespace(HOST)?,
            permissions,
            authorization_timeout,
        )
        .await
    }

    async fn start_with_scope(
        scope: ResourceScope,
        permissions: PermissionSet,
        authorization_timeout: Duration,
    ) -> Result<Self, Box<dyn Error>> {
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(MemoryStore::default()),
            ManualClock::at(1_000),
        ));
        let namespace = domain::NamespaceName::new("tenant")?;
        broker.handle().submit_blocking(
            namespace.clone(),
            domain::EntityPath::new("orders")?,
            CommandKind::CreateQueue {
                config: QueueConfig::default(),
            },
        )?;
        broker.handle().submit_blocking(
            namespace.clone(),
            domain::EntityPath::new("orders")?,
            CommandKind::Send {
                message_id: String::from("seed"),
                body: b"seed".to_vec(),
                time_to_live_millis: None,
                session_id: None,
                scheduled_enqueue_at: None,
                envelope: None,
            },
        )?;

        let rule =
            SharedAccessRule::new(RULE, scope, SharedAccessKey::new(KEY)?, None, permissions)?;
        let authentication =
            protocol_amqp::SharedAccessAuthentication::new(SharedAccessPolicy::new([rule])?, HOST)?
                .with_authorization_timeout(authorization_timeout);

        let CertifiedKey { cert, key_pair } =
            generate_simple_self_signed(vec![String::from("localhost")])?;
        let tls = protocol_amqp::tls_server_config(
            cert.pem().as_bytes(),
            key_pair.serialize_pem().as_bytes(),
        )?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?.to_string();
        let handle = broker.handle();
        tokio::spawn(async move {
            let _ = protocol_amqp::AmqpListener::new(handle, namespace)
                .with_tls(tls)
                .with_shared_access_authentication(authentication)
                .serve(listener)
                .await;
        });

        Ok(Self {
            _broker: broker,
            address,
            certificate: cert.der().clone(),
        })
    }

    async fn connect(&self) -> Result<Connection, Box<dyn Error>> {
        self.connect_with_profile(SaslProfile::Anonymous).await
    }

    async fn connect_with_profile(
        &self,
        profile: SaslProfile,
    ) -> Result<Connection, Box<dyn Error>> {
        let mut roots = RootCertStore::empty();
        roots.add(self.certificate.clone())?;
        let config = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_protocol_versions(&[&TLS13, &TLS12])?
            .with_root_certificates(roots)
            .with_no_client_auth();
        let tcp = TcpStream::connect(&self.address).await?;
        let tls = TlsConnector::from(Arc::new(config))
            .connect(ServerName::try_from("localhost")?, tcp)
            .await?;
        let init = match profile {
            SaslProfile::Anonymous => SaslInit {
                mechanism: Symbol::from("ANONYMOUS"),
                initial_response: None,
                hostname: Some(String::from(HOST)),
            },
            SaslProfile::Plain { username, password } => SaslInit {
                mechanism: Symbol::from("PLAIN"),
                initial_response: Some(
                    [
                        b"\0".as_slice(),
                        username.as_bytes(),
                        b"\0",
                        password.as_bytes(),
                    ]
                    .concat()
                    .into(),
                ),
                hostname: Some(String::from(HOST)),
            },
        };
        Ok(Connection::builder()
            .container_id("test-client")
            .sasl(init)
            .open_with_stream(tls)
            .await?)
    }
}

fn expiry_after(seconds: u64) -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + seconds
}

fn sas_token(audience: &str, expiry: u64) -> String {
    let encoded_resource: String = byte_serialize(audience.as_bytes()).collect();
    let input = format!("{encoded_resource}\n{expiry}");
    let mut hmac = Hmac::<Sha256>::new_from_slice(KEY.as_bytes()).unwrap();
    hmac.update(input.as_bytes());
    let signature = STANDARD.encode(hmac.finalize().into_bytes());
    let encoded_signature: String = byte_serialize(signature.as_bytes()).collect();
    format!(
        "SharedAccessSignature sr={encoded_resource}&sig={encoded_signature}&se={expiry}&skn={RULE}"
    )
}

async fn put_token(session: &mut Session, token: String) -> Result<i32, Box<dyn Error>> {
    put_token_for_audience(session, AUDIENCE, token).await
}

async fn put_token_for_audience(
    session: &mut Session,
    audience: &str,
    token: String,
) -> Result<i32, Box<dyn Error>> {
    let mut request_link = Sender::attach(session, "cbs-request", protocol_amqp::CBS_NODE).await?;
    let mut response_link = Receiver::builder()
        .name("cbs-response")
        .source(protocol_amqp::CBS_NODE)
        .target(REPLY_TO)
        .attach(session)
        .await?;

    let message_id = String::from("request-1");
    let request = Message::builder()
        .properties(Properties {
            message_id: Some(message_id.clone().into()),
            reply_to: Some(String::from(REPLY_TO)),
            ..Properties::default()
        })
        .application_properties(
            ApplicationProperties::builder()
                .insert("operation", String::from("put-token"))
                .insert("type", String::from("servicebus.windows.net:sastoken"))
                .insert("name", String::from(audience))
                .build(),
        )
        .body(Body::Value(Value::String(token)))
        .build();
    request_link.send(request).await?;

    let response = response_link.recv().await?;
    assert_eq!(
        response
            .message()
            .properties
            .as_ref()
            .and_then(|properties| properties.correlation_id.clone()),
        Some(message_id.into())
    );
    let status = match response
        .message()
        .application_properties
        .as_ref()
        .and_then(|properties| properties.get("status-code"))
    {
        Some(Value::Int(status)) => *status,
        other => panic!("expected an int CBS status, got {other:?}"),
    };
    assert!(matches!(
        response
            .message()
            .application_properties
            .as_ref()
            .and_then(|properties| properties.get("status-description")),
        Some(Value::String(_))
    ));
    response_link.accept(&response).await?;
    request_link.close().await?;
    response_link.close().await?;
    Ok(status)
}

fn body(text: &str) -> Body {
    Body::Data(vec![text.as_bytes().to_vec().into()])
}

async fn management_request(
    requests: &mut Sender,
    responses: &mut Receiver,
    reply_to: &str,
    message_id: &str,
    operation: &str,
    body: OrderedMap<Value, Value>,
) -> Result<Message, Box<dyn Error>> {
    let request = Message::builder()
        .properties(Properties {
            message_id: Some(message_id.to_owned().into()),
            reply_to: Some(reply_to.to_owned()),
            ..Properties::default()
        })
        .application_properties(
            ApplicationProperties::builder()
                .insert(protocol_amqp::OPERATION_PROPERTY, operation)
                .build(),
        )
        .body(Body::Value(Value::Map(body)))
        .build();
    assert!(matches!(
        requests.send(request).await?,
        Outcome::Accepted(_)
    ));

    let response = tokio::time::timeout(Duration::from_secs(2), responses.recv()).await??;
    let message = response.message().clone();
    responses.accept(&response).await?;
    Ok(message)
}

fn assert_management_status(message: &Message, expected: i32) {
    assert_eq!(
        message
            .application_properties
            .as_ref()
            .and_then(|properties| properties.get(protocol_amqp::STATUS_CODE_PROPERTY)),
        Some(&Value::Int(expected))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn entity_scoped_cbs_authorization_and_links_share_case_insensitive_identity()
-> Result<(), Box<dyn Error>> {
    let node = AuthNode::start_with_scope(
        ResourceScope::entity(HOST, "ORDERS")?,
        PermissionSet::SEND | PermissionSet::LISTEN,
        Duration::from_secs(20),
    )
    .await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    assert_eq!(
        put_token_for_audience(
            &mut session,
            MIXED_CASE_AUDIENCE,
            sas_token(MIXED_CASE_AUDIENCE, expiry_after(60)),
        )
        .await?,
        202
    );

    let mut receiver = Receiver::attach(&mut session, "case-folded-receiver", "OrDeRs").await?;
    let seed = receiver.recv().await?;
    assert_eq!(seed.message().body, body("seed"));
    receiver.accept(&seed).await?;

    let mut sender = Sender::attach(&mut session, "case-folded-sender", "orders").await?;
    assert!(matches!(
        sender
            .send(Message::builder().body(body("case-authorized")).build())
            .await?,
        Outcome::Accepted(_)
    ));
    let delivery = receiver.recv().await?;
    assert_eq!(delivery.message().body, body("case-authorized"));
    receiver.accept(&delivery).await?;

    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn cbs_grants_send_but_not_listen() -> Result<(), Box<dyn Error>> {
    let node = AuthNode::start(PermissionSet::SEND, Duration::from_secs(20)).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;

    let mut premature = Sender::attach(&mut session, "premature", "orders").await?;
    assert!(
        premature
            .send(Message::builder().body(body("too-early")).build())
            .await
            .is_err(),
        "an entity link worked before CBS authorization"
    );

    assert_eq!(
        put_token(&mut session, sas_token(AUDIENCE, expiry_after(60))).await?,
        202
    );
    let mut sender = Sender::attach(&mut session, "authorized-sender", "orders").await?;
    assert!(matches!(
        sender
            .send(Message::builder().body(body("authorized")).build())
            .await?,
        Outcome::Accepted(_)
    ));

    let reply_to = "send-management-replies";
    let mut management_responses = Receiver::builder()
        .name("send-management-response")
        .source("orders/$management")
        .target(reply_to)
        .attach(&mut session)
        .await?;
    let mut management_requests = Sender::attach(
        &mut session,
        "send-management-request",
        "orders/$management",
    )
    .await?;

    let mut scheduled = Message::builder()
        .properties(Properties {
            message_id: Some("send-only-scheduled".into()),
            ..Properties::default()
        })
        .body(body("send-only-scheduled"))
        .build();
    let mut annotations = MessageAnnotations::default();
    annotations.insert(
        AnnotationKey::from("x-opt-scheduled-enqueue-time"),
        Value::Timestamp(5_000_i64.into()),
    );
    scheduled.message_annotations = Some(annotations);
    let mut schedule_entry = OrderedMap::new();
    schedule_entry.insert(
        Value::String(String::from("message")),
        Value::Binary(amqp::encode_message(&scheduled)?.into()),
    );
    schedule_entry.insert(
        Value::String(String::from("message-id")),
        Value::String(String::from("send-only-scheduled")),
    );
    let mut schedule_body = OrderedMap::new();
    schedule_body.insert(
        Value::String(String::from("messages")),
        Value::List(vec![Value::Map(schedule_entry)]),
    );
    let schedule_response = management_request(
        &mut management_requests,
        &mut management_responses,
        reply_to,
        "send-schedule-1",
        protocol_amqp::SCHEDULE_MESSAGE_OPERATION,
        schedule_body,
    )
    .await?;
    assert_management_status(&schedule_response, 200);
    let Body::Value(Value::Map(schedule_response_body)) = &schedule_response.body else {
        return Err("schedule response body was not an AMQP map".into());
    };
    let Some(Value::Array(sequence_values)) =
        schedule_response_body.get(&Value::String(String::from("sequence-numbers")))
    else {
        return Err("schedule response omitted sequence-numbers".into());
    };
    let [Value::Long(scheduled_sequence)] = sequence_values.as_slice() else {
        return Err("schedule response did not contain one long sequence".into());
    };

    let mut peek_body = OrderedMap::new();
    peek_body.insert(
        Value::String(String::from("from-sequence-number")),
        Value::Long(*scheduled_sequence),
    );
    peek_body.insert(Value::String(String::from("message-count")), Value::Int(1));
    let peek_response = management_request(
        &mut management_requests,
        &mut management_responses,
        reply_to,
        "send-peek-1",
        protocol_amqp::PEEK_MESSAGE_OPERATION,
        peek_body,
    )
    .await?;
    assert_management_status(&peek_response, 401);

    let mut rules_body = OrderedMap::new();
    rules_body.insert(Value::String(String::from("skip")), Value::Int(0));
    rules_body.insert(Value::String(String::from("top")), Value::Int(100));
    let rules_response = management_request(
        &mut management_requests,
        &mut management_responses,
        reply_to,
        "send-rules-1",
        protocol_amqp::ENUMERATE_RULES_OPERATION,
        rules_body,
    )
    .await?;
    assert_management_status(&rules_response, 401);

    let mut cancel_body = OrderedMap::new();
    cancel_body.insert(
        Value::String(String::from("sequence-numbers")),
        Value::Array(Array::from(vec![Value::Long(*scheduled_sequence)])),
    );
    let cancel_response = management_request(
        &mut management_requests,
        &mut management_responses,
        reply_to,
        "send-cancel-1",
        protocol_amqp::CANCEL_SCHEDULED_MESSAGE_OPERATION,
        cancel_body,
    )
    .await?;
    assert_management_status(&cancel_response, 200);

    let mut receiver = Receiver::attach(&mut session, "forbidden-receiver", "orders").await?;
    let refused = tokio::time::timeout(Duration::from_secs(2), receiver.recv()).await?;
    assert!(refused.is_err(), "a Send grant also granted Listen");
    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn sasl_plain_grants_the_rules_rights_during_the_handshake() -> Result<(), Box<dyn Error>> {
    let node = AuthNode::start(PermissionSet::SEND, Duration::from_secs(20)).await?;
    let mut connection = node
        .connect_with_profile(SaslProfile::Plain {
            username: String::from(RULE),
            password: String::from(KEY),
        })
        .await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "plain-sender", "orders").await?;
    assert!(matches!(
        sender
            .send(Message::builder().body(body("plain")).build())
            .await?,
        Outcome::Accepted(_)
    ));

    let refused = node
        .connect_with_profile(SaslProfile::Plain {
            username: String::from(RULE),
            password: String::from("wrong"),
        })
        .await;
    assert!(refused.is_err(), "SASL PLAIN accepted the wrong key");
    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn cbs_grants_listen_but_not_send() -> Result<(), Box<dyn Error>> {
    let node = AuthNode::start(PermissionSet::LISTEN, Duration::from_secs(20)).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    assert_eq!(
        put_token(&mut session, sas_token(AUDIENCE, expiry_after(60))).await?,
        202
    );

    let reply_to = "authorized-management-replies";
    let mut management_responses = Receiver::builder()
        .name("authorized-management-response")
        .source("orders/$management")
        .target(reply_to)
        .attach(&mut session)
        .await?;
    let mut management_requests = Sender::attach(
        &mut session,
        "authorized-management-request",
        "orders/$management",
    )
    .await?;
    let request = Message::builder()
        .properties(Properties {
            message_id: Some("listen-management-1".into()),
            reply_to: Some(reply_to.to_owned()),
            ..Properties::default()
        })
        .application_properties(
            ApplicationProperties::builder()
                .insert("operation", "unsupported-auth-probe")
                .build(),
        )
        .body(Body::Value(Value::Null))
        .build();
    assert!(matches!(
        management_requests.send(request).await?,
        Outcome::Accepted(_)
    ));
    let management_response =
        tokio::time::timeout(Duration::from_secs(2), management_responses.recv()).await??;
    assert_eq!(
        management_response
            .message()
            .application_properties
            .as_ref()
            .and_then(|properties| properties.get("statusCode")),
        Some(&Value::Int(400)),
        "a Listen grant did not reach management request processing"
    );
    management_responses.accept(&management_response).await?;

    let mut receiver = Receiver::attach(&mut session, "authorized-receiver", "orders").await?;
    let delivery = receiver.recv().await?;
    receiver.accept(&delivery).await?;

    let mut sender = Sender::attach(&mut session, "forbidden-sender", "orders").await?;
    assert!(
        sender
            .send(Message::builder().body(body("forbidden")).build())
            .await
            .is_err(),
        "a Listen grant also granted Send"
    );
    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn an_invalid_token_gets_a_correlated_unauthorized_response() -> Result<(), Box<dyn Error>> {
    let node = AuthNode::start(PermissionSet::SEND, Duration::from_secs(20)).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let token = sas_token(AUDIENCE, expiry_after(60)).replace("sig=", "sig=tampered");

    assert_eq!(put_token(&mut session, token).await?, 401);
    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_grant_expires_under_an_open_link() -> Result<(), Box<dyn Error>> {
    let node = AuthNode::start(PermissionSet::SEND, Duration::from_secs(20)).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    assert_eq!(
        put_token(&mut session, sas_token(AUDIENCE, expiry_after(3))).await?,
        202
    );
    let mut sender = Sender::attach(&mut session, "expiring-sender", "orders").await?;

    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(
        sender
            .send(Message::builder().body(body("too-late")).build())
            .await
            .is_err(),
        "an expired grant kept its link usable"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_connection_without_cbs_is_closed_on_its_deadline() -> Result<(), Box<dyn Error>> {
    let node = AuthNode::start(PermissionSet::SEND, Duration::from_millis(100)).await?;
    let mut connection = node.connect().await?;

    tokio::time::timeout(Duration::from_secs(2), connection.on_close()).await?;
    Ok(())
}

//! TLS, SASL ANONYMOUS, CBS, and scoped link authorization as one wire path.

use std::{
    error::Error,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use amqp::{
    ApplicationProperties, Array, Body, ClientConnection as Connection, ClientReceiver as Receiver,
    ClientSender as Sender, ClientSession as Session, Message, OrderedMap, Outcome, Properties,
    SaslInit, Symbol, Value, encode_message,
};
use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use base64::{Engine, engine::general_purpose::STANDARD};
use domain::{
    CommandKind, CommandOutcome, MessageStatus, QueueConfig, ScheduledMessage, SequenceNumber,
    StateMachine, Timestamp,
};
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
            },
        )?;

        let rule = SharedAccessRule::new(
            RULE,
            ResourceScope::namespace(HOST)?,
            SharedAccessKey::new(KEY)?,
            None,
            permissions,
        )?;
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
                .insert("name", String::from(AUDIENCE))
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

struct ManagementClient {
    requests: Sender,
    responses: Receiver,
}

impl ManagementClient {
    async fn attach(session: &mut Session) -> Result<Self, Box<dyn Error>> {
        let responses = Receiver::builder()
            .name("management-responses")
            .source("orders/$management")
            .target("management-replies")
            .attach(session)
            .await?;
        let requests = Sender::attach(session, "management-requests", "orders/$management").await?;
        Ok(Self {
            requests,
            responses,
        })
    }

    async fn request(
        &mut self,
        message_id: &str,
        operation: &str,
        body: OrderedMap<Value, Value>,
    ) -> Result<Message, Box<dyn Error>> {
        let request = Message::builder()
            .properties(Properties {
                message_id: Some(message_id.to_owned().into()),
                reply_to: Some(String::from("management-replies")),
                ..Properties::default()
            })
            .application_properties(
                ApplicationProperties::builder()
                    .insert(protocol_amqp::OPERATION_PROPERTY, operation.to_owned())
                    .build(),
            )
            .body(Body::Value(Value::Map(body)))
            .build();
        assert!(matches!(
            self.requests.send(request).await?,
            Outcome::Accepted(_)
        ));
        let response =
            tokio::time::timeout(Duration::from_secs(2), self.responses.recv()).await??;
        let message = response.message().clone();
        assert_eq!(
            message
                .properties
                .as_ref()
                .and_then(|properties| properties.correlation_id.clone()),
            Some(message_id.to_owned().into()),
        );
        self.responses.accept(&response).await?;
        Ok(message)
    }
}

fn assert_management_status(response: &Message, expected: i32) {
    assert_eq!(
        response
            .application_properties
            .as_ref()
            .and_then(|properties| properties.get(protocol_amqp::STATUS_CODE_PROPERTY)),
        Some(&Value::Int(expected)),
    );
}

fn scheduling_body() -> Result<OrderedMap<Value, Value>, Box<dyn Error>> {
    let mut message = Message::data(b"scheduled".to_vec());
    let mut annotations = OrderedMap::new();
    annotations.insert(
        Symbol::from(protocol_amqp::SCHEDULED_ENQUEUE_TIME_ANNOTATION),
        Value::Timestamp(2_000_i64.into()),
    );
    message.message_annotations = Some(annotations.into());
    let mut entry = OrderedMap::new();
    entry.insert(
        Value::String(protocol_amqp::MESSAGE.to_owned()),
        Value::Binary(encode_message(&message)?.into()),
    );
    let mut body = OrderedMap::new();
    body.insert(
        Value::String(protocol_amqp::MESSAGES.to_owned()),
        Value::List(vec![Value::Map(entry)]),
    );
    Ok(body)
}

fn cancel_body(sequence: SequenceNumber) -> OrderedMap<Value, Value> {
    let mut body = OrderedMap::new();
    body.insert(
        Value::String(protocol_amqp::SEQUENCE_NUMBERS.to_owned()),
        Value::Array(Array::from(vec![Value::Long(
            i64::try_from(sequence.as_u64()).expect("test sequence fits in an AMQP long"),
        )])),
    );
    body
}

fn peek_body() -> OrderedMap<Value, Value> {
    let mut body = OrderedMap::new();
    body.insert(
        Value::String(protocol_amqp::FROM_SEQUENCE_NUMBER.to_owned()),
        Value::Long(0),
    );
    body.insert(
        Value::String(protocol_amqp::MESSAGE_COUNT.to_owned()),
        Value::Int(10),
    );
    body
}

async fn broker_messages(node: &AuthNode) -> Result<Vec<domain::Delivery>, Box<dyn Error>> {
    let outcome = node
        ._broker
        .handle()
        .submit(
            domain::NamespaceName::new("tenant")?,
            domain::EntityPath::new("orders")?,
            CommandKind::Peek {
                from_sequence: SequenceNumber::new(0),
                max_messages: 10,
                session_id: None,
            },
        )
        .await?;
    let CommandOutcome::Peeked(messages) = outcome else {
        panic!("a broker peek returns messages");
    };
    Ok(messages)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_send_grant_schedules_and_cancels_but_cannot_peek() -> Result<(), Box<dyn Error>> {
    let node = AuthNode::start(PermissionSet::SEND, Duration::from_secs(20)).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    assert_eq!(
        put_token(&mut session, sas_token(AUDIENCE, expiry_after(60))).await?,
        202
    );
    let mut management = ManagementClient::attach(&mut session).await?;

    let denied = management
        .request(
            "denied-peek",
            protocol_amqp::PEEK_MESSAGE_OPERATION,
            peek_body(),
        )
        .await?;
    assert_management_status(&denied, 401);
    let scheduled = management
        .request(
            "schedule",
            protocol_amqp::SCHEDULE_MESSAGE_OPERATION,
            scheduling_body()?,
        )
        .await?;
    assert_management_status(&scheduled, 200);
    let Body::Value(Value::Map(body)) = scheduled.body else {
        panic!("scheduling returns a map");
    };
    let Some(Value::Array(sequences)) =
        body.get(&Value::String(protocol_amqp::SEQUENCE_NUMBERS.to_owned()))
    else {
        panic!("scheduling returns sequence numbers");
    };
    let [Value::Long(sequence)] = sequences.as_slice() else {
        panic!("scheduling returns one long sequence number");
    };
    let sequence = SequenceNumber::new(u64::try_from(*sequence)?);
    assert_eq!(broker_messages(&node).await?.len(), 2);
    let cancelled = management
        .request(
            "cancel",
            protocol_amqp::CANCEL_SCHEDULED_MESSAGE_OPERATION,
            cancel_body(sequence),
        )
        .await?;
    assert_management_status(&cancelled, 200);
    let messages = broker_messages(&node).await?;
    assert_eq!(messages.len(), 1, "Send may cancel its scheduled message");
    assert_eq!(messages[0].body, b"seed");
    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_listen_grant_peeks_but_cannot_schedule_or_cancel() -> Result<(), Box<dyn Error>> {
    let node = AuthNode::start(PermissionSet::LISTEN, Duration::from_secs(20)).await?;
    let outcome = node
        ._broker
        .handle()
        .submit(
            domain::NamespaceName::new("tenant")?,
            domain::EntityPath::new("orders")?,
            CommandKind::Schedule {
                messages: vec![ScheduledMessage {
                    message_id: String::from("scheduled-seed"),
                    body: b"scheduled-seed".to_vec(),
                    time_to_live_millis: None,
                    session_id: None,
                    enqueue_at: Timestamp::from_millis(2_000),
                }],
            },
        )
        .await?;
    let CommandOutcome::Scheduled { sequences } = outcome else {
        panic!("the broker returns a scheduling handle");
    };
    let sequence = sequences[0];
    let before = broker_messages(&node).await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    assert_eq!(
        put_token(&mut session, sas_token(AUDIENCE, expiry_after(60))).await?,
        202
    );
    let mut management = ManagementClient::attach(&mut session).await?;

    let peeked = management
        .request("peek", protocol_amqp::PEEK_MESSAGE_OPERATION, peek_body())
        .await?;
    assert_management_status(&peeked, 200);
    let Body::Value(Value::Map(body)) = peeked.body else {
        panic!("peek returns a map");
    };
    let Some(Value::List(messages)) = body.get(&Value::String(protocol_amqp::MESSAGES.to_owned()))
    else {
        panic!("peek returns messages");
    };
    assert_eq!(messages.len(), 2);
    let denied_schedule = management
        .request(
            "denied-schedule",
            protocol_amqp::SCHEDULE_MESSAGE_OPERATION,
            scheduling_body()?,
        )
        .await?;
    assert_management_status(&denied_schedule, 401);
    assert_eq!(
        broker_messages(&node).await?,
        before,
        "unauthorized scheduling changed the queue"
    );
    let denied_cancel = management
        .request(
            "denied-cancel",
            protocol_amqp::CANCEL_SCHEDULED_MESSAGE_OPERATION,
            cancel_body(sequence),
        )
        .await?;
    assert_management_status(&denied_cancel, 401);
    assert_eq!(
        broker_messages(&node).await?,
        before,
        "unauthorized cancellation changed the queue"
    );
    assert_eq!(before[1].status, MessageStatus::Scheduled);
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

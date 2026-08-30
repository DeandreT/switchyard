//! AMQP tunneled through the Service Bus WebSocket endpoint over TLS.

use std::{error::Error, sync::Arc};

use amqp::{
    Body, ClientConnection as Connection, ClientReceiver as Receiver, ClientSender as Sender,
    ClientSession as Session, Message, Outcome,
};
use domain::{CommandKind, QueueConfig, StateMachine};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustls::{
    ClientConfig, RootCertStore,
    crypto::ring,
    pki_types::{CertificateDer, ServerName},
    version::{TLS12, TLS13},
};
use server::{Broker, LocalProposer, ManualClock};
use storage::MemoryStore;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsConnector;
use tokio_tungstenite::{
    client_async,
    tungstenite::{
        client::IntoClientRequest,
        http::{HeaderValue, header::SEC_WEBSOCKET_PROTOCOL},
    },
};

struct WebSocketNode {
    _broker: Broker,
    address: String,
    certificate: CertificateDer<'static>,
}

impl WebSocketNode {
    async fn start() -> Result<Self, Box<dyn Error>> {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
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
                .serve_websockets(listener)
                .await;
        });

        Ok(Self {
            _broker: broker,
            address,
            certificate: cert.der().clone(),
        })
    }

    async fn connect(&self) -> Result<Connection, Box<dyn Error>> {
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
        let mut request = format!(
            "wss://{}{path}",
            self.address,
            path = protocol_amqp::SERVICE_BUS_WEBSOCKET_PATH
        )
        .into_client_request()?;
        request.headers_mut().insert(
            SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_static(protocol_amqp::AMQP_WEBSOCKET_SUBPROTOCOL),
        );
        let (websocket, response) = client_async(request, tls).await?;
        assert_eq!(
            response.headers().get(SEC_WEBSOCKET_PROTOCOL),
            Some(&HeaderValue::from_static(
                protocol_amqp::AMQP_WEBSOCKET_SUBPROTOCOL
            ))
        );
        let connection = Connection::builder()
            .container_id("websocket-test-client")
            .open_with_stream(protocol_amqp::WebSocketIo::new(websocket))
            .await?;
        Ok(connection)
    }
}

fn body(text: &str) -> Body {
    Body::Data(vec![text.as_bytes().to_vec().into()])
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wss_client_sends_receives_and_settles() -> Result<(), Box<dyn Error>> {
    let node = WebSocketNode::start().await?;
    let deadline = std::time::Duration::from_secs(3);
    let mut connection = tokio::time::timeout(deadline, node.connect()).await??;
    let mut session = tokio::time::timeout(deadline, Session::begin(&mut connection)).await??;
    let mut sender = tokio::time::timeout(
        deadline,
        Sender::attach(&mut session, "wss-sender", "orders"),
    )
    .await??;

    assert!(matches!(
        tokio::time::timeout(
            deadline,
            sender.send(Message::builder().body(body("through-wss")).build())
        )
        .await??,
        Outcome::Accepted(_)
    ));
    let mut receiver = tokio::time::timeout(
        deadline,
        Receiver::attach(&mut session, "wss-receiver", "orders"),
    )
    .await??;
    let delivery = tokio::time::timeout(deadline, receiver.recv()).await??;
    assert_eq!(delivery.message().body, body("through-wss"));
    tokio::time::timeout(deadline, receiver.accept(&delivery)).await??;

    tokio::time::timeout(deadline, receiver.close()).await??;
    tokio::time::timeout(deadline, sender.close()).await??;
    tokio::time::timeout(deadline, session.end()).await??;
    tokio::time::timeout(deadline, connection.close()).await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn plaintext_http_cannot_upgrade_the_wss_listener() -> Result<(), Box<dyn Error>> {
    let node = WebSocketNode::start().await?;
    let tcp = TcpStream::connect(&node.address).await?;
    let mut request = format!(
        "ws://{}{path}",
        node.address,
        path = protocol_amqp::SERVICE_BUS_WEBSOCKET_PATH
    )
    .into_client_request()?;
    request.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static(protocol_amqp::AMQP_WEBSOCKET_SUBPROTOCOL),
    );
    let upgraded = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        client_async(request, tcp),
    )
    .await?;
    assert!(upgraded.is_err(), "plaintext upgraded a WSS listener");
    Ok(())
}

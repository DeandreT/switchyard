//! AMQP on a socket secured before the protocol handshake, as port 5671 expects.

use std::{error::Error, sync::Arc};

use domain::{CommandKind, QueueConfig, StateMachine};
use amqp_runtime::{
    Connection, Receiver, Sender, Session,
    connection::ConnectionHandle,
    types::{
        messaging::{Body, Message, Outcome},
        primitives::Binary,
    },
};
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

struct TlsNode {
    _broker: Broker,
    address: String,
    certificate: CertificateDer<'static>,
}

impl TlsNode {
    async fn start() -> Result<Self, Box<dyn Error>> {
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
                .serve(listener)
                .await;
        });

        Ok(Self {
            _broker: broker,
            address,
            certificate: cert.der().clone(),
        })
    }

    async fn connect(&self) -> Result<ConnectionHandle<()>, Box<dyn Error>> {
        let mut roots = RootCertStore::empty();
        roots.add(self.certificate.clone())?;
        self.connect_with_roots(roots).await
    }

    async fn connect_with_roots(
        &self,
        roots: RootCertStore,
    ) -> Result<ConnectionHandle<()>, Box<dyn Error>> {
        let config = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
            .with_protocol_versions(&[&TLS13, &TLS12])?
            .with_root_certificates(roots)
            .with_no_client_auth();
        let tcp = TcpStream::connect(&self.address).await?;
        let tls = TlsConnector::from(Arc::new(config))
            .connect(ServerName::try_from("localhost")?, tcp)
            .await?;
        Ok(Connection::builder()
            .container_id("test-client")
            .open_with_stream(tls)
            .await?)
    }
}

fn body(text: &str) -> Body<Binary> {
    Body::Data(amqp_runtime::types::messaging::Batch::new(vec![
        amqp_runtime::types::messaging::Data(Binary::from(text.as_bytes().to_vec())),
    ]))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_trusted_tls_client_sends_and_receives() -> Result<(), Box<dyn Error>> {
    let node = TlsNode::start().await?;
    let mut connection = node.connect().await?;
    let mut session = Session::begin(&mut connection).await?;
    let mut sender = Sender::attach(&mut session, "test-sender", "orders").await?;

    let outcome = sender
        .send(Message::builder().body(body("secured")).build())
        .await?;
    assert!(matches!(outcome, Outcome::Accepted(_)));

    let mut receiver = Receiver::attach(&mut session, "test-receiver", "orders").await?;
    let delivery = receiver.recv::<Body<Binary>>().await?;
    assert_eq!(
        delivery.message().body,
        Message::builder().body(body("secured")).build().body
    );
    receiver.accept(&delivery).await?;
    connection.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn an_untrusted_server_certificate_stops_the_connection() -> Result<(), Box<dyn Error>> {
    let node = TlsNode::start().await?;
    let result = node.connect_with_roots(RootCertStore::empty()).await;
    assert!(result.is_err(), "an untrusted certificate opened AMQP");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn plaintext_cannot_cross_a_tls_listener() -> Result<(), Box<dyn Error>> {
    let node = TlsNode::start().await?;
    let opened = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        Connection::builder()
            .container_id("plaintext-client")
            .open(format!("amqp://{}", node.address).as_str()),
    )
    .await?;
    assert!(opened.is_err(), "plaintext opened a TLS listener");
    Ok(())
}

use std::{net::SocketAddr, sync::Arc};

use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use base64::{Engine, engine::general_purpose::STANDARD};
use domain::StateMachine;
use hmac::{Hmac, Mac};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use rustls::{
    ClientConfig, RootCertStore,
    crypto::ring,
    pki_types::{CertificateDer, ServerName},
    version::{TLS12, TLS13},
};
use server::{Broker, LocalProposer, ManualClock, NativeAdminListener, NativeAdminService};
use sha2::Sha256;
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use tokio_rustls::TlsConnector;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use url::form_urlencoded::byte_serialize;

use super::*;

pub(super) struct Node<P: StoreProvider> {
    broker: Option<Broker>,
    store: Option<P::Store>,
    provider: P,
    pub(super) clock: ManualClock,
    endpoint: String,
    address: SocketAddr,
    certificate: CertificateDer<'static>,
    certificate_pem: String,
    private_key_pem: String,
    listeners: Vec<JoinHandle<()>>,
}

impl<P: StoreProvider> Node<P> {
    pub(super) async fn start(provider: P) -> TestResult<Self> {
        let CertifiedKey { cert, key_pair } =
            generate_simple_self_signed(vec!["localhost".into()])?;
        let mut node = Self {
            broker: None,
            store: Some(provider.open()?),
            provider,
            clock: ManualClock::at(10_000),
            endpoint: String::new(),
            address: "127.0.0.1:0".parse()?,
            certificate: cert.der().clone(),
            certificate_pem: cert.pem(),
            private_key_pem: key_pair.serialize_pem(),
            listeners: Vec::new(),
        };
        node.spawn().await?;
        Ok(node)
    }

    async fn spawn(&mut self) -> TestResult {
        let namespace = NamespaceName::new("tenant")?;
        self.broker = Some(Broker::spawn(LocalProposer::new(
            StateMachine::new(self.store.as_ref().expect("open store").clone()),
            self.clock.clone(),
        )));
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
        ])?;
        let handle = self.broker.as_ref().expect("broker").handle();
        let service = NativeAdminService::new(handle.clone(), namespace.clone())
            .with_shared_access_policy(policy.clone(), HOST)?;
        let admin = NativeAdminListener::new(service).with_tls(
            self.certificate_pem.as_bytes(),
            self.private_key_pem.as_bytes(),
        )?;
        let socket = timeout(DEADLINE, TcpListener::bind("127.0.0.1:0")).await??;
        self.endpoint = format!("https://{}", socket.local_addr()?);
        self.listeners.push(tokio::spawn(async move {
            let _ = admin.serve(socket).await;
        }));
        let socket = timeout(DEADLINE, TcpListener::bind("127.0.0.1:0")).await??;
        self.address = socket.local_addr()?;
        let listener = protocol_amqp::AmqpListener::new(handle, namespace)
            .with_tls(protocol_amqp::tls_server_config(
                self.certificate_pem.as_bytes(),
                self.private_key_pem.as_bytes(),
            )?)
            .with_shared_access_authentication(protocol_amqp::SharedAccessAuthentication::new(
                policy, HOST,
            )?);
        self.listeners.push(tokio::spawn(async move {
            let _ = listener.serve(socket).await;
        }));
        Ok(())
    }

    pub(super) async fn channel(&self) -> TestResult<Channel> {
        let endpoint = Endpoint::from_shared(self.endpoint.clone())?
            .connect_timeout(DEADLINE)
            .timeout(DEADLINE)
            .tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(Certificate::from_pem(&self.certificate_pem))
                    .domain_name("localhost"),
            )?;
        Ok(timeout(DEADLINE, endpoint.connect()).await??)
    }

    pub(super) async fn clients(
        &self,
    ) -> TestResult<(EntityServiceClient<Channel>, RuleServiceClient<Channel>)> {
        let channel = self.channel().await?;
        Ok((
            EntityServiceClient::new(channel.clone()),
            RuleServiceClient::new(channel),
        ))
    }

    pub(super) async fn topology(&self, client: &mut EntityServiceClient<Channel>) -> TestResult {
        for (path, kind) in [
            ("Orders", EntityKind::Topic),
            (CHILD, EntityKind::Subscription),
            ("Orders/subscriptions/Beta", EntityKind::Subscription),
        ] {
            timeout(
                DEADLINE,
                client.create_entity(request(
                    CreateEntityRequest {
                        namespace: "tenant".into(),
                        path: path.into(),
                        kind: kind as i32,
                        ..Default::default()
                    },
                    Some(sas("", "manage")),
                )),
            )
            .await??;
        }
        Ok(())
    }

    pub(super) async fn connect_amqp(&self) -> TestResult<amqp::ClientConnection> {
        timeout(DEADLINE, async {
            let mut roots = RootCertStore::empty();
            roots.add(self.certificate.clone())?;
            let config = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
                .with_protocol_versions(&[&TLS13, &TLS12])?
                .with_root_certificates(roots)
                .with_no_client_auth();
            let tcp = TcpStream::connect(self.address).await?;
            let tls = TlsConnector::from(Arc::new(config))
                .connect(ServerName::try_from("localhost")?, tcp)
                .await?;
            Ok::<_, Box<dyn Error>>(
                amqp::ClientConnection::builder()
                    .container_id("native-rule-routing")
                    .sasl(amqp::SaslInit {
                        mechanism: amqp::Symbol::from("PLAIN"),
                        initial_response: Some(
                            [b"\0manage\0".as_slice(), KEY.as_bytes()].concat().into(),
                        ),
                        hostname: Some(HOST.into()),
                    })
                    .open_with_stream(tls)
                    .await?,
            )
        })
        .await?
    }

    pub(super) fn snapshot(&self) -> TestResult<storage::StoreSnapshot> {
        Ok(self.store.as_ref().expect("open store").snapshot()?)
    }

    pub(super) async fn peek(&self, path: &str) -> TestResult<Vec<domain::Delivery>> {
        let outcome = timeout(
            DEADLINE,
            self.broker.as_ref().expect("broker").handle().submit(
                NamespaceName::new("tenant")?,
                domain::EntityPath::new(path)?,
                domain::CommandKind::Peek {
                    from_sequence: domain::SequenceNumber::new(0),
                    max_messages: 32,
                    session_id: None,
                },
            ),
        )
        .await??;
        let domain::CommandOutcome::Peeked(deliveries) = outcome else {
            return Err("bounded peek outcome missing".into());
        };
        Ok(deliveries)
    }

    pub(super) async fn wait_empty(&self, path: &str) -> TestResult {
        timeout(DEADLINE, async {
            loop {
                if self.peek(path).await?.is_empty() {
                    return Ok::<_, Box<dyn Error>>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await?
    }

    pub(super) async fn restart(&mut self) -> TestResult {
        for listener in self.listeners.drain(..) {
            listener.abort();
            let _ = listener.await;
        }
        drop(self.broker.take());
        drop(self.store.take());
        self.store = Some(self.provider.open()?);
        self.spawn().await
    }
}

impl<P: StoreProvider> Drop for Node<P> {
    fn drop(&mut self) {
        for listener in &self.listeners {
            listener.abort();
        }
    }
}

pub(super) fn sas(path: &str, rule: &str) -> String {
    let audience = if path.is_empty() {
        format!("amqps://{HOST}")
    } else {
        format!("amqps://{HOST}/{path}")
    };
    let resource: String = byte_serialize(audience.as_bytes()).collect();
    let expiry = 4_102_444_800_u64;
    let mut mac = Hmac::<Sha256>::new_from_slice(KEY.as_bytes()).expect("HMAC key");
    mac.update(format!("{resource}\n{expiry}").as_bytes());
    let signature: String =
        byte_serialize(STANDARD.encode(mac.finalize().into_bytes()).as_bytes()).collect();
    format!("SharedAccessSignature sr={resource}&sig={signature}&se={expiry}&skn={rule}")
}

pub(super) fn request<T>(input: T, token: Option<String>) -> Request<T> {
    let mut request = Request::new(input);
    request.set_timeout(DEADLINE);
    if let Some(token) = token {
        request
            .metadata_mut()
            .insert("authorization", token.parse().expect("ASCII token"));
    }
    request
}

pub(super) fn create(path: &str, name: &str, filter: Filter) -> CreateRuleRequest {
    CreateRuleRequest {
        namespace: "tenant".into(),
        subscription_path: path.into(),
        name: name.into(),
        filter: Some(RuleFilter {
            filter: Some(filter),
        }),
    }
}

pub(super) fn get(path: &str, name: &str) -> GetRuleRequest {
    GetRuleRequest {
        namespace: "tenant".into(),
        subscription_path: path.into(),
        name: name.into(),
    }
}

pub(super) fn list(path: &str) -> ListRulesRequest {
    ListRulesRequest {
        namespace: "tenant".into(),
        subscription_path: path.into(),
    }
}

pub(super) fn delete(path: &str, name: &str) -> DeleteRuleRequest {
    DeleteRuleRequest {
        namespace: "tenant".into(),
        subscription_path: path.into(),
        name: name.into(),
    }
}

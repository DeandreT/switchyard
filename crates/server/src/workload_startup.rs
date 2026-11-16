//! Bounded immutable candidate preparation. Called before ANY durable open.
use super::*;
use auth::workload::{PreparedWorkloadTls, WorkloadPolicy};
use std::sync::{Arc, OnceLock};

fn invalid() -> StartupError {
    StartupError::Runtime("invalid workload transport configuration".into())
}
fn read(path: &Path, limit: u64) -> Result<Vec<u8>, StartupError> {
    let mut bytes = Vec::new();
    fs::File::open(path)
        .and_then(|file| file.take(limit + 1).read_to_end(&mut bytes))
        .map_err(|_| invalid())?;
    if bytes.len() as u64 > limit {
        return Err(invalid());
    }
    Ok(bytes)
}
fn root(arguments: &Arguments) -> Result<auth::ResourceScope, StartupError> {
    let host = if arguments.namespace.contains('.') {
        arguments.namespace.to_ascii_lowercase()
    } else {
        format!("{}.servicebus.windows.net", arguments.namespace)
    };
    Ok(auth::ResourceScope::namespace(&host)?)
}
fn endpoint(
    arguments: &Arguments,
    policy: &Path,
    cert: Option<&Path>,
    key: Option<&Path>,
    alpn: Vec<Vec<u8>>,
) -> Result<PreparedWorkloadTls, StartupError> {
    let (cert, key) = (cert.ok_or_else(invalid)?, key.ok_or_else(invalid)?);
    let json = read(policy, 256 * 1024)?;
    let policy = WorkloadPolicy::from_json(
        std::str::from_utf8(&json).map_err(|_| invalid())?,
        root(arguments)?,
    )
    .map_err(|_| invalid())?;
    PreparedWorkloadTls::new(
        policy,
        &read(cert, 64 * 1024)?,
        &read(key, 32 * 1024)?,
        alpn,
        Duration::from_secs(arguments.admission_timeout_secs.get()),
    )
    .map_err(|_| invalid())
}
fn jwt(path: Option<&Path>) -> Result<Option<auth::JwtPolicy>, StartupError> {
    path.map(|path| {
        let bytes = read(path, 64 * 1024)?;
        auth::JwtPolicy::from_json(std::str::from_utf8(&bytes).map_err(|_| invalid())?)
            .map_err(|_| invalid())
    })
    .transpose()
}
pub(super) fn admin(arguments: &Arguments) -> Result<AdminTlsConfig, StartupError> {
    arguments.admin_listen.ok_or_else(invalid)?;
    let endpoint = endpoint(
        arguments,
        arguments
            .admin_workload_policy_file
            .as_deref()
            .ok_or_else(invalid)?,
        arguments.admin_tls_certificate.as_deref(),
        arguments.admin_tls_private_key.as_deref(),
        vec![b"h2".to_vec()],
    )?;
    AdminTlsConfig::workload(
        endpoint,
        jwt(arguments.admin_jwt_policy_file.as_deref())?,
        root(arguments)?,
    )
    .map_err(|_| invalid())
}
pub(super) fn atom(arguments: &Arguments) -> Result<AtomConfiguration, StartupError> {
    arguments.atom_listen.ok_or_else(invalid)?;
    let endpoint = endpoint(
        arguments,
        arguments
            .atom_workload_policy_file
            .as_deref()
            .ok_or_else(invalid)?,
        arguments.atom_tls_certificate.as_deref(),
        arguments.atom_tls_private_key.as_deref(),
        vec![b"http/1.1".to_vec()],
    )?;
    AtomConfiguration::workload(
        endpoint,
        jwt(arguments.atom_jwt_policy_file.as_deref())?,
        namespace_from_hostname(&arguments.namespace)?,
        root(arguments)?,
    )
    .map_err(|_| invalid())
}

// A one-time backend slot allows the ACTUAL validated opaque listener to be
// prepared before durable open. It is filled once before any socket is served;
// no replacement, raw TLS extraction or authorization callback is provided.
#[derive(Clone)]
pub(super) struct StartupBroker(Arc<OnceLock<BrokerHandle>>);
impl protocol_amqp::Broker for StartupBroker {
    async fn bind_entity(
        &self,
        ns: domain::NamespaceName,
        entity: domain::EntityPath,
    ) -> Result<domain::EntityBinding, protocol_amqp::BrokerRejection> {
        protocol_amqp::Broker::bind_entity(
            self.0.get().expect("backend installed before serving"),
            ns,
            entity,
        )
        .await
    }
    async fn submit_bound(
        &self,
        binding: domain::EntityBinding,
        kind: domain::CommandKind,
    ) -> Result<domain::CommandOutcome, protocol_amqp::BrokerRejection> {
        protocol_amqp::Broker::submit_bound(
            self.0.get().expect("backend installed before serving"),
            binding,
            kind,
        )
        .await
    }
    async fn submit(
        &self,
        ns: domain::NamespaceName,
        entity: domain::EntityPath,
        kind: domain::CommandKind,
    ) -> Result<domain::CommandOutcome, protocol_amqp::BrokerRejection> {
        protocol_amqp::Broker::submit(
            self.0.get().expect("backend installed before serving"),
            ns,
            entity,
            kind,
        )
        .await
    }
    async fn deliverable(&self, ns: &domain::NamespaceName, entity: &domain::EntityPath) {
        protocol_amqp::Broker::deliverable(
            self.0.get().expect("backend installed before serving"),
            ns,
            entity,
        )
        .await;
    }
}
pub(super) enum PreparedAmqp {
    Legacy(AmqpListener<StartupBroker>),
    Workload(protocol_amqp::WorkloadAmqpListener<StartupBroker>),
}
impl PreparedAmqp {
    pub(super) fn into_service(
        self,
        socket: tokio::net::TcpListener,
        transport: TransportArgument,
    ) -> protocol_amqp::AmqpListenerService<StartupBroker> {
        match (self, transport) {
            (Self::Legacy(listener), TransportArgument::AmqpTcp) => {
                listener.into_tcp_service(socket)
            }
            (Self::Legacy(listener), TransportArgument::AmqpWebSockets) => {
                listener.into_wss_service(socket)
            }
            (Self::Workload(listener), TransportArgument::AmqpTcp) => {
                listener.into_tcp_service(socket)
            }
            (Self::Workload(listener), TransportArgument::AmqpWebSockets) => {
                listener.into_wss_service(socket)
            }
        }
    }
}
pub(super) fn amqp(
    arguments: &Arguments,
    namespace: domain::NamespaceName,
    tls: Option<ServerConfig>,
    authentication: Option<SharedAccessAuthentication>,
) -> Result<(PreparedAmqp, Arc<OnceLock<BrokerHandle>>), StartupError> {
    let slot = Arc::new(OnceLock::new());
    let mut listener = AmqpListener::new(StartupBroker(slot.clone()), namespace)
        .with_admission_timeout(Duration::from_secs(arguments.admission_timeout_secs.get()));
    if let Some(limit) = arguments.max_connections {
        listener = listener.with_max_connections(limit);
    }
    if let Some(tls) = tls {
        listener = listener.with_tls(tls);
    }
    if let Some(authentication) = authentication {
        listener = listener.with_shared_access_authentication(authentication);
    }
    let listener = if let Some(path) = &arguments.amqp_workload_policy_file {
        let endpoint = endpoint(
            arguments,
            path,
            arguments.tls_certificate.as_deref(),
            arguments.tls_private_key.as_deref(),
            vec![],
        )?;
        PreparedAmqp::Workload(
            listener
                .with_workload_tls(endpoint)
                .map_err(|_| invalid())?,
        )
    } else {
        PreparedAmqp::Legacy(listener)
    };
    Ok((listener, slot))
}

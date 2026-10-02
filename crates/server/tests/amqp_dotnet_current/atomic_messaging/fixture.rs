use std::fs;

use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use domain::{CommandKind, EntityPath, NamespaceName, QueueConfig, StateMachine};
use server::{Broker, LocalProposer, SystemClock};
use testkit::StoreProvider;
use tokio::{net::TcpListener, task::JoinHandle};

use super::*;

pub(super) struct Fixture<P: StoreProvider> {
    pub(super) atomic_endpoint: String,
    pub(super) ordinary_endpoint: String,
    pub(super) ca_file: std::path::PathBuf,
    pub(super) ca_directory: std::path::PathBuf,
    broker: Option<Broker>,
    listeners: Vec<JoinHandle<std::io::Result<()>>>,
    store: Option<P::Store>,
    provider: Option<P>,
    namespace: NamespaceName,
    _certificates: tempfile::TempDir,
}

impl<P: StoreProvider> Fixture<P> {
    pub(super) async fn start(provider: P) -> TestResult<Self> {
        let store = provider.open()?;
        let namespace = NamespaceName::new("tenant")?;
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            SystemClock,
        ));
        for name in [SEND_QUEUE, HELD_QUEUE, CONTROL_QUEUE] {
            broker.handle().submit_blocking(
                namespace.clone(),
                EntityPath::new(name)?,
                CommandKind::CreateQueue {
                    config: QueueConfig {
                        lock_duration_millis: domain::MAX_LOCK_DURATION_MILLIS,
                        default_time_to_live_millis: None,
                        ..QueueConfig::default()
                    },
                },
            )?;
        }
        let authentication = protocol_amqp::SharedAccessAuthentication::new(
            SharedAccessPolicy::new([SharedAccessRule::new(
                RULE,
                ResourceScope::namespace(HOST)?,
                SharedAccessKey::new(KEY)?,
                None,
                PermissionSet::MANAGE,
            )?])?,
            HOST,
        )?
        .with_authorization_timeout(Duration::from_secs(15));
        let (tls, ca_pem) = websocket::signed_localhost_config()?;
        let certificates = tempfile::TempDir::new()?;
        let ca_file = certificates.path().join("trusted-ca.pem");
        let ca_directory = certificates.path().join("empty-ca-directory");
        fs::write(&ca_file, ca_pem)?;
        fs::create_dir(&ca_directory)?;
        let ordinary_socket = TcpListener::bind("127.0.0.1:0").await?;
        let atomic_socket = TcpListener::bind("127.0.0.1:0").await?;
        let ordinary_endpoint = format!("sb://localhost:{}", ordinary_socket.local_addr()?.port());
        let atomic_endpoint = format!("sb://localhost:{}", atomic_socket.local_addr()?.port());
        let ordinary = protocol_amqp::AmqpListener::new(broker.handle(), namespace.clone())
            .with_tls(tls.clone())
            .with_shared_access_authentication(authentication.clone());
        let atomic = protocol_amqp::AmqpListener::new(broker.handle(), namespace.clone())
            .with_tls(tls)
            .with_shared_access_authentication(authentication);
        let listeners = vec![
            tokio::spawn(ordinary.serve(ordinary_socket)),
            tokio::spawn(atomic.serve_atomic_messaging_ingress(atomic_socket)),
        ];
        Ok(Self {
            atomic_endpoint,
            ordinary_endpoint,
            ca_file,
            ca_directory,
            broker: Some(broker),
            listeners,
            store: Some(store),
            provider: Some(provider),
            namespace,
            _certificates: certificates,
        })
    }

    pub(super) async fn stop(&mut self) -> TestResult {
        let mut failure: Option<Box<dyn Error>> = None;
        for listener in &self.listeners {
            listener.abort();
        }
        for listener in self.listeners.drain(..) {
            match tokio::time::timeout(Duration::from_secs(5), listener).await {
                Ok(Err(error)) if error.is_cancelled() => {}
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => {
                    failure.get_or_insert_with(|| Box::new(error));
                }
                Ok(Err(error)) => {
                    failure.get_or_insert_with(|| Box::new(error));
                }
                Err(error) => {
                    failure.get_or_insert_with(|| Box::new(error));
                }
            }
        }
        // Broker Drop closes admission and joins its owner, even with outstanding handles.
        drop(self.broker.take());
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub(super) fn into_stopped_parts(mut self) -> (P, P::Store, NamespaceName) {
        assert!(self.broker.is_none() && self.listeners.is_empty());
        (
            self.provider.take().expect("fixture provider"),
            self.store.take().expect("fixture store"),
            self.namespace.clone(),
        )
    }
}

impl<P: StoreProvider> Drop for Fixture<P> {
    fn drop(&mut self) {
        for listener in &self.listeners {
            listener.abort();
        }
        drop(self.broker.take());
    }
}

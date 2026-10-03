use std::fs;

use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use domain::{
    CommandKind, EntityPath, NamespaceName, QueueConfig, StateMachine, SubscriptionConfig,
    SubscriptionName, TopicConfig,
};
use server::{Broker, LocalProposer, SystemClock};
use testkit::StoreProvider;
use tokio::{net::TcpListener, task::JoinHandle};

use super::*;

pub(super) fn queue_config() -> QueueConfig {
    QueueConfig {
        lock_duration_millis: domain::MAX_LOCK_DURATION_MILLIS,
        default_time_to_live_millis: None,
        ..QueueConfig::default()
    }
}

pub(super) fn subscription_config() -> SubscriptionConfig {
    SubscriptionConfig {
        lock_duration_millis: domain::MAX_LOCK_DURATION_MILLIS,
        default_time_to_live_millis: None,
        ..SubscriptionConfig::default()
    }
}

pub(super) struct Fixture<P: StoreProvider> {
    pub(super) endpoint: String,
    pub(super) ca_file: std::path::PathBuf,
    pub(super) ca_directory: std::path::PathBuf,
    broker: Option<Broker>,
    listener: Option<JoinHandle<std::io::Result<()>>>,
    store: Option<P::Store>,
    provider: Option<P>,
    namespace: NamespaceName,
    _certificates: tempfile::TempDir,
}

impl<P: StoreProvider> Fixture<P> {
    pub(super) async fn start(provider: P) -> TestResult<Self> {
        let store = provider.open()?;
        let namespace = NamespaceName::new("tenant")?;
        let topic = EntityPath::new(TOPIC)?;
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            SystemClock,
        ));
        broker.handle().submit_blocking(
            namespace.clone(),
            topic.clone(),
            CommandKind::CreateTopic {
                config: TopicConfig::default(),
            },
        )?;
        for name in ["Alpha", "beta"] {
            broker.handle().submit_blocking(
                namespace.clone(),
                topic.clone(),
                CommandKind::CreateSubscription {
                    name: SubscriptionName::new(name)?,
                    config: subscription_config(),
                },
            )?;
        }
        broker.handle().submit_blocking(
            namespace.clone(),
            EntityPath::new(QUEUE)?,
            CommandKind::CreateQueue {
                config: queue_config(),
            },
        )?;
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
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("sb://localhost:{}", socket.local_addr()?.port());
        let listener = protocol_amqp::AmqpListener::new(broker.handle(), namespace.clone())
            .with_tls(tls)
            .with_shared_access_authentication(authentication);
        let listener = Some(tokio::spawn(listener.serve(socket)));
        Ok(Self {
            endpoint,
            ca_file,
            ca_directory,
            broker: Some(broker),
            listener,
            store: Some(store),
            provider: Some(provider),
            namespace,
            _certificates: certificates,
        })
    }

    pub(super) async fn stop(&mut self) -> TestResult {
        let mut failure: Option<Box<dyn Error>> = None;
        if let Some(listener) = self.listener.take() {
            listener.abort();
            match tokio::time::timeout(Duration::from_secs(5), listener).await {
                Ok(Err(error)) if error.is_cancelled() => {}
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => {
                    failure = Some(Box::new(error));
                }
                Ok(Err(error)) => {
                    failure = Some(Box::new(error));
                }
                Err(error) => {
                    failure = Some(Box::new(error));
                }
            }
        }
        // Broker Drop joins its owner before releasing the proposer's store.
        drop(self.broker.take());
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub(super) fn into_stopped_parts(mut self) -> (P, P::Store, NamespaceName) {
        assert!(self.broker.is_none() && self.listener.is_none());
        (
            self.provider.take().expect("fixture provider"),
            self.store.take().expect("fixture store"),
            self.namespace.clone(),
        )
    }
}

impl<P: StoreProvider> Drop for Fixture<P> {
    fn drop(&mut self) {
        if let Some(listener) = &self.listener {
            listener.abort();
        }
        drop(self.broker.take());
    }
}

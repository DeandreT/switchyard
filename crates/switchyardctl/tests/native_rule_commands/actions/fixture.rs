use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use domain::{NamespaceName, StateMachine};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use server::{
    Broker, BrokerHandle, LocalProposer, ManualClock, NativeAdminListener, NativeAdminService,
};
use tokio::task::JoinHandle;

use super::*;

pub(super) struct ActionNode<S: StateStore> {
    broker: Option<Broker>,
    store: Option<S>,
    pub(super) clock: ManualClock,
    pub(super) files: TempDir,
    arguments: Vec<String>,
    listener: Option<JoinHandle<()>>,
}

impl<S: StateStore> ActionNode<S> {
    pub(super) async fn start(store: S) -> TestResult<Self> {
        let clock = ManualClock::at(10_000);
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            clock.clone(),
        ));
        let policy = SharedAccessPolicy::new([SharedAccessRule::new(
            "manage",
            ResourceScope::namespace(HOST)?,
            SharedAccessKey::new(KEY)?,
            None,
            PermissionSet::MANAGE,
        )?])?;
        let service = NativeAdminService::new(broker.handle(), NamespaceName::new("tenant")?)
            .with_shared_access_policy(policy, HOST)?;
        let CertifiedKey { cert, key_pair } =
            generate_simple_self_signed(vec!["localhost".into()])?;
        let files = TempDir::new()?;
        let certificate = cert.pem();
        let ca = files.path().join("ca.pem");
        let token = files.path().join("token");
        std::fs::write(&ca, &certificate)?;
        std::fs::write(&token, format!("{}\n", crate::fixture::sas("", "manage")))?;
        let admin = NativeAdminListener::new(service)
            .with_tls(certificate.as_bytes(), key_pair.serialize_pem().as_bytes())?;
        let socket = timeout(DEADLINE, TcpListener::bind("127.0.0.1:0")).await??;
        let arguments = vec![
            "--namespace".into(),
            "tenant".into(),
            "--endpoint".into(),
            format!("https://{}", socket.local_addr()?),
            "--ca-certificate".into(),
            ca.display().to_string(),
            "--tls-server-name".into(),
            "localhost".into(),
            "--token-file".into(),
            token.display().to_string(),
        ];
        let listener = tokio::spawn(async move {
            let _ = admin.serve(socket).await;
        });
        Ok(Self {
            broker: Some(broker),
            store: Some(store),
            clock,
            files,
            arguments,
            listener: Some(listener),
        })
    }

    pub(super) fn handle(&self) -> BrokerHandle {
        self.broker.as_ref().expect("broker").handle()
    }
    pub(super) fn store(&self) -> &S {
        self.store.as_ref().expect("store")
    }

    pub(super) fn file(&self, name: &str, value: &Value) -> TestResult<String> {
        let path = self.files.path().join(name);
        std::fs::write(&path, serde_json::to_vec(value)?)?;
        Ok(path.display().to_string())
    }

    pub(super) async fn run(&self, command: &[&str]) -> TestResult<Output> {
        let mut arguments = self.arguments.clone();
        arguments.extend(command.iter().map(|argument| (*argument).into()));
        run(arguments).await
    }

    pub(super) async fn json(&self, command: &[&str]) -> TestResult<Value> {
        let output = self.run(command).await?;
        assert!(
            output.status.success(),
            "CLI failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stderr.is_empty());
        Ok(serde_json::from_slice(&output.stdout)?)
    }

    pub(super) async fn stop(mut self) -> TestResult<S> {
        if let Some(listener) = self.listener.take() {
            listener.abort();
            let joined = timeout(DEADLINE, listener).await?;
            if let Err(error) = joined {
                assert!(error.is_cancelled());
            }
        }
        drop(self.broker.take());
        Ok(self.store.take().expect("store"))
    }
}

impl<S: StateStore> Drop for ActionNode<S> {
    fn drop(&mut self) {
        if let Some(listener) = &self.listener {
            listener.abort();
        }
        drop(self.broker.take());
    }
}

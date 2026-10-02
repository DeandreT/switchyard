use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use admin_api::v1::{
    CreateRuleRequest, CreateRuleWithActionRequest, DeleteRuleRequest, GetRuleRequest,
    ListRulesRequest, ListRulesResponse, Rule, RuleMutationResponse, SqlRuleAction,
    rule_service_server::{RuleService, RuleServiceServer},
};
use domain::{SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig};
use server::{Broker, Clock, LocalProposer, ManualClock, NativeAdminService};
use tokio::task::JoinHandle;
use tonic::{
    Request, Response, Status,
    transport::{Server, server::TcpIncoming},
};

use super::*;

#[derive(Clone)]
struct ObservedClock {
    clock: ManualClock,
    reads: Arc<AtomicUsize>,
}

impl Clock for ObservedClock {
    fn now(&self) -> Timestamp {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.clock.now()
    }
}

#[derive(Clone)]
struct LegacyEndpoint {
    current: NativeAdminService,
    legacy_creates: Arc<AtomicUsize>,
    action_creates: Arc<AtomicUsize>,
    malformed_replies: bool,
}

#[tonic::async_trait]
impl RuleService for LegacyEndpoint {
    async fn create_rule(
        &self,
        input: Request<CreateRuleRequest>,
    ) -> Result<Response<RuleMutationResponse>, Status> {
        self.legacy_creates.fetch_add(1, Ordering::SeqCst);
        self.current.create_rule(input).await
    }

    async fn create_rule_with_action(
        &self,
        _: Request<CreateRuleWithActionRequest>,
    ) -> Result<Response<RuleMutationResponse>, Status> {
        self.action_creates.fetch_add(1, Ordering::SeqCst);
        Err(Status::unimplemented("private legacy method diagnostic"))
    }

    async fn get_rule(&self, input: Request<GetRuleRequest>) -> Result<Response<Rule>, Status> {
        assert!(
            input.get_ref().include_actions,
            "CLI must request action metadata"
        );
        if self.malformed_replies {
            let mut response = self.current.get_rule(input).await?.into_inner();
            response.action = Some(SqlRuleAction {
                expression: "REMOVE private-reply-source".into(),
                semantic_version: None,
            });
            Ok(Response::new(response))
        } else {
            self.current.get_rule(input).await
        }
    }

    async fn list_rules(
        &self,
        input: Request<ListRulesRequest>,
    ) -> Result<Response<ListRulesResponse>, Status> {
        assert!(
            input.get_ref().include_actions,
            "CLI must request action metadata"
        );
        let mut response = self.current.list_rules(input).await?.into_inner();
        if self.malformed_replies {
            let mut bad = response.rules[0].clone();
            bad.name = "Z-invalid-action".into();
            bad.action = Some(SqlRuleAction {
                expression: "REMOVE private-reply-source".into(),
                semantic_version: Some(2),
            });
            response.rules.push(bad);
        }
        Ok(Response::new(response))
    }

    async fn delete_rule(
        &self,
        input: Request<DeleteRuleRequest>,
    ) -> Result<Response<RuleMutationResponse>, Status> {
        self.current.delete_rule(input).await
    }
}

struct Fixture {
    broker: Option<Broker>,
    store: MemoryStore,
    clock: ObservedClock,
    service: LegacyEndpoint,
    files: TempDir,
    arguments: Vec<String>,
    listener: Option<JoinHandle<()>>,
}

impl Fixture {
    async fn start(malformed_replies: bool) -> TestResult<Self> {
        let store = MemoryStore::default();
        let clock = ObservedClock {
            clock: ManualClock::at(10_000),
            reads: Arc::new(AtomicUsize::new(0)),
        };
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            clock.clone(),
        ));
        let handle = broker.handle();
        let namespace = NamespaceName::new("tenant")?;
        let topic = EntityPath::new("Orders")?;
        timeout(
            DEADLINE,
            handle.submit(
                namespace.clone(),
                topic.clone(),
                CommandKind::CreateTopic {
                    config: TopicConfig::default(),
                },
            ),
        )
        .await??;
        timeout(
            DEADLINE,
            handle.submit(
                namespace.clone(),
                topic,
                CommandKind::CreateSubscription {
                    name: SubscriptionName::new("Alpha")?,
                    config: SubscriptionConfig::default(),
                },
            ),
        )
        .await??;
        let service = LegacyEndpoint {
            current: NativeAdminService::new(handle, namespace),
            legacy_creates: Arc::new(AtomicUsize::new(0)),
            action_creates: Arc::new(AtomicUsize::new(0)),
            malformed_replies,
        };
        let incoming =
            TcpIncoming::from(timeout(DEADLINE, TcpListener::bind("127.0.0.1:0")).await??);
        let arguments = vec![
            "--endpoint".into(),
            format!("http://{}", incoming.local_addr()?),
            "--allow-insecure".into(),
            "--namespace".into(),
            "tenant".into(),
        ];
        let routed = service.clone();
        let listener = tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(
                    RuleServiceServer::new(routed)
                        .max_decoding_message_size(64 * 1024)
                        .max_encoding_message_size(1024 * 1024),
                )
                .serve_with_incoming(incoming)
                .await;
        });
        Ok(Self {
            broker: Some(broker),
            store,
            clock,
            service,
            files: TempDir::new()?,
            arguments,
            listener: Some(listener),
        })
    }

    async fn run(&self, command: &[&str]) -> TestResult<Output> {
        let mut arguments = self.arguments.clone();
        arguments.extend(command.iter().map(|argument| (*argument).into()));
        run(arguments).await
    }

    fn file(&self, name: &str, value: &Value) -> TestResult<String> {
        let path = self.files.path().join(name);
        std::fs::write(&path, serde_json::to_vec(value)?)?;
        Ok(path.display().to_string())
    }

    async fn stop(mut self) -> TestResult {
        if let Some(listener) = self.listener.take() {
            listener.abort();
            if let Err(error) = timeout(DEADLINE, listener).await? {
                assert!(error.is_cancelled());
            }
        }
        drop(self.broker.take());
        Ok(())
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(listener) = &self.listener {
            listener.abort();
        }
        drop(self.broker.take());
    }
}

pub(in super::super) async fn no_fallback() -> TestResult {
    let node = Fixture::start(false).await?;
    let filter = node.file("filter.json", &json!({"type":"true"}))?;
    let action = node.file(
        "action.json",
        &json!({"type":"sql","expression":"REMOVE [private]"}),
    )?;
    let before = node.store.snapshot()?;
    let reads = node.clock.reads.load(Ordering::SeqCst);
    let applied = node
        .broker
        .as_ref()
        .unwrap()
        .handle()
        .last_applied_blocking()?;
    node.clock.clock.set(0);
    let stderr = failed(
        &node
            .run(&[
                "rule",
                "create",
                "Orders",
                "Alpha",
                "ForbiddenFallback",
                "--filter-file",
                &filter,
                "--action-file",
                &action,
            ])
            .await?,
    );
    assert!(stderr.contains("administration request failed (Unimplemented)"));
    assert!(!stderr.contains("private"));
    assert_eq!(node.service.action_creates.load(Ordering::SeqCst), 1);
    assert_eq!(node.service.legacy_creates.load(Ordering::SeqCst), 0);
    assert_eq!(node.clock.reads.load(Ordering::SeqCst), reads);
    assert_eq!(node.clock.clock.now(), Timestamp::from_millis(0));
    assert_eq!(
        node.broker
            .as_ref()
            .unwrap()
            .handle()
            .last_applied_blocking()?,
        applied
    );
    assert_eq!(node.store.snapshot()?, before);
    node.clock.clock.set(10_000);
    let output = node
        .run(&[
            "rule",
            "create",
            "Orders",
            "Alpha",
            "Healthy",
            "--filter-file",
            &filter,
        ])
        .await?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout)?,
        json!({"namespace":"tenant","subscription_path":CHILD,"name":"Healthy","completed":true})
    );
    assert_eq!(node.service.action_creates.load(Ordering::SeqCst), 1);
    assert_eq!(node.service.legacy_creates.load(Ordering::SeqCst), 1);
    assert_ne!(node.store.snapshot()?, before);
    node.stop().await
}

pub(in super::super) async fn invalid_reply() -> TestResult {
    let node = Fixture::start(true).await?;
    let before = node.store.snapshot()?;
    let reads = node.clock.reads.load(Ordering::SeqCst);
    node.clock.clock.set(0);
    for command in [
        vec!["rule", "get", "Orders", "Alpha", "$Default"],
        vec!["rule", "list", "Orders", "Alpha"],
    ] {
        let stderr = failed(&node.run(&command).await?);
        assert!(stderr.contains("invalid rule response"));
        assert!(!stderr.contains("private"));
    }
    assert_eq!(node.clock.reads.load(Ordering::SeqCst), reads);
    assert_eq!(node.store.snapshot()?, before);
    node.stop().await
}

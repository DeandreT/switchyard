use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use amqp::{Attach, ReceiverSettleMode, Role, SenderSettleMode, Source};
use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use domain::{CommandKind, CommandOutcome, EntityBinding, EntityPath, NamespaceName, SessionId};
use tokio::sync::{Semaphore, mpsc};

use super::routing::plan_link;
use crate::{
    Attachment, Broker, BrokerRejection, EntityAdmission, EntityMetadata,
    authorization::{ConnectionAuthorization, SharedAccessAuthentication},
    stamp_session_filter,
};

#[derive(Clone)]
struct PausedAdmissionBroker {
    current: Arc<AtomicU64>,
    binds: Arc<AtomicUsize>,
    captured: mpsc::Sender<EntityBinding>,
    proceed: Arc<Semaphore>,
    attempts: Arc<Mutex<Vec<EntityBinding>>>,
    acquired: Arc<AtomicUsize>,
}

impl Broker for PausedAdmissionBroker {
    async fn bind(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> Result<Option<EntityAdmission>, BrokerRejection> {
        self.binds.fetch_add(1, Ordering::SeqCst);
        let metadata = match &target {
            Attachment::Queue(_) => EntityMetadata::Queue(domain::QueueConfig {
                requires_session: true,
                ..domain::QueueConfig::default()
            }),
            Attachment::Subscription { .. } => {
                EntityMetadata::Subscription(domain::SubscriptionConfig {
                    requires_session: true,
                    ..domain::SubscriptionConfig::default()
                })
            }
            _ => panic!("unsupported fixture target"),
        };
        let mut admission = crate::broker::test_admission(namespace, target, metadata);
        let binding = &admission.binding;
        admission.binding = EntityBinding::new(
            binding.namespace().clone(),
            binding.target().clone(),
            binding.owner().clone(),
            binding.kind(),
            self.current.load(Ordering::SeqCst),
        )
        .expect("current identity");
        self.captured
            .send(admission.binding.clone())
            .await
            .expect("test receives admission");
        self.proceed
            .acquire()
            .await
            .expect("test releases admission")
            .forget();
        Ok(Some(admission))
    }

    async fn submit_fenced(
        &self,
        binding: EntityBinding,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        assert_eq!(&entity, binding.target());
        assert!(
            matches!(kind, CommandKind::AcceptSession { session_id: Some(id), .. } if id.as_str() == "A")
        );
        self.attempts
            .lock()
            .expect("attempts")
            .push(binding.clone());
        if binding.generation() != self.current.load(Ordering::SeqCst) {
            return Err(BrokerRejection::Refused(
                domain::BrokerError::EntityBindingStale,
            ));
        }
        self.acquired.fetch_add(1, Ordering::SeqCst);
        Ok(CommandOutcome::SessionAccepted(None))
    }

    async fn rules_fenced(
        &self,
        _: EntityBinding,
        _: EntityPath,
        _: domain::SubscriptionName,
    ) -> Result<Vec<domain::RuleDefinition>, BrokerRejection> {
        panic!("planning must not read rules")
    }

    async fn entity_metadata(
        &self,
        _: NamespaceName,
        _: Attachment,
    ) -> Result<Option<EntityMetadata>, BrokerRejection> {
        panic!("admission metadata and identity must be one owner read")
    }

    async fn submit(
        &self,
        _: NamespaceName,
        _: EntityPath,
        _: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        panic!("AcceptSession must use the captured identity")
    }

    async fn rules(
        &self,
        _: NamespaceName,
        _: EntityPath,
        _: domain::SubscriptionName,
    ) -> Result<Vec<domain::RuleDefinition>, BrokerRejection> {
        panic!("planning must not read rules")
    }

    async fn deliverable(&self, _: &NamespaceName, _: &EntityPath) {
        panic!("planning must not wait for messages")
    }
}

fn receiver(address: &str) -> Attach {
    let mut source = Source::new(address);
    stamp_session_filter(&mut source, &SessionId::new("A").expect("session"));
    Attach {
        name: "session".into(),
        handle: 0,
        role: Role::Receiver,
        snd_settle_mode: SenderSettleMode::Mixed,
        rcv_settle_mode: ReceiverSettleMode::First,
        source: Some(source),
        target: None,
        unsettled: None,
        incomplete_unsettled: false,
        initial_delivery_count: None,
        max_message_size: None,
        offered_capabilities: None,
        desired_capabilities: None,
        properties: None,
    }
}

fn fixture() -> (PausedAdmissionBroker, mpsc::Receiver<EntityBinding>) {
    let (captured, receiver) = mpsc::channel(1);
    (
        PausedAdmissionBroker {
            current: Arc::new(AtomicU64::new(1)),
            binds: Arc::default(),
            captured,
            proceed: Arc::new(Semaphore::new(0)),
            attempts: Arc::default(),
            acquired: Arc::default(),
        },
        receiver,
    )
}

#[tokio::test]
async fn replacing_an_entity_between_admission_and_session_acquisition_never_acquires_the_replacement()
 {
    for address in ["Orders", "Orders/Subscriptions/Alpha"] {
        let (broker, mut observed) = fixture();
        let planning = broker.clone();
        let task = tokio::spawn(async move {
            let namespace = NamespaceName::new("tenant").expect("namespace");
            let attach = receiver(address);
            plan_link(&planning, &namespace, address, &attach, None).await
        });
        let captured = tokio::time::timeout(Duration::from_secs(2), observed.recv())
            .await
            .expect("admission deadline")
            .expect("captured admission");
        assert_eq!(captured.generation(), 1);
        broker.current.store(2, Ordering::SeqCst);
        broker.proceed.add_permits(1);
        let result = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("planning deadline")
            .expect("planning task");
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("stale plan must refuse"),
        };
        assert_eq!(
            error.condition.as_symbol(),
            amqp::Symbol::from(crate::NOT_FOUND)
        );
        assert_eq!(*broker.attempts.lock().expect("attempts"), [captured]);
        assert_eq!(broker.acquired.load(Ordering::SeqCst), 0);
        assert_eq!(broker.binds.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn authorization_denial_precedes_admission_even_when_a_session_filter_is_valid() {
    let (broker, _observed) = fixture();
    let host = "tenant.servicebus.windows.net";
    let policy = SharedAccessPolicy::new([SharedAccessRule::new(
        "sender",
        ResourceScope::namespace(host).expect("scope"),
        SharedAccessKey::new("secret").expect("key"),
        None,
        PermissionSet::SEND,
    )
    .expect("rule")])
    .expect("policy");
    let grant = policy
        .authenticate_plain("sender", "secret")
        .expect("grant");
    let authorization = ConnectionAuthorization::new(
        SharedAccessAuthentication::new(policy, host).expect("authentication"),
        Some(grant),
    );
    for address in ["Orders", "Orders/subscriptions/Alpha"] {
        let attach = receiver(address);
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            plan_link(
                &broker,
                &NamespaceName::new("tenant").expect("namespace"),
                address,
                &attach,
                Some(&authorization),
            ),
        )
        .await
        .expect("authorization deadline");
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("Listen must be denied"),
        };
        assert_eq!(
            error.condition.as_symbol(),
            amqp::Symbol::from("amqp:unauthorized-access")
        );
    }
    assert_eq!(broker.binds.load(Ordering::SeqCst), 0);
    assert!(broker.attempts.lock().expect("attempts").is_empty());
    assert_eq!(broker.acquired.load(Ordering::SeqCst), 0);
}

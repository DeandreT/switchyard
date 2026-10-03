use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use domain::{EntityIncarnationKind, RuleFilter, RuleName};

use super::*;

mod action_bindings;
mod receive_bindings;

#[derive(Clone, Debug, Eq, PartialEq)]
enum Call {
    Submit(EntityBinding, EntityPath, Box<CommandKind>),
    Rules(EntityBinding, EntityPath, SubscriptionName),
}

#[derive(Clone)]
struct FencedOnlyBroker {
    admission: Arc<Mutex<EntityAdmission>>,
    calls: Arc<Mutex<Vec<Call>>>,
    watched: Arc<AtomicUsize>,
}

impl FencedOnlyBroker {
    fn new(binding: EntityBinding) -> Self {
        Self {
            admission: Arc::new(Mutex::new(EntityAdmission {
                metadata: EntityMetadata::Subscription(SubscriptionConfig::default()),
                binding,
            })),
            calls: Arc::default(),
            watched: Arc::default(),
        }
    }
}

impl Broker for FencedOnlyBroker {
    async fn bind(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> Result<Option<EntityAdmission>, BrokerRejection> {
        let admission = self.admission.lock().expect("admission").clone();
        assert_eq!(&namespace, admission.binding.namespace());
        assert_eq!(
            target.canonical_entity().expect("target"),
            *admission.binding.target()
        );
        Ok(Some(admission))
    }

    async fn submit_fenced(
        &self,
        binding: EntityBinding,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        self.calls.lock().expect("calls").push(Call::Submit(
            binding.clone(),
            entity,
            Box::new(kind),
        ));
        if self.admission.lock().expect("admission").binding != binding {
            return Err(BrokerRejection::Refused(BrokerError::EntityBindingStale));
        }
        Ok(CommandOutcome::RuleCreated)
    }

    async fn rules_fenced(
        &self,
        binding: EntityBinding,
        topic: EntityPath,
        subscription: SubscriptionName,
    ) -> Result<Vec<RuleDefinition>, BrokerRejection> {
        self.calls
            .lock()
            .expect("calls")
            .push(Call::Rules(binding.clone(), topic, subscription));
        if self.admission.lock().expect("admission").binding != binding {
            return Err(BrokerRejection::Refused(BrokerError::EntityBindingStale));
        }
        Ok(Vec::new())
    }

    async fn submit(
        &self,
        _: NamespaceName,
        _: EntityPath,
        _: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        panic!("an admitted endpoint must not submit an unfenced command")
    }

    async fn rules(
        &self,
        _: NamespaceName,
        _: EntityPath,
        _: SubscriptionName,
    ) -> Result<Vec<RuleDefinition>, BrokerRejection> {
        panic!("an admitted endpoint must not read unfenced rules")
    }

    async fn entity_metadata(
        &self,
        _: NamespaceName,
        _: Attachment,
    ) -> Result<Option<EntityMetadata>, BrokerRejection> {
        panic!("admission must not split metadata from identity")
    }

    fn deliverable(&self, _: &NamespaceName, _: &EntityPath) -> impl Future<Output = ()> + Send {
        self.watched.fetch_add(1, Ordering::SeqCst);
        std::future::ready(())
    }
}

fn namespace() -> NamespaceName {
    NamespaceName::new("tenant").expect("namespace")
}

fn binding(generation: u64) -> EntityBinding {
    let owner = EntityPath::new("Orders/subscriptions/Alpha").expect("owner");
    EntityBinding::new(
        namespace(),
        owner.clone(),
        owner,
        EntityIncarnationKind::Subscription,
        generation,
    )
    .expect("binding")
}

fn create_rule() -> CommandKind {
    CommandKind::CreateRule {
        subscription: SubscriptionName::new("Alpha").expect("subscription"),
        name: RuleName::new("match").expect("rule"),
        filter: RuleFilter::True,
    }
}

#[tokio::test]
async fn old_shaped_operations_forward_the_subscription_identity_and_parent_command_target() {
    let identity = binding(1);
    let inner = FencedOnlyBroker::new(identity.clone());
    let bound = BoundBroker::new(inner.clone(), identity.clone());
    let topic = EntityPath::new("Orders").expect("topic");
    let subscription = SubscriptionName::new("Alpha").expect("subscription");
    assert_eq!(
        bound
            .submit(namespace(), topic.clone(), create_rule())
            .await,
        Ok(CommandOutcome::RuleCreated)
    );
    assert_eq!(
        bound
            .rules(namespace(), topic.clone(), subscription.clone())
            .await,
        Ok(Vec::new())
    );
    assert_eq!(
        *inner.calls.lock().expect("calls"),
        [
            Call::Submit(identity.clone(), topic.clone(), Box::new(create_rule())),
            Call::Rules(identity, topic, subscription),
        ]
    );
}

#[tokio::test]
async fn foreign_namespace_or_nested_identity_is_refused_before_owner_work() {
    let identity = binding(1);
    let inner = FencedOnlyBroker::new(identity.clone());
    let bound = BoundBroker::new(inner.clone(), identity);
    let topic = EntityPath::new("Orders").expect("topic");
    let foreign = NamespaceName::new("other").expect("namespace");
    let subscription = SubscriptionName::new("Alpha").expect("subscription");
    let expected = Err(BrokerRejection::Refused(BrokerError::InvalidEntityBinding));
    assert_eq!(
        bound
            .submit(foreign.clone(), topic.clone(), create_rule())
            .await,
        expected
    );
    assert_eq!(
        bound
            .rules(foreign, topic.clone(), subscription.clone())
            .await,
        Err(BrokerRejection::Refused(BrokerError::InvalidEntityBinding))
    );
    assert_eq!(
        bound
            .submit_fenced(binding(2), topic.clone(), create_rule())
            .await,
        Err(BrokerRejection::Refused(BrokerError::InvalidEntityBinding))
    );
    assert_eq!(
        bound.rules_fenced(binding(2), topic, subscription).await,
        Err(BrokerRejection::Refused(BrokerError::InvalidEntityBinding))
    );
    assert!(inner.calls.lock().expect("calls").is_empty());
}

#[tokio::test]
async fn a_stale_wrapper_cannot_refresh_itself_or_redirect_rule_operations() {
    let old = binding(1);
    let inner = FencedOnlyBroker::new(binding(2));
    let bound = BoundBroker::new(inner.clone(), old.clone());
    let topic = EntityPath::new("Orders").expect("topic");
    let subscription = SubscriptionName::new("Alpha").expect("subscription");
    let target = Attachment::Subscription {
        topic: topic.clone(),
        subscription: subscription.clone(),
    };
    assert_eq!(
        bound.bind(namespace(), target.clone()).await,
        Err(BrokerRejection::Refused(BrokerError::EntityBindingStale))
    );
    assert_eq!(
        bound.entity_metadata(namespace(), target).await,
        Err(BrokerRejection::Refused(BrokerError::EntityBindingStale))
    );
    assert_eq!(
        bound
            .submit(namespace(), topic.clone(), create_rule())
            .await,
        Err(BrokerRejection::Refused(BrokerError::EntityBindingStale))
    );
    assert_eq!(
        bound
            .rules(namespace(), topic.clone(), subscription.clone())
            .await,
        Err(BrokerRejection::Refused(BrokerError::EntityBindingStale))
    );
    assert_eq!(
        *inner.calls.lock().expect("calls"),
        [
            Call::Submit(old.clone(), topic.clone(), Box::new(create_rule())),
            Call::Rules(old, topic, subscription),
        ]
    );
}

#[tokio::test]
async fn exact_metadata_reads_keep_identity_and_notification_registration_is_synchronous() {
    let identity = binding(1);
    let inner = FencedOnlyBroker::new(identity.clone());
    let bound = BoundBroker::new(inner.clone(), identity.clone());
    let target = Attachment::Subscription {
        topic: EntityPath::new("Orders").expect("topic"),
        subscription: SubscriptionName::new("Alpha").expect("subscription"),
    };
    assert_eq!(
        bound.entity_metadata(namespace(), target).await,
        Ok(Some(EntityMetadata::Subscription(
            SubscriptionConfig::default()
        )))
    );
    let ns = namespace();
    let wait = bound.deliverable(&ns, identity.target());
    assert_eq!(inner.watched.load(Ordering::SeqCst), 1);
    wait.await;
}

use crate::{ReceiveClaimPermit, ReceiveClaimState};

use super::*;

#[derive(Clone)]
struct ReceiveBroker {
    inner: FencedOnlyBroker,
    received: Arc<Mutex<Vec<(EntityBinding, EntityPath)>>>,
}

struct ArmedReceive {
    _guard: crate::ReceiveClaimAbortGuard,
    submission: OwnedReceiveSubmission,
    inner: FencedOnlyBroker,
}

impl ArmedReceive {
    async fn run(self) -> Result<Option<domain::Delivery>, ReceiveSubmitError> {
        let (ticket, binding, _, _, _) = self.submission.into_owner_parts();
        assert_eq!(ticket.claim_expiry_epoch_seconds(), u64::MAX);
        ticket.try_claim().map_err(ReceiveSubmitError::Claim)?;
        if self.inner.admission.lock().expect("admission").binding != binding {
            return Err(ReceiveSubmitError::Refused(BrokerError::EntityBindingStale));
        }
        Ok(None)
    }
}

impl Broker for ReceiveBroker {
    fn receive_fenced_owned(
        &self,
        submission: OwnedReceiveSubmission,
    ) -> impl Future<Output = Result<Option<domain::Delivery>, ReceiveSubmitError>> + Send + 'static
    {
        let guard = submission.permit().abort_on_drop();
        self.received
            .lock()
            .expect("received")
            .push((submission.binding().clone(), submission.entity().clone()));
        ArmedReceive {
            _guard: guard,
            submission,
            inner: self.inner.clone(),
        }
        .run()
    }

    async fn bind(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> Result<Option<EntityAdmission>, BrokerRejection> {
        self.inner.bind(namespace, target).await
    }
    async fn submit_fenced(
        &self,
        binding: EntityBinding,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        self.inner.submit_fenced(binding, entity, kind).await
    }
    async fn rules_fenced(
        &self,
        binding: EntityBinding,
        topic: EntityPath,
        subscription: SubscriptionName,
    ) -> Result<Vec<RuleDefinition>, BrokerRejection> {
        self.inner.rules_fenced(binding, topic, subscription).await
    }
    async fn rules(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
        subscription: SubscriptionName,
    ) -> Result<Vec<RuleDefinition>, BrokerRejection> {
        self.inner.rules(namespace, topic, subscription).await
    }
    async fn entity_metadata(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> Result<Option<EntityMetadata>, BrokerRejection> {
        self.inner.entity_metadata(namespace, target).await
    }
    async fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        self.inner.submit(namespace, entity, kind).await
    }
    fn deliverable(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> impl Future<Output = ()> + Send {
        self.inner.deliverable(namespace, entity)
    }
}

fn physical_binding(owner: &str, shadow: bool, generation: u64) -> EntityBinding {
    let owner = EntityPath::new(owner).expect("owner");
    let target = if shadow {
        owner.dead_letter_queue().expect("shadow")
    } else {
        owner.clone()
    };
    let kind = if owner.is_subscription_path() {
        EntityIncarnationKind::Subscription
    } else {
        EntityIncarnationKind::Queue
    };
    EntityBinding::new(namespace(), target, owner, kind, generation).expect("binding")
}

fn work(binding: EntityBinding, entity: EntityPath) -> OwnedReceiveSubmission {
    let (_, ticket) = ReceiveClaimPermit::new(u64::MAX);
    OwnedReceiveSubmission::new(binding, entity, domain::ReceiveMode::PeekLock, None, ticket)
}

fn receiver(binding: EntityBinding) -> ReceiveBroker {
    ReceiveBroker {
        inner: FencedOnlyBroker::new(binding),
        received: Arc::default(),
    }
}

#[tokio::test]
async fn default_is_owned_failclosed_and_never_uses_legacy_submit() {
    let binding = physical_binding("Orders", false, 1);
    let broker = FencedOnlyBroker::new(binding.clone());
    let submission = work(binding.clone(), binding.target().clone());
    let permit = submission.permit().clone();
    let operation = broker.receive_fenced_owned(submission);
    assert_eq!(permit.state(), ReceiveClaimState::Pending);
    drop(operation);
    assert_eq!(permit.state(), ReceiveClaimState::Cancelled);
    let submission = work(binding.clone(), binding.target().clone());
    let permit = submission.permit().clone();
    assert!(matches!(
        broker.receive_fenced_owned(submission).await,
        Err(ReceiveSubmitError::Unsupported)
    ));
    assert_eq!(permit.state(), ReceiveClaimState::Cancelled);
    assert!(broker.calls.lock().expect("calls").is_empty());
}

#[tokio::test]
async fn queue_subscription_and_their_exact_shadows_delegate_before_first_poll() {
    for owner in ["Orders", "Topic/subscriptions/Alpha"] {
        for shadow in [false, true] {
            let binding = physical_binding(owner, shadow, 1);
            let inner = receiver(binding.clone());
            let bound = BoundBroker::new(inner.clone(), binding.clone());
            let submission = work(binding.clone(), binding.target().clone());
            let permit = submission.permit().clone();
            let operation = bound.receive_fenced_owned(submission);
            assert_eq!(inner.received.lock().expect("received").len(), 1);
            assert_eq!(permit.state(), ReceiveClaimState::Pending);
            assert!(matches!(operation.await, Ok(None)));
            assert_eq!(permit.state(), ReceiveClaimState::Started);
            assert_eq!(
                *inner.received.lock().expect("received"),
                [(binding.clone(), binding.target().clone())]
            );
            assert!(inner.inner.calls.lock().expect("calls").is_empty());
        }
    }
}

#[tokio::test]
async fn family_sibling_namespace_and_generation_swaps_do_not_reach_inner_factory() {
    let binding = physical_binding("Topic/subscriptions/Alpha", true, 1);
    let inner = receiver(binding.clone());
    let bound = BoundBroker::new(inner.clone(), binding.clone());
    let foreign_namespace = EntityBinding::new(
        NamespaceName::new("other").expect("namespace"),
        binding.target().clone(),
        binding.owner().clone(),
        binding.kind(),
        1,
    )
    .expect("foreign binding");
    let attempts = [
        (binding.clone(), binding.owner().clone()),
        (
            binding.clone(),
            EntityPath::new("Topic/subscriptions/Beta/$deadletterqueue").expect("sibling"),
        ),
        (
            physical_binding("Topic/subscriptions/Alpha", false, 1),
            binding.target().clone(),
        ),
        (
            physical_binding("Topic/subscriptions/Alpha", true, 2),
            binding.target().clone(),
        ),
        (foreign_namespace, binding.target().clone()),
    ];
    for (identity, target) in attempts {
        let submission = work(identity, target);
        let permit = submission.permit().clone();
        assert!(matches!(
            bound.receive_fenced_owned(submission).await,
            Err(ReceiveSubmitError::Refused(
                BrokerError::InvalidEntityBinding
            ))
        ));
        assert_eq!(permit.state(), ReceiveClaimState::Cancelled);
    }
    assert!(inner.received.lock().expect("received").is_empty());
}

#[tokio::test]
async fn an_exact_stale_route_is_not_refreshed_and_started_does_not_mean_success() {
    let old = physical_binding("Orders", true, 1);
    let inner = receiver(physical_binding("Orders", true, 2));
    let bound = BoundBroker::new(inner.clone(), old.clone());
    let submission = work(old.clone(), old.target().clone());
    let permit = submission.permit().clone();
    assert!(matches!(
        bound.receive_fenced_owned(submission).await,
        Err(ReceiveSubmitError::Refused(BrokerError::EntityBindingStale))
    ));
    assert_eq!(permit.state(), ReceiveClaimState::Started);
    assert_eq!(inner.received.lock().expect("received").len(), 1);
}

#[test]
fn bound_delegate_drop_cancels_synchronously_without_an_async_mapper_gap() {
    let binding = physical_binding("Orders", false, 1);
    let bound = BoundBroker::new(receiver(binding.clone()), binding.clone());
    let submission = work(binding.clone(), binding.target().clone());
    let permit = submission.permit().clone();
    let operation = bound.receive_fenced_owned(submission);
    drop(operation);
    assert_eq!(permit.state(), ReceiveClaimState::Cancelled);
}

use domain::{
    DeleteEntityTarget, EntityBinding, EntityIncarnationKind, ReceiveMode, RuleFilter, RuleName,
};

use super::*;

fn binding<P: StoreProvider>(node: &Node<P>, target: Attachment) -> TestResult<EntityBinding> {
    Ok(node
        .broker
        .handle()
        .bind_blocking(namespace(), target)?
        .expect("live admission")
        .binding)
}

fn stale<T: std::fmt::Debug>(result: Result<T, SubmitError>) {
    assert!(
        matches!(
            result,
            Err(SubmitError::Propose(ProposeError::Broker(
                BrokerError::EntityBindingStale
            )))
        ),
        "{result:?}"
    );
}

fn admissions_capture_exact_scopes_without_stamping<P: StoreProvider>(provider: P) -> TestResult {
    let node = Node::new(provider)?;
    node.queue("Orders", QueueConfig::default())?;
    node.topic("Events", TopicConfig::default())?;
    node.subscription("Events", "Alpha", SubscriptionConfig::default())?;
    node.clock.forbidden.store(true, Ordering::SeqCst);
    let before = node.store.snapshot()?;
    let reads = node.clock.reads.load(Ordering::SeqCst);
    for (target, owner, kind) in [
        (primary("Orders"), "Orders", EntityIncarnationKind::Queue),
        (shadow("Orders"), "Orders", EntityIncarnationKind::Queue),
        (primary("Events"), "Events", EntityIncarnationKind::Topic),
        (
            subscription("Events", "Alpha", false),
            "Events/subscriptions/Alpha",
            EntityIncarnationKind::Subscription,
        ),
        (
            subscription("Events", "Alpha", true),
            "Events/subscriptions/Alpha",
            EntityIncarnationKind::Subscription,
        ),
    ] {
        let bound = binding(&node, target.clone())?;
        assert_eq!(bound.namespace(), &namespace());
        assert_eq!(bound.target(), &target.canonical_entity()?);
        assert_eq!(bound.owner().as_str(), owner);
        assert_eq!(bound.kind(), kind);
        assert_eq!(bound.generation(), 1);
    }
    assert_eq!(
        node.broker
            .handle()
            .bind_blocking(namespace(), primary("orders"))?,
        None
    );
    assert_eq!(
        node.broker
            .handle()
            .bind_blocking(namespace(), shadow("Events"))?,
        None,
        "a healthy topic has no parent dead-letter endpoint"
    );
    assert_eq!(node.store.snapshot()?, before);
    assert_eq!(node.clock.reads.load(Ordering::SeqCst), reads);
    Ok(())
}

fn stale_commands_and_rule_reads_refuse_before_the_host_clock<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    node.queue("Orders", QueueConfig::default())?;
    node.topic("Events", TopicConfig::default())?;
    node.subscription("Events", "Alpha", SubscriptionConfig::default())?;
    let queue = binding(&node, primary("Orders"))?;
    let dlq = binding(&node, shadow("Orders"))?;
    let member = binding(&node, subscription("Events", "Alpha", false))?;
    node.submit(
        "Orders",
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Queue,
        },
    )?;
    node.queue("Orders", QueueConfig::default())?;
    node.submit(
        "Events",
        CommandKind::DeleteEntity {
            target: DeleteEntityTarget::Subscription {
                name: SubscriptionName::new("Alpha")?,
            },
        },
    )?;
    node.subscription("Events", "Alpha", SubscriptionConfig::default())?;
    let current = binding(&node, primary("Orders"))?;
    assert_eq!(current.generation(), queue.generation() + 1);
    let current_member = binding(&node, subscription("Events", "Alpha", false))?;
    assert_eq!(current_member.generation(), member.generation() + 1);
    node.clock.inner.set(0);
    node.clock.forbidden.store(true, Ordering::SeqCst);
    let before = node.store.snapshot()?;
    let clock_reads = node.clock.reads.load(Ordering::SeqCst);
    for old in [queue, dlq] {
        stale(node.broker.handle().submit_fenced_blocking(
            old.clone(),
            old.target().clone(),
            CommandKind::Receive {
                mode: ReceiveMode::ReceiveAndDelete,
                lock_duration_millis: None,
                session: None,
            },
        ));
    }
    stale(node.broker.handle().submit_fenced_blocking(
        member.clone(),
        EntityPath::new("Events")?,
        CommandKind::CreateRule {
            subscription: SubscriptionName::new("Alpha")?,
            name: RuleName::new("replacement-leak")?,
            filter: RuleFilter::True,
        },
    ));
    stale(node.broker.handle().rules_fenced_blocking(
        member,
        EntityPath::new("Events")?,
        SubscriptionName::new("Alpha")?,
    ));
    let current_rules = node.broker.handle().rules_fenced_blocking(
        current_member,
        EntityPath::new("Events")?,
        SubscriptionName::new("Alpha")?,
    )?;
    assert_eq!(current_rules.len(), 1);
    assert_eq!(current_rules[0].name.as_str(), "$Default");
    assert_eq!(node.store.snapshot()?, before);
    assert_eq!(node.clock.reads.load(Ordering::SeqCst), clock_reads);
    Ok(())
}

fn current_identity_still_requires_a_healthy_clock_and_exact_target<P: StoreProvider>(
    provider: P,
) -> TestResult {
    let node = Node::new(provider)?;
    node.queue("Orders", QueueConfig::default())?;
    let current = binding(&node, primary("Orders"))?;
    let dlq = binding(&node, shadow("Orders"))?;
    let before = node.store.snapshot()?;
    node.clock.forbidden.store(true, Ordering::SeqCst);
    let invalid = node.broker.handle().submit_fenced_blocking(
        dlq,
        current.target().clone(),
        CommandKind::Receive {
            mode: ReceiveMode::ReceiveAndDelete,
            lock_duration_millis: None,
            session: None,
        },
    );
    assert!(matches!(
        invalid,
        Err(SubmitError::Propose(ProposeError::Broker(
            BrokerError::InvalidEntityBinding
        )))
    ));
    assert_eq!(node.store.snapshot()?, before);
    node.clock.forbidden.store(false, Ordering::SeqCst);
    node.clock.inner.set(0);
    let failed = node.broker.handle().submit_fenced_blocking(
        current.clone(),
        current.target().clone(),
        CommandKind::Send {
            message_id: "healthy-identity".into(),
            body: Vec::new(),
            time_to_live_millis: None,
            session_id: None,
        },
    );
    assert!(matches!(
        failed,
        Err(SubmitError::Propose(ProposeError::ClockWentBackward { .. }))
    ));
    assert_eq!(node.store.snapshot()?, before);
    node.clock.inner.set(2_000);
    node.broker.handle().submit_fenced_blocking(
        current.clone(),
        current.target().clone(),
        CommandKind::Send {
            message_id: "healthy-identity".into(),
            body: Vec::new(),
            time_to_live_millis: None,
            session_id: None,
        },
    )?;
    assert_eq!(binding(&node, primary("Orders"))?, current);
    Ok(())
}

macro_rules! for_each_backend {
    ($($case:ident),+ $(,)?) => {
        mod memory {
            $(#[test] fn $case() -> super::TestResult {
                super::$case(testkit::MemoryProvider::new())
            })+
        }
        mod durable {
            $(#[test] fn $case() -> super::TestResult {
                super::$case(testkit::DurableProvider::temporary()?)
            })+
        }
    };
}

for_each_backend! {
    admissions_capture_exact_scopes_without_stamping,
    stale_commands_and_rule_reads_refuse_before_the_host_clock,
    current_identity_still_requires_a_healthy_clock_and_exact_target,
}

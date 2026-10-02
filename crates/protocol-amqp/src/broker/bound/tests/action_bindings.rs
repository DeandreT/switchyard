use super::*;

fn action_command() -> CommandKind {
    CommandKind::CreateRuleWithAction {
        subscription: SubscriptionName::new("Alpha").expect("subscription"),
        name: RuleName::new("action").expect("rule"),
        filter: RuleFilter::True,
        action: domain::SqlAction::new(" REMOVE [private-marker]; ").expect("action"),
    }
}

#[tokio::test]
async fn action_submission_and_enumeration_retain_the_exact_admitted_child_binding() {
    let identity = binding(1);
    let inner = FencedOnlyBroker::new(identity.clone());
    let bound = BoundBroker::new(inner.clone(), identity.clone());
    let topic = EntityPath::new("Orders").expect("topic");
    let subscription = SubscriptionName::new("Alpha").expect("subscription");
    assert_eq!(
        bound
            .submit(namespace(), topic.clone(), action_command())
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
            Call::Submit(identity.clone(), topic.clone(), Box::new(action_command())),
            Call::Rules(identity, topic, subscription)
        ]
    );
}

#[tokio::test]
async fn foreign_namespace_and_nested_binding_refuse_action_submission_before_owner_work() {
    let identity = binding(1);
    let inner = FencedOnlyBroker::new(identity.clone());
    let bound = BoundBroker::new(inner.clone(), identity);
    let topic = EntityPath::new("Orders").expect("topic");
    assert_eq!(
        bound
            .submit(
                NamespaceName::new("other").expect("foreign namespace"),
                topic.clone(),
                action_command()
            )
            .await,
        Err(BrokerRejection::Refused(BrokerError::InvalidEntityBinding))
    );
    assert_eq!(
        bound
            .submit_fenced(binding(2), topic, action_command())
            .await,
        Err(BrokerRejection::Refused(BrokerError::InvalidEntityBinding))
    );
    assert!(inner.calls.lock().expect("calls").is_empty());
}

#[tokio::test]
async fn stale_action_wrapper_never_refreshes_itself_to_the_replacement() {
    let old = binding(1);
    let inner = FencedOnlyBroker::new(binding(2));
    let bound = BoundBroker::new(inner.clone(), old.clone());
    let topic = EntityPath::new("Orders").expect("topic");
    let subscription = SubscriptionName::new("Alpha").expect("subscription");
    assert_eq!(
        bound
            .submit(namespace(), topic.clone(), action_command())
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
            Call::Submit(old.clone(), topic.clone(), Box::new(action_command())),
            Call::Rules(old, topic, subscription)
        ]
    );
}

use domain::{
    EntityPath, NamespaceName, QueueCounters, RuleFilter, StateMachine, SubscriptionName,
    TopicConfig, codec, keys,
};
use storage::StateStore;

use super::*;

pub(super) fn check<S: StateStore>(store: &S, namespace: &NamespaceName) -> TestResult {
    let topic = EntityPath::new(TOPIC)?;
    let queue = EntityPath::new(QUEUE)?;
    let machine = StateMachine::new(store.clone());
    assert_eq!(
        machine.topic_config(namespace, &topic)?,
        Some(TopicConfig::default())
    );
    assert_eq!(
        machine.queue_config(namespace, &queue)?,
        Some(fixture::queue_config())
    );
    assert_next_sequence(store, namespace, &topic, 4)?;
    assert_next_sequence(store, namespace, &queue, 7)?;
    let subscriptions = machine.subscriptions(namespace, &topic)?;
    assert_eq!(subscriptions.len(), 2);
    for name in ["Alpha", "beta"] {
        let name = SubscriptionName::new(name)?;
        assert_eq!(
            machine.subscription_config(namespace, &topic, &name)?,
            Some(fixture::subscription_config())
        );
        let rules = machine.rules(namespace, &topic, &name)?;
        assert_eq!(rules.len(), 1, "unexpected retained receiving action rule");
        assert_eq!(rules[0].name.as_str(), "$Default");
        assert_eq!(rules[0].filter, RuleFilter::True);
        assert!(rules[0].action.is_none());
        let child = topic.subscription(&name)?;
        assert_empty_runtime(store, namespace, &child)?;
        assert_empty_runtime(store, namespace, &child.dead_letter_queue()?)?;
    }
    assert_empty_runtime(store, namespace, &topic)?;
    assert_empty_runtime(store, namespace, &queue)?;
    assert_empty_runtime(store, namespace, &queue.dead_letter_queue()?)?;
    Ok(())
}

fn assert_next_sequence<S: StateStore>(
    store: &S,
    namespace: &NamespaceName,
    entity: &EntityPath,
    expected: u64,
) -> TestResult {
    let counters = store
        .get(&keys::queue_counters(namespace, entity))?
        .expect("SDK receiving workflow retained its canonical sequence counter");
    let counters: QueueCounters = codec::decode(&counters)?;
    assert_eq!(
        counters.next_sequence, expected,
        "unexpected canonical publication count in {entity}"
    );
    Ok(())
}

fn assert_empty_runtime<S: StateStore>(
    store: &S,
    namespace: &NamespaceName,
    entity: &EntityPath,
) -> TestResult {
    for prefix in [
        keys::message_prefix(namespace, entity),
        keys::ready_prefix(namespace, entity),
        keys::lock_prefix(namespace, entity),
        keys::expiry_prefix(namespace, entity),
        keys::scheduled_prefix(namespace, entity),
        keys::duplicate_history_prefix(namespace, entity),
        keys::duplicate_history_expiry_prefix(namespace, entity),
        keys::entity_session_prefix(namespace, entity),
        keys::entity_session_ready_prefix(namespace, entity),
        keys::session_lock_prefix(namespace, entity),
    ] {
        assert!(
            store.scan_prefix(&prefix, 1)?.is_empty(),
            "SDK receiving workflow left a runtime record in {entity}"
        );
    }
    Ok(())
}

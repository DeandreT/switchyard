use std::{
    future::pending,
    panic::{AssertUnwindSafe, catch_unwind},
};

use super::*;
use crate::management::{EntityPath, LockToken, SequenceNumber, test_binding};

const LINK: &str = "owned-association";

fn register(
    management: &Arc<ConnectionManagement>,
    link: &str,
    token: u64,
    entity: &str,
    sequence: u64,
) -> DeliveryRegistration {
    let entity = EntityPath::new(entity).expect("entity");
    management
        .register_delivery_owned(
            link,
            entity.clone(),
            SequenceNumber::new(sequence),
            LockToken::new(token),
            test_binding(&entity),
        )
        .expect("owned registration")
}

#[tokio::test]
async fn exact_owned_drop_removes_only_its_association() {
    let management = ConnectionManagement::new();
    let first = register(&management, LINK, 1, "orders", 7);
    let second = register(&management, LINK, 2, "orders", 8);
    let other_link = register(&management, "neighbor", 1, "orders", 7);
    assert!(management.delivery(LINK, LockToken::new(1)).await.is_some());
    drop(first);
    assert!(management.delivery(LINK, LockToken::new(1)).await.is_none());
    assert_eq!(
        management
            .delivery(LINK, LockToken::new(2))
            .await
            .expect("neighbor token")
            .sequence,
        SequenceNumber::new(8)
    );
    assert!(
        management
            .delivery("neighbor", LockToken::new(1))
            .await
            .is_some()
    );
    drop(second);
    drop(other_link);
    assert!(management.deliveries.is_empty());
}

#[tokio::test]
async fn identical_replacement_is_not_removed_by_the_old_owned_guard() {
    let management = ConnectionManagement::new();
    let old = register(&management, LINK, 1, "orders", 7);
    let replacement = register(&management, LINK, 1, "orders", 7);
    drop(old);
    assert!(management.delivery(LINK, LockToken::new(1)).await.is_some());
    drop(replacement);
    assert!(management.deliveries.is_empty());
}

#[tokio::test]
async fn replacement_binding_and_sequence_are_not_owned_by_old_cleanup() {
    let management = ConnectionManagement::new();
    let old = register(&management, LINK, 1, "orders", 7);
    let replacement = register(&management, LINK, 1, "topic/subscriptions/Alpha", 8);
    drop(old);
    let current = management
        .delivery(LINK, LockToken::new(1))
        .await
        .expect("replacement");
    assert_eq!(current.entity.as_str(), "topic/subscriptions/Alpha");
    assert_eq!(current.sequence, SequenceNumber::new(8));
    drop(replacement);
    assert!(management.deliveries.is_empty());
}

#[tokio::test]
async fn identical_legacy_replacement_survives_owned_drop() {
    let management = ConnectionManagement::new();
    let old = register(&management, LINK, 1, "orders", 7);
    let entity = EntityPath::new("orders").expect("entity");
    let binding = test_binding(&entity);
    management
        .register_delivery(
            LINK,
            entity,
            SequenceNumber::new(7),
            LockToken::new(1),
            binding.clone(),
        )
        .await;
    drop(old);
    assert!(management.delivery(LINK, LockToken::new(1)).await.is_some());
    management
        .unregister_delivery(LINK, LockToken::new(1), &binding)
        .await;
    assert!(management.deliveries.is_empty());
}

#[tokio::test]
async fn explicit_unregister_then_replacement_is_not_removed_by_old_guard() {
    let management = ConnectionManagement::new();
    let old = register(&management, LINK, 1, "orders", 7);
    let entity = EntityPath::new("orders").expect("entity");
    management
        .unregister_delivery(LINK, LockToken::new(1), &test_binding(&entity))
        .await;
    assert!(management.deliveries.is_empty());
    let replacement = register(&management, LINK, 1, "orders", 7);
    drop(old);
    assert!(management.delivery(LINK, LockToken::new(1)).await.is_some());
    drop(replacement);
    assert!(management.deliveries.is_empty());
}

#[tokio::test]
async fn legacy_bound_unregister_remains_fenced_with_owned_entries() {
    let management = ConnectionManagement::new();
    let registration = register(&management, LINK, 1, "topic/subscriptions/Alpha", 7);
    let other = EntityPath::new("topic/subscriptions/Beta").expect("sibling");
    management
        .unregister_delivery(LINK, LockToken::new(1), &test_binding(&other))
        .await;
    assert!(management.delivery(LINK, LockToken::new(1)).await.is_some());
    drop(registration);
    assert!(management.deliveries.is_empty());
}

#[tokio::test]
async fn an_unpolled_owned_job_cleans_its_published_registration() {
    let management = ConnectionManagement::new();
    let registration = register(&management, LINK, 1, "orders", 7);
    let job = async move {
        let _registration = registration;
        pending::<()>().await;
    };
    drop(job);
    assert!(management.deliveries.is_empty());
}

#[tokio::test]
async fn aborting_a_send_static_job_cleans_registration_without_an_async_finalizer() {
    let management = ConnectionManagement::new();
    let registration = register(&management, LINK, 1, "orders", 7);
    let (ready, started) = tokio::sync::oneshot::channel();
    let job = tokio::spawn(async move {
        let _registration = registration;
        ready.send(()).expect("job started");
        pending::<()>().await;
    });
    started.await.expect("job entered");
    assert!(management.delivery(LINK, LockToken::new(1)).await.is_some());
    job.abort();
    assert!(job.await.expect_err("aborted job").is_cancelled());
    assert!(management.deliveries.is_empty());
}

#[tokio::test]
async fn unwinding_outside_the_short_registry_lock_cleans_and_preserves_admission() {
    let management = ConnectionManagement::new();
    let registration = register(&management, LINK, 1, "orders", 7);
    assert!(
        catch_unwind(AssertUnwindSafe(move || {
            let _registration = registration;
            panic!("owned job unwound");
        }))
        .is_err()
    );
    assert!(management.deliveries.is_empty());
    assert!(!management.deliveries.entries.is_poisoned());
    let replacement = register(&management, LINK, 1, "orders", 7);
    drop(replacement);
    assert!(management.deliveries.is_empty());
}

#[tokio::test]
async fn poison_refuses_new_admission_and_lookup_but_exact_cleanup_still_refunds() {
    let management = ConnectionManagement::new();
    let first = register(&management, LINK, 1, "orders", 7);
    let second = register(&management, LINK, 2, "orders", 8);
    let poisoning = Arc::clone(&management);
    assert!(
        std::thread::spawn(move || {
            let _entries = poisoning.deliveries.entries.lock().expect("registry lock");
            panic!("registry poisoned");
        })
        .join()
        .is_err()
    );
    let entity = EntityPath::new("orders").expect("entity");
    assert!(matches!(
        management.register_delivery_owned(
            LINK,
            entity.clone(),
            SequenceNumber::new(9),
            LockToken::new(2),
            test_binding(&entity),
        ),
        Err(DeliveryRegistrationError)
    ));
    management
        .register_delivery(
            LINK,
            entity.clone(),
            SequenceNumber::new(9),
            LockToken::new(3),
            test_binding(&entity),
        )
        .await;
    assert!(management.delivery(LINK, LockToken::new(1)).await.is_none());
    drop(first);
    {
        let entries = management
            .deliveries
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        assert_eq!(entries.len(), 1);
        assert!(entries.contains_key(&DeliveryKey {
            link_name: LINK.into(),
            lock_token: LockToken::new(2)
        }));
    }
    drop(second);
    assert!(management.deliveries.is_empty());
}

#[test]
fn registration_diagnostics_do_not_reveal_associated_metadata() {
    let management = ConnectionManagement::new();
    let registration = register(&management, "private-link", 4, "private-queue", 7);
    assert_eq!(format!("{registration:?}"), "DeliveryRegistration { .. }");
    assert_eq!(
        DeliveryRegistrationError.to_string(),
        "the delivery association is unavailable"
    );
    drop(registration);
    assert!(management.deliveries.is_empty());
}

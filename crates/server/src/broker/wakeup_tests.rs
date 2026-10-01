use super::*;
use std::{
    future::{Future, poll_fn},
    pin::Pin,
    task::Poll,
    time::Duration,
};

const DEADLINE: Duration = Duration::from_secs(1);

fn target() -> (NamespaceName, EntityPath) {
    (
        NamespaceName::new("tenant").expect("namespace"),
        EntityPath::new("orders").expect("entity"),
    )
}

async fn ready(wait: impl Future<Output = ()>) {
    tokio::time::timeout(DEADLINE, wait)
        .await
        .expect("registered wait must observe the broadcast");
}

async fn pending<F: Future<Output = ()>>(wait: &mut Pin<Box<F>>) {
    poll_fn(|context| {
        assert!(
            wait.as_mut().poll(context).is_pending(),
            "later wait must not consume an older broadcast"
        );
        Poll::Ready(())
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn one_broadcast_wakes_every_synchronously_registered_unpolled_wait() {
    let watchers = Arc::new(Watchers::default());
    let (namespace, entity) = target();
    let waiting: Vec<_> = (0..32)
        .map(|_| watchers.watch(&namespace, &entity).wait())
        .collect();
    assert_eq!(watchers.entry_count(), 1);
    assert_eq!(watchers.waiter_count(&namespace, &entity), 32);
    watchers.notify(&namespace, &entity);
    for wait in waiting {
        ready(wait).await;
    }
    assert_eq!(watchers.waiter_count(&namespace, &entity), 0);
    assert_eq!(watchers.entry_count(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn one_broadcast_wakes_both_polled_and_unpolled_registrations() {
    let watchers = Arc::new(Watchers::default());
    let (namespace, entity) = target();
    let mut first = Box::pin(watchers.watch(&namespace, &entity).wait());
    let mut second = Box::pin(watchers.watch(&namespace, &entity).wait());
    let third = watchers.watch(&namespace, &entity).wait();
    pending(&mut first).await;
    pending(&mut second).await;
    assert_eq!(watchers.waiter_count(&namespace, &entity), 3);
    watchers.notify(&namespace, &entity);
    ready(third).await;
    ready(second).await;
    ready(first).await;
    assert_eq!(watchers.entry_count(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn factory_futures_capture_a_broadcast_before_their_first_poll() {
    let watchers = Arc::new(Watchers::default());
    let (namespace, entity) = target();
    let (requests, _receiver) = super::request_queue::bounded(1);
    let handle = BrokerHandle {
        requests,
        watchers: watchers.clone(),
    };
    let first = protocol_amqp::Broker::deliverable(&handle, &namespace, &entity);
    let second = protocol_amqp::Broker::deliverable(&handle, &namespace, &entity);
    assert_eq!(watchers.waiter_count(&namespace, &entity), 2);
    watchers.notify(&namespace, &entity);
    ready(first).await;
    ready(second).await;
    assert_eq!(watchers.entry_count(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn a_later_registration_cannot_consume_the_previous_broadcast() {
    let watchers = Arc::new(Watchers::default());
    let (namespace, entity) = target();
    let first = watchers.watch(&namespace, &entity).wait();
    watchers.notify(&namespace, &entity);
    let mut later = Box::pin(watchers.watch(&namespace, &entity).wait());
    assert_eq!(watchers.waiter_count(&namespace, &entity), 2);
    pending(&mut later).await;
    ready(first).await;
    assert_eq!(watchers.waiter_count(&namespace, &entity), 1);
    pending(&mut later).await;
    watchers.notify(&namespace, &entity);
    ready(later).await;
    assert_eq!(watchers.entry_count(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn repeated_broadcasts_coalesce_without_carrying_a_permit_into_the_next_wait() {
    let watchers = Arc::new(Watchers::default());
    let (namespace, entity) = target();
    let first = watchers.watch(&namespace, &entity).wait();
    for _ in 0..3 {
        watchers.notify(&namespace, &entity);
    }
    let mut second = Box::pin(watchers.watch(&namespace, &entity).wait());
    pending(&mut second).await;
    ready(first).await;
    pending(&mut second).await;
    for _ in 0..3 {
        watchers.notify(&namespace, &entity);
    }
    ready(second).await;
    assert_eq!(watchers.entry_count(), 0);
    let mut third = Box::pin(watchers.watch(&namespace, &entity).wait());
    pending(&mut third).await;
    watchers.notify(&namespace, &entity);
    ready(third).await;
    assert_eq!(watchers.entry_count(), 0);
}

#[tokio::test(flavor = "current_thread")]
async fn cancellation_after_a_broadcast_does_not_steal_surviving_wakes() {
    let watchers = Arc::new(Watchers::default());
    let (namespace, entity) = target();
    for poll_canceled in [false, true] {
        let mut canceled = Box::pin(watchers.watch(&namespace, &entity).wait());
        let first = watchers.watch(&namespace, &entity).wait();
        let second = watchers.watch(&namespace, &entity).wait();
        if poll_canceled {
            pending(&mut canceled).await;
        }
        assert_eq!(watchers.waiter_count(&namespace, &entity), 3);
        watchers.notify(&namespace, &entity);
        drop(canceled);
        assert_eq!(watchers.waiter_count(&namespace, &entity), 2);
        ready(first).await;
        ready(second).await;
        assert_eq!(watchers.entry_count(), 0);
    }
}

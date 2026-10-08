//! Exact ordinary-registration custody during cancellable residual cleanup.

use storage::StateStore;
use tokio::time::timeout;

use super::{test_support::*, unregister_deliveries};
use crate::management::ConnectionManagement;

#[tokio::test(flavor = "current_thread")]
async fn cancelled_residual_cleanup_retains_originals_and_preserves_newer_registration() {
    for durable in [false, true] {
        for preserve_newer in [false, true] {
            let mut actor = Actor::new(durable, false);
            actor.send("residual-registration", None);
            let delivery = actor.receive();
            let token = delivery.lock.unwrap().token;
            let before = actor.store().snapshot().unwrap();
            let management = ConnectionManagement::new();
            let original = management
                .register_delivery(LINK, actor.entity.clone(), delivery.sequence, token)
                .await;
            let payload = management.delivery(LINK, token).await.unwrap();
            let replacement = management
                .register_delivery(LINK, actor.entity.clone(), delivery.sequence, token)
                .await;
            assert_ne!(original, replacement);
            assert_eq!(
                management.delivery(LINK, token).await,
                Some(payload.clone())
            );
            let mut retained = vec![original.clone(), replacement.clone()];
            let newer = if preserve_newer {
                let registration = management
                    .register_delivery(LINK, actor.entity.clone(), delivery.sequence, token)
                    .await;
                assert_ne!(registration, original);
                assert_ne!(registration, replacement);
                assert_eq!(
                    management.delivery(LINK, token).await,
                    Some(payload.clone())
                );
                Some(registration)
            } else {
                None
            };

            let held = timeout(WAIT, management.delivery_write_lock())
                .await
                .unwrap();
            for _ in 0..2 {
                let mut borrowed = Box::pin(unregister_deliveries(&management, &mut retained));
                pending_once(borrowed.as_mut()).await;
                drop(borrowed);
                assert_eq!(retained, vec![original.clone(), replacement.clone()]);
            }
            drop(held);
            timeout(WAIT, unregister_deliveries(&management, &mut retained))
                .await
                .unwrap();
            assert!(retained.is_empty());
            timeout(WAIT, unregister_deliveries(&management, &mut retained))
                .await
                .unwrap();
            assert!(retained.is_empty());

            if let Some(newer) = newer {
                assert_eq!(
                    management.delivery(LINK, token).await,
                    Some(payload.clone())
                );
                management.unregister_delivery(&replacement).await;
                management.unregister_delivery(&original).await;
                assert_eq!(management.delivery(LINK, token).await, Some(payload));
                let mut current = vec![newer];
                timeout(WAIT, unregister_deliveries(&management, &mut current))
                    .await
                    .unwrap();
                assert!(current.is_empty());
            }
            assert!(management.delivery(LINK, token).await.is_none());
            assert!(actor.log.lock().unwrap().is_empty());
            assert_eq!(actor.store().snapshot().unwrap(), before);
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), before);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_partial_residual_cleanup_retains_only_the_unfinished_original() {
    for durable in [false, true] {
        for preserve_newer in [false, true] {
            let mut actor = Actor::new(durable, false);
            actor.send("residual-first", None);
            actor.send("residual-remaining", None);
            let first_delivery = actor.receive();
            let remaining_delivery = actor.receive();
            let first_token = first_delivery.lock.unwrap().token;
            let remaining_token = remaining_delivery.lock.unwrap().token;
            assert_ne!(first_token, remaining_token);
            let before = actor.store().snapshot().unwrap();
            let management = ConnectionManagement::new();
            let first = management
                .register_delivery(
                    LINK,
                    actor.entity.clone(),
                    first_delivery.sequence,
                    first_token,
                )
                .await;
            let remaining = management
                .register_delivery(
                    LINK,
                    actor.entity.clone(),
                    remaining_delivery.sequence,
                    remaining_token,
                )
                .await;
            assert!(management.delivery(LINK, first_token).await.is_some());
            let remaining_payload = management.delivery(LINK, remaining_token).await.unwrap();
            let newer = if preserve_newer {
                let registration = management
                    .register_delivery(
                        LINK,
                        actor.entity.clone(),
                        remaining_delivery.sequence,
                        remaining_token,
                    )
                    .await;
                assert_ne!(registration, remaining);
                assert_eq!(
                    management.delivery(LINK, remaining_token).await,
                    Some(remaining_payload.clone())
                );
                Some(registration)
            } else {
                None
            };
            let mut retained = vec![remaining.clone(), first];

            // FIFO writers interpose a real map guard after the first cleanup
            // write, making the second write a deterministic Pending frontier.
            let initial = timeout(WAIT, management.delivery_write_lock())
                .await
                .unwrap();
            let mut borrowed = Box::pin(unregister_deliveries(&management, &mut retained));
            pending_once(borrowed.as_mut()).await;
            let mut intervening = Box::pin(management.delivery_write_lock());
            pending_once(intervening.as_mut()).await;
            drop(initial);
            pending_once(borrowed.as_mut()).await;
            let held = timeout(WAIT, intervening.as_mut()).await.unwrap();
            drop(borrowed);
            assert_eq!(retained, vec![remaining.clone()]);

            let mut retry = Box::pin(unregister_deliveries(&management, &mut retained));
            pending_once(retry.as_mut()).await;
            drop(retry);
            assert_eq!(retained, vec![remaining.clone()]);
            drop(held);
            drop(intervening);
            assert!(management.delivery(LINK, first_token).await.is_none());
            assert_eq!(
                management.delivery(LINK, remaining_token).await,
                Some(remaining_payload.clone())
            );

            timeout(WAIT, unregister_deliveries(&management, &mut retained))
                .await
                .unwrap();
            assert!(retained.is_empty());
            timeout(WAIT, unregister_deliveries(&management, &mut retained))
                .await
                .unwrap();
            assert!(retained.is_empty());
            assert!(management.delivery(LINK, first_token).await.is_none());
            if let Some(newer) = newer {
                assert_eq!(
                    management.delivery(LINK, remaining_token).await,
                    Some(remaining_payload)
                );
                let mut current = vec![newer];
                timeout(WAIT, unregister_deliveries(&management, &mut current))
                    .await
                    .unwrap();
                assert!(current.is_empty());
            }
            assert!(management.delivery(LINK, remaining_token).await.is_none());
            assert!(actor.log.lock().unwrap().is_empty());
            assert_eq!(actor.store().snapshot().unwrap(), before);
            actor.reopen();
            assert_eq!(actor.store().snapshot().unwrap(), before);
        }
    }
}

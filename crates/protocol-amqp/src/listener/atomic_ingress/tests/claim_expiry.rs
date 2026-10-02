use std::sync::{Arc, atomic::Ordering};

use amqp::{DeliveryState, NativeTransactionState};
use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use tokio::time::timeout;

use super::super::operations::Operation;
use super::{
    drive_provisional,
    fixture::{CONTROL, DEADLINE, Fixture, POST, TestResult},
};
use crate::{AtomicCommitClaimError, AtomicCommitState, NativeAtomicOwnerError};
use crate::{SharedAccessAuthentication, authorization::ConnectionAuthorization};

const HOST: &str = "tenant.servicebus.windows.net";
const SEND_EXPIRY: u64 = 2_000_000_000;
const AUDIENCE: &str = "amqps://tenant.servicebus.windows.net/orders";
const TOKEN: &str = "SharedAccessSignature sr=amqps%3A%2F%2Ftenant.servicebus.windows.net%2Forders&sig=R8KtgcCb7NeOCrECrMXtQ13KLGC8CiJYw0fUnUQCznw%3D&se=2000000000&skn=send";

async fn authorization() -> TestResult<Arc<ConnectionAuthorization>> {
    let policy = SharedAccessPolicy::new([
        SharedAccessRule::new(
            "unrelated-listen",
            ResourceScope::entity(HOST, "other")?,
            SharedAccessKey::new("listen-secret")?,
            None,
            PermissionSet::LISTEN,
        )?,
        SharedAccessRule::new(
            "send",
            ResourceScope::namespace(HOST)?,
            SharedAccessKey::new("secret")?,
            None,
            PermissionSet::SEND,
        )?,
    ])?;
    let grant = policy.authenticate_plain("unrelated-listen", "listen-secret")?;
    let connection =
        ConnectionAuthorization::new(SharedAccessAuthentication::new(policy, HOST)?, Some(grant));
    connection.validate_and_add(TOKEN, AUDIENCE).await?;
    Ok(connection)
}

async fn captured_horizon_is_forwarded(empty: bool, expiry: u64) -> TestResult {
    // Native authority is real; authorization is an explicit trusted private context.
    let mut fixture = Fixture::new_with_authorization(Some(authorization().await?)).await?;
    let transaction = fixture.declare().await?;
    if !empty {
        let posting = fixture
            .posting(&transaction, b"captured authorization horizon")
            .await?;
        fixture.stage(posting);
        drive_provisional(&mut fixture, &transaction).await?;
    }
    let (observed, release) = fixture.recorder.pause_handoff();
    let sealed = fixture.sealed(&transaction).await?;
    fixture.discharge(sealed);
    let key = timeout(DEADLINE, async {
        loop {
            let operation = fixture
                .owner
                .next_operation()
                .await
                .ok_or("authorization operation missing")?;
            if let Operation::Authorized { key, result } = operation {
                assert_eq!(
                    result.expect("validated captured authorization"),
                    Some(if empty { u64::MAX } else { SEND_EXPIRY }),
                    "unrelated unlimited Listen cannot extend this queue's Send horizon",
                );
                return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(key);
            }
            fixture.owner.accept_completion(operation);
        }
    })
    .await??;
    // Replace only the completed internal numeric sample, not its native authority.
    fixture.owner.accept_completion(Operation::Authorized {
        key,
        result: Ok(Some(expiry)),
    });
    let permit = timeout(DEADLINE, observed).await??;
    assert_eq!(permit.state(), AtomicCommitState::Pending);
    assert_eq!(fixture.recorder.handoffs.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.recorder.claimed.load(Ordering::Relaxed), 0);
    release
        .send(())
        .expect("actual paired owner future remains pending");
    let expected = if expiry == 0 {
        NativeTransactionState::Aborted
    } else {
        NativeTransactionState::Committed
    };
    let replies = async {
        if !empty {
            let post = fixture.peer.disposition(POST, 0).await?;
            assert!(post.settled);
            if expected == NativeTransactionState::Committed {
                assert!(matches!(post.state, Some(DeliveryState::Accepted(_))));
            } else {
                assert!(post.state.is_none());
            }
        }
        let control = fixture.peer.disposition(CONTROL, 1).await?;
        assert!(control.settled);
        if expected == NativeTransactionState::Committed {
            assert!(matches!(control.state, Some(DeliveryState::Accepted(_))));
        } else {
            assert!(matches!(control.state,
                Some(DeliveryState::Rejected(amqp::Rejected { error: Some(error) }))
                    if error.condition.as_symbol().as_str() == "amqp:transaction:rollback"
            ));
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    let mut replies = Box::pin(replies);
    let mut application_seen = false;
    timeout(DEADLINE, async {
        loop {
            tokio::select! {
                result = &mut replies => return result,
                operation = fixture.owner.next_operation() => {
                    let operation = operation.ok_or("paired owner operation missing")?;
                    if let Operation::Applied { result, .. } = &operation {
                        let completion = result.as_ref().expect("actual owner completion");
                        if expected == NativeTransactionState::Aborted {
                            assert!(matches!(completion.application(), Err(
                                NativeAtomicOwnerError::LogicalClaim(AtomicCommitClaimError::Aborted)
                            )));
                        } else {
                            assert!(completion.application().is_ok());
                        }
                        application_seen = true;
                    }
                    fixture.owner.accept_completion(operation);
                }
            }
        }
    }).await??;
    drop(replies);
    assert!(application_seen);
    assert_eq!(
        fixture
            .observer
            .as_ref()
            .expect("actual declared identity")
            .state(),
        expected
    );
    {
        let bodies = fixture
            .recorder
            .bodies
            .lock()
            .expect("claimed payload observer");
        if expected == NativeTransactionState::Aborted {
            assert_eq!(permit.state(), AtomicCommitState::Aborted);
            assert_eq!(fixture.recorder.claimed.load(Ordering::Relaxed), 0);
            assert!(bodies.is_empty());
        } else {
            assert_eq!(permit.state(), AtomicCommitState::Committed);
            assert_eq!(fixture.recorder.claimed.load(Ordering::Relaxed), 1);
            assert_eq!(bodies.len(), 1);
            let expected_bodies = if empty {
                Vec::new()
            } else {
                vec![b"captured authorization horizon".to_vec()]
            };
            assert_eq!(bodies[0], expected_bodies);
        }
    }
    fixture.shutdown().await
}

#[tokio::test]
async fn completed_authorization_horizon_reaches_the_unique_bound_owner_ticket() -> TestResult {
    for expiry in [0, u64::MAX] {
        captured_horizon_is_forwarded(false, expiry).await?;
    }
    Ok(())
}

#[tokio::test]
async fn coordinator_only_horizon_reaches_the_unique_empty_owner_ticket() -> TestResult {
    for expiry in [0, u64::MAX] {
        captured_horizon_is_forwarded(true, expiry).await?;
    }
    Ok(())
}

use crate::{RetainedDelivery, ServerConnectionOwner};

use super::*;

#[path = "correlation/fixture.rs"]
mod fixture;
use fixture::{CorrelationFixture, Route, TestResult, caught, finish};

async fn ordinary_receipt(receiver: &mut TransactionalReceiver) -> TestResult<RetainedDelivery> {
    let TransactionalIngress::Ordinary(receipt) =
        tokio::time::timeout(DEADLINE, receiver.recv()).await??
    else {
        panic!("expected the original ordinary receipt")
    };
    assert!(receipt.message() == &Message::data(vec![0x31]));
    Ok(receipt)
}

async fn declare_receipt(
    coordinator: &mut CoordinatorEndpoint,
) -> TestResult<PendingDeclareReceipt> {
    let CoordinatorRequest::Declare(receipt) =
        tokio::time::timeout(DEADLINE, coordinator.recv()).await??
    else {
        panic!("expected the original Declare receipt")
    };
    Ok(receipt)
}

async fn accepted(
    fixture: &mut CorrelationFixture,
    receiver: &TransactionalReceiver,
    receipt: &RetainedDelivery,
    route: Route,
    id: u32,
    mode: ReceiverSettleMode,
) -> TestResult {
    let (result, outcome) = tokio::join!(
        tokio::time::timeout(DEADLINE, receiver.accept_retained(receipt)),
        fixture.outcome(route, id, mode.clone(), None),
    );
    result??;
    outcome?;
    if mode == ReceiverSettleMode::Second {
        fixture.acknowledge(route, id).await?;
    }
    Ok(())
}

async fn declared(
    fixture: &mut CorrelationFixture,
    receipt: PendingDeclareReceipt,
    route: Route,
    id: u32,
    mode: ReceiverSettleMode,
) -> TestResult<NativeTransactionIdentity> {
    let transaction = TransactionId::new([0x43]).expect("bounded synthetic transaction ID");
    let (result, outcome) = tokio::join!(
        tokio::time::timeout(DEADLINE, receipt.declared(transaction.clone())),
        fixture.outcome(route, id, mode.clone(), Some(&transaction)),
    );
    let original = result??;
    outcome?;
    if mode == ReceiverSettleMode::Second {
        fixture.acknowledge(route, id).await?;
    }
    Ok(original)
}

async fn interleaved(
    mode: ReceiverSettleMode,
    ordinary_first: bool,
    accepted_first: bool,
    same_session: bool,
) -> TestResult {
    let (mut owner, acceptor) = ServerConnectionOwner::new(tokio::runtime::Handle::current(), ());
    let observed = caught(async {
        let mut fixture = CorrelationFixture::open(acceptor).await?;
        let (mut control_session, control) = fixture.session(CONTROL_CHANNEL).await?;
        let mut coordinator = fixture
            .coordinator(&mut control_session, control, mode.clone())
            .await?;
        let mut other_session = None;
        let (data, mut receiver) = if same_session {
            let receiver = fixture
                .receiver(&mut control_session, control, mode.clone())
                .await?;
            (control, receiver)
        } else {
            let (data_session, route) = fixture.session(POST_CHANNEL).await?;
            other_session = Some(data_session);
            let receiver = fixture
                .receiver(
                    other_session.as_mut().expect("original data session"),
                    route,
                    mode.clone(),
                )
                .await?;
            (route, receiver)
        };
        let ordinary_id = u32::from(same_session && !ordinary_first);
        let declare_id = u32::from(same_session && ordinary_first);
        if ordinary_first {
            fixture.ordinary(data, ordinary_id).await?;
            fixture.declare(control, declare_id).await?;
        } else {
            fixture.declare(control, declare_id).await?;
            fixture.ordinary(data, ordinary_id).await?;
        }
        // Both original deliveries are retained before either application outcome.
        let ordinary = ordinary_receipt(&mut receiver).await?;
        let pending = declare_receipt(&mut coordinator).await?;
        fixture.barrier(control).await?;
        if !same_session {
            fixture.barrier(data).await?;
        }
        let _original_transaction = if accepted_first {
            accepted(
                &mut fixture,
                &receiver,
                &ordinary,
                data,
                ordinary_id,
                mode.clone(),
            )
            .await?;
            declared(&mut fixture, pending, control, declare_id, mode).await?
        } else {
            let original =
                declared(&mut fixture, pending, control, declare_id, mode.clone()).await?;
            accepted(&mut fixture, &receiver, &ordinary, data, ordinary_id, mode).await?;
            original
        };
        fixture.barrier(control).await?;
        if !same_session {
            fixture.barrier(data).await?;
        }
        fixture.close().await?;
        let _original_data_session = other_session;
        Ok(())
    })
    .await;
    finish(&mut owner, observed).await
}

#[tokio::test]
async fn ordinary_and_declare_outcomes_keep_same_session_correlation() -> TestResult {
    for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
        for ordinary_first in [false, true] {
            for accepted_first in [false, true] {
                interleaved(mode.clone(), ordinary_first, accepted_first, true).await?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn matching_delivery_ids_on_other_sessions_keep_outcome_scope() -> TestResult {
    for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
        for ordinary_first in [false, true] {
            for accepted_first in [false, true] {
                interleaved(mode.clone(), ordinary_first, accepted_first, false).await?;
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn settled_ordinary_receipt_cannot_accept_reused_declare_delivery() -> TestResult {
    for mode in [ReceiverSettleMode::First, ReceiverSettleMode::Second] {
        let (mut owner, acceptor) =
            ServerConnectionOwner::new(tokio::runtime::Handle::current(), ());
        let observed = caught(async {
            let mut fixture = CorrelationFixture::open(acceptor).await?;
            let (mut session, route) = fixture.session(CONTROL_CHANNEL).await?;
            let mut coordinator = fixture
                .coordinator(&mut session, route, mode.clone())
                .await?;
            let mut receiver = fixture.receiver(&mut session, route, mode.clone()).await?;
            fixture.ordinary(route, 0).await?;
            let original = ordinary_receipt(&mut receiver).await?;
            accepted(&mut fixture, &receiver, &original, route, 0, mode.clone()).await?;
            fixture.barrier(route).await?;

            fixture.declare(route, 0).await?;
            let pending = declare_receipt(&mut coordinator).await?;
            tokio::time::timeout(DEADLINE, receiver.accept_retained(&original)).await??;
            fixture.barrier(route).await?;
            let _original_transaction =
                declared(&mut fixture, pending, route, 0, mode.clone()).await?;
            tokio::time::timeout(DEADLINE, receiver.accept_retained(&original)).await??;
            fixture.barrier(route).await?;

            fixture.ordinary(route, 1).await?;
            let replacement = ordinary_receipt(&mut receiver).await?;
            assert!(original.message() == replacement.message());
            accepted(&mut fixture, &receiver, &replacement, route, 1, mode).await?;
            fixture.barrier(route).await?;
            fixture.close().await
        })
        .await;
        finish(&mut owner, observed).await?;
    }
    Ok(())
}

use std::sync::{Arc, atomic::Ordering};

use amqp::{Begin, DeliveryState, Frame, NativeTransactionState, Performative};
use domain::NamespaceName;
use futures_util::StreamExt;
use tokio::{
    sync::{Semaphore, oneshot},
    time::timeout,
};

use super::super::{
    Driver, Event, IngressMode, MAX_LINKS, MAX_SESSIONS, WorkerIdentity, owner::Owner, routing,
    run_driver,
};
use super::{
    drive_provisional,
    fixture::{CONTROL, DEADLINE, Fixture, POST, TestResult},
};

const ADMISSION: u16 = 31;

async fn blocked_begin_does_not_block_pending_abort(controller: bool) -> TestResult {
    let mut fixture = Fixture::new().await?;
    let transaction = fixture.declare().await?;
    let posting = fixture
        .posting(&transaction, b"cancel before blocked Begin flush")
        .await?;
    fixture.stage(posting);
    drive_provisional(&mut fixture, &transaction).await?;
    let (observed, release_owner) = fixture.recorder.pause_handoff();
    let sealed = fixture.sealed(&transaction).await?;
    fixture.discharge(sealed);
    timeout(DEADLINE, async {
        while fixture.recorder.handoffs.load(Ordering::Relaxed) == 0 {
            let operation = fixture
                .owner
                .next_operation()
                .await
                .expect("pending native operation");
            fixture.owner.accept_completion(operation);
        }
    })
    .await?;
    let permit = timeout(DEADLINE, observed).await??;
    assert_eq!(permit.state(), crate::AtomicCommitState::Pending);
    fixture.peer.barrier().await?;
    let native = fixture
        .observer
        .as_ref()
        .expect("registered native transaction")
        .clone();
    assert_eq!(native.state(), NativeTransactionState::Ready);
    let source = if controller {
        WorkerIdentity::Controller(fixture.coordinator.controller_identity().clone())
    } else {
        WorkerIdentity::Producer(fixture.receiver.receiver_identity())
    };
    let gate = Arc::clone(&fixture.begin_gate);
    let mut driver = Driver::new(
        fixture.connection.connection_identity().clone(),
        fixture.recorder.clone(),
    );
    driver.owner = std::mem::replace(
        &mut fixture.owner,
        Owner::new(
            fixture.connection.connection_identity().clone(),
            fixture.recorder.clone(),
        ),
    );
    let events = driver.events.clone();
    gate.block(ADMISSION);
    fixture
        .peer
        .send(ADMISSION, Performative::Begin(Begin::default()), Vec::new())
        .await?;
    let mut running = Box::pin(run_driver(
        &mut fixture.connection,
        NamespaceName::new("tenant")?,
        fixture.recorder.clone(),
        None,
        driver,
    ));
    timeout(DEADLINE, async {
        tokio::select! {
            () = gate.entered() => Ok::<_, Box<dyn std::error::Error + Send + Sync>>(()),
            result = &mut running => Err(format!("driver ended before Begin was gated: {result:?}").into()),
        }
    }).await??;
    let (reply, stopped) = oneshot::channel();
    events.send(Event::WorkerStopped { source, reply }).await?;
    timeout(DEADLINE, async {
        tokio::select! {
            result = stopped => result.map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>),
            result = &mut running => Err(format!("driver ended before logical cancellation: {result:?}").into()),
        }
    }).await??;
    assert_eq!(permit.state(), crate::AtomicCommitState::Aborted);
    assert_eq!(fixture.recorder.claimed.load(Ordering::Relaxed), 0);
    assert!(
        fixture
            .recorder
            .bodies
            .lock()
            .expect("owner work observer")
            .is_empty()
    );
    assert_eq!(native.state(), NativeTransactionState::Ready);

    gate.release();
    release_owner.send(()).expect("paused owner still retained");
    let response = async {
        let Frame::Amqp {
            channel: ADMISSION,
            performative: Some(Performative::Begin(begin)),
            payload,
        } = fixture.peer.non_flow().await?
        else {
            return Err("late successful Begin missing".into());
        };
        assert_eq!(begin.remote_channel, Some(ADMISSION));
        assert!(payload.is_empty());
        let posting = fixture.peer.disposition(POST, 0).await?;
        assert!(posting.settled);
        assert!(posting.state.is_none());
        let control = fixture.peer.disposition(CONTROL, 1).await?;
        assert!(control.settled);
        let Some(DeliveryState::Rejected(rejected)) = control.state else {
            return Err("canceled native owner response must not accept discharge".into());
        };
        assert_eq!(
            rejected
                .error
                .expect("rollback condition")
                .condition
                .as_symbol()
                .as_str(),
            "amqp:transaction:rollback"
        );
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };
    timeout(DEADLINE, async {
        tokio::select! {
            result = response => result,
            result = &mut running => Err(format!("driver ended before cancellation completion: {result:?}").into()),
        }
    }).await??;
    assert_eq!(permit.state(), crate::AtomicCommitState::Aborted);
    assert_eq!(native.state(), NativeTransactionState::Aborted);
    assert_eq!(fixture.recorder.claimed.load(Ordering::Relaxed), 0);
    assert!(
        fixture
            .recorder
            .bodies
            .lock()
            .expect("owner work observer")
            .is_empty()
    );
    let (reply, stopped) = oneshot::channel();
    events.send(Event::StopConnection { reply }).await?;
    assert!(timeout(DEADLINE, running.as_mut()).await?.is_err());
    stopped.await?;
    drop(running);
    fixture.shutdown().await
}

#[tokio::test]
async fn blocked_begin_flush_does_not_stall_producer_cancellation() -> TestResult {
    blocked_begin_does_not_block_pending_abort(false).await
}

#[tokio::test]
async fn blocked_begin_flush_does_not_stall_controller_cancellation() -> TestResult {
    blocked_begin_does_not_block_pending_abort(true).await
}

#[tokio::test]
async fn pending_admissions_and_actual_collectors_share_one_session_budget() -> TestResult {
    let mut fixture = Fixture::new().await?;
    // Release setup sessions so both native and adapter budgets have all 32 slots.
    for channel in [CONTROL, POST] {
        fixture
            .peer
            .send(channel, Performative::End(amqp::End::default()), Vec::new())
            .await?;
        assert!(matches!(fixture.peer.non_flow().await?, Frame::Amqp {
            channel: actual,
            performative: Some(Performative::End(_)),
            payload,
        } if actual == channel && payload.is_empty()));
    }
    let mut driver = Driver::new(
        fixture.connection.connection_identity().clone(),
        fixture.recorder.clone(),
    );
    assert_eq!(driver.session_count(), 0);
    fixture
        .peer
        .send(ADMISSION, Performative::Begin(Begin::default()), Vec::new())
        .await?;
    let incoming = timeout(DEADLINE, fixture.connection.next_incoming_session())
        .await?
        .ok_or("actual pending session missing")?;
    driver
        .admissions
        .push(Box::pin(fixture.connection.accept_session(incoming)));
    assert_eq!(driver.session_count(), 1);
    fixture.begin_gate.block(ADMISSION);
    timeout(DEADLINE, async {
        tokio::select! {
            () = fixture.begin_gate.entered() => Ok::<_, Box<dyn std::error::Error + Send + Sync>>(()),
            _ = driver.admissions.next() => Err("admission finished before its Begin flush".into()),
        }
    }).await??;
    assert_eq!(driver.session_count(), 1);
    fixture.begin_gate.release();
    let (session, response) = tokio::join!(
        timeout(DEADLINE, driver.admissions.next()),
        fixture.peer.non_flow(),
    );
    let session = session?.ok_or("completed native admission missing")??;
    assert!(matches!(response?, Frame::Amqp {
        channel: ADMISSION,
        performative: Some(Performative::Begin(begin)),
        payload,
    } if begin.remote_channel == Some(ADMISSION) && payload.is_empty()));
    assert_eq!(driver.session_count(), 0);
    driver.sessions.spawn(routing::serve_session(
        session,
        NamespaceName::new("tenant")?,
        fixture.recorder.clone(),
        None,
        driver.events.clone(),
        Arc::new(Semaphore::new(MAX_LINKS)),
        IngressMode::Posting,
    ));
    assert_eq!(driver.session_count(), 1);

    for offset in 0..MAX_SESSIONS - 1 {
        let channel = 40 + u16::try_from(offset)?;
        fixture
            .peer
            .send(channel, Performative::Begin(Begin::default()), Vec::new())
            .await?;
        let incoming = timeout(DEADLINE, fixture.connection.next_incoming_session())
            .await?
            .ok_or("actual pending sibling session missing")?;
        driver
            .admissions
            .push(Box::pin(fixture.connection.accept_session(incoming)));
        assert_eq!(driver.session_count(), offset + 2);
    }
    assert_eq!(driver.admissions.len(), MAX_SESSIONS - 1);
    assert_eq!(driver.sessions.len(), 1);
    assert_eq!(driver.session_count(), MAX_SESSIONS);
    assert_eq!(fixture.recorder.handoffs.load(Ordering::Relaxed), 0);
    assert_eq!(fixture.recorder.claimed.load(Ordering::Relaxed), 0);
    drop(driver);
    fixture.shutdown().await
}

use amqp::{Begin, Close, Frame, Performative};
use std::sync::atomic::Ordering;

use super::super::{
    RetainedAtomicMessagingLimits as Limits, RetainedAtomicMessagingLimitsError as LimitsError,
};
use super::fixture::{
    Anchor, CHANNELS, Fixture, TestResult, caught, controller, facts, producer, rethrow,
};

#[tokio::test]
async fn invalid_session_attempt_caps_are_refused_before_spawn() -> TestResult {
    let anchor = Anchor::new(None);
    let witness = anchor.drops.clone();
    for invalid in [0, 33, usize::MAX] {
        assert_eq!(Limits::new(invalid, 1), Err(LimitsError::SessionAttempts));
    }
    assert_eq!(Limits::new(1, 1)?.session_attempts(), 1);
    assert_eq!(Limits::new(32, 1)?.session_attempts(), 32);
    assert_eq!(witness.load(Ordering::SeqCst), 0);
    drop(anchor);
    assert_eq!(witness.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn invalid_worker_history_caps_are_refused_before_spawn() -> TestResult {
    let anchor = Anchor::new(None);
    let witness = anchor.drops.clone();
    for invalid in [0, 129, usize::MAX] {
        assert_eq!(Limits::new(1, invalid), Err(LimitsError::WorkerHistory));
    }
    assert_eq!(Limits::new(1, 1)?.worker_history(), 1);
    assert_eq!(Limits::new(1, 128)?.worker_history(), 128);
    assert_eq!(witness.load(Ordering::SeqCst), 0);
    drop(anchor);
    assert_eq!(witness.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn session_history_is_not_reused_after_end() -> TestResult {
    let mut fixture = Fixture::new(1, 4).await?;
    let observed = caught(async {
        fixture.hello().await?;
        fixture.bound().await?;
        fixture.begin(0).await?;
        fixture.wait_sessions(1).await?;
        fixture.end(0).await?;
        let control = fixture.owner.control();
        fixture
            .drive(async {
                while control.progress().session_joins() != 1 {
                    tokio::task::yield_now().await;
                }
                Ok(())
            })
            .await?;
        fixture
            .send(CHANNELS[1], Performative::Begin(Begin::default()))
            .await?;
        let mut seen = false;
        for _ in 0..64 {
            match fixture.frame().await? {
                Frame::Amqp {
                    performative: Some(Performative::Close(close)),
                    ..
                } => {
                    assert_eq!(
                        close.error.as_ref().map(|error| &error.condition),
                        Some(&amqp::AmqpError::ResourceLimitExceeded.into())
                    );
                    seen = true;
                    break;
                }
                Frame::Amqp {
                    performative: Some(Performative::End(_) | Performative::Flow(_)),
                    ..
                } => {}
                _ => return Err("unexpected session-history refusal response".into()),
            }
        }
        assert!(seen);
        fixture
            .send(0, Performative::Close(Close::default()))
            .await?;
        fixture
            .drive(async {
                while !control.progress().bridge_done {
                    tokio::task::yield_now().await;
                }
                Ok(())
            })
            .await?;
        assert_eq!(control.progress().session_attempts(), 1);
        Ok(())
    })
    .await;
    let cleanup = fixture.complete().await;
    rethrow(observed)?;
    let report = cleanup?;
    assert_eq!(facts(&report), (1, 1, 0, true));
    assert_eq!(
        report.admissions().len(),
        2,
        "one bounded original overflow receipt, not another attempt"
    );
    assert!(report.admissions()[1]._incoming.is_some());
    assert!(report.admissions()[1]._original.is_none());
    assert!(
        report.native_close().is_some(),
        "actual native Close result retained separately"
    );
    Ok(())
}

#[tokio::test]
async fn worker_history_is_shared_across_sessions() -> TestResult {
    let mut fixture = Fixture::new(2, 1).await?;
    let observed = caught(async {
        fixture.hello().await?;
        fixture.bound().await?;
        fixture.begin(0).await?;
        fixture.wait_sessions(1).await?;
        let accepted = fixture.attach(0, producer(0)).await?;
        assert!(accepted.target.is_some());
        fixture.wait_workers(1).await?;
        fixture.end(0).await?;
        let control = fixture.owner.control();
        fixture
            .drive(async {
                while control.progress().worker_joins() != 1
                    || control.progress().session_joins() != 1
                {
                    tokio::task::yield_now().await;
                }
                Ok(())
            })
            .await?;
        fixture.begin(1).await?;
        fixture.wait_sessions(2).await?;
        fixture
            .send(CHANNELS[1], Performative::Attach(Box::new(controller(0))))
            .await?;
        fixture
            .drive(async {
                while !control.progress().authority_closed() {
                    tokio::task::yield_now().await;
                }
                Ok(())
            })
            .await?;
        assert_eq!(control.progress().worker_launches(), 1);
        assert_eq!(fixture.owner.budget_counts().1.0, 1);
        Ok(())
    })
    .await;
    let cleanup = fixture.complete().await;
    rethrow(observed)?;
    let report = cleanup?;
    assert_eq!(facts(&report), (2, 2, 1, true));
    let rows: Vec<_> = report.workers().collect();
    assert_eq!(rows[0].ordinal(), 0);
    assert_eq!(
        rows[0].branch(),
        super::super::RetainedAtomicMessagingWorkerBranch::Producer
    );
    assert_eq!(report.worker_launches(), 1);
    Ok(())
}

#[tokio::test]
async fn aborted_acceptance_refunds_reservation_without_erasing_attempt() -> TestResult {
    let mut fixture = Fixture::new(2, 4).await?;
    let hooks = fixture.hooks.clone();
    let observed = caught(async {
        fixture.hello().await?;
        fixture.bound().await?;
        hooks.conversion.arm();
        fixture
            .send(CHANNELS[0], Performative::Begin(Begin::default()))
            .await?;
        fixture.gate(&hooks.conversion).await?;
        assert_eq!(fixture.owner.budget_counts().0, (0, 1, false));
        assert_eq!(fixture.owner.control().progress().session_attempts(), 1);
        fixture.owner.abort_wrapper_for_test();
        fixture.owner.stop();
        hooks.conversion.release();
        let control = fixture.owner.control();
        fixture
            .drive(async {
                while !control.progress().bridge_done {
                    tokio::task::yield_now().await;
                }
                Ok(())
            })
            .await?;
        tokio::time::timeout(super::fixture::DEADLINE, fixture.owner.drive_step()).await?;
        assert_eq!(fixture.owner.budget_counts().0, (0, 0, true));
        assert_eq!(control.progress().session_attempts(), 1);
        Ok(())
    })
    .await;
    let cleanup = fixture.complete().await;
    rethrow(observed)?;
    let report = cleanup?;
    assert_eq!(facts(&report), (1, 0, 0, true));
    assert!(report.admissions()[0]._original.is_some());
    assert!(report.socket().wrapper().is_some_and(Result::is_err));
    Ok(())
}

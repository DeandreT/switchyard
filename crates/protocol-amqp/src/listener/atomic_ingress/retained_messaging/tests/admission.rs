use std::{future::poll_fn, sync::atomic::Ordering, task::Poll};

use amqp::{Begin, Performative};

use super::super::{
    RetainedAtomicMessagingLimits as Limits, RetainedAtomicMessagingOwner as Owner,
};
use super::fixture::{
    Anchor, CHANNELS, Fixture, TestResult, caught, facts, original_pointer, rethrow,
};

#[tokio::test]
async fn incoming_discovery_ready_is_rooted_before_future_drop() -> TestResult {
    let mut fixture = Fixture::new(2, 4).await?;
    let hooks = fixture.hooks.clone();
    let observed = caught(async {
        fixture.hello().await?;
        fixture.bound().await?;
        hooks.discovery_ready.arm();
        fixture
            .send(CHANNELS[0], Performative::Begin(Begin::default()))
            .await?;
        fixture.gate(&hooks.discovery_ready).await?;
        assert!(fixture.owner.incoming_is_rooted(0));
        assert!(fixture.owner.native_address(0).is_none());
        assert!(fixture.owner.original_session_ids().is_empty());
        Ok(())
    })
    .await;
    let cleanup = fixture.complete().await;
    rethrow(observed)?;
    let report = cleanup?;
    assert_eq!(facts(&report), (1, 0, 0, true));
    assert!(report.admissions()[0]._incoming.is_some());
    assert!(report.admissions()[0]._original.is_none());
    Ok(())
}

#[tokio::test]
async fn cancelled_conversion_restores_original_cold_admission() -> TestResult {
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
        let address = hooks.native_address.load(Ordering::SeqCst);
        assert_ne!(address, 0);
        assert!(
            fixture.owner.native_address(0).is_none(),
            "whole packet is still loaned to its creator"
        );
        fixture.owner.abort_wrapper_for_test();
        fixture.owner.stop();
        Ok(address)
    })
    .await;
    let cleanup = fixture.complete().await;
    let address = rethrow(observed)?;
    let report = cleanup?;
    assert_eq!(facts(&report), (1, 0, 0, true));
    let original = report.admissions()[0]
        ._original
        .as_ref()
        .expect("same original restored native future");
    assert_eq!(original_pointer(original), address);
    assert!(
        report
            .socket()
            .wrapper()
            .is_some_and(|row| row.as_ref().is_err_and(|error| error.is_cancelled()))
    );
    assert!(
        report.has_failures(),
        "raw wrapper cancellation is not suppressed"
    );
    Ok(())
}

#[tokio::test]
async fn admission_ready_is_rooted_before_session_launch() -> TestResult {
    let mut fixture = Fixture::new(2, 4).await?;
    let hooks = fixture.hooks.clone();
    let observed = caught(async {
        fixture.hello().await?;
        fixture.bound().await?;
        hooks.admission_ready.arm();
        fixture
            .send(CHANNELS[0], Performative::Begin(Begin::default()))
            .await?;
        fixture.gate(&hooks.admission_ready).await?;
        let ids = fixture.owner.original_session_ids();
        assert_eq!(ids.len(), 1);
        let address = fixture
            .owner
            .native_address(0)
            .expect("rooted original accepted future");
        assert_eq!(address, hooks.native_address.load(Ordering::SeqCst));
        Ok((ids[0], address))
    })
    .await;
    let cleanup = fixture.complete().await;
    let (id, address) = rethrow(observed)?;
    let report = cleanup?;
    assert_eq!(facts(&report), (1, 1, 0, true));
    assert_eq!(report.sessions()[0].id(), id);
    assert_eq!(
        report.admissions()[0].launched(),
        Some((id, report.sessions()[0].ordinal()))
    );
    assert_eq!(
        original_pointer(
            report.admissions()[0]
                ._original
                .as_ref()
                .expect("original completed native future")
        ),
        address
    );
    Ok(())
}

#[tokio::test]
async fn cancelled_pending_native_session_accept_keeps_original_future() -> TestResult {
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
        let expected = hooks.native_address.load(Ordering::SeqCst);
        hooks.conversion.release();
        // Yield only until that original creator's loan has actually been restored.
        tokio::time::timeout(super::fixture::DEADLINE, async {
            while fixture.owner.native_address(0).is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let pending = poll_fn(|cx| {
            // Poll the native owned future once, then abandon only this borrowed observer.
            Poll::Ready(fixture.owner.poll_native_once(0, cx).is_pending())
        })
        .await;
        assert!(
            pending,
            "real actor acceptance response has not yet been polled on this current-thread runtime"
        );
        assert_eq!(fixture.owner.native_address(0), Some(expected));
        fixture.owner.stop();
        Ok(expected)
    })
    .await;
    let cleanup = fixture.complete().await;
    let expected = rethrow(observed)?;
    let report = cleanup?;
    assert_eq!(report.session_attempts(), 1);
    assert_eq!(
        original_pointer(
            report.admissions()[0]
                ._original
                .as_ref()
                .expect("original Pending native future retained through completion")
        ),
        expected
    );
    assert!(report.socket().wrapper().is_some());
    Ok(())
}

#[tokio::test]
async fn unpolled_starter_creates_no_roles() -> TestResult {
    let anchor = Anchor::new(None);
    let drops = anchor.drops.clone();
    let (mut owner, starter) = Owner::<_, super::fixture::Recorder>::new(
        tokio::runtime::Handle::current(),
        Limits::new(2, 4)?,
        anchor,
    )
    .map_err(|_| "bounded cold root allocation refused")?;
    let control = owner.control();
    drop(starter);
    let report = owner.finish().await.expect("first no-role original report");
    assert!(report.socket().wrapper().is_none());
    assert!(report.socket().actor().is_none());
    assert!(report.socket().reader().is_none());
    assert_eq!(
        (
            report.session_attempts(),
            report.session_joins(),
            report.worker_joins()
        ),
        (0, 0, 0)
    );
    assert!(!control.progress().bound());
    assert!(control.progress().reported());
    assert!(owner.finish().await.is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(report);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn broker_clone_unwind_preserves_original_open_binding() -> TestResult {
    let mut fixture = Fixture::new(2, 4).await?;
    let hooks = fixture.hooks.clone();
    let control = fixture.owner.control();
    hooks.binding.arm();
    let (payload, identity, drops) = super::fixture::payload();
    let observed = caught(async {
        fixture.hello().await?;
        fixture.gate(&hooks.binding).await?;
        let address = fixture
            .owner
            .original_open_address()
            .expect("same rooted original Open");
        *super::super::outcomes::locked(&fixture.recorder.clone_fault) = Some(Box::new(payload));
        hooks.binding.release();
        let original = caught(async {
            fixture.owner.drive_step().await;
            Ok(())
        })
        .await;
        let Err(panic) = original else {
            return Err("actual Broker Clone did not unwind".into());
        };
        assert_eq!(fixture.owner.original_open_address(), Some(address));
        assert!(!fixture.owner.control().progress().bound());
        assert!(fixture.owner.original_session_ids().is_empty());
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        fixture.bound().await?;
        assert!(
            fixture.owner.original_open_address().is_none(),
            "same Open transferred only after successful clone"
        );
        Ok(panic)
    })
    .await;
    let cleanup = fixture.complete().await;
    let original = rethrow(observed)?;
    let report = cleanup?;
    assert_eq!(facts(&report), (0, 0, 0, true));
    assert!(control.progress().reported());
    assert_eq!(report.anchor().drops.load(Ordering::SeqCst), 0);
    let payload = original
        .downcast_ref::<super::fixture::Payload>()
        .expect("original Clone panic payload");
    assert!(std::sync::Arc::ptr_eq(&payload.identity, &identity));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let anchor_witness = report.anchor().drops.clone();
    drop(original);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(report);
    assert_eq!(anchor_witness.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn broker_clone_unwind_preserves_ready_native_admission() -> TestResult {
    let mut fixture = Fixture::new(2, 4).await?;
    let hooks = fixture.hooks.clone();
    let (payload, identity, drops) = super::fixture::payload();
    let observed = caught(async {
        fixture.hello().await?;
        fixture.bound().await?;
        hooks.conversion.arm();
        fixture
            .send(CHANNELS[0], Performative::Begin(Begin::default()))
            .await?;
        fixture.gate(&hooks.conversion).await?;
        let address = hooks.native_address.load(Ordering::SeqCst);
        hooks.conversion.release();
        tokio::time::timeout(super::fixture::DEADLINE, async {
            while fixture.owner.native_address(0).is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        tokio::time::timeout(
            super::fixture::DEADLINE,
            poll_fn(|cx| fixture.owner.poll_native_once(0, cx)),
        )
        .await?;
        let ready = fixture
            .owner
            .native_ready_address(0)
            .expect("actual native Ready Session rooted before clone");
        assert_eq!(fixture.owner.native_address(0), Some(address));
        assert!(fixture.owner.original_session_ids().is_empty());
        *super::super::outcomes::locked(&fixture.recorder.clone_fault) = Some(Box::new(payload));
        let original = tokio::time::timeout(
            super::fixture::DEADLINE,
            caught(async {
                while fixture.owner.original_session_ids().is_empty() {
                    fixture.owner.drive_step().await;
                }
                Ok(())
            }),
        )
        .await?;
        let Err(panic) = original else {
            return Err("actual launch Broker Clone did not unwind".into());
        };
        assert_eq!(fixture.owner.native_address(0), Some(address));
        assert_eq!(fixture.owner.native_ready_address(0), Some(ready));
        assert!(
            fixture.owner.original_session_ids().is_empty(),
            "no duplicate or premature Session launch"
        );
        assert_eq!(fixture.owner.control().progress().session_attempts(), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        fixture.wait_sessions(1).await?;
        let ids = fixture.owner.original_session_ids();
        assert_eq!(ids.len(), 1);
        Ok((panic, address, ids[0]))
    })
    .await;
    let cleanup = fixture.complete().await;
    let (original, address, id) = rethrow(observed)?;
    let report = cleanup?;
    assert_eq!(facts(&report), (1, 1, 0, true));
    assert_eq!(report.sessions()[0].id(), id);
    assert_eq!(
        original_pointer(
            report.admissions()[0]
                ._original
                .as_ref()
                .expect("same completed original native future")
        ),
        address
    );
    assert_eq!(
        report.admissions()[0].launched(),
        Some((id, report.sessions()[0].ordinal()))
    );
    assert_eq!(report.anchor().drops.load(Ordering::SeqCst), 0);
    let payload = original
        .downcast_ref::<super::fixture::Payload>()
        .expect("original launch Clone panic payload");
    assert!(std::sync::Arc::ptr_eq(&payload.identity, &identity));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let anchor_witness = report.anchor().drops.clone();
    drop(original);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(report);
    assert_eq!(anchor_witness.load(Ordering::SeqCst), 1);
    Ok(())
}

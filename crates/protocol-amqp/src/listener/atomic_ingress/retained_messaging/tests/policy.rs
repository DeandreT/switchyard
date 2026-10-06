use std::{sync::atomic::Ordering, time::Duration};

use amqp::{Close, Detach, End, Frame, Performative, Role};

use super::fixture::{
    Anchor, CHANNELS, Fixture, TestResult, authentication, caught, controller, facts, producer,
    request, rethrow,
};

// Fixed non-secret test key, encoded audience + LF + expiry; no signing dependency.
const GRANT: &str = "SharedAccessSignature sr=amqps%3A%2F%2Ftenant.servicebus.windows.net%2Forders&sig=t%2BJqGjYXAhFvGHj0Ah2YRKCttTeRfTm31ZGniQQrdoA%3D&se=4102444800&skn=test-rule";
const RENEWED: &str = "SharedAccessSignature sr=amqps%3A%2F%2Ftenant.servicebus.windows.net%2Forders&sig=AiKhsDhgDDZNlEI7K30BSbMl5iFc7rkfxZXnez8Hm08%3D&se=4133980800&skn=test-rule";
const EXPIRED: &str = "SharedAccessSignature sr=amqps%3A%2F%2Ftenant.servicebus.windows.net%2Forders&sig=kXckLa%2BW95XJrfcZPXZborXixts97ZqwHOcLFNBG9J8%3D&se=1&skn=test-rule";

#[tokio::test]
async fn initial_metadata_grace_never_authorizes_queue_or_commit() -> TestResult {
    let mut fixture = Fixture::with_anchor(
        2,
        4,
        Anchor::new(None),
        Some(authentication(Duration::from_secs(1))?),
        false,
    )
    .await?;
    let hooks = fixture.hooks.clone();
    hooks.binding.arm();
    let observed = caught(async {
        fixture.hello().await?;
        fixture.gate(&hooks.binding).await?;
        let authorization = fixture
            .owner
            .original_authorization()
            .expect("same captured original authorization");
        let deadline = fixture
            .owner
            .original_deadline()
            .expect("absolute post-Open deadline saved before binding delay");
        assert!(authorization.can_control_metadata().await);
        assert!(!authorization.can_control_commit().await);
        assert!(
            authorization
                .authorize_entity("orders", auth::Permission::Send)
                .await
                .is_err()
        );
        assert_eq!(fixture.recorder.binds.load(Ordering::SeqCst), 0);
        tokio::time::sleep_until(deadline + Duration::from_millis(1)).await;
        assert!(!authorization.can_control_metadata().await);
        assert!(!authorization.can_control_commit().await);
        assert!(
            authorization
                .authorize_entity("orders", auth::Permission::Send)
                .await
                .is_err()
        );
        assert_eq!(fixture.owner.original_deadline(), Some(deadline));
        assert!(!fixture.owner.control().progress().bound());
        hooks.binding.release();
        let mut close = None;
        for _ in 0..64 {
            if let Frame::Amqp {
                performative: Some(Performative::Close(original)),
                ..
            } = fixture.frame().await?
            {
                close = Some(original);
                break;
            }
        }
        let close = close.ok_or("missing original initial-grace expiry Close")?;
        assert_eq!(
            close
                .error
                .as_ref()
                .map(|error| error.condition.as_symbol()),
            Some("amqp:unauthorized-access".into())
        );
        assert!(
            fixture.owner.control().progress().authority_closed(),
            "logical authority precedes native Close"
        );
        fixture
            .send(0, Performative::Close(Close::default()))
            .await?;
        let control = fixture.owner.control();
        fixture
            .drive(async {
                while !control.progress().bridge_done {
                    tokio::task::yield_now().await;
                }
                Ok(())
            })
            .await?;
        assert_eq!(fixture.recorder.submitted.load(Ordering::SeqCst), 0);
        Ok(())
    })
    .await;
    let cleanup = fixture.complete().await;
    rethrow(observed)?;
    let report = cleanup?;
    assert_eq!(facts(&report), (0, 0, 0, true));
    assert!(
        report.native_close().is_some(),
        "actual expiry Close result, not inferred success"
    );
    Ok(())
}

#[tokio::test]
async fn cbs_renewal_and_expiry_keep_existing_messaging_policy() -> TestResult {
    let mut fixture = Fixture::with_anchor(
        2,
        8,
        Anchor::new(None),
        Some(authentication(Duration::from_secs(20))?),
        false,
    )
    .await?;
    let observed = caught(async {
        fixture.hello().await?;
        fixture.bound().await?;
        fixture.begin(0).await?;
        fixture.wait_sessions(1).await?;
        let authorization = fixture
            .owner
            .original_authorization()
            .expect("same original CBS authorization");
        assert!(!authorization.can_control_commit().await);
        let local = fixture.cbs_links().await?;
        fixture.cbs_token(local, GRANT, 202).await?;
        assert!(authorization.can_control_commit().await);
        assert!(
            authorization
                .authorize_entity("orders", auth::Permission::Send)
                .await
                .is_ok()
        );
        fixture.cbs_token(local, RENEWED, 202).await?;
        assert!(authorization.can_control_commit().await);
        fixture.cbs_token(local, EXPIRED, 401).await?;
        // An expired token is refused, not installed in place of the valid renewal.
        assert!(
            authorization
                .authorize_entity("orders", auth::Permission::Send)
                .await
                .is_ok()
        );
        assert!(authorization.can_control_commit().await);
        let accepted = fixture.attach(0, producer(20)).await?;
        assert!(accepted.target.is_some());
        fixture.wait_workers(3).await?;
        assert_eq!(fixture.recorder.binds.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.recorder.submitted.load(Ordering::SeqCst), 0);
        Ok(())
    })
    .await;
    let cleanup = fixture.complete().await;
    rethrow(observed)?;
    let report = cleanup?;
    assert_eq!(facts(&report), (1, 1, 3, true));
    let branches: Vec<_> = report.workers().map(|row| row.branch()).collect();
    assert!(branches.contains(&super::super::RetainedAtomicMessagingWorkerBranch::CbsRequests));
    assert!(branches.contains(&super::super::RetainedAtomicMessagingWorkerBranch::CbsReplies));
    assert!(branches.contains(&super::super::RetainedAtomicMessagingWorkerBranch::Producer));
    Ok(())
}

#[tokio::test]
async fn management_links_remain_refused_on_collected_endpoint() -> TestResult {
    let mut fixture = Fixture::new(2, 4).await?;
    let observed = caught(async {
        fixture.hello().await?;
        fixture.bound().await?;
        fixture.begin(0).await?;
        fixture.wait_sessions(1).await?;
        let refused = fixture
            .attach(
                0,
                request(
                    "retained-management-refusal",
                    3,
                    Role::Sender,
                    "orders/$management",
                ),
            )
            .await?;
        assert_eq!(refused.role, Role::Receiver);
        assert_eq!(refused.name, "retained-management-refusal");
        assert!(refused.source.is_none() && refused.target.is_none());
        let mut detached = false;
        for _ in 0..64 {
            match fixture.frame().await? {
                Frame::Amqp {
                    performative: Some(Performative::Detach(detach)),
                    ..
                } => {
                    assert_eq!(detach.handle, refused.handle);
                    assert!(detach.closed);
                    assert_eq!(
                        detach
                            .error
                            .as_ref()
                            .map(|error| error.condition.as_symbol()),
                        Some("amqp:not-implemented".into())
                    );
                    detached = true;
                    break;
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                _ => return Err("unexpected management refusal response".into()),
            }
        }
        assert!(detached);
        fixture
            .send(
                CHANNELS[0],
                Performative::Detach(Detach {
                    handle: 3,
                    closed: true,
                    error: None,
                }),
            )
            .await?;
        assert_eq!(fixture.recorder.binds.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.owner.control().progress().worker_launches(), 0);
        let ordinary = fixture.attach(0, producer(4)).await?;
        assert!(ordinary.target.is_some());
        fixture.wait_workers(1).await?;
        assert_eq!(fixture.recorder.binds.load(Ordering::SeqCst), 1);
        Ok(())
    })
    .await;
    let cleanup = fixture.complete().await;
    rethrow(observed)?;
    let report = cleanup?;
    assert_eq!(facts(&report), (1, 1, 1, true));
    Ok(())
}

#[tokio::test]
async fn default_listener_transaction_refusal_is_unchanged() -> TestResult {
    let mut fixture = Fixture::with_anchor(2, 4, Anchor::new(None), None, true).await?;
    let observed = caught(async {
        fixture.hello().await?;
        assert!(!fixture.owner.control().progress().bound());
        fixture.begin(0).await?;
        fixture
            .send(CHANNELS[0], Performative::Attach(Box::new(controller(3))))
            .await?;
        let mut refused = false;
        for _ in 0..64 {
            match fixture.frame().await? {
                Frame::Amqp {
                    channel,
                    performative: Some(Performative::End(end)),
                    ..
                } => {
                    assert_eq!(channel, CHANNELS[0]);
                    assert_eq!(
                        end.error.as_ref().map(|error| error.condition.as_symbol()),
                        Some("amqp:not-implemented".into())
                    );
                    refused = true;
                    break;
                }
                Frame::Amqp {
                    performative: Some(Performative::Flow(_)),
                    ..
                } => {}
                _ => return Err("ordinary coordinator unexpectedly accepted".into()),
            }
        }
        assert!(refused);
        fixture
            .send(CHANNELS[0], Performative::End(End::default()))
            .await?;
        assert_eq!(fixture.recorder.binds.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.recorder.submitted.load(Ordering::SeqCst), 0);
        Ok(())
    })
    .await;
    let cleanup = fixture.complete().await;
    rethrow(observed)?;
    let report = cleanup?;
    assert_eq!(facts(&report), (0, 0, 0, true));
    // This negative default probe reports only covered socket roles, not legacy descendant cleanup.
    Ok(())
}

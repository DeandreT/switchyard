use std::{sync::Arc, sync::atomic::Ordering, time::Duration};

use amqp::{Begin, Performative};
use auth::SharedAccessPolicy;
use domain::NamespaceName;
use tokio::time::timeout;

use super::super::{Driver, IngressMode, run_driver};
use super::fixture::{DEADLINE, Fixture, TestResult};
use crate::{
    SharedAccessAuthentication,
    authorization::{ConnectionAuthorization, InitialControlState},
};

#[tokio::test]
async fn unrepresentable_initial_deadline_fails_before_session_dispatch_or_owner_work() -> TestResult
{
    let mut fixture = Fixture::new().await?;
    let authorization = ConnectionAuthorization::new(
        SharedAccessAuthentication::new(
            SharedAccessPolicy::new([])?,
            "tenant.servicebus.windows.net",
        )?
        .with_authorization_timeout(Duration::MAX),
        None,
    );
    assert_eq!(
        authorization.initial_control_state().await,
        InitialControlState::Disabled
    );

    fixture
        .peer
        .send(
            31,
            Performative::Begin(Begin {
                next_outgoing_id: 37,
                ..Begin::default()
            }),
            Vec::new(),
        )
        .await?;
    fixture.peer.barrier().await?;
    let driver = Driver::with_mode(
        fixture.connection.connection_identity().clone(),
        fixture.recorder.clone(),
        IngressMode::Messaging,
    );
    let error = timeout(
        DEADLINE,
        run_driver(
            &mut fixture.connection,
            NamespaceName::new("tenant")?,
            fixture.recorder.clone(),
            Some(Arc::clone(&authorization)),
            driver,
        ),
    )
    .await?
    .expect_err("unrepresentable authorization deadline must refuse startup");
    let error = error
        .downcast_ref::<std::io::Error>()
        .expect("typed deadline configuration error");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(
        error.to_string(),
        "the configured authorization timeout cannot be represented"
    );
    assert_eq!(
        authorization.initial_control_state().await,
        InitialControlState::Disabled
    );
    assert_eq!(fixture.recorder.handoffs.load(Ordering::Relaxed), 0);
    assert_eq!(fixture.recorder.claimed.load(Ordering::Relaxed), 0);
    assert!(
        fixture
            .recorder
            .bodies
            .lock()
            .expect("owner work observer")
            .is_empty()
    );

    let incoming = timeout(DEADLINE, fixture.connection.next_incoming_session())
        .await?
        .ok_or("invalid driver consumed the queued native session")?;
    assert_eq!(incoming.begin.next_outgoing_id, 37);
    drop(incoming);
    fixture.shutdown().await
}

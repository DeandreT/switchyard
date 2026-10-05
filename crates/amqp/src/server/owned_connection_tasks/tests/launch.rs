use super::*;
use crate::Open;
use crate::server::{
    ConnectionOptions, EngineError, NativeIngressPolicy, ServerConnection, connection_launch,
};

#[tokio::test]
async fn empty_owner_and_cached_finish_do_not_invent_tasks() {
    let anchor = std::rc::Rc::new(());
    let mut owner = PairOwner::new(Handle::current(), anchor.clone());
    let report = owner.finish().await.expect("first report");
    assert!(report.actor.is_none() && report.reader.is_none());
    assert!(std::rc::Rc::ptr_eq(&report.anchor, &anchor));
    assert!(owner.finish().await.is_none());
    assert_eq!(std::rc::Rc::strong_count(&anchor), 2);
}

#[tokio::test]
async fn closed_launch_returns_original_transport_without_identity() -> TestResult {
    let (negotiated, peer, witness) = negotiated().await?;
    let mut owner = PairOwner::new(Handle::current(), ());
    let empty = owner.finish().await.expect("empty report");
    let refused = owner.launch(negotiated);
    let original = match refused {
        Err(LaunchRefused(negotiated)) => negotiated.into_transport(),
        Ok(connection) => {
            drop(connection);
            panic!("sealed launch accepted");
        }
    };
    let unchanged = Arc::ptr_eq(&original.witness, &witness);
    let no_identity = locked(&owner.observations).identity.is_none();
    drop(original);
    drop(peer);
    assert!(unchanged && no_identity);
    assert!(empty.actor.is_none() && empty.reader.is_none());
    assert_eq!(witness.dropped.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn duplicate_launch_returns_input_and_keeps_original_identity() -> TestResult {
    let (first, first_peer, _) = negotiated().await?;
    let (second, second_peer, second_witness) = negotiated().await?;
    let mut owner = PairOwner::new(Handle::current(), ());
    let connection = launch(&owner, first);
    let identity = connection.lifecycle.identity.clone();
    let original = match owner.launch(second) {
        Err(LaunchRefused(second)) => second.into_transport(),
        Ok(second) => {
            drop(second);
            drop(connection);
            owner.finish().await;
            panic!("second actor accepted");
        }
    };
    let same_input = Arc::ptr_eq(&original.witness, &second_witness);
    let same_identity = locked(&owner.observations)
        .identity
        .as_ref()
        .is_some_and(|observed| observed.same_connection(&identity));
    drop(original);
    drop(connection);
    let report = owner.finish().await.expect("actual report");
    drop(first_peer);
    drop(second_peer);
    assert!(same_input && same_identity);
    assert!(report.actor.is_some_and(|result| result.is_ok()));
    assert!(!identity.is_active());
    Ok(())
}

#[tokio::test]
async fn unpolled_negotiation_loss_creates_no_roles() {
    let (io, peer, witness) = fixture::transport();
    let mut owner = PairOwner::new(Handle::current(), ());
    let future = connection_launch::negotiate(
        io,
        "server",
        None,
        ConnectionOptions::default(),
        NativeIngressPolicy::Disabled,
    );
    drop(future);
    let report = owner.finish().await.expect("empty report");
    drop(peer);
    assert!(report.actor.is_none() && report.reader.is_none());
    assert_eq!(witness.dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn pending_negotiation_loss_creates_no_roles() {
    let (io, peer, witness) = fixture::transport();
    let mut owner = PairOwner::new(Handle::current(), ());
    let mut future = Box::pin(connection_launch::negotiate(
        io,
        "server",
        None,
        ConnectionOptions::default(),
        NativeIngressPolicy::Disabled,
    ));
    let pending = poll_once(future.as_mut()).is_pending();
    drop(future);
    let report = owner.finish().await.expect("empty report");
    drop(peer);
    assert!(pending && report.actor.is_none() && report.reader.is_none());
    assert_eq!(witness.dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn extracted_and_public_accept_forward_original_typed_io_cause() {
    let marker = Arc::new(());
    let extracted = connection_launch::negotiate(
        fixture::FailedIo {
            marker: marker.clone(),
        },
        "server",
        None,
        ConnectionOptions::default(),
        NativeIngressPolicy::Disabled,
    )
    .await;
    let legacy = ServerConnection::accept(
        fixture::FailedIo {
            marker: marker.clone(),
        },
        "server",
        None,
    )
    .await;
    let same_extracted = match extracted {
        Err(error) => fixture::same_cause(error, &marker),
        Ok(_) => false,
    };
    let same_legacy = match legacy {
        Err(error) => fixture::same_cause(error, &marker),
        Ok(_) => false,
    };
    assert!(same_extracted && same_legacy);
}

#[tokio::test]
async fn options_validation_retains_original_timeout_variant() {
    let marker = Arc::new(());
    let options = ConnectionOptions::default().write_timeout(Duration::ZERO);
    let result = connection_launch::negotiate(
        fixture::FailedIo { marker },
        "server",
        None,
        options,
        NativeIngressPolicy::Disabled,
    )
    .await;
    assert!(matches!(result, Err(EngineError::Timeout("write"))));
}

#[tokio::test]
async fn scoped_handshake_bytes_match_original_protocol() -> TestResult {
    let (negotiated, peer, witness) = negotiated().await?;
    let mut owner = PairOwner::new(Handle::current(), ());
    let connection = launch(&owner, negotiated);
    drop(connection);
    let report = owner.finish().await.expect("actual report");
    drop(peer);
    let scoped_wire = locked(&witness.wire).clone();
    let mut expected = b"AMQP\0\x01\0\0".to_vec();
    expected.extend(crate::encode_frame(&crate::server::checked_open_frame(
        Open {
            max_frame_size: crate::server::DEFAULT_MAX_FRAME_SIZE,
            idle_time_out: Some(ConnectionOptions::default().advertised_idle_timeout()),
            ..Open::new("server")
        },
    )?)?);
    assert_eq!(scoped_wire, expected);
    assert!(report.actor.is_some_and(|result| result.is_ok()));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn queued_unpolled_actor_abort_retires_identity_and_really_joins() -> TestResult {
    let (negotiated, peer, witness) = negotiated().await?;
    let mut owner = PairOwner::new(Handle::current(), ());
    let connection = launch(&owner, negotiated);
    let identity = connection.lifecycle.identity.clone();
    fixture::actor_abort(&owner).abort();
    let report = owner.finish().await.expect("actual report");
    let notified = *connection.lifecycle.terminated.borrow();
    drop(connection);
    drop(peer);
    assert!(
        report
            .actor
            .is_some_and(|result| result.is_err_and(|error| error.is_cancelled()))
    );
    assert!(report.reader.is_none());
    assert!(notified && !identity.is_active());
    assert_eq!(witness.dropped.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn parent_drop_before_actor_poll_seals_reader_without_new_abort_policy() -> TestResult {
    let (negotiated, peer, witness) = negotiated().await?;
    let mut owner = PairOwner::new(Handle::current(), ());
    let connection = launch(&owner, negotiated);
    drop(connection);
    let report = owner.finish().await.expect("actual report");
    drop(peer);
    assert!(report.actor.is_some_and(|result| result.is_ok()));
    assert!(report.reader.is_none());
    assert_eq!(witness.dropped.load(Ordering::SeqCst), 1);
    Ok(())
}

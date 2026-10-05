use super::*;
use crate::{ConnectionOptions, EngineError, ScopedConnectionAcceptance, ServerConnectionOwner};

mod public_policy;
#[path = "public_api/fixture.rs"]
mod support;

use support::{Mode, expected_wire, open};

#[tokio::test]
async fn public_factory_retains_non_send_non_static_anchor_and_cached_report() {
    let value = std::cell::Cell::new(7_u8);
    let anchor = std::rc::Rc::new(&value);
    let (mut owner, acceptor) = ServerConnectionOwner::new(Handle::current(), anchor.clone());
    let dormant = matches!(*locked(&owner.actor.state), State::Dormant)
        && matches!(*locked(&owner.reader.state), State::Dormant);
    drop(acceptor);
    let report = owner.finish().await.expect("first report");
    let anchor_same = std::rc::Rc::ptr_eq(report.anchor(), &anchor);
    let no_tasks = report.actor().is_none() && report.reader().is_none();
    let (joins, returned) = report.into_parts();
    let cached = owner.finish().await.is_none();
    assert!(dormant && anchor_same && no_tasks && cached);
    assert!(joins.actor.is_none() && joins.reader.is_none());
    assert!(std::rc::Rc::ptr_eq(&returned, &anchor));
    assert_eq!(returned.get(), 7);
}

struct PendingFailureIo {
    ready: Arc<std::sync::atomic::AtomicBool>,
    marker: Arc<()>,
}

impl tokio::io::AsyncRead for PendingFailureIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.ready.load(Ordering::SeqCst) {
            tokio::io::AsyncRead::poll_read(
                Pin::new(&mut fixture::FailedIo {
                    marker: self.marker.clone(),
                }),
                cx,
                buffer,
            )
        } else {
            Poll::Pending
        }
    }
}
impl tokio::io::AsyncWrite for PendingFailureIo {
    fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, _: &[u8]) -> Poll<io::Result<usize>> {
        Poll::Pending
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Pending
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn first_polled_negotiation_failure_is_not_replaced_by_later_seal() {
    let marker = Arc::new(());
    let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (mut owner, acceptor) = ServerConnectionOwner::new(Handle::current(), ());
    let mut accepting = Box::pin(acceptor.accept_with_options(
        PendingFailureIo {
            ready: ready.clone(),
            marker: marker.clone(),
        },
        "server",
        None,
        ConnectionOptions::default(),
    ));
    let pending = poll_once(accepting.as_mut()).is_pending();
    owner.stop();
    ready.store(true, Ordering::SeqCst);
    let result = accepting.await;
    let report = owner.finish().await.expect("empty report");
    let same = match result {
        Err(error) => fixture::same_cause(error, &marker),
        _ => false,
    };
    assert!(pending && same && report.actor().is_none() && report.reader().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_panic_report_formatting_retains_original_payload_without_traversal() -> TestResult {
    let (io, mut peer, witness) = fixture::transport();
    let payload = Arc::new(PayloadCounter::default());
    let (mut owner, mut acceptor) = ServerConnectionOwner::new(Handle::current(), SecretAnchor);
    owner.controls = Arc::new(Controls {
        actor_panic: true,
        actor_payload: Some(payload.clone()),
        ..Controls::default()
    });
    acceptor.controls = owner.controls.clone();
    let mut setup = open(acceptor, io, &mut peer, Mode::Ordinary).await;
    let actor_seen = fixture::observe_actor_ready(&owner).await;
    drop(setup.connection.take());
    let report = owner.finish().await.expect("actual report");
    drop(peer);
    setup.checked()?;
    actor_seen?;
    let panic_result = report
        .actor()
        .is_some_and(|result| result.as_ref().is_err_and(JoinError::is_panic));
    let text = format!("{report:?}");
    let before = payload.0.load(Ordering::SeqCst);
    let (joins, _anchor) = report.into_parts();
    let joins_text = format!("{joins:?}");
    let after_format = payload.0.load(Ordering::SeqCst);
    drop(joins);
    assert!(panic_result);
    assert_eq!(text, "ServerConnectionJoinReport { .. }");
    assert_eq!(joins_text, "ServerConnectionTaskJoins { .. }");
    assert_eq!(
        (before, after_format, payload.0.load(Ordering::SeqCst)),
        (0, 0, 1)
    );
    assert_eq!(witness.dropped.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn public_async_accept_on_dying_b_roots_actual_tasks_on_retained_a() -> TestResult {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let (io, mut peer, witness) = fixture::transport();
    let anchor = std::rc::Rc::new(());
    let (mut owner, acceptor) =
        ServerConnectionOwner::new(runtime.handle().clone(), anchor.clone());
    let host = std::thread::spawn(
        move || -> io::Result<(support::Setup, tokio::io::DuplexStream)> {
            let current = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let setup = current.block_on(open(acceptor, io, &mut peer, Mode::Ordinary));
            drop(current);
            Ok((setup, peer))
        },
    )
    .join();
    let queued = matches!(*locked(&owner.actor.state), State::Pending(_))
        && matches!(*locked(&owner.reader.state), State::Dormant);
    let report = runtime.block_on(owner.finish()).expect("actual report");
    let setup = match host {
        Ok(Ok((setup, peer))) => {
            drop(peer);
            Some(setup)
        }
        original => {
            drop(original);
            None
        }
    };
    let setup = setup.ok_or_else(|| io::Error::other("separate acceptance host did not finish"))?;
    setup.checked()?;
    assert!(queued && report.actor().is_some_and(|result| result.is_ok()));
    assert!(report.reader().is_none());
    assert!(std::rc::Rc::ptr_eq(report.anchor(), &anchor));
    assert_eq!(witness.dropped.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn public_acceptance_keeps_original_handshake_bytes_and_actual_actor() -> TestResult {
    let (io, mut peer, witness) = fixture::transport();
    let (mut owner, acceptor) = ServerConnectionOwner::new(Handle::current(), ());
    let setup = open(acceptor, io, &mut peer, Mode::Ordinary).await;
    let created = setup
        .connection
        .as_ref()
        .is_some_and(|connection| connection.connection_identity().is_active());
    let report = owner.finish().await.expect("actual report");
    drop(peer);
    setup.checked()?;
    assert!(created && report.actor().is_some_and(|result| result.is_ok()));
    assert_eq!(*locked(&witness.wire), expected_wire()?);
    assert_eq!(witness.dropped.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn unpolled_public_accept_loss_creates_no_roles() {
    let (io, peer, witness) = fixture::transport();
    let (mut owner, acceptor) = ServerConnectionOwner::new(Handle::current(), ());
    let future = acceptor.accept_with_options(io, "server", None, ConnectionOptions::default());
    drop(future);
    let report = owner.finish().await.expect("empty report");
    drop(peer);
    assert!(report.actor().is_none() && report.reader().is_none());
    assert_eq!(witness.dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn first_polled_public_accept_loss_creates_no_roles() {
    let (io, peer, witness) = fixture::transport();
    let (mut owner, acceptor) = ServerConnectionOwner::new(Handle::current(), ());
    let mut future =
        Box::pin(acceptor.accept_with_options(io, "server", None, ConnectionOptions::default()));
    let pending = poll_once(future.as_mut()).is_pending();
    drop(future);
    let report = owner.finish().await.expect("empty report");
    drop(peer);
    assert!(pending && report.actor().is_none() && report.reader().is_none());
    assert_eq!(witness.dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn sealed_public_accept_forwards_original_typed_io_cause() {
    let marker = Arc::new(());
    let (mut owner, acceptor) = ServerConnectionOwner::new(Handle::current(), ());
    owner.stop();
    let result = acceptor
        .accept_with_options(
            fixture::FailedIo {
                marker: marker.clone(),
            },
            "server",
            None,
            ConnectionOptions::default(),
        )
        .await;
    let report = owner.finish().await.expect("empty report");
    let same = match result {
        Err(error) => fixture::same_cause(error, &marker),
        _ => false,
    };
    assert!(same && report.actor().is_none() && report.reader().is_none());
}

#[tokio::test]
async fn sealed_public_accept_retains_original_validation_timeout() {
    let marker = Arc::new(());
    let (mut owner, acceptor) = ServerConnectionOwner::new(Handle::current(), ());
    owner.stop();
    let result = acceptor
        .accept_with_options(
            fixture::FailedIo { marker },
            "server",
            None,
            ConnectionOptions::default().write_timeout(Duration::ZERO),
        )
        .await;
    let report = owner.finish().await.expect("empty report");
    assert!(matches!(result, Err(EngineError::Timeout("write"))));
    assert!(report.actor().is_none() && report.reader().is_none());
}

#[tokio::test]
async fn sealed_public_accept_returns_original_advanced_transport_without_identity() -> TestResult {
    let (io, mut peer, witness) = fixture::transport();
    let (mut owner, acceptor) = ServerConnectionOwner::new(Handle::current(), ());
    owner.stop();
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(
            acceptor.accept_with_options(io, "server", None, ConnectionOptions::default()),
            fixture::hello(&mut peer),
        )
    })
    .await;
    let report = owner.finish().await.expect("empty report");
    drop(peer);
    let (accepted, greeted) = result?;
    greeted?;
    let original = match accepted? {
        ScopedConnectionAcceptance::Refused(refused) => refused.into_transport(),
        ScopedConnectionAcceptance::Accepted(connection) => {
            drop(connection);
            return Err(io::Error::other("sealed root accepted a connection").into());
        }
    };
    let same = Arc::ptr_eq(&original.witness, &witness);
    let no_identity = locked(&owner.observations).identity.is_none();
    let bytes = locked(&witness.wire).clone();
    drop(original);
    assert!(same && no_identity && report.actor().is_none() && report.reader().is_none());
    assert_eq!(bytes, expected_wire()?);
    assert_eq!(witness.dropped.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn completed_public_root_refuses_later_acceptor_without_new_join_work() -> TestResult {
    let (io, mut peer, witness) = fixture::transport();
    let (mut owner, acceptor) = ServerConnectionOwner::new(Handle::current(), ());
    let report = owner.finish().await.expect("empty report");
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(
            acceptor.accept_with_options(io, "server", None, ConnectionOptions::default()),
            fixture::hello(&mut peer),
        )
    })
    .await;
    let cached = owner.finish().await.is_none();
    drop(peer);
    let (accepted, greeted) = result?;
    greeted?;
    let refused = matches!(accepted, Ok(ScopedConnectionAcceptance::Refused(_)));
    drop(accepted);
    assert!(cached && refused && report.actor().is_none() && report.reader().is_none());
    assert!(locked(&owner.observations).identity.is_none());
    assert_eq!(witness.dropped.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_shutdown_signal_and_cancelled_finish_do_not_release_anchor() -> TestResult {
    let (io, mut peer, witness) = fixture::transport();
    let anchor = std::rc::Rc::new(());
    let (mut owner, mut acceptor) = ServerConnectionOwner::new(Handle::current(), anchor.clone());
    let gate = Arc::new(Gate::default());
    owner.controls = Arc::new(Controls {
        actor_final: Some(gate.clone()),
        ..Controls::default()
    });
    acceptor.controls = owner.controls.clone();
    let mut setup = open(acceptor, io, &mut peer, Mode::Ordinary).await;
    let reader = fixture::observe_reader(&owner).await;
    let shutdown = if let Some(connection) = setup.connection.as_ref() {
        Some(tokio::time::timeout(Duration::from_secs(2), connection.shutdown()).await)
    } else {
        None
    };
    let observed = observe_gate(&gate).await;
    let notified = shutdown.as_ref().is_some_and(Result::is_ok);
    let mut waiter = Box::pin(owner.finish());
    let pending = poll_once(waiter.as_mut()).is_pending();
    drop(waiter);
    let retained = matches!(*locked(&owner.actor.state), State::Pending(_));
    let anchors_before = std::rc::Rc::strong_count(&anchor);
    gate.release();
    drop(setup.connection.take());
    let report = owner.finish().await.expect("actual report");
    drop(peer);
    setup.checked()?;
    reader?;
    observed?;
    assert!(notified && pending && retained);
    assert_eq!(anchors_before, 2);
    assert!(report.actor().is_some_and(|result| result.is_ok()));
    assert!(report.reader().is_some());
    assert!(std::rc::Rc::ptr_eq(report.anchor(), &anchor));
    assert_eq!(witness.dropped.load(Ordering::SeqCst), 1);
    Ok(())
}

struct SecretAnchor;
impl std::fmt::Debug for SecretAnchor {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        panic!("anchor formatting must not be traversed");
    }
}

#[tokio::test]
async fn public_debug_is_opaque_for_non_debug_io_and_private_anchor() -> TestResult {
    let (io, mut peer, witness) = fixture::transport();
    let (mut owner, acceptor) = ServerConnectionOwner::new(Handle::current(), SecretAnchor);
    let owner_text = format!("{owner:?}");
    let acceptor_text = format!("{acceptor:?}");
    owner.stop();
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(
            acceptor.accept_with_options(io, "server", None, ConnectionOptions::default()),
            fixture::hello(&mut peer),
        )
    })
    .await;
    let report = owner.finish().await.expect("empty report");
    let report_text = format!("{report:?}");
    drop(peer);
    let (accepted, greeted) = result?;
    greeted?;
    let accepted = accepted?;
    let acceptance_text = format!("{accepted:?}");
    let refused_text = match accepted {
        ScopedConnectionAcceptance::Refused(refused) => {
            let text = format!("{refused:?}");
            drop(refused);
            text
        }
        ScopedConnectionAcceptance::Accepted(connection) => {
            drop(connection);
            return Err(io::Error::other("sealed root accepted a connection").into());
        }
    };
    let (joins, _anchor) = report.into_parts();
    let joins_text = format!("{joins:?}");
    assert_eq!(owner_text, "ServerConnectionOwner { .. }");
    assert_eq!(acceptor_text, "ServerConnectionAcceptor { .. }");
    assert_eq!(report_text, "ServerConnectionJoinReport { .. }");
    assert_eq!(joins_text, "ServerConnectionTaskJoins { .. }");
    assert_eq!(refused_text, "RefusedServerConnection { .. }");
    assert_eq!(acceptance_text, "ScopedConnectionAcceptance::Refused(..)");
    assert_eq!(witness.dropped.load(Ordering::SeqCst), 1);
    Ok(())
}

struct ParentPayload(Arc<std::sync::atomic::AtomicUsize>);
impl Drop for ParentPayload {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("controlled parent payload disposal after both actual joins");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_accepting_parent_panic_payload_waits_for_both_socket_joins() -> TestResult {
    let (io, mut peer, witness) = fixture::transport();
    let (mut owner, acceptor) = ServerConnectionOwner::new(Handle::current(), ());
    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let payload = ParentPayload(drops.clone());
    let release = Arc::new(tokio::sync::Notify::new());
    let parent_release = release.clone();
    let mut parent = tokio::spawn(async move {
        let accepted = acceptor
            .accept_with_options(io, "server", None, ConnectionOptions::default())
            .await;
        match accepted {
            Ok(ScopedConnectionAcceptance::Accepted(connection)) => {
                let _connection = connection;
                parent_release.notified().await;
                std::panic::panic_any(payload);
            }
            other => (other, payload),
        }
    });
    let greeted = tokio::time::timeout(Duration::from_secs(2), fixture::hello(&mut peer)).await;
    let reader_seen = fixture::observe_reader(&owner).await;
    release.notify_one();
    let observed = tokio::time::timeout(Duration::from_secs(2), &mut parent).await;
    let mut parent_result = Some(match observed {
        Ok(result) => result,
        Err(_) => {
            parent.abort();
            parent.await
        }
    });
    let before = drops.load(Ordering::SeqCst);
    let report = owner.finish().await.expect("actual report");
    drop(peer);
    let before_disposal = drops.load(Ordering::SeqCst);
    let original = parent_result
        .as_ref()
        .is_some_and(|result| result.as_ref().err().is_some_and(JoinError::is_panic));
    let disposal = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drop(parent_result.take());
    }));
    greeted??;
    reader_seen?;
    assert!(original && disposal.is_err());
    assert_eq!(
        (before, before_disposal, drops.load(Ordering::SeqCst)),
        (0, 0, 1)
    );
    assert!(report.actor().is_some_and(|result| result.is_ok()));
    assert!(report.reader().is_some());
    assert_eq!(witness.dropped.load(Ordering::SeqCst), 1);
    Ok(())
}

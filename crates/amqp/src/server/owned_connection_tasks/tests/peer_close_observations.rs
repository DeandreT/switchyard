use super::observation_fixture as obs;
use super::*;
use crate::server::peer_close_observation::{PeerCloseCell, ReplyLoan, observe_reply};
use crate::{
    AmqpError, Close, Error, Fields, Frame, Performative, ServerPeerCloseReplyState as ReplyState,
    Value,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn received_peer_close_precedes_pending_reply_poll() -> TestResult {
    let anchor = std::rc::Rc::new(());
    let mut owner = PairOwner::new_for_test(Handle::current(), anchor.clone());
    let (connection, mut peer, control) = obs::open(&mut owner).await?;
    control.arm(1);
    let observed: Result<obs::TestResult<_>, _> = obs::caught(async {
        obs::send_close(&mut peer, Close::default(), 0, Vec::new()).await?;
        obs::received(&owner).await?;
        control.entered().await?;
        Ok(owner.peer_close.received().map(|row| {
            (
                row.reply_state(),
                row.reply_result().is_none(),
                row.channel(),
                row.payload().is_empty(),
                row.connection_identity()
                    .same_connection(&connection.lifecycle.identity),
            )
        }))
    })
    .await;
    control.release();
    let report = owner.finish().await.expect("original report");
    drop(connection);
    drop(peer);
    let before = obs::resume(observed)?;
    assert_eq!(before, Some((ReplyState::Pending, true, 0, true, true)));
    assert!(std::rc::Rc::ptr_eq(report.anchor(), &anchor));
    assert_eq!(std::rc::Rc::strong_count(&anchor), 2);
    assert!(report.observations().peer_close().is_some());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn error_free_peer_close_keeps_original_late_stopped() -> TestResult {
    let mut owner = PairOwner::new_for_test(Handle::current(), ());
    let (connection, mut peer, _) = obs::open(&mut owner).await?;
    let observed: Result<obs::TestResult<_>, _> = obs::caught(async {
        obs::send_close(&mut peer, Close::default(), 0, Vec::new()).await?;
        obs::read_close(&mut peer).await?;
        fixture::observe_actor_ready(&owner).await?;
        Ok(connection.close().await)
    })
    .await;
    let report = owner.finish().await.expect("original report");
    drop(connection);
    drop(peer);
    let late = obs::resume(observed)?;
    assert!(matches!(late, Err(EngineError::Stopped)));
    let row = report.observations().peer_close().expect("original Close");
    assert_eq!(row.reply_state(), ReplyState::Ready);
    assert!(row.reply_result().is_some_and(Result::is_ok));
    assert!(row.close().error.is_none() && !row.locally_closing());
    assert!(report.actor().is_some_and(Result::is_ok));
    assert!(report.reader().is_some());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_close_error_remains_original_in_receipt() -> TestResult {
    let mut owner = PairOwner::new_for_test(Handle::current(), ());
    let (connection, mut peer, _) = obs::open(&mut owner).await?;
    let observed: Result<TestResult, _> = obs::caught(async {
        let mut info = Fields::new();
        info.insert("private-key".into(), Value::Binary(vec![1, 2, 3].into()));
        let close = Close {
            error: Some(Error {
                condition: AmqpError::InternalError.into(),
                description: Some("private-description".into()),
                info: Some(info),
            }),
        };
        obs::send_close(&mut peer, close, 0, Vec::new()).await?;
        obs::read_close(&mut peer).await?;
        fixture::observe_actor_ready(&owner).await?;
        Ok(())
    })
    .await;
    let report = owner.finish().await.expect("original report");
    drop(connection);
    drop(peer);
    obs::resume(observed)?;
    let row = report.observations().peer_close().expect("original Close");
    let error = row.close().error.as_ref().expect("original peer error");
    assert_eq!(error.condition, AmqpError::InternalError.into());
    assert_eq!(error.description.as_deref(), Some("private-description"));
    assert_eq!(
        error
            .info
            .as_ref()
            .and_then(|info| info.get(&crate::Symbol::from("private-key"))),
        Some(&Value::Binary(vec![1, 2, 3].into()))
    );
    let rendered = format!("{row:?} {:?}", report.observations());
    assert!(!rendered.contains("private-description") && !rendered.contains("private-key"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn locally_closing_peer_close_records_not_required() -> TestResult {
    let mut owner = PairOwner::new_for_test(Handle::current(), ());
    let (connection, mut peer, _) = obs::open(&mut owner).await?;
    let observed = obs::caught(tokio::time::timeout(obs::DEADLINE, async {
        tokio::join!(connection.close(), async {
            obs::read_close(&mut peer).await?;
            obs::send_close(&mut peer, Close::default(), 0, Vec::new()).await
        })
    }))
    .await;
    let report = owner.finish().await.expect("original report");
    drop(connection);
    drop(peer);
    let (local, peer) = obs::resume(observed)?;
    local?;
    peer?;
    let row = report
        .observations()
        .peer_close()
        .expect("original acknowledgment");
    assert!(row.locally_closing());
    assert_eq!(row.reply_state(), ReplyState::NotRequired);
    assert!(row.reply_result().is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eof_and_protocol_failure_have_no_peer_close_receipt() -> TestResult {
    for duplicate_open in [false, true] {
        let mut owner = PairOwner::new_for_test(Handle::current(), ());
        let (connection, mut peer, _) = obs::open(&mut owner).await?;
        let written: Result<TestResult, _> = obs::caught(async {
            if duplicate_open {
                tokio::time::timeout(
                    obs::DEADLINE,
                    crate::write_frame(
                        &mut peer,
                        &Frame::Amqp {
                            channel: 0,
                            performative: Some(Performative::Open(crate::Open::new("duplicate"))),
                            payload: Vec::new(),
                        },
                    ),
                )
                .await??;
            }
            Ok(())
        })
        .await;
        drop(peer);
        let observed = obs::caught(fixture::observe_actor_ready(&owner)).await;
        let report = owner.finish().await.expect("original report");
        drop(connection);
        obs::resume(written)?;
        obs::resume(observed)?;
        assert!(report.observations().peer_close().is_none());
        assert!(report.actor().is_some() && report.reader().is_some());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reply_write_error_keeps_original_private_cause() -> TestResult {
    let mut owner = PairOwner::new_for_test(Handle::current(), ());
    let (connection, mut peer, control) = obs::open(&mut owner).await?;
    control.arm(2);
    let observed: Result<TestResult, _> = obs::caught(async {
        obs::send_close(&mut peer, Close::default(), 0, Vec::new()).await?;
        fixture::observe_actor_ready(&owner).await?;
        Ok(())
    })
    .await;
    control.release();
    let report = owner.finish().await.expect("original report");
    drop(connection);
    drop(peer);
    obs::resume(observed)?;
    let row = report.observations().peer_close().expect("original Close");
    assert_eq!(row.reply_state(), ReplyState::Ready);
    assert!(obs::same_write_cause(row, &control.marker));
    assert!(report.actor().is_some_and(Result::is_ok));
    assert!(format!("{report:?} {row:?}").contains("ServerPeerCloseObservation"));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reply_write_unwind_keeps_received_only_and_actor_panic() -> TestResult {
    let mut owner = PairOwner::new_for_test(Handle::current(), ());
    let (connection, mut peer, control) = obs::open(&mut owner).await?;
    control.arm(3);
    let observed: Result<TestResult, _> = obs::caught(async {
        obs::send_close(&mut peer, Close::default(), 0, Vec::new()).await?;
        fixture::observe_actor_ready(&owner).await?;
        Ok(())
    })
    .await;
    control.release();
    let report = owner.finish().await.expect("original report");
    drop(connection);
    drop(peer);
    obs::resume(observed)?;
    let row = report
        .observations()
        .peer_close()
        .expect("received before write panic");
    assert_eq!(row.reply_state(), ReplyState::AbandonedBeforeReady);
    assert!(row.reply_result().is_none());
    let (parts, _) = report.into_parts();
    let actor = parts
        .actor
        .expect("original Actor")
        .expect_err("original panic");
    assert!(actor.is_panic());
    let payload = actor.into_panic();
    assert!(
        payload
            .downcast_ref::<obs::WritePanic>()
            .is_some_and(|panic| Arc::ptr_eq(&panic.0, &control.marker))
    );
    assert!(parts.reader.is_some());
    assert!(parts.observations.peer_close().is_some());
    Ok(())
}

struct ReadyThenDrop {
    marker: Arc<()>,
}
impl Future for ReadyThenDrop {
    type Output = io::Result<()>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Err(io::Error::new(
            io::ErrorKind::BrokenPipe,
            obs::PrivateWriteCause(self.marker.clone()),
        )))
    }
}
impl Drop for ReadyThenDrop {
    fn drop(&mut self) {
        std::panic::panic_any(obs::WritePanic(self.marker.clone()));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reply_ready_precedes_completed_future_drop() -> TestResult {
    let mut owner = PairOwner::new_for_test(Handle::current(), ());
    let (connection, peer, control) = obs::open(&mut owner).await?;
    let cell = PeerCloseCell::new();
    let result = obs::caught(async {
        let row = cell.receive(
            &connection.lifecycle.identity,
            Close::default(),
            0,
            Vec::new(),
            false,
        );
        observe_reply(
            ReplyLoan::new(row),
            ReadyThenDrop {
                marker: control.marker.clone(),
            },
        )
        .await
    })
    .await;
    let report = owner.finish().await.expect("original roles joined");
    drop(connection);
    drop(peer);
    let row = cell.received().expect("original Ready-before-Drop receipt");
    let payload = result.expect_err("completed original future Drop panic");
    assert!(
        payload
            .downcast_ref::<obs::WritePanic>()
            .is_some_and(|panic| Arc::ptr_eq(&panic.0, &control.marker))
    );
    assert_eq!(row.reply_state(), ReplyState::Ready);
    assert!(obs::same_write_cause(row, &control.marker));
    assert!(report.observations().peer_close().is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_close_receipts_keep_exact_native_connection() -> TestResult {
    let mut first = PairOwner::new_for_test(Handle::current(), ());
    let mut second = PairOwner::new_for_test(Handle::current(), ());
    let (one, mut peer_one, _) = obs::open(&mut first).await?;
    let observed: Result<obs::TestResult<_>, _> = obs::caught(async {
        let (two, mut peer_two, _) = obs::open(&mut second).await?;
        obs::send_close(&mut peer_one, Close::default(), 0, Vec::new()).await?;
        obs::send_close(&mut peer_two, Close::default(), 0, Vec::new()).await?;
        obs::read_close(&mut peer_one).await?;
        obs::read_close(&mut peer_two).await?;
        Ok((two, peer_two))
    })
    .await;
    let first_report = first.finish().await.expect("first original report");
    let second_report = second.finish().await;
    let (two, peer_two) = obs::resume(observed)?;
    let second_report = second_report.expect("second original report");
    let a = first_report
        .observations()
        .peer_close()
        .expect("first receipt")
        .connection_identity();
    let b = second_report
        .observations()
        .peer_close()
        .expect("second receipt")
        .connection_identity();
    assert!(
        a.same_connection(&one.lifecycle.identity) && b.same_connection(&two.lifecycle.identity)
    );
    assert!(!a.same_connection(b));
    drop((one, two, peer_one, peer_two));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrong_channel_and_payload_close_preserve_original_framing() -> TestResult {
    for (channel, payload) in [(1, Vec::new()), (0, vec![9, 8, 7])] {
        let mut owner = PairOwner::new_for_test(Handle::current(), ());
        let (connection, mut peer, _) = obs::open(&mut owner).await?;
        let observed: Result<TestResult, _> = obs::caught(async {
            obs::send_close(&mut peer, Close::default(), channel, payload.clone()).await?;
            obs::read_close(&mut peer).await?;
            Ok(())
        })
        .await;
        let report = owner.finish().await.expect("original report");
        drop(connection);
        drop(peer);
        obs::resume(observed)?;
        let row = report
            .observations()
            .peer_close()
            .expect("original malformed framing");
        assert_eq!(row.channel(), channel);
        assert_eq!(row.payload(), payload);
        assert!(row.channel() != 0 || !row.payload().is_empty());
        assert!(row.reply_result().is_some_and(Result::is_ok));
    }
    Ok(())
}

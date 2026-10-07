use super::*;
use amqp::{
    Close, Frame, Open, Performative, ProtocolHeader, ServerConnectionAbortSource,
    ServerPeerCloseReplyState, read_frame, read_protocol_header, write_frame,
    write_protocol_header,
};
use domain::{
    CommandKind, CommandOutcome, EntityPath, NamespaceName, RuleDefinition, SubscriptionName,
};
use std::{
    any::Any,
    future::{Future, poll_fn},
    pin::pin,
    task::Poll,
};
use tokio::{net::TcpListener, time::timeout};

use crate::{Attachment, Broker, BrokerRejection, EntityMetadata};

const DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);
type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone)]
struct NoBroker;

impl Broker for NoBroker {
    crate::broker::fixture_binding_methods!();
    async fn submit(
        &self,
        _: NamespaceName,
        _: EntityPath,
        _: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        panic!("no-session observation fixture must not submit");
    }
    async fn rules(
        &self,
        _: NamespaceName,
        _: EntityPath,
        _: SubscriptionName,
    ) -> Result<Vec<RuleDefinition>, BrokerRejection> {
        panic!("no-session observation fixture must not read rules");
    }
    async fn entity_metadata(
        &self,
        _: NamespaceName,
        _: Attachment,
    ) -> Result<Option<EntityMetadata>, BrokerRejection> {
        panic!("no-session observation fixture must not bind");
    }
    async fn deliverable(&self, _: &NamespaceName, _: &EntityPath) {
        panic!("no-session observation fixture must not await delivery");
    }
}

struct SetupFailure(RetainedConnectionStartError<NoBroker>);
impl fmt::Debug for SetupFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SetupFailure(..)")
    }
}
impl fmt::Display for SetupFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let _original = &self.0;
        formatter.write_str("original retained setup failure")
    }
}
impl std::error::Error for SetupFailure {}

async fn socket<A>(
    anchor: A,
) -> TestResult<(
    RetainedConnectionOwner<A>,
    TcpStream,
    Result<(), RetainedConnectionStartError<NoBroker>>,
)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let peer = timeout(DEADLINE, TcpStream::connect(listener.local_addr()?)).await??;
    peer.set_nodelay(true)?;
    let (stream, _) = timeout(DEADLINE, listener.accept()).await??;
    let (mut owner, starter) = RetainedConnectionOwner::new(Handle::current(), anchor);
    let started = caught(async {
        AmqpListener::new(
            NoBroker,
            NamespaceName::new("observation").expect("namespace"),
        )
        .with_idle_timeout_millis(0)
        .start_retained_connection(stream, starter)
    })
    .await;
    match started {
        Ok(started) => Ok((owner, peer, started)),
        Err(payload) => {
            let report = owner.finish().await;
            let _original_report = &report;
            std::panic::resume_unwind(payload)
        }
    }
}

async fn peer_close<A>(
    owner: &RetainedConnectionOwner<A>,
    peer: &mut TcpStream,
    started: Result<(), RetainedConnectionStartError<NoBroker>>,
) -> TestResult {
    started.map_err(|error| {
        Box::new(SetupFailure(error)) as Box<dyn std::error::Error + Send + Sync>
    })?;
    timeout(DEADLINE, write_protocol_header(peer, ProtocolHeader::AMQP)).await??;
    if timeout(DEADLINE, read_protocol_header(peer)).await?? != ProtocolHeader::AMQP {
        return Err(io::Error::other("unexpected original header").into());
    }
    timeout(
        DEADLINE,
        write_frame(
            peer,
            &Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Open(Open::new("observation-peer"))),
                payload: Vec::new(),
            },
        ),
    )
    .await??;
    if !matches!(
        timeout(DEADLINE, read_frame(peer)).await??,
        Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Open(_)),
            ..
        }
    ) {
        return Err(io::Error::other("unexpected original Open").into());
    }
    timeout(
        DEADLINE,
        write_frame(
            peer,
            &Frame::Amqp {
                channel: 0,
                performative: Some(Performative::Close(Close::default())),
                payload: Vec::new(),
            },
        ),
    )
    .await??;
    if !matches!(timeout(DEADLINE, read_frame(peer)).await??, Frame::Amqp { channel: 0, performative: Some(Performative::Close(_)), payload } if payload.is_empty())
    {
        return Err(io::Error::other("unexpected original Close reply").into());
    }
    timeout(DEADLINE, owner.wrapper.join()).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_report_forwards_original_peer_close_and_write_result() -> TestResult {
    let anchor = std::rc::Rc::new(());
    let (mut owner, mut peer, started) = socket(anchor.clone()).await?;
    let business = caught(peer_close(&owner, &mut peer, started)).await;
    let report = owner.finish().await.expect("original retained report");
    drop(peer);
    resume(business)?;
    let native = report.native_observations();
    let row = native.peer_close().expect("same original peer Close");
    assert_eq!(row.channel(), 0);
    assert!(row.payload().is_empty() && row.close().error.is_none() && !row.locally_closing());
    assert_eq!(row.reply_state(), ServerPeerCloseReplyState::Ready);
    assert!(row.reply_result().is_some_and(Result::is_ok));
    assert!(!row.connection_identity().is_active());
    assert!(report.wrapper().is_some_and(Result::is_ok));
    assert!(report.actor().is_some_and(Result::is_ok));
    assert!(report.reader().is_some());
    assert!(std::rc::Rc::ptr_eq(report.anchor(), &anchor));
    assert_eq!(std::rc::Rc::strong_count(&anchor), 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_into_parts_keeps_original_native_abort_receipts() -> TestResult {
    let anchor = std::rc::Rc::new(());
    let (mut owner, mut peer, started) = socket(anchor.clone()).await?;
    let business = caught(peer_close(&owner, &mut peer, started)).await;
    let report = owner.finish().await.expect("original retained report");
    drop(peer);
    resume(business)?;
    let before = report.native_observations();
    let actor_id = before.actor().expect("original Actor ID").id();
    let reader_id = before.reader().expect("original Reader ID").id();
    let receipt_address = before.peer_close().expect("original receipt") as *const _;
    let cached = owner.finish().await;
    let (parts, outcomes, retained_anchor) = report.into_parts();
    let native = &parts.native_observations;
    assert!(cached.is_none());
    assert_eq!(native.actor().expect("Actor ID").id(), actor_id);
    assert_eq!(native.reader().expect("Reader ID").id(), reader_id);
    assert_eq!(
        native.peer_close().expect("same receipt") as *const _,
        receipt_address
    );
    assert!(
        native
            .reader()
            .expect("Reader")
            .requested_by(ServerConnectionAbortSource::ActorReaderShutdown)
    );
    assert!(
        !native
            .reader()
            .expect("Reader")
            .requested_by(ServerConnectionAbortSource::OwnerFinish)
    );
    assert!(!native.actor().expect("Actor").abort_requested());
    assert!(parts.wrapper.is_some() && parts.actor.is_some() && parts.reader.is_some());
    assert!(outcomes.primary.is_some());
    assert!(std::rc::Rc::ptr_eq(&retained_anchor, &anchor));
    Ok(())
}

async fn caught<F: Future>(future: F) -> Result<F::Output, Box<dyn Any + Send>> {
    let mut future = pin!(future);
    poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Err(payload) => Poll::Ready(Err(payload)),
        }
    })
    .await
}

fn resume<T>(observed: Result<T, Box<dyn Any + Send>>) -> T {
    match observed {
        Ok(value) => value,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrapper_join_cancellation_restores_token_without_sealing_engine_launch() -> TestResult {
    let anchor = std::rc::Rc::new(());
    let (mut owner, mut peer, started) = socket(anchor.clone()).await?;
    let controls = owner.controls();
    let original_id = owner.wrapper.abort_handle().expect("original Wrapper").id();
    let business = caught(async {
        {
            let mut waiting = pin!(owner.join_wrapper());
            poll_fn(|context| match waiting.as_mut().poll(context) {
                Poll::Pending => Poll::Ready(()),
                Poll::Ready(()) => panic!("original negotiation must still be pending"),
            })
            .await;
        }
        assert_eq!(
            owner.wrapper.abort_handle().expect("restored Wrapper").id(),
            original_id,
        );
        assert!(!controls.snapshot().opened);
        assert_eq!(owner.published(), (false, false));
        peer_close(&owner, &mut peer, started).await?;
        timeout(DEADLINE, owner.join_wrapper()).await?;
        assert!(controls.snapshot().opened);
        assert_eq!(owner.published(), (true, false));
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await;
    drop(peer);
    let report = timeout(DEADLINE, owner.finish())
        .await?
        .expect("original retained report");
    resume(business)?;
    assert!(report.wrapper().is_some_and(Result::is_ok));
    assert!(report.actor().is_some_and(Result::is_ok));
    let native = report.native_observations();
    assert!(native.actor().is_some() && native.reader().is_some());
    assert!(!native.actor().expect("original Actor").abort_requested());
    let close = native
        .peer_close()
        .expect("engine accepted the original peer Close");
    assert!(close.close().error.is_none() && !close.locally_closing());
    assert_eq!(close.reply_state(), ServerPeerCloseReplyState::Ready);
    assert!(close.reply_result().is_some_and(Result::is_ok));
    assert!(matches!(
        &report.outcomes().primary,
        Some(RetainedConnectionOutcome::Finished(Ok(())))
    ));
    assert!(std::rc::Rc::ptr_eq(report.anchor(), &anchor));
    assert!(timeout(DEADLINE, owner.finish()).await?.is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrapper_join_keeps_original_negotiation_error_until_finish() -> TestResult {
    let (mut owner, peer, started) = socket(()).await?;
    drop(peer);
    let business = caught(async {
        started.map_err(|error| {
            Box::new(SetupFailure(error)) as Box<dyn std::error::Error + Send + Sync>
        })?;
        timeout(DEADLINE, owner.join_wrapper()).await?;
        assert_eq!(owner.published(), (true, false));
        timeout(DEADLINE, owner.join_wrapper()).await?;
        assert_eq!(owner.published(), (true, false));
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await;
    let report = timeout(DEADLINE, owner.finish())
        .await?
        .expect("original retained report");
    resume(business)?;
    assert!(report.wrapper().is_some_and(Result::is_ok));
    assert!(report.actor().is_none() && report.reader().is_none());
    let Some(RetainedConnectionOutcome::Finished(Err(error))) = &report.outcomes().primary else {
        panic!("original negotiation error must remain raw");
    };
    assert!(error.downcast_ref::<amqp::EngineError>().is_some());
    assert!(report.outcomes().websocket_close.is_none());
    assert!(timeout(DEADLINE, owner.finish()).await?.is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrapper_join_keeps_original_task_panic_and_payload_until_finish() -> TestResult {
    use super::controls::Site;

    struct OriginalPanic(u64);
    let payload = Box::new(OriginalPanic(0x5177_7261_7070_6572));
    let address = &*payload as *const OriginalPanic;
    let (mut owner, mut peer, started) = socket(()).await?;
    let original_id = owner.wrapper.abort_handle().expect("original Wrapper").id();
    owner.controls().panic_at(Site::PrimaryReady, payload);
    let business = caught(async {
        peer_close(&owner, &mut peer, started).await?;
        timeout(DEADLINE, owner.join_wrapper()).await?;
        assert_eq!(owner.published(), (true, false));
        timeout(DEADLINE, owner.join_wrapper()).await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await;
    drop(peer);
    let report = timeout(DEADLINE, owner.finish())
        .await?
        .expect("original retained report");
    resume(business)?;
    assert!(matches!(
        &report.outcomes().primary,
        Some(RetainedConnectionOutcome::Finished(Ok(())))
    ));
    let (parts, outcomes, ()) = report.into_parts();
    let error = parts
        .wrapper
        .expect("original Wrapper result")
        .expect_err("original Wrapper panic");
    assert_eq!(error.id(), original_id);
    assert!(error.is_panic());
    let payload = error.into_panic();
    let payload = payload
        .downcast::<Box<dyn Any + Send>>()
        .expect("original boxed dynamic panic payload");
    let payload = payload
        .downcast::<OriginalPanic>()
        .expect("original panic payload");
    assert_eq!(&*payload as *const OriginalPanic, address);
    assert_eq!(payload.0, 0x5177_7261_7070_6572);
    assert!(outcomes.primary.is_some());
    assert!(timeout(DEADLINE, owner.finish()).await?.is_none());
    Ok(())
}

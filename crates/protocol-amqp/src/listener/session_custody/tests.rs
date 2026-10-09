//! Qualified task/result custody primitives, not actual six-role admission.
//! Explicitly caught task faults do not recover a leaf's panicked originals.

use super::*;
use std::{
    error::Error,
    fmt,
    future::{Future, poll_fn},
    panic::{AssertUnwindSafe, panic_any},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{sync::oneshot, time::timeout};

const WAIT: Duration = Duration::from_secs(5);

#[derive(Debug)]
struct RawError(Arc<str>);

impl fmt::Display for RawError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for RawError {}

fn failure(value: &str) -> (LeafResult, Arc<str>) {
    let identity: Arc<str> = Arc::from(value);
    (Err(Box::new(RawError(Arc::clone(&identity)))), identity)
}

struct PollWitness<F> {
    original: Pin<Box<F>>,
    polls: Arc<AtomicUsize>,
}

impl<F: Future> Future for PollWitness<F> {
    type Output = F::Output;
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        self.original.as_mut().poll(context)
    }
}

fn witness<F: Future>(future: F, polls: &Arc<AtomicUsize>) -> PollWitness<F> {
    PollWitness {
        original: Box::pin(future),
        polls: Arc::clone(polls),
    }
}

async fn completed(task: &AdmittedTask) {
    timeout(WAIT, async {
        while !task.task.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("original Tokio task positively completed");
}

async fn pending_once<F: Future + ?Sized>(mut future: Pin<&mut F>) {
    let polled = tokio::task::unconstrained(poll_fn(|context| {
        Poll::Ready(future.as_mut().poll(context))
    }))
    .await;
    assert!(
        matches!(polled, Poll::Pending),
        "positive held original remains pending"
    );
}

fn exact_error(error: &(dyn Error + Send + Sync + 'static), expected: &Arc<str>) {
    assert!(Arc::ptr_eq(
        &error.downcast_ref::<RawError>().unwrap().0,
        expected
    ));
}

fn exact_panic<T>(result: std::thread::Result<T>, expected: &Arc<str>) {
    let payload = match result {
        Err(payload) => payload,
        Ok(_) => panic!("raw primary panic resumes"),
    };
    assert!(Arc::ptr_eq(
        payload.downcast_ref::<Arc<str>>().unwrap(),
        expected
    ));
}

const KINDS: [LeafKind; 6] = [
    LeafKind::DataSend,
    LeafKind::DataReceive,
    LeafKind::CbsRequest,
    LeafKind::CbsReply,
    LeafKind::ManagementRequest,
    LeafKind::ManagementReply,
];

// Six rows: actual Tokio originals, synthetic leaf results; not native admission.
#[tokio::test(flavor = "current_thread")]
async fn qualified_family_keeps_boxed_packets_across_twice_cancelled_finish() {
    for kind in KINDS {
        let mut family = SessionFamily::new();
        let polls = Arc::new(AtomicUsize::new(0));
        let (raw, identity) = failure("completed raw leaf");
        let first = PreparedLeafTask::new(kind).spawn(witness(std::future::ready(raw), &polls));
        completed(&first).await;
        let first_id = first.task.id();
        assert_eq!(family.adopt(first), first_id);
        let (release, held) = oneshot::channel::<LeafResult>();
        let runs = Arc::new(AtomicUsize::new(0));
        let entered = Arc::clone(&runs);
        let second = PreparedLeafTask::new(kind).spawn(async move {
            entered.fetch_add(1, Ordering::SeqCst);
            held.await.unwrap()
        });
        let second_id = second.task.id();
        assert_eq!(family.adopt(second), second_id);
        let mut address = None;
        for _ in 0..2 {
            let mut finish = Box::pin(family.finish());
            pending_once(finish.as_mut()).await;
            drop(finish);
            assert_eq!(family.len(), 1);
            assert_eq!(family.finished().len(), 1);
            let packet = &family.finished()[0];
            assert_eq!((packet.id, packet.kind), (first_id, kind));
            assert!(packet.joined.is_ok() && packet.bridge_fault.is_none());
            exact_error(
                packet.leaf.as_ref().unwrap().as_ref().unwrap_err().as_ref(),
                &identity,
            );
            let now = (&**packet as *const JoinedLeafTask) as usize;
            assert_eq!(*address.get_or_insert(now), now);
            assert_eq!(polls.load(Ordering::SeqCst), 1);
        }
        release.send(Ok(())).unwrap();
        timeout(WAIT, family.finish()).await.unwrap();
        assert_eq!(family.finished().len(), 2);
        assert_eq!(
            (&*family.finished()[0] as *const JoinedLeafTask) as usize,
            address.unwrap()
        );
        assert_eq!(family.finished()[1].id, second_id);
        assert!(family.finished()[1].leaf.as_ref().unwrap().is_ok());
        timeout(WAIT, family.finish()).await.unwrap();
        assert_eq!(family.finished().len(), 2);
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        exact_error(family.into_result().unwrap_err().as_ref(), &identity);
    }
}

// Two rows: cancel next twice, then adopt a receipt spawned before retirement.
#[tokio::test(flavor = "current_thread")]
async fn qualified_family_cancelled_next_and_late_receipt_keep_same_originals() {
    for kind in [LeafKind::DataSend, LeafKind::ManagementReply] {
        let mut family = SessionFamily::new();
        let (release, held) = oneshot::channel::<LeafResult>();
        let pending = PreparedLeafTask::new(kind).spawn(async move { held.await.unwrap() });
        let pending_id = pending.task.id();
        family.adopt(pending);
        for _ in 0..2 {
            let mut next = Box::pin(family.next());
            pending_once(next.as_mut()).await;
        }
        assert_eq!(family.len(), 1);
        let (raw, identity) = failure("late captured original");
        let late = PreparedLeafTask::new(kind).spawn(std::future::ready(raw));
        completed(&late).await;
        let late_id = late.task.id();
        family.retire();
        assert!(!family.next().await);
        assert_eq!(family.adopt(late), late_id);
        for _ in 0..2 {
            let mut finish = Box::pin(family.finish());
            pending_once(finish.as_mut()).await;
        }
        assert_eq!(family.finished().len(), 1);
        assert_eq!(family.finished()[0].id, late_id);
        let address = (&*family.finished()[0] as *const JoinedLeafTask) as usize;
        release.send(Ok(())).unwrap();
        timeout(WAIT, family.finish()).await.unwrap();
        assert_eq!(family.finished()[1].id, pending_id);
        assert_eq!(
            (&*family.finished()[0] as *const JoinedLeafTask) as usize,
            address
        );
        exact_error(family.into_result().unwrap_err().as_ref(), &identity);
    }
}

fn composed(kind: LeafKind, raw: Option<LeafResult>, panic: Option<Arc<str>>) -> AdmittedTask {
    // Qualified composition: the production bridge has no callback after send.
    let (sender, result) = oneshot::channel();
    let task = tokio::spawn(async move {
        if let Some(raw) = raw {
            let _ = sender.send(raw);
        }
        if let Some(payload) = panic {
            panic_any(payload);
        }
    });
    AdmittedTask { task, kind, result }
}

// Three rows: missing bridge, panic alone, and raw result plus later task panic.
#[tokio::test(flavor = "current_thread")]
async fn qualified_family_distinguishes_join_failure_raw_leaf_and_bridge_fault() {
    for case in 0..3 {
        let mut family = SessionFamily::new();
        let panic: Arc<str> = Arc::from("raw original task panic");
        let (raw, identity) = failure("raw result precedes synthetic task panic");
        let receipt = composed(
            LeafKind::CbsReply,
            (case == 2).then_some(raw),
            (case != 0).then(|| Arc::clone(&panic)),
        );
        completed(&receipt).await;
        let id = receipt.task.id();
        family.adopt(receipt);
        timeout(WAIT, family.finish()).await.unwrap();
        let packet = &family.finished()[0];
        assert_eq!(packet.id, id);
        if case == 0 {
            assert!(packet.joined.is_ok() && packet.leaf.is_none());
            assert_eq!(packet.bridge_fault.as_ref().unwrap().id, id);
            assert_eq!(
                family
                    .into_result()
                    .unwrap_err()
                    .downcast::<BridgeFault>()
                    .unwrap()
                    .id,
                id
            );
        } else {
            assert!(packet.joined.as_ref().unwrap_err().is_panic());
            assert!(packet.bridge_fault.is_none());
            if case == 2 {
                exact_error(
                    packet.leaf.as_ref().unwrap().as_ref().unwrap_err().as_ref(),
                    &identity,
                );
            } else {
                assert!(packet.leaf.is_none());
            }
            let error = *family
                .into_result()
                .unwrap_err()
                .downcast::<tokio::task::JoinError>()
                .unwrap();
            assert_eq!(error.id(), id);
            assert!(Arc::ptr_eq(
                error.into_panic().downcast_ref::<Arc<str>>().unwrap(),
                &panic
            ));
        }
    }
}

// Forty live completions plus two receiver-discard companions; no cap is claimed.
#[tokio::test(flavor = "current_thread")]
async fn qualified_family_live_reaping_discards_success_history_not_first_raw_error() {
    let mut family = SessionFamily::new();
    let (raw, identity) = failure("first reaped raw error");
    let receipt = PreparedLeafTask::new(LeafKind::DataSend).spawn(std::future::ready(raw));
    completed(&receipt).await;
    family.adopt(receipt);
    assert!(family.next().await);
    assert!(family.is_empty() && family.finished().is_empty());
    for offset in 0..40 {
        let result = if offset == 20 {
            failure("later raw error").0
        } else {
            Ok(())
        };
        let receipt = PreparedLeafTask::new(LeafKind::DataSend).spawn(std::future::ready(result));
        completed(&receipt).await;
        family.adopt(receipt);
        assert!(family.next().await);
        assert!(family.is_empty() && family.finished().is_empty());
    }
    timeout(WAIT, family.finish()).await.unwrap();
    exact_error(family.into_result().unwrap_err().as_ref(), &identity);

    // Explicitly discard the result before the original's send, for Ok and Err.
    for error in [false, true] {
        let (release, held) = oneshot::channel::<()>();
        let raw = if error {
            failure("discarded raw result").0
        } else {
            Ok(())
        };
        let receipt = PreparedLeafTask::new(LeafKind::DataReceive).spawn(async move {
            held.await.unwrap();
            raw
        });
        let AdmittedTask { task, result, .. } = receipt;
        let id = task.id();
        drop(result);
        assert_eq!(task.id(), id);
        release.send(()).unwrap();
        timeout(WAIT, task).await.unwrap().unwrap();
    }
}

struct ReportFault {
    reached: Arc<AtomicUsize>,
    payload: Arc<str>,
}
impl tracing::Subscriber for ReportFault {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target().ends_with("::listener::session_custody")
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, _: &tracing::Event<'_>) {
        self.reached.fetch_add(1, Ordering::SeqCst);
        panic_any(Arc::clone(&self.payload));
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

// Two rows: a reached report panic never outranks the raw leaf error/join error.
#[tokio::test(flavor = "current_thread")]
async fn qualified_family_reporting_fault_preserves_cached_raw_failure_priority() {
    use tracing::instrument::WithSubscriber;
    for joined_panic in [false, true] {
        let mut family = SessionFamily::new();
        let (raw, identity) = failure("raw leaf before report panic");
        let task_panic: Arc<str> = Arc::from("raw task before report panic");
        let receipt = composed(
            LeafKind::ManagementRequest,
            Some(raw),
            joined_panic.then(|| Arc::clone(&task_panic)),
        );
        completed(&receipt).await;
        family.adopt(receipt);
        let other = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
        let reached = Arc::new(AtomicUsize::new(0));
        let payload: Arc<str> = Arc::from("secondary family report panic");
        let dispatch = tracing::Dispatch::new(ReportFault {
            reached: Arc::clone(&reached),
            payload,
        });
        std::thread::spawn(tracing::callsite::rebuild_interest_cache)
            .join()
            .unwrap();
        timeout(WAIT, family.finish().with_subscriber(dispatch))
            .await
            .unwrap();
        assert_eq!(reached.load(Ordering::SeqCst), 1);
        assert_eq!(family.finished().len(), 1);
        if joined_panic {
            let error = *family
                .into_result()
                .unwrap_err()
                .downcast::<tokio::task::JoinError>()
                .unwrap();
            assert!(Arc::ptr_eq(
                error.into_panic().downcast_ref::<Arc<str>>().unwrap(),
                &task_panic
            ));
        } else {
            exact_error(family.into_result().unwrap_err().as_ref(), &identity);
        }
        drop(other);
    }
}

// One qualified primitive row; rejection is not a cancellation/recovery API.
#[tokio::test(flavor = "current_thread")]
async fn qualified_family_refuses_premature_resolution_with_original_still_pending() {
    let mut family = SessionFamily::new();
    let (release, held) = oneshot::channel::<LeafResult>();
    let (sender, result) = oneshot::channel::<LeafResult>();
    let (completed, completion) = oneshot::channel();
    let task = tokio::spawn(async move {
        let raw = held.await.unwrap();
        let _ = sender.send(raw);
        // No await/callback follows this task's terminal-body witness.
        completed.send(tokio::task::id()).unwrap();
    });
    let receipt = AdmittedTask {
        task,
        kind: LeafKind::CbsRequest,
        result,
    };
    let id = receipt.task.id();
    family.adopt(receipt);
    let panic = std::panic::catch_unwind(AssertUnwindSafe(|| family.into_result()));
    assert!(
        panic.is_err(),
        "a pending original must not masquerade as terminal success"
    );
    // The owner was deliberately consumed by this negative API probe. This is
    // not supported owner-drop recovery or a join through the consumed owner.
    release.send(Ok(())).unwrap();
    assert_eq!(timeout(WAIT, completion).await.unwrap().unwrap(), id);
}

async fn native_session() -> (
    amqp::ServerConnection,
    tokio::io::DuplexStream,
    amqp::ServerSession,
) {
    use amqp::{
        Begin, Frame, Open, Performative, ProtocolHeader, read_frame, read_protocol_header,
        write_frame, write_protocol_header,
    };
    let frame = |channel, performative| Frame::Amqp {
        channel,
        performative: Some(performative),
        payload: Vec::new(),
    };
    let (stream, mut peer) = tokio::io::duplex(64 * 1024);
    let (connection, ()) = timeout(WAIT, async {
        tokio::join!(
            amqp::ServerConnection::accept(stream, "primitive-session", None),
            async {
                write_protocol_header(&mut peer, ProtocolHeader::AMQP)
                    .await
                    .unwrap();
                assert_eq!(
                    read_protocol_header(&mut peer).await.unwrap(),
                    ProtocolHeader::AMQP
                );
                write_frame(
                    &mut peer,
                    &frame(0, Performative::Open(Open::new("primitive-peer"))),
                )
                .await
                .unwrap();
                assert!(matches!(
                    read_frame(&mut peer).await.unwrap(),
                    Frame::Amqp {
                        performative: Some(Performative::Open(_)),
                        ..
                    }
                ));
            }
        )
    })
    .await
    .unwrap();
    let mut connection = connection.unwrap();
    write_frame(&mut peer, &frame(1, Performative::Begin(Begin::default())))
        .await
        .unwrap();
    let incoming = timeout(WAIT, connection.next_incoming_session())
        .await
        .unwrap()
        .unwrap();
    let session = connection.accept_session(incoming).await.unwrap();
    assert!(matches!(
        read_frame(&mut peer).await.unwrap(),
        Frame::Amqp {
            performative: Some(Performative::Begin(_)),
            ..
        }
    ));
    (connection, peer, session)
}

// Six primitive rows with a real captured request, not ordinary admission.
#[tokio::test(flavor = "current_thread")]
async fn qualified_session_primary_packets_keep_priority_and_child_errors_do_not_retire_connection()
{
    for joined_panic in [false, true] {
        for primary in 0..3 {
            let (mut connection, _peer, session) = native_session().await;
            let notice = ConnectionRetirementRequest::capture(&connection);
            let mut custody = SessionCustody::new(session, Some(notice.clone()));
            let (raw, child) = failure("raw child failure");
            let child_panic: Arc<str> = Arc::from("raw child task panic below parent packet");
            let receipt = if joined_panic {
                composed(
                    LeafKind::DataSend,
                    Some(raw),
                    Some(Arc::clone(&child_panic)),
                )
            } else {
                PreparedLeafTask::new(LeafKind::DataSend).spawn(std::future::ready(raw))
            };
            completed(&receipt).await;
            custody.pending_admitted = Some(receipt);
            let parent_panic: Arc<str> = Arc::from("raw first parent panic");
            let (parent_error, parent_identity) = failure("raw first parent error");
            let parent = match primary {
                0 => Ok(Ok(SessionPumpExit::Complete)),
                1 => Ok(parent_error.map(|()| SessionPumpExit::Complete)),
                _ => Err(Box::new(Arc::clone(&parent_panic)) as PanicPayload),
            };
            custody.record_primary(parent);
            custody.record_primary(Err(Box::new(Arc::<str>::from(
                "later primary must not replace first",
            ))));
            assert_eq!(notice.is_requested(), primary != 0);
            timeout(WAIT, custody.finish()).await.unwrap();
            let packet = &custody.family().finished()[0];
            exact_error(
                packet.leaf.as_ref().unwrap().as_ref().unwrap_err().as_ref(),
                &child,
            );
            let resolved = std::panic::catch_unwind(AssertUnwindSafe(|| custody.into_result()));
            match primary {
                0 if joined_panic => {
                    let error = *resolved
                        .unwrap()
                        .unwrap_err()
                        .downcast::<tokio::task::JoinError>()
                        .unwrap();
                    assert!(Arc::ptr_eq(
                        error.into_panic().downcast_ref::<Arc<str>>().unwrap(),
                        &child_panic
                    ));
                }
                0 => exact_error(resolved.unwrap().unwrap_err().as_ref(), &child),
                1 => exact_error(resolved.unwrap().unwrap_err().as_ref(), &parent_identity),
                _ => exact_panic(resolved, &parent_panic),
            }
            assert_eq!(notice.is_requested(), primary != 0);
            connection.stop();
            timeout(WAIT, connection.shutdown()).await.unwrap().unwrap();
        }
    }
}

// Three qualified compositions: report-only stays below all retained originals.
#[tokio::test(flavor = "current_thread")]
async fn qualified_session_report_only_exit_retains_raw_priority_across_cancelled_finish() {
    for child_failure in 0..3 {
        let (mut connection, _peer, session) = native_session().await;
        let notice = ConnectionRetirementRequest::capture(&connection);
        let mut custody = SessionCustody::new(session, Some(notice.clone()));
        let (raw, child_error) = failure("raw original outranks admission reporting");
        let child_panic: Arc<str> =
            Arc::from("synthetic original task panic outranks admission report");
        let receipt = if child_failure == 2 {
            composed(
                LeafKind::DataSend,
                Some(Ok(())),
                Some(Arc::clone(&child_panic)),
            )
        } else {
            PreparedLeafTask::new(LeafKind::DataSend).spawn(std::future::ready(
                if child_failure == 1 { raw } else { Ok(()) },
            ))
        };
        completed(&receipt).await;
        let first_id = receipt.task.id();
        custody.pending_admitted = Some(receipt);
        let (release, held) = oneshot::channel::<LeafResult>();
        let pending =
            PreparedLeafTask::new(LeafKind::DataReceive).spawn(async move { held.await.unwrap() });
        let pending_id = pending.task.id();
        custody.family.adopt(pending);
        let report: Arc<str> = Arc::from("raw report-only parent diagnostic");
        let payload = Box::new(Arc::clone(&report)) as PanicPayload;
        let report_address = (&*payload as *const (dyn std::any::Any + Send) as *const ()) as usize;
        custody.record_primary(Ok(Ok(SessionPumpExit::ReportOnly(payload))));
        assert!(!notice.is_requested());
        let mut packet_address = None;
        for _ in 0..2 {
            let mut finish = Box::pin(custody.finish());
            pending_once(finish.as_mut()).await;
            drop(finish);
            assert_eq!(custody.family().finished().len(), 1);
            let packet = &custody.family().finished()[0];
            assert_eq!(packet.id, first_id);
            let address = (&**packet as *const JoinedLeafTask) as usize;
            assert_eq!(*packet_address.get_or_insert(address), address);
            let retained = custody.report_payload().unwrap();
            assert_eq!(
                (&**retained as *const (dyn std::any::Any + Send) as *const ()) as usize,
                report_address
            );
            assert!(Arc::ptr_eq(
                retained.downcast_ref::<Arc<str>>().unwrap(),
                &report
            ));
            assert!(!notice.is_requested());
        }
        release.send(Ok(())).unwrap();
        timeout(WAIT, custody.finish()).await.unwrap();
        assert_eq!(custody.family().finished()[1].id, pending_id);
        assert_eq!(
            (&*custody.family().finished()[0] as *const JoinedLeafTask) as usize,
            packet_address.unwrap()
        );
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| custody.into_result()));
        match child_failure {
            0 => exact_panic(result, &report),
            1 => exact_error(result.unwrap().unwrap_err().as_ref(), &child_error),
            _ => {
                let error = *result
                    .unwrap()
                    .unwrap_err()
                    .downcast::<tokio::task::JoinError>()
                    .unwrap();
                assert!(Arc::ptr_eq(
                    error.into_panic().downcast_ref::<Arc<str>>().unwrap(),
                    &child_panic
                ));
            }
        }
        assert!(!notice.is_requested());
        connection.stop();
        timeout(WAIT, connection.shutdown()).await.unwrap().unwrap();
    }
}

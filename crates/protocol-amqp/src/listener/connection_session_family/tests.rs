//! Qualified original task/result controls, not native session admissions.
//! Fixture timeouts bound reachability, not product completion latency.

use super::*;
use crate::listener::connection_custody::{ConnectionTerminalParts, resolve_terminal};
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
    task::Poll,
    time::Duration,
};
use tokio::{sync::oneshot, task::Id, time::timeout};
use tracing::instrument::WithSubscriber;

const WAIT: Duration = Duration::from_secs(5);

#[derive(Debug)]
struct RawError(Arc<str>);
impl fmt::Display for RawError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str(&self.0)
    }
}
impl Error for RawError {}

fn address<T: ?Sized>(value: &T) -> usize {
    value as *const T as *const () as usize
}
fn identity(label: &str) -> Arc<str> {
    Arc::from(label)
}
fn raw_error(id: &Arc<str>) -> ConnectionError {
    Box::new(RawError(Arc::clone(id)))
}
fn raw_panic(id: &Arc<str>) -> PanicPayload {
    Box::new(Arc::clone(id))
}
fn empty_failures() -> SessionFamilyFailures {
    SessionFamilyFailures {
        join_error: None,
        returned_error: None,
        bridge_fault: None,
        report_only: None,
        diagnostic: None,
    }
}
fn terminal(family: SessionFamilyFailures) -> ConnectionTerminalParts {
    ConnectionTerminalParts {
        primary: Ok(Ok(())),
        native_error: None,
        shutdown: Some(Ok(())),
        secondary_panic: None,
        family,
        diagnostic: None,
    }
}
fn same_error(error: &(dyn Error + Send + Sync + 'static), expected: &Arc<str>) {
    assert!(Arc::ptr_eq(
        &error.downcast_ref::<RawError>().unwrap().0,
        expected
    ));
}
fn same_panic(payload: &(dyn std::any::Any + Send), expected: &Arc<str>) {
    assert!(Arc::ptr_eq(
        payload.downcast_ref::<Arc<str>>().unwrap(),
        expected
    ));
}
async fn completed(receipt: &AdmittedSessionTask) {
    timeout(WAIT, async {
        while !receipt.task.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("same original task positively completed");
}
async fn pending_once<F: Future + ?Sized>(mut future: Pin<&mut F>) {
    assert!(
        tokio::task::unconstrained(poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))))
            .await
            .is_pending(),
        "same held original stays pending"
    );
}
fn packet_exit(packet: &JoinedSessionTask, expected: &Arc<str>, raw_address: usize, report: bool) {
    assert!(packet.joined.is_ok() && packet.bridge_fault.is_none());
    match packet.exit.as_ref().unwrap() {
        SessionTaskExit::ReportOnly(payload) if report => {
            assert_eq!(address(&**payload), raw_address);
            same_panic(&**payload, expected);
        }
        SessionTaskExit::Complete(Err(error)) if !report => {
            assert_eq!(address(&**error), raw_address);
            same_error(&**error, expected);
        }
        _ => panic!("same original typed packet"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn qualified_connection_family_typed_bridge_retains_raw_original_packets() {
    for case in 0..3 {
        let mut family = ConnectionSessionFamily::new();
        let id = identity("same typed bridge original");
        let error = raw_error(&id);
        let payload = raw_panic(&id);
        let raw_address = if case == 2 {
            address(&*payload)
        } else {
            address(&*error)
        };
        let exit = match case {
            0 => SessionTaskExit::Complete(Ok(())),
            1 => SessionTaskExit::Complete(Err(error)),
            _ => SessionTaskExit::ReportOnly(payload),
        };
        let runs = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&runs);
        let receipt = PreparedSessionTask::new().spawn(async move {
            count.fetch_add(1, Ordering::SeqCst);
            exit
        });
        completed(&receipt).await;
        let original_id = receipt.task.id();
        assert_eq!(receipt.id, original_id);
        assert_eq!(family.adopt(receipt), original_id);
        timeout(WAIT, family.finish()).await.unwrap();
        let packet = &family.finished()[0];
        assert_eq!(packet.id, original_id);
        if case == 0 {
            assert!(matches!(
                packet.exit,
                Some(SessionTaskExit::Complete(Ok(())))
            ));
        } else {
            packet_exit(packet, &id, raw_address, case == 2);
        }
        let parts = terminal(family.take_failures());
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| resolve_terminal(parts)));
        match case {
            0 => assert!(result.unwrap().is_ok()),
            1 => {
                let error = result.unwrap().unwrap_err();
                assert_eq!(address(&*error), raw_address);
                same_error(&*error, &id);
            }
            _ => {
                let payload = result.expect_err("typed report remains original raw panic");
                assert_eq!(address(&*payload), raw_address);
                same_panic(&*payload, &id);
            }
        }
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn qualified_connection_family_twice_cancelled_finish_keeps_completed_first_and_pending_second()
 {
    for report in [false, true] {
        let mut family = ConnectionSessionFamily::new();
        let id = identity("completed first typed original");
        let error = raw_error(&id);
        let payload = raw_panic(&id);
        let raw_address = if report {
            address(&*payload)
        } else {
            address(&*error)
        };
        let exit = if report {
            SessionTaskExit::ReportOnly(payload)
        } else {
            SessionTaskExit::Complete(Err(error))
        };
        let first = PreparedSessionTask::new().spawn(std::future::ready(exit));
        completed(&first).await;
        let first_id = first.id;
        assert_eq!(family.adopt(first), first_id);
        let (release, held) = oneshot::channel();
        let (entered, reached) = oneshot::channel();
        let runs = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&runs);
        let second = PreparedSessionTask::new().spawn(async move {
            count.fetch_add(1, Ordering::SeqCst);
            let _ = entered.send(());
            held.await.unwrap()
        });
        let second_id = second.id;
        assert_eq!(family.adopt(second), second_id);
        timeout(WAIT, reached).await.unwrap().unwrap();
        let mut packet_address = None;
        for _ in 0..2 {
            let mut finish = Box::pin(family.finish());
            pending_once(finish.as_mut()).await;
            drop(finish);
            assert_eq!(family.pending_ids(), vec![second_id]);
            assert_eq!(family.finished().len(), 1);
            let packet = &family.finished()[0];
            assert_eq!(packet.id, first_id);
            packet_exit(packet, &id, raw_address, report);
            let now = address(&**packet);
            assert_eq!(*packet_address.get_or_insert(now), now);
            assert_eq!(runs.load(Ordering::SeqCst), 1);
        }
        assert!(release.send(SessionTaskExit::Complete(Ok(()))).is_ok());
        timeout(WAIT, family.finish()).await.unwrap();
        assert_eq!(family.finished().len(), 2);
        assert_eq!(address(&*family.finished()[0]), packet_address.unwrap());
        assert_eq!(family.finished()[1].id, second_id);
        timeout(WAIT, family.finish()).await.unwrap();
        assert_eq!(family.finished().len(), 2);
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            resolve_terminal(terminal(family.take_failures()))
        }));
        if report {
            let payload = result.expect_err("report identity wins after drain");
            assert_eq!(address(&*payload), raw_address);
            same_panic(&*payload, &id);
        } else {
            let error = result.unwrap().unwrap_err();
            assert_eq!(address(&*error), raw_address);
            same_error(&*error, &id);
        }
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn qualified_connection_family_cancelled_next_and_late_receipt_keep_same_originals() {
    let mut family = ConnectionSessionFamily::new();
    let (release, held) = oneshot::channel();
    let pending = PreparedSessionTask::new().spawn(async move { held.await.unwrap() });
    let pending_id = pending.id;
    family.adopt(pending);
    for _ in 0..2 {
        let mut next = Box::pin(family.next());
        pending_once(next.as_mut()).await;
    }
    assert_eq!(family.pending_ids(), vec![pending_id]);
    assert!(std::panic::catch_unwind(AssertUnwindSafe(|| family.take_failures())).is_err());
    assert_eq!(family.pending_ids(), vec![pending_id]);
    let raw = identity("late original receipt");
    let late = PreparedSessionTask::new().spawn(std::future::ready(SessionTaskExit::Complete(
        Err(raw_error(&raw)),
    )));
    completed(&late).await;
    let late_id = late.id;
    family.retire();
    assert!(!family.next().await);
    assert_eq!(family.adopt(late), late_id);
    for _ in 0..2 {
        let mut finish = Box::pin(family.finish());
        pending_once(finish.as_mut()).await;
    }
    assert_eq!(family.finished()[0].id, late_id);
    let kept = address(&*family.finished()[0]);
    assert!(release.send(SessionTaskExit::Complete(Ok(()))).is_ok());
    timeout(WAIT, family.finish()).await.unwrap();
    assert_eq!(family.finished()[1].id, pending_id);
    assert_eq!(address(&*family.finished()[0]), kept);
    let error = resolve_terminal(terminal(family.take_failures())).unwrap_err();
    same_error(&*error, &raw);
    assert!(std::panic::catch_unwind(AssertUnwindSafe(|| family.take_failures())).is_err());
}

fn composed(exit: Option<SessionTaskExit>, panic: Option<Arc<str>>) -> AdmittedSessionTask {
    // Missing packets / post-send panics are qualified producer compositions.
    let (sender, result) = oneshot::channel();
    let task = tokio::spawn(async move {
        if let Some(exit) = exit {
            let _ = sender.send(exit);
        }
        if let Some(payload) = panic {
            panic_any(payload);
        }
    });
    AdmittedSessionTask {
        id: task.id(),
        task,
        exit: result,
    }
}
async fn original_join_error(payload: &Arc<str>) -> tokio::task::JoinError {
    let payload = Arc::clone(payload);
    let task = tokio::spawn(async move {
        panic_any(payload);
    });
    task.await.unwrap_err()
}

#[tokio::test(flavor = "current_thread")]
async fn qualified_connection_family_outer_join_returned_error_and_missing_packet_stay_distinct() {
    for case in 0..4 {
        let mut family = ConnectionSessionFamily::new();
        let panic = identity("same outer task panic");
        let returned = identity("same raw returned error");
        let nested = if case == 3 {
            Some(original_join_error(&panic).await)
        } else {
            None
        };
        let nested_id = nested.as_ref().map(tokio::task::JoinError::id);
        let exit = if case == 2 {
            Some(SessionTaskExit::Complete(Err(raw_error(&returned))))
        } else {
            nested.map(|error| SessionTaskExit::Complete(Err(Box::new(error))))
        };
        let receipt = composed(exit, (case == 1 || case == 2).then(|| Arc::clone(&panic)));
        completed(&receipt).await;
        let id = receipt.id;
        family.adopt(receipt);
        timeout(WAIT, family.finish()).await.unwrap();
        let packet = &family.finished()[0];
        assert_eq!(packet.id, id);
        let failures = family.take_failures();
        if case == 0 {
            assert!(failures.join_error.is_none() && failures.returned_error.is_none());
            assert_eq!(failures.bridge_fault.as_ref().unwrap().id, id);
        } else if case == 3 {
            assert!(failures.join_error.is_none() && failures.bridge_fault.is_none());
        } else {
            assert!(failures.join_error.as_ref().unwrap().is_panic());
            assert!(failures.bridge_fault.is_none());
            if case == 2 {
                same_error(&**failures.returned_error.as_ref().unwrap(), &returned);
            }
        }
        let error = resolve_terminal(terminal(failures)).unwrap_err();
        if case == 0 {
            assert_eq!(error.downcast_ref::<SessionBridgeFault>().unwrap().id, id);
        } else {
            let joined = *error.downcast::<tokio::task::JoinError>().unwrap();
            assert_eq!(joined.id(), nested_id.unwrap_or(id));
            same_panic(&*joined.into_panic(), &panic);
        }
    }
}

#[derive(Clone, Copy)]
enum ChildFault {
    Join,
    Returned,
    Bridge,
    Report,
}
fn fault_receipt(kind: ChildFault, raw: &Arc<str>) -> AdmittedSessionTask {
    match kind {
        ChildFault::Join => composed(None, Some(Arc::clone(raw))),
        ChildFault::Returned => PreparedSessionTask::new().spawn(std::future::ready(
            SessionTaskExit::Complete(Err(raw_error(raw))),
        )),
        ChildFault::Bridge => composed(None, None),
        ChildFault::Report => PreparedSessionTask::new().spawn(std::future::ready(
            SessionTaskExit::ReportOnly(raw_panic(raw)),
        )),
    }
}
fn selected_child(
    kind: ChildFault,
    result: std::thread::Result<Result<(), ConnectionError>>,
    raw: &Arc<str>,
    id: Id,
) {
    match kind {
        ChildFault::Report => same_panic(&*result.expect_err("original typed report resumes"), raw),
        ChildFault::Returned => same_error(&*result.unwrap().unwrap_err(), raw),
        ChildFault::Join => {
            let error = *result
                .unwrap()
                .unwrap_err()
                .downcast::<tokio::task::JoinError>()
                .unwrap();
            assert_eq!(error.id(), id);
            same_panic(&*error.into_panic(), raw);
        }
        ChildFault::Bridge => assert_eq!(
            result
                .unwrap()
                .unwrap_err()
                .downcast_ref::<SessionBridgeFault>()
                .unwrap()
                .id,
            id
        ),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn qualified_connection_family_first_category_cache_survives_healthy_reaping() {
    for kind in [
        ChildFault::Join,
        ChildFault::Returned,
        ChildFault::Bridge,
        ChildFault::Report,
    ] {
        let mut family = ConnectionSessionFamily::new();
        let first = identity("first original in category");
        let receipt = fault_receipt(kind, &first);
        completed(&receipt).await;
        let id = receipt.id;
        family.adopt(receipt);
        assert!(family.next().await);
        for turn in 0..40 {
            let receipt = if turn == 20 {
                fault_receipt(kind, &identity("later category original"))
            } else {
                PreparedSessionTask::new()
                    .spawn(std::future::ready(SessionTaskExit::Complete(Ok(()))))
            };
            completed(&receipt).await;
            family.adopt(receipt);
            assert!(family.next().await);
            assert!(family.pending_ids().is_empty() && family.finished().is_empty());
            assert_eq!(family.live_ready_count(), 0);
        }
        timeout(WAIT, family.finish()).await.unwrap();
        assert!(family.finished().is_empty());
        selected_child(
            kind,
            std::panic::catch_unwind(AssertUnwindSafe(|| {
                resolve_terminal(terminal(family.take_failures()))
            })),
            &first,
            id,
        );
    }
}

#[derive(Clone, Copy, Debug)]
enum Rank {
    PrimaryPanic,
    PrimaryError,
    Native,
    Shutdown,
    Cleanup,
    Join,
    Returned,
    Bridge,
    Report,
    ConnectionReport,
    FamilyReport,
}
const RANKS: [Rank; 11] = [
    Rank::PrimaryPanic,
    Rank::PrimaryError,
    Rank::Native,
    Rank::Shutdown,
    Rank::Cleanup,
    Rank::Join,
    Rank::Returned,
    Rank::Bridge,
    Rank::Report,
    Rank::ConnectionReport,
    Rank::FamilyReport,
];
struct Expected {
    identity: Arc<str>,
    address: usize,
    task: Option<Id>,
}
async fn install(parts: &mut ConnectionTerminalParts, rank: Rank, label: &str) -> Expected {
    let raw = identity(label);
    let mut expected = Expected {
        identity: Arc::clone(&raw),
        address: 0,
        task: None,
    };
    match rank {
        Rank::PrimaryPanic
        | Rank::Cleanup
        | Rank::Report
        | Rank::ConnectionReport
        | Rank::FamilyReport => {
            let payload = raw_panic(&raw);
            expected.address = address(&*payload);
            match rank {
                Rank::PrimaryPanic => parts.primary = Err(payload),
                Rank::Cleanup => parts.secondary_panic = Some(payload),
                Rank::Report => parts.family.report_only = Some(payload),
                Rank::ConnectionReport => parts.diagnostic = Some(payload),
                _ => parts.family.diagnostic = Some(payload),
            }
        }
        Rank::PrimaryError | Rank::Returned => {
            let error = raw_error(&raw);
            expected.address = address(&*error);
            if matches!(rank, Rank::PrimaryError) {
                parts.primary = Ok(Err(error));
            } else {
                parts.family.returned_error = Some(error);
            }
        }
        Rank::Native => {
            let message = label.to_owned();
            expected.address = message.as_ptr() as usize;
            parts.native_error = Some(amqp::EngineError::InvalidState(message));
        }
        Rank::Shutdown => {
            let message = label.to_owned();
            expected.address = message.as_ptr() as usize;
            parts.shutdown = Some(Err(amqp::ConnectionShutdownError::DriverFailed(message)));
        }
        Rank::Join => {
            let error = original_join_error(&raw).await;
            expected.task = Some(error.id());
            parts.family.join_error = Some(error);
        }
        Rank::Bridge => {
            let task = tokio::spawn(std::future::ready(()));
            let id = task.id();
            task.await.unwrap();
            expected.task = Some(id);
            parts.family.bridge_fault = Some(SessionBridgeFault { id });
        }
    }
    expected
}
fn selected(
    rank: Rank,
    result: std::thread::Result<Result<(), ConnectionError>>,
    expected: &Expected,
) {
    match rank {
        Rank::PrimaryPanic
        | Rank::Cleanup
        | Rank::Report
        | Rank::ConnectionReport
        | Rank::FamilyReport => {
            let payload = result.expect_err("selected raw original panic");
            assert_eq!(address(&*payload), expected.address);
            same_panic(&*payload, &expected.identity);
        }
        Rank::PrimaryError | Rank::Returned => {
            let error = result.unwrap().unwrap_err();
            assert_eq!(address(&*error), expected.address);
            same_error(&*error, &expected.identity);
        }
        Rank::Native => {
            let error = result.unwrap().unwrap_err();
            let Some(amqp::EngineError::InvalidState(value)) =
                error.downcast_ref::<amqp::EngineError>()
            else {
                panic!("native category");
            };
            assert_eq!(value.as_ptr() as usize, expected.address);
        }
        Rank::Shutdown => {
            let error = result.unwrap().unwrap_err();
            let Some(amqp::ConnectionShutdownError::DriverFailed(value)) =
                error.downcast_ref::<amqp::ConnectionShutdownError>()
            else {
                panic!("shutdown category");
            };
            assert_eq!(value.as_ptr() as usize, expected.address);
        }
        Rank::Join => {
            let error = *result
                .unwrap()
                .unwrap_err()
                .downcast::<tokio::task::JoinError>()
                .unwrap();
            assert_eq!(Some(error.id()), expected.task);
            same_panic(&*error.into_panic(), &expected.identity);
        }
        Rank::Bridge => assert_eq!(
            Some(
                result
                    .unwrap()
                    .unwrap_err()
                    .downcast_ref::<SessionBridgeFault>()
                    .unwrap()
                    .id
            ),
            expected.task
        ),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn qualified_connection_terminal_parent_native_cleanup_precedence_keeps_original_payloads() {
    // Synthetic terminal parts do not claim actual native/cleanup reachability.
    for (higher, rank) in RANKS[..5].iter().copied().enumerate() {
        for lower in RANKS.iter().copied().skip(higher + 1) {
            // Primary panic/error are alternatives of one retained raw result.
            if matches!(rank, Rank::PrimaryPanic) && matches!(lower, Rank::PrimaryError) {
                continue;
            }
            let mut parts = terminal(empty_failures());
            let expected = install(&mut parts, rank, "original stronger parent/native").await;
            install(&mut parts, lower, "lower qualified category").await;
            selected(
                rank,
                std::panic::catch_unwind(AssertUnwindSafe(|| resolve_terminal(parts))),
                &expected,
            );
        }
    }
}

struct Reporter {
    reached: Arc<AtomicUsize>,
    payload: Arc<str>,
}
impl tracing::Subscriber for Reporter {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target().ends_with("::connection_session_family")
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

#[tokio::test(flavor = "current_thread")]
async fn qualified_connection_terminal_child_reporting_precedence_is_diagnostic_only() {
    for (offset, rank) in RANKS[5..].iter().copied().enumerate() {
        for lower in RANKS.iter().copied().skip(offset + 6) {
            let mut parts = terminal(empty_failures());
            let expected = install(&mut parts, rank, "same stronger child/report").await;
            install(&mut parts, lower, "lower diagnostic category").await;
            selected(
                rank,
                std::panic::catch_unwind(AssertUnwindSafe(|| resolve_terminal(parts))),
                &expected,
            );
        }
    }
    let mut family = ConnectionSessionFamily::new();
    let raw = identity("original synthetic returned error triggers real family reporter");
    let error = raw_error(&raw);
    let original_address = address(&*error);
    let receipt =
        PreparedSessionTask::new().spawn(std::future::ready(SessionTaskExit::Complete(Err(error))));
    completed(&receipt).await;
    let id = receipt.id;
    family.adopt(receipt);
    let reached = Arc::new(AtomicUsize::new(0));
    let report = identity("reached family reporting diagnostic");
    let other = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    let dispatch = tracing::Dispatch::new(Reporter {
        reached: Arc::clone(&reached),
        payload: report,
    });
    std::thread::spawn(tracing::callsite::rebuild_interest_cache)
        .join()
        .unwrap();
    timeout(WAIT, family.finish().with_subscriber(dispatch))
        .await
        .unwrap();
    assert_eq!(reached.load(Ordering::SeqCst), 1);
    assert_eq!(family.finished()[0].id, id);
    let error = resolve_terminal(terminal(family.take_failures())).unwrap_err();
    assert_eq!(address(&*error), original_address);
    same_error(&*error, &raw);
    drop(other);
    assert!(resolve_terminal(terminal(empty_failures())).is_ok());
}

fn selected_typed(
    rank: Rank,
    result: std::thread::Result<crate::listener::connection_custody::ConnectionTaskExit>,
    expected: &Expected,
) {
    use crate::listener::connection_custody::ConnectionTaskExit;
    if matches!(
        rank,
        Rank::Report | Rank::ConnectionReport | Rank::FamilyReport
    ) {
        let ConnectionTaskExit::ReportOnly(payload) = result.unwrap() else {
            panic!("known reporting origin stays typed");
        };
        assert_eq!(address(&*payload), expected.address);
        same_panic(&*payload, &expected.identity);
    } else {
        selected(
            rank,
            result.map(|exit| {
                let ConnectionTaskExit::Complete(result) = exit else {
                    panic!("genuine failure is not report-only");
                };
                result
            }),
            expected,
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn qualified_typed_connection_exit_preserves_all_terminal_ranks_and_raw_identity() {
    use crate::listener::connection_custody::{ConnectionTaskExit, resolve_terminal_exit};
    use std::panic::catch_unwind;
    // These are qualified terminal parts, not native fault reachability.
    for (higher, rank) in RANKS.iter().copied().enumerate() {
        let mut parts = terminal(empty_failures());
        let expected = install(&mut parts, rank, "same single typed category").await;
        selected_typed(
            rank,
            catch_unwind(AssertUnwindSafe(|| resolve_terminal_exit(parts))),
            &expected,
        );
        for lower in RANKS.iter().copied().skip(higher + 1) {
            if matches!(rank, Rank::PrimaryPanic) && matches!(lower, Rank::PrimaryError) {
                continue;
            }
            let mut parts = terminal(empty_failures());
            let expected = install(&mut parts, rank, "same stronger typed category").await;
            install(&mut parts, lower, "lower typed category").await;
            selected_typed(
                rank,
                catch_unwind(AssertUnwindSafe(|| resolve_terminal_exit(parts))),
                &expected,
            );
        }
    }
    assert!(matches!(
        resolve_terminal_exit(terminal(empty_failures())),
        ConnectionTaskExit::Complete(Ok(()))
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn qualified_typed_connection_exit_legacy_adapter_keeps_boxes_and_join_origin() {
    use crate::listener::connection_custody::ConnectionTaskExit;
    // Actual Tokio tasks, but composed exits rather than listener adoption.
    for legacy in [false, true] {
        for case in 0..4 {
            let raw = identity("same typed connection adapter payload");
            let mut nested_id = None;
            let exit = match case {
                0 => ConnectionTaskExit::Complete(Ok(())),
                1 => ConnectionTaskExit::Complete(Err(raw_error(&raw))),
                2 => ConnectionTaskExit::ReportOnly(raw_panic(&raw)),
                _ => {
                    let error = original_join_error(&raw).await;
                    nested_id = Some(error.id());
                    ConnectionTaskExit::Complete(Err(Box::new(error)))
                }
            };
            let raw_address = match &exit {
                ConnectionTaskExit::Complete(Err(error)) => Some(address(&**error)),
                ConnectionTaskExit::ReportOnly(payload) => Some(address(&**payload)),
                _ => None,
            };
            let task = tokio::spawn(async move {
                if legacy {
                    ConnectionTaskExit::Complete(exit.into_result())
                } else {
                    exit
                }
            });
            let id = task.id();
            let joined = task.await;
            if legacy && case == 2 {
                let Err(error) = joined else {
                    panic!("legacy report remains a raw task panic");
                };
                assert_eq!(error.id(), id);
                let payload = error.into_panic();
                assert_eq!(Some(address(&*payload)), raw_address);
                same_panic(&*payload, &raw);
                continue;
            }
            match joined.unwrap() {
                ConnectionTaskExit::Complete(Ok(())) => assert_eq!(case, 0),
                ConnectionTaskExit::ReportOnly(payload) => {
                    assert!(!legacy && case == 2);
                    assert_eq!(Some(address(&*payload)), raw_address);
                    same_panic(&*payload, &raw);
                }
                ConnectionTaskExit::Complete(Err(error)) => {
                    assert_eq!(Some(address(&*error)), raw_address);
                    if case == 1 {
                        same_error(&*error, &raw);
                    } else {
                        assert_eq!(case, 3);
                        let error = *error.downcast::<tokio::task::JoinError>().unwrap();
                        assert_eq!(Some(error.id()), nested_id);
                        same_panic(&*error.into_panic(), &raw);
                    }
                }
            }
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn qualified_typed_connection_exit_reached_family_report_does_not_hide_raw_error() {
    use crate::listener::connection_custody::{ConnectionTaskExit, resolve_terminal_exit};
    let mut family = ConnectionSessionFamily::new();
    let raw = identity("composed returned error with actual reached warning");
    let error = raw_error(&raw);
    let raw_address = address(&*error);
    let receipt =
        PreparedSessionTask::new().spawn(std::future::ready(SessionTaskExit::Complete(Err(error))));
    completed(&receipt).await;
    let id = receipt.id;
    family.adopt(receipt);
    let reached = Arc::new(AtomicUsize::new(0));
    let other = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    let dispatch = tracing::Dispatch::new(Reporter {
        reached: Arc::clone(&reached),
        payload: identity("reached lower family diagnostic"),
    });
    std::thread::spawn(tracing::callsite::rebuild_interest_cache)
        .join()
        .unwrap();
    timeout(WAIT, family.finish().with_subscriber(dispatch))
        .await
        .unwrap();
    assert_eq!(reached.load(Ordering::SeqCst), 1);
    assert_eq!(family.finished()[0].id, id);
    let ConnectionTaskExit::Complete(Err(error)) =
        resolve_terminal_exit(terminal(family.take_failures()))
    else {
        panic!("raw returned error outranks reached reporting fault");
    };
    assert_eq!(address(&*error), raw_address);
    same_error(&*error, &raw);
    drop(other);
}

//! Original session tasks and typed terminal packets belong to one connection.

use std::{
    error::Error,
    fmt,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::{StreamExt, stream::FuturesUnordered};
use tokio::{
    sync::oneshot,
    task::{Id, JoinError, JoinHandle},
};
use tracing::warn;

use super::{
    ConnectionAuthorization, ConnectionManagement, NamespaceName,
    connection_custody::{ConnectionCustody, ConnectionError, NativePacket},
    session_custody::{PanicPayload, SessionTaskExit},
};
use crate::Broker;
use std::sync::Arc;

pub(super) struct PreparedSessionTask {
    sender: oneshot::Sender<SessionTaskExit>,
    exit: oneshot::Receiver<SessionTaskExit>,
    #[cfg(test)]
    dispatch: tracing::Dispatch,
}

impl PreparedSessionTask {
    /// Allocate while the accepted native packet still has connection custody.
    pub(super) fn new() -> Self {
        let (sender, exit) = oneshot::channel();
        Self {
            sender,
            exit,
            // Test subscribers are captured before take, not inherited by Tokio.
            #[cfg(test)]
            dispatch: tracing::dispatcher::get_default(Clone::clone),
        }
    }

    pub(super) fn spawn(
        self,
        future: impl Future<Output = SessionTaskExit> + Send + 'static,
    ) -> AdmittedSessionTask {
        let Self {
            sender,
            exit,
            #[cfg(test)]
            dispatch,
        } = self;
        let producer = async move {
            let packet = future.await;
            let _ = sender.send(packet);
        };
        #[cfg(test)]
        let producer = {
            use tracing::instrument::WithSubscriber;
            producer.with_subscriber(dispatch)
        };
        let task = tokio::spawn(producer);
        let id = task.id();
        AdmittedSessionTask { id, task, exit }
    }
}

pub(super) struct AdmittedSessionTask {
    pub(super) id: Id,
    pub(super) task: JoinHandle<()>,
    pub(super) exit: oneshot::Receiver<SessionTaskExit>,
}

#[derive(Debug)]
pub(super) struct SessionBridgeFault {
    pub(super) id: Id,
}

impl fmt::Display for SessionBridgeFault {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "original session task {} completed without its typed result",
            self.id
        )
    }
}

impl Error for SessionBridgeFault {}

pub(super) struct JoinedSessionTask {
    pub(super) id: Id,
    pub(super) joined: Result<(), JoinError>,
    pub(super) exit: Option<SessionTaskExit>,
    pub(super) bridge_fault: Option<SessionBridgeFault>,
}

struct OriginalSessionTask {
    admitted: AdmittedSessionTask,
}

impl Future for OriginalSessionTask {
    type Output = Box<JoinedSessionTask>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let joined = match Pin::new(&mut self.admitted.task).poll(context) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(joined) => joined,
        };
        // The producer sends synchronously before returning unit.
        let exit = self.admitted.exit.try_recv().ok();
        let bridge_fault = if joined.is_ok() && exit.is_none() {
            Some(SessionBridgeFault {
                id: self.admitted.id,
            })
        } else {
            None
        };
        Poll::Ready(Box::new(JoinedSessionTask {
            id: self.admitted.id,
            joined,
            exit,
            bridge_fault,
        }))
    }
}

pub(super) struct SessionFamilyFailures {
    pub(super) join_error: Option<JoinError>,
    pub(super) returned_error: Option<ConnectionError>,
    pub(super) bridge_fault: Option<SessionBridgeFault>,
    pub(super) report_only: Option<PanicPayload>,
    pub(super) diagnostic: Option<PanicPayload>,
}

impl SessionFamilyFailures {
    pub(super) fn new() -> Self {
        Self {
            join_error: None,
            returned_error: None,
            bridge_fault: None,
            report_only: None,
            diagnostic: None,
        }
    }

    fn retain(&mut self, packet: JoinedSessionTask) {
        if self.join_error.is_none() {
            self.join_error = packet.joined.err();
        }
        match packet.exit {
            Some(complete @ SessionTaskExit::Complete(_)) => {
                let result = complete.into_result();
                if self.returned_error.is_none() {
                    self.returned_error = result.err();
                }
            }
            Some(SessionTaskExit::ReportOnly(payload)) if self.report_only.is_none() => {
                self.report_only = Some(payload);
            }
            _ => {}
        }
        if self.bridge_fault.is_none() {
            self.bridge_fault = packet.bridge_fault;
        }
    }
}

pub(super) struct ConnectionSessionFamily {
    pending: FuturesUnordered<OriginalSessionTask>,
    live_ready: Option<Box<JoinedSessionTask>>,
    #[expect(
        clippy::vec_box,
        reason = "terminal packet addresses survive later joins"
    )]
    finished: Vec<Box<JoinedSessionTask>>,
    failures: SessionFamilyFailures,
    retired: bool,
    finish_started: bool,
    reported: bool,
    extracted: bool,
    #[cfg(test)]
    last_reaped_id: Option<Id>,
}

impl ConnectionSessionFamily {
    pub(super) fn new() -> Self {
        Self {
            pending: FuturesUnordered::new(),
            live_ready: None,
            finished: Vec::new(),
            failures: SessionFamilyFailures::new(),
            retired: false,
            finish_started: false,
            reported: false,
            extracted: false,
            #[cfg(test)]
            last_reaped_id: None,
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub(super) fn adopt(&mut self, admitted: AdmittedSessionTask) -> Id {
        assert!(
            !self.extracted,
            "resolved family cannot adopt another original"
        );
        assert_eq!(
            admitted.id,
            admitted.task.id(),
            "receipt retains the original task Id"
        );
        let id = admitted.id;
        self.pending.push(OriginalSessionTask { admitted });
        // A late receipt is still owed reporting after an earlier empty drain.
        self.reported = false;
        id
    }

    pub(super) fn retire(&mut self) {
        self.retired = true;
    }

    fn report_packet(packet: &JoinedSessionTask) {
        if let Err(error) = &packet.joined {
            warn!(id = %packet.id, %error, "original session task failed");
        }
        if let Some(SessionTaskExit::Complete(Err(error))) = &packet.exit {
            warn!(id = %packet.id, %error, "original session ended");
        }
        if let Some(error) = &packet.bridge_fault {
            warn!(id = %packet.id, %error, "original session result missing");
        }
    }

    fn retain_diagnostic(&mut self, diagnostic: Option<PanicPayload>) {
        if self.failures.diagnostic.is_none() {
            self.failures.diagnostic = diagnostic;
        }
    }

    /// Ready packets enter custody before reporting; live success history is
    /// then consumed, retaining independent first raw failure categories.
    pub(super) async fn next(&mut self) -> bool {
        if self.retired || self.finish_started {
            return false;
        }
        let Some(packet) = self.pending.next().await else {
            return false;
        };
        #[cfg(test)]
        {
            self.last_reaped_id = Some(packet.id);
        }
        self.live_ready = Some(packet);
        let diagnostic = catch_unwind(AssertUnwindSafe(|| {
            if let Some(packet) = self.live_ready.as_ref() {
                Self::report_packet(packet);
            }
        }))
        .err();
        self.retain_diagnostic(diagnostic);
        if let Some(packet) = self.live_ready.take() {
            self.failures.retain(*packet);
        }
        true
    }

    /// Cancellation drops only this borrower. Boxes retain packet addresses
    /// while other original handles are still pending.
    pub(super) async fn finish(&mut self) -> &[Box<JoinedSessionTask>] {
        self.retire();
        self.finish_started = true;
        while let Some(packet) = self.pending.next().await {
            self.finished.push(packet);
        }
        if !self.reported {
            self.reported = true;
            let diagnostic = catch_unwind(AssertUnwindSafe(|| {
                for packet in &self.finished {
                    Self::report_packet(packet);
                }
            }))
            .err();
            self.retain_diagnostic(diagnostic);
        }
        &self.finished
    }

    pub(super) fn assert_drained(&self) {
        assert!(
            self.pending.is_empty() && self.live_ready.is_none(),
            "all original session tasks must join before resolution"
        );
        assert!(!self.extracted, "family failures are resolved once");
    }

    pub(super) fn take_failures(&mut self) -> SessionFamilyFailures {
        self.assert_drained();
        self.extracted = true;
        for packet in self.finished.drain(..) {
            self.failures.retain(*packet);
        }
        std::mem::replace(&mut self.failures, SessionFamilyFailures::new())
    }

    #[cfg(test)]
    pub(super) fn pending_ids(&self) -> Vec<Id> {
        self.pending
            .iter()
            .map(|original| original.admitted.id)
            .collect()
    }

    #[cfg(test)]
    pub(super) fn finished(&self) -> &[Box<JoinedSessionTask>] {
        &self.finished
    }

    #[cfg(test)]
    pub(super) fn live_ready_count(&self) -> usize {
        usize::from(self.live_ready.is_some())
    }

    #[cfg(test)]
    pub(super) fn failures(&self) -> &SessionFamilyFailures {
        &self.failures
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.pending.len()
    }

    #[cfg(test)]
    pub(super) fn reaped_id(&self) -> Option<Id> {
        self.last_reaped_id
    }
}

/// Prepare every fallible context/channel operation while the accepted native
/// packet remains cached. The take/spawn/external-slot region has no callback.
pub(super) async fn admit_session<B: Broker>(
    custody: &mut ConnectionCustody,
    namespace: &NamespaceName,
    broker: &B,
    authorization: &Option<Arc<ConnectionAuthorization>>,
    management: &Arc<ConnectionManagement>,
) {
    checkpoint(custody, FamilyPoint::AcceptedPacket, None).await;
    if custody.is_retired() {
        return;
    }
    let broker = broker.clone();
    let namespace = namespace.clone();
    let authorization = authorization.clone();
    let management = Arc::clone(management);
    let retirement = custody.request_handle();
    let prepared = PreparedSessionTask::new();
    checkpoint(custody, FamilyPoint::Prepared, None).await;
    if custody.is_retired() {
        return;
    }
    let session = match custody.take_packet() {
        Some(NativePacket::Accepted(Ok(session))) => session,
        None => return,
        _ => unreachable!("acceptance retains its original successful session"),
    };
    let receipt = prepared.spawn(super::serve_session_task(
        session,
        namespace,
        broker,
        authorization,
        management,
        Some(retirement),
    ));
    let id = receipt.id;
    custody.capture_session(receipt);
    checkpoint(custody, FamilyPoint::ReceiptCached, Some(id)).await;
    let adopted = custody.adopt_pending();
    checkpoint(custody, FamilyPoint::Adopted, adopted).await;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FamilyPoint {
    AcceptedPacket,
    Prepared,
    ReceiptCached,
    Adopted,
    NativeShutdownCached,
    LiveReaped,
}

pub(super) async fn checkpoint(
    custody: &mut ConnectionCustody,
    point: FamilyPoint,
    id: Option<Id>,
) {
    #[cfg(test)]
    if let Ok(observer) = FAMILY_OBSERVER.try_with(Arc::clone) {
        if point == FamilyPoint::AcceptedPacket {
            let mut slot = observer.accepted_end.lock().unwrap();
            if slot.is_none()
                && let Some(ended) = custody.accepted_end_observer()
            {
                *slot = Some(Box::pin(ended));
            }
        }
        observer
            .records
            .lock()
            .unwrap()
            .push(custody.family_facts(point, id));
        observer.reached.notify_one();
        if observer.pause == Some(point) {
            observer.release.notified().await;
            if observer.fault {
                std::panic::panic_any(Arc::clone(&observer.payload));
            }
        }
    }
    let _ = (custody, point, id);
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
pub(super) struct FamilyFacts {
    pub(super) point: FamilyPoint,
    pub(super) id: Option<Id>,
    pub(super) pending: usize,
    pub(super) live_ready: usize,
    pub(super) finished: usize,
    pub(super) accepted_cached: bool,
    pub(super) receipt_cached: bool,
    pub(super) shutdown_cached: bool,
    pub(super) packet_address: Option<usize>,
}

#[cfg(test)]
pub(super) struct FamilyObserver {
    pub(super) records: std::sync::Mutex<Vec<FamilyFacts>>,
    pub(super) reached: tokio::sync::Notify,
    pub(super) release: tokio::sync::Notify,
    pub(super) pause: Option<FamilyPoint>,
    pub(super) fault: bool,
    pub(super) payload: Arc<str>,
    pub(super) accepted_end: std::sync::Mutex<Option<Pin<Box<dyn Future<Output = ()> + Send>>>>,
}

#[cfg(test)]
impl FamilyObserver {
    pub(super) fn new(pause: Option<FamilyPoint>, fault: bool) -> Arc<Self> {
        Arc::new(Self {
            records: std::sync::Mutex::new(Vec::new()),
            reached: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            pause,
            fault,
            payload: Arc::from("controlled connection family checkpoint panic"),
            accepted_end: std::sync::Mutex::new(None),
        })
    }
}

#[cfg(test)]
tokio::task_local! { pub(super) static FAMILY_OBSERVER: Arc<FamilyObserver>; }

#[cfg(test)]
mod tests;

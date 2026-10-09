//! Original link tasks and raw results belong to their admitting session.

use std::{
    any::Any,
    error::Error,
    fmt,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::Pin,
    task::{Context, Poll},
};

use amqp::{Attach, ServerSession};
use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered};
use tokio::{
    sync::oneshot,
    task::{Id, JoinError},
};
use tracing::warn;

use super::{ConnectionRetirementRequest, control_attachment_custody::AdmittedTask};

pub(crate) type LeafResult = Result<(), Box<dyn Error + Send + Sync>>;
pub(crate) type PanicPayload = Box<dyn Any + Send>;
pub(crate) type SessionPumpResult = Result<SessionPumpExit, Box<dyn Error + Send + Sync>>;
type Primary = std::thread::Result<SessionPumpResult>;

pub(crate) enum SessionPumpExit {
    Complete,
    // Retire and drain locally without promoting diagnostics to Stop evidence.
    ReportOnly(PanicPayload),
}

#[cfg(test)]
mod tests;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LeafKind {
    DataSend,
    DataReceive,
    CbsRequest,
    CbsReply,
    ManagementRequest,
    ManagementReply,
}

pub(crate) struct PreparedLeafTask {
    kind: LeafKind,
    sender: oneshot::Sender<LeafResult>,
    result: oneshot::Receiver<LeafResult>,
}

impl PreparedLeafTask {
    /// Allocate the result channel while the endpoint still has admission custody.
    pub(crate) fn new(kind: LeafKind) -> Self {
        let (sender, result) = oneshot::channel();
        Self {
            kind,
            sender,
            result,
        }
    }

    pub(crate) fn spawn(
        self,
        leaf: impl Future<Output = LeafResult> + Send + 'static,
    ) -> AdmittedTask {
        let Self {
            kind,
            sender,
            result,
        } = self;
        let task = tokio::spawn(async move {
            let raw_result = leaf.await;
            // Old callers may keep only the unit-return original handle.
            let _ = sender.send(raw_result);
        });
        AdmittedTask { task, kind, result }
    }
}

#[derive(Debug)]
pub(crate) struct BridgeFault {
    pub(crate) id: Id,
}

impl fmt::Display for BridgeFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "original link task {} completed without its raw result",
            self.id
        )
    }
}

impl Error for BridgeFault {}

pub(crate) struct JoinedLeafTask {
    pub(crate) id: Id,
    pub(crate) kind: LeafKind,
    pub(crate) joined: Result<(), JoinError>,
    pub(crate) leaf: Option<LeafResult>,
    pub(crate) bridge_fault: Option<BridgeFault>,
}

struct OriginalLeafTask {
    id: Id,
    admitted: AdmittedTask,
}

impl Future for OriginalLeafTask {
    type Output = Box<JoinedLeafTask>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let joined = match Pin::new(&mut self.admitted.task).poll(context) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(joined) => joined,
        };
        // The task sends synchronously before returning unit. There is no second
        // waiter to cancel, and a successful join cannot precede that send.
        let leaf = self.admitted.result.try_recv().ok();
        let bridge_fault = if joined.is_ok() && leaf.is_none() {
            Some(BridgeFault { id: self.id })
        } else {
            None
        };
        Poll::Ready(Box::new(JoinedLeafTask {
            id: self.id,
            kind: self.admitted.kind,
            joined,
            leaf,
            bridge_fault,
        }))
    }
}

pub(crate) struct SessionFamily {
    pending: FuturesUnordered<OriginalLeafTask>,
    live_ready: Option<Box<JoinedLeafTask>>,
    #[expect(
        clippy::vec_box,
        reason = "terminal packet addresses survive later joins"
    )]
    finished: Vec<Box<JoinedLeafTask>>,
    first_join_error: Option<JoinError>,
    first_leaf_error: Option<Box<dyn Error + Send + Sync>>,
    first_bridge_fault: Option<BridgeFault>,
    diagnostics: Option<PanicPayload>,
    retired: bool,
    finish_started: bool,
    reported: bool,
}

impl SessionFamily {
    pub(crate) fn new() -> Self {
        Self {
            pending: FuturesUnordered::new(),
            live_ready: None,
            finished: Vec::new(),
            first_join_error: None,
            first_leaf_error: None,
            first_bridge_fault: None,
            diagnostics: None,
            retired: false,
            finish_started: false,
            reported: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.pending.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// This adopts an already spawned original, including a receipt discovered
    /// after retirement. It never starts work or applies a new admission policy.
    pub(crate) fn adopt(&mut self, admitted: AdmittedTask) -> Id {
        let id = admitted.task.id();
        self.pending.push(OriginalLeafTask { id, admitted });
        id
    }

    pub(crate) fn retire(&mut self) {
        self.retired = true;
    }

    fn report_packet(packet: &JoinedLeafTask) {
        if let Err(error) = &packet.joined {
            warn!(id = %packet.id, kind = ?packet.kind, %error, "original link task failed");
        }
        if let Some(Err(error)) = &packet.leaf {
            warn!(id = %packet.id, kind = ?packet.kind, %error, "original link ended");
        }
        if let Some(error) = &packet.bridge_fault {
            warn!(id = %packet.id, kind = ?packet.kind, %error, "original link result missing");
        }
    }

    fn consume_live(&mut self) {
        let Some(packet) = self.live_ready.take() else {
            return;
        };
        let JoinedLeafTask {
            joined,
            leaf,
            bridge_fault,
            ..
        } = *packet;
        if self.first_join_error.is_none() {
            self.first_join_error = joined.err();
        }
        if self.first_leaf_error.is_none() {
            self.first_leaf_error = leaf.and_then(Result::err);
        }
        if self.first_bridge_fault.is_none() {
            self.first_bridge_fault = bridge_fault;
        }
    }

    /// Cancellation of this observer retains the original handles. A Ready
    /// packet is cached before reporting, then success/Id history is consumed.
    pub(crate) async fn next(&mut self) -> bool {
        if self.retired || self.finish_started {
            return false;
        }
        let Some(packet) = self.pending.next().await else {
            return false;
        };
        self.live_ready = Some(packet);
        let diagnostics = catch_unwind(AssertUnwindSafe(|| {
            if let Some(packet) = self.live_ready.as_ref() {
                Self::report_packet(packet);
            }
        }))
        .err();
        if self.diagnostics.is_none() {
            self.diagnostics = diagnostics;
        }
        self.consume_live();
        true
    }

    /// Boxed terminal packets keep their addresses as later originals join.
    pub(crate) async fn finish(&mut self) -> &[Box<JoinedLeafTask>] {
        self.retire();
        self.finish_started = true;
        while let Some(packet) = self.pending.next().await {
            self.finished.push(packet);
        }
        if !self.reported {
            self.reported = true;
            let diagnostics = catch_unwind(AssertUnwindSafe(|| {
                for packet in &self.finished {
                    Self::report_packet(packet);
                }
            }))
            .err();
            if self.diagnostics.is_none() {
                self.diagnostics = diagnostics;
            }
        }
        &self.finished
    }

    #[cfg(test)]
    pub(crate) fn finished(&self) -> &[Box<JoinedLeafTask>] {
        &self.finished
    }

    pub(crate) fn into_result(self) -> LeafResult {
        self.resolve(None)
    }

    fn resolve(mut self, admission_report: Option<PanicPayload>) -> LeafResult {
        assert!(
            self.pending.is_empty() && self.live_ready.is_none(),
            "original link tasks must finish before final resolution"
        );
        for packet in self.finished {
            let JoinedLeafTask {
                joined,
                leaf,
                bridge_fault,
                ..
            } = *packet;
            if self.first_join_error.is_none() {
                self.first_join_error = joined.err();
            }
            if self.first_leaf_error.is_none() {
                self.first_leaf_error = leaf.and_then(Result::err);
            }
            if self.first_bridge_fault.is_none() {
                self.first_bridge_fault = bridge_fault;
            }
        }
        if let Some(error) = self.first_join_error {
            return Err(Box::new(error));
        }
        if let Some(error) = self.first_leaf_error {
            return Err(error);
        }
        if let Some(error) = self.first_bridge_fault {
            return Err(Box::new(error));
        }
        if let Some(payload) = admission_report.or(self.diagnostics) {
            resume_unwind(payload);
        }
        Ok(())
    }
}

pub(crate) struct SessionCustody {
    session: ServerSession,
    ended: Pin<Box<dyn Future<Output = ()> + Send>>,
    retirement: Option<ConnectionRetirementRequest>,
    requested: Pin<Box<dyn Future<Output = ()> + Send>>,
    family: SessionFamily,
    pending_admitted: Option<AdmittedTask>,
    primary: Option<Primary>,
    retired: bool,
}

impl SessionCustody {
    pub(crate) fn new(
        session: ServerSession,
        retirement: Option<ConnectionRetirementRequest>,
    ) -> Self {
        let ended = Box::pin(session.on_end_owned());
        let requested = match retirement.as_ref() {
            Some(retirement) => {
                Box::pin(retirement.observer()) as Pin<Box<dyn Future<Output = ()> + Send>>
            }
            None => Box::pin(std::future::pending()),
        };
        Self {
            session,
            ended,
            retirement,
            requested,
            family: SessionFamily::new(),
            pending_admitted: None,
            primary: None,
            retired: false,
        }
    }

    fn retire(&mut self) {
        self.retired = true;
        self.family.retire();
    }

    pub(crate) fn is_retired(&mut self) -> bool {
        if !self.retired
            && (self.session.is_ended()
                || self.ended.as_mut().now_or_never().is_some()
                || self.requested.as_mut().now_or_never().is_some())
        {
            self.retire();
        }
        self.retired
    }

    pub(crate) async fn next_attach(&mut self) -> Option<Attach> {
        loop {
            if self.is_retired() {
                return None;
            }
            let incoming = tokio::select! {
                biased;
                () = &mut self.ended => { self.retire(); return None; },
                () = &mut self.requested => { self.retire(); return None; },
                () = pump_fault(PumpPoint::Intake) => unreachable!("session fault checkpoint panics"),
                true = self.family.next(), if !self.family.is_empty() => continue,
                incoming = self.session.next_incoming_attach() => incoming,
            };
            if incoming.is_none() || self.is_retired() {
                self.retire();
                return None;
            }
            return incoming;
        }
    }

    pub(crate) fn admission_parts(
        &mut self,
    ) -> (
        &ServerSession,
        &mut Option<AdmittedTask>,
        Option<&ConnectionRetirementRequest>,
    ) {
        (
            &self.session,
            &mut self.pending_admitted,
            self.retirement.as_ref(),
        )
    }

    pub(crate) fn adopt_pending(&mut self) -> Option<(LeafKind, Id)> {
        let admitted = self.pending_admitted.take()?;
        let kind = admitted.kind;
        let id = self.family.adopt(admitted);
        Some((kind, id))
    }

    pub(crate) fn record_primary(&mut self, primary: Primary) {
        if self.primary.is_none() {
            let fault = matches!(&primary, Err(_) | Ok(Err(_)));
            self.primary = Some(primary);
            if fault && let Some(retirement) = self.retirement.as_ref() {
                retirement.request();
            }
        }
        self.retire();
    }

    pub(crate) async fn finish(&mut self) -> &[Box<JoinedLeafTask>] {
        self.retire();
        let _ = self.adopt_pending();
        self.family.finish().await
    }

    pub(crate) fn into_result(self) -> LeafResult {
        assert!(
            self.pending_admitted.is_none()
                && self.family.pending.is_empty()
                && self.family.live_ready.is_none(),
            "session must adopt and join its originals before final resolution"
        );
        match self.primary {
            Some(Err(payload)) => resume_unwind(payload),
            Some(Ok(Err(error))) => Err(error),
            Some(Ok(Ok(SessionPumpExit::Complete))) => self.family.into_result(),
            Some(Ok(Ok(SessionPumpExit::ReportOnly(payload)))) => {
                self.family.resolve(Some(payload))
            }
            None => Err(Box::new(std::io::Error::other(
                "session has no retained primary result",
            ))),
        }
    }

    #[cfg(test)]
    pub(crate) fn family(&self) -> &SessionFamily {
        &self.family
    }

    #[cfg(test)]
    pub(crate) fn report_payload(&self) -> Option<&PanicPayload> {
        match self.primary.as_ref() {
            Some(Ok(Ok(SessionPumpExit::ReportOnly(payload)))) => Some(payload),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum PumpPoint {
    Intake,
    #[cfg(test)]
    AfterReceipt,
}

async fn pump_fault(point: PumpPoint) {
    #[cfg(test)]
    if let Ok(fault) = PUMP_FAULT.try_with(std::sync::Arc::clone)
        && fault.point == point
    {
        fault.reached.notify_one();
        fault.trigger.notified().await;
        std::panic::panic_any(std::sync::Arc::clone(&fault.payload));
    }
    let _ = point;
    std::future::pending::<()>().await;
}

pub(crate) async fn adopted_checkpoint(kind: LeafKind, id: Id) {
    #[cfg(test)]
    if let Ok(observer) = ADOPTIONS.try_with(std::sync::Arc::clone) {
        observer.records.lock().unwrap().push((kind, id));
        observer.reached.notify_one();
        if observer.pause {
            observer.release.notified().await;
        }
    }
    #[cfg(test)]
    if PUMP_FAULT
        .try_with(|fault| fault.point == PumpPoint::AfterReceipt)
        .unwrap_or(false)
    {
        pump_fault(PumpPoint::AfterReceipt).await;
    }
    let _ = (kind, id);
}

#[cfg(test)]
pub(crate) struct PumpFault {
    pub(crate) point: PumpPoint,
    pub(crate) reached: tokio::sync::Notify,
    pub(crate) trigger: tokio::sync::Notify,
    pub(crate) payload: std::sync::Arc<str>,
}

#[cfg(test)]
impl PumpFault {
    pub(crate) fn new(point: PumpPoint) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            point,
            reached: tokio::sync::Notify::new(),
            trigger: tokio::sync::Notify::new(),
            payload: std::sync::Arc::from("controlled outer session panic"),
        })
    }
}

#[cfg(test)]
pub(crate) struct AdoptionObserver {
    pub(crate) records: std::sync::Mutex<Vec<(LeafKind, Id)>>,
    pub(crate) reached: tokio::sync::Notify,
    pub(crate) release: tokio::sync::Notify,
    pub(crate) pause: bool,
}

#[cfg(test)]
impl AdoptionObserver {
    pub(crate) fn new(pause: bool) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            records: std::sync::Mutex::new(Vec::new()),
            reached: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            pause,
        })
    }
}

#[cfg(test)]
tokio::task_local! {
    pub(crate) static PUMP_FAULT: std::sync::Arc<PumpFault>;
    pub(crate) static ADOPTIONS: std::sync::Arc<AdoptionObserver>;
}

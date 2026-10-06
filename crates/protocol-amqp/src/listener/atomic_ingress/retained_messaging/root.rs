use std::{
    collections::TryReserveError,
    future::{Future, pending, poll_fn},
    pin::Pin,
    sync::{Arc, Mutex, OnceLock},
    task::{Context, Poll},
    time::Duration,
};

use tokio::{
    runtime::Handle,
    sync::{Semaphore, mpsc},
    task::{JoinError, JoinHandle},
    time::MissedTickBehavior,
};

use super::super::{EVENT_CAPACITY, Event, MAX_LINKS, operations::Operation, routing::WorkerTasks};
use super::{
    admission::{self, Payload, Record},
    bridge::{Bridge, Exchange, Open},
    collector::{Collector, SessionExit},
    outcomes::{
        Data, Reason, RetainedAtomicMessagingAdmissionOutcome as AdmissionOutcome,
        RetainedAtomicMessagingBuildError as BuildError, RetainedAtomicMessagingControl as Control,
        RetainedAtomicMessagingDrain as Drain, RetainedAtomicMessagingLimits as Limits,
        RetainedAtomicMessagingReport as Report,
        RetainedAtomicMessagingSessionOutcome as SessionOutcome, locked,
    },
    worker_history::{self, Budget, Packet},
};
use crate::{
    NativeAtomicBroker,
    listener::retained_connection::{
        RetainedConnectionJoinReport, RetainedConnectionOwner, RetainedConnectionStarter,
    },
};

enum SessionSlot {
    Empty,
    Installed(JoinHandle<SessionExit>),
    Joined {
        handle: JoinHandle<SessionExit>,
        original: Result<SessionExit, JoinError>,
        drain: Drain,
    },
}

struct Slot {
    record: Option<Record>,
    admission: Option<AdmissionOutcome>,
    session: SessionSlot,
    session_ordinal: Option<usize>,
    abort_requested: bool,
    cell: Arc<worker_history::Cell>,
    loan: Option<worker_history::Loan>,
    packet: Option<Packet>,
}

struct Reserved {
    slots: Vec<Slot>,
    cells: Vec<Arc<admission::Cell>>,
    admissions: Vec<AdmissionOutcome>,
    sessions: Vec<SessionOutcome>,
    packets: Vec<Packet>,
}

impl Reserved {
    fn new(limits: Limits) -> Result<Self, TryReserveError> {
        let sessions_limit = limits.session_attempts();
        let mut slots = Vec::new();
        slots.try_reserve_exact(sessions_limit)?;
        let mut cells = Vec::new();
        cells.try_reserve_exact(sessions_limit)?;
        let mut admissions = Vec::new();
        admissions.try_reserve_exact(sessions_limit + 1)?;
        let mut sessions = Vec::new();
        sessions.try_reserve_exact(sessions_limit)?;
        let mut packets = Vec::new();
        packets.try_reserve_exact(sessions_limit)?;
        for _ in 0..sessions_limit {
            let packet = Packet::try_new(limits.worker_history())?;
            slots.push(Slot {
                record: None,
                admission: None,
                session: SessionSlot::Empty,
                session_ordinal: None,
                abort_requested: false,
                cell: worker_history::Cell::new(packet),
                loan: None,
                packet: None,
            });
            cells.push(admission::Cell::new());
        }
        Ok(Self {
            slots,
            cells,
            admissions,
            sessions,
            packets,
        })
    }
}

/// Caller-driven custody for one opt-in socket and its finite original history.
#[must_use = "retain and finish on the captured live, driven runtime"]
pub struct RetainedAtomicMessagingOwner<A, B: NativeAtomicBroker> {
    holder: Option<Box<Root<A, B>>>,
}

/// Unique consuming capability; dropping it unused creates no role.
#[must_use = "start once while retaining the separate owner"]
pub struct RetainedAtomicMessagingStarter<B: NativeAtomicBroker> {
    pub(super) socket: RetainedConnectionStarter,
    pub(super) bridge: Bridge<B>,
}

struct Root<A, B: NativeAtomicBroker> {
    anchor: Option<A>,
    runtime: Handle,
    socket: RetainedConnectionOwner<()>,
    socket_report: Option<RetainedConnectionJoinReport<()>>,
    exchange: Arc<Exchange<B>>,
    data: Arc<Data>,
    workers: Arc<Budget>,
    admissions_budget: Arc<Budget>,
    slots: Vec<Slot>,
    output_admissions: Vec<AdmissionOutcome>,
    output_sessions: Vec<SessionOutcome>,
    output_packets: Vec<Packet>,
    incoming: mpsc::Receiver<Event>,
    sender: Option<mpsc::Sender<Event>>,
    links: Arc<Semaphore>,
    open_pending: Option<Open<B>>,
    collector: Option<Collector<B>>,
    tick: tokio::time::Interval,
    external: bool,
    creator_extracted: bool,
    pending_admission_ready: Option<usize>,
    pending_session_ready: Option<usize>,
    #[cfg(test)]
    hooks: Arc<super::outcomes::Hooks>,
}

impl<A, B: NativeAtomicBroker> RetainedAtomicMessagingOwner<A, B> {
    /// Allocate finite append-only vectors before publishing the starter.
    /// Other standard/native allocations are not a fallible-allocation or RSS proof.
    pub fn new(
        runtime: Handle,
        limits: Limits,
        anchor: A,
    ) -> Result<(Self, RetainedAtomicMessagingStarter<B>), BuildError<A>> {
        let reserved = match Reserved::new(limits) {
            Ok(reserved) => reserved,
            Err(reserve) => return Err(BuildError { anchor, reserve }),
        };
        let data = Data::new();
        let workers = Budget::new(limits.worker_history());
        let admissions_budget = Budget::new(limits.session_attempts());
        #[cfg(test)]
        let hooks = Arc::new(super::outcomes::Hooks::default());
        let exchange = Arc::new(Exchange {
            open: Mutex::new(None),
            cells: reserved.cells,
            overflow: Mutex::new(None),
            close: Arc::new(OnceLock::new()),
            data: Arc::clone(&data),
            admissions: Arc::clone(&admissions_budget),
            #[cfg(test)]
            hooks: Arc::clone(&hooks),
        });
        let (socket, starter) = RetainedConnectionOwner::new(runtime.clone(), ());
        let (sender, incoming) = mpsc::channel(EVENT_CAPACITY);
        let tick = {
            let _entered = runtime.enter();
            let mut tick = tokio::time::interval(Duration::from_millis(100));
            tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
            tick
        };
        // This one opaque holder already exists when the capability becomes public.
        let holder = Box::new(Root {
            anchor: Some(anchor),
            runtime,
            socket,
            socket_report: None,
            exchange: Arc::clone(&exchange),
            data,
            workers,
            admissions_budget,
            slots: reserved.slots,
            output_admissions: reserved.admissions,
            output_sessions: reserved.sessions,
            output_packets: reserved.packets,
            incoming,
            sender: Some(sender),
            links: Arc::new(Semaphore::new(MAX_LINKS)),
            open_pending: None,
            collector: None,
            tick,
            external: false,
            creator_extracted: false,
            pending_admission_ready: None,
            pending_session_ready: None,
            #[cfg(test)]
            hooks,
        });
        Ok((
            Self {
                holder: Some(holder),
            },
            RetainedAtomicMessagingStarter {
                socket: starter,
                bridge: Bridge { exchange },
            },
        ))
    }

    pub fn control(&self) -> Control {
        Control {
            data: Arc::clone(
                &self
                    .holder
                    .as_ref()
                    .expect("unreported retained owner")
                    .data,
            ),
        }
    }

    /// A data request alone is inert; this operation closes authority before cancellation.
    pub fn stop(&mut self) {
        if let Some(root) = &mut self.holder {
            root.stop();
        }
    }

    /// One borrowed pump step. Losing the loan leaves original tokens and Ready values here.
    pub async fn drive_step(&mut self) {
        if let Some(root) = &mut self.holder {
            root.step().await;
        }
    }

    /// One-shot actual covered-role joins; no broker, provider or native-buffer certificate.
    pub async fn finish(&mut self) -> Option<Report<A>> {
        let root = self.holder.as_mut()?;
        root.stop();
        while !root.complete() {
            root.prepare().await;
            if root.complete() {
                break;
            }
            if root.socket_report.is_none() {
                let ready = {
                    let Root {
                        socket,
                        socket_report,
                        slots,
                        collector,
                        incoming,
                        tick,
                        data,
                        external,
                        pending_session_ready,
                        ..
                    } = root.as_mut();
                    // Cache the exact socket report inside Ready before the borrowed future drops.
                    let mut original = std::pin::pin!(socket.finish());
                    let observed = poll_fn(|cx| match original.as_mut().poll(cx) {
                        Poll::Pending => Poll::Pending,
                        Poll::Ready(report) => {
                            *socket_report =
                                Some(report.expect("one unreported original socket root"));
                            Poll::Ready(())
                        }
                    });
                    tokio::pin!(observed);
                    // Root fields are borrowed independently; no new pump or aggregate task.
                    tokio::select! {
                        () = &mut observed => Step::Socket,
                        ready = select_step(slots, collector, incoming, tick, data, *external, pending_session_ready) => ready,
                    }
                };
                root.apply(ready).await;
            } else {
                root.step().await;
            }
        }
        let report = self
            .holder
            .as_mut()
            .expect("completed one-shot original holder")
            .report();
        self.holder.take();
        Some(report)
    }

    #[cfg(test)]
    pub(super) fn hooks(&self) -> Arc<super::outcomes::Hooks> {
        Arc::clone(&self.holder.as_ref().expect("unreported owner").hooks)
    }
    #[cfg(test)]
    pub(super) fn original_session_ids(&self) -> Vec<tokio::task::Id> {
        self.holder
            .as_ref()
            .expect("unreported owner")
            .slots
            .iter()
            .filter_map(|slot| match &slot.session {
                SessionSlot::Installed(handle) | SessionSlot::Joined { handle, .. } => {
                    Some(handle.id())
                }
                SessionSlot::Empty => None,
            })
            .collect()
    }

    #[cfg(test)]
    pub(super) fn native_address(&self, index: usize) -> Option<usize> {
        let root = self.holder.as_ref().expect("unreported owner");
        root.slots[index]
            .record
            .as_ref()
            .map(|record| record.original.as_ref().get_ref() as *const _ as *const () as usize)
            .or_else(|| {
                root.slots[index]
                    .admission
                    .as_ref()
                    .and_then(|row| row._original.as_ref())
                    .map(|original| original.as_ref().get_ref() as *const _ as *const () as usize)
            })
            .or_else(|| root.exchange.cells[index].original_address())
    }

    #[cfg(test)]
    pub(super) fn poll_native_once(&mut self, index: usize, cx: &mut Context<'_>) -> Poll<()> {
        let root = self.holder.as_mut().expect("unreported owner");
        root.transfer();
        root.slots[index]
            .record
            .as_mut()
            .expect("same original native record")
            .poll_original(cx)
    }

    #[cfg(test)]
    pub(super) fn abort_session_without_request(&mut self, index: usize) {
        let root = self.holder.as_ref().expect("unreported owner");
        let SessionSlot::Installed(handle) = &root.slots[index].session else {
            panic!("original installed Session token");
        };
        handle.abort();
    }

    #[cfg(test)]
    pub(super) fn abort_wrapper_for_test(&self) {
        self.holder
            .as_ref()
            .expect("unreported owner")
            .socket
            .abort_wrapper();
    }

    #[cfg(test)]
    pub(super) fn original_authorization(
        &self,
    ) -> Option<Arc<crate::authorization::ConnectionAuthorization>> {
        let root = self.holder.as_ref().expect("unreported owner");
        root.open_pending
            .as_ref()
            .and_then(|open| open.authorization.clone())
            .or_else(|| {
                root.collector
                    .as_ref()
                    .and_then(|collector| collector.open.authorization.clone())
            })
    }

    #[cfg(test)]
    pub(super) fn original_deadline(&self) -> Option<tokio::time::Instant> {
        let root = self.holder.as_ref().expect("unreported owner");
        root.open_pending
            .as_ref()
            .and_then(|open| open.deadline)
            .or_else(|| {
                root.collector
                    .as_ref()
                    .and_then(|collector| collector.open.deadline)
            })
    }

    #[cfg(test)]
    pub(super) fn original_open_address(&self) -> Option<usize> {
        self.holder
            .as_ref()
            .expect("unreported owner")
            .open_pending
            .as_ref()
            .map(|open| open as *const Open<B> as usize)
    }

    #[cfg(test)]
    pub(super) fn native_ready_address(&self, index: usize) -> Option<usize> {
        self.holder.as_ref().expect("unreported owner").slots[index]
            .record
            .as_ref()
            .and_then(|record| record.result.as_ref())
            .and_then(|result| result.as_ref().ok())
            .map(|session| session as *const amqp::ServerSession as usize)
    }

    #[cfg(test)]
    pub(super) fn incoming_is_rooted(&self, index: usize) -> bool {
        let root = self.holder.as_ref().expect("unreported owner");
        root.exchange.cells[index].has_incoming()
    }

    #[cfg(test)]
    pub(super) fn budget_counts(&self) -> ((usize, usize, bool), (usize, usize, bool)) {
        let root = self.holder.as_ref().expect("unreported owner");
        (root.admissions_budget.counts(), root.workers.counts())
    }

    #[cfg(test)]
    pub(super) fn restored_worker_ids(&self, index: usize) -> Vec<tokio::task::Id> {
        self.holder.as_ref().expect("unreported owner").slots[index]
            .cell
            .original_ids()
    }
}

impl<A, B: NativeAtomicBroker> Drop for RetainedAtomicMessagingOwner<A, B> {
    fn drop(&mut self) {
        if let Some(original) = self.holder.take() {
            struct Permanent<T>(Option<Box<T>>);
            impl<T> Drop for Permanent<T> {
                fn drop(&mut self) {
                    if let Some(original) = self.0.take() {
                        std::mem::forget(original);
                    }
                }
            }
            // Arm the original holder before a user/backend destructor can unwind in stop.
            let mut permanent = Permanent(Some(original));
            permanent
                .0
                .as_mut()
                .expect("original abandonment holder")
                .stop();
            // Deliberate permanent refusal: tokens, raw values and the original anchor remain together.
            // No detached rescue task or reusable acceptance capacity is manufactured by Drop.
            drop(permanent);
        }
    }
}

impl<A, B: NativeAtomicBroker> std::fmt::Debug for RetainedAtomicMessagingOwner<A, B> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RetainedAtomicMessagingOwner")
            .field("reported", &self.holder.is_none())
            .finish_non_exhaustive()
    }
}
impl<B: NativeAtomicBroker> std::fmt::Debug for RetainedAtomicMessagingStarter<B> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RetainedAtomicMessagingStarter")
            .finish_non_exhaustive()
    }
}

enum Step {
    Admission(usize),
    Session(usize),
    Worker,
    Event(Option<Event>),
    Operation(Operation),
    Tick,
    Changed,
    Socket,
}

fn poll_admissions(slots: &mut [Slot], cx: &mut Context<'_>) -> Poll<usize> {
    for (index, slot) in slots.iter_mut().enumerate() {
        if let Some(record) = &mut slot.record
            && record.poll_original(cx).is_ready()
        {
            return Poll::Ready(index);
        }
    }
    Poll::Pending
}

fn poll_sessions(
    slots: &mut [Slot],
    data: &Data,
    external: bool,
    pending_ready: &mut Option<usize>,
    cx: &mut Context<'_>,
) -> Poll<usize> {
    for (index, slot) in slots.iter_mut().enumerate() {
        let original = match &mut slot.session {
            SessionSlot::Installed(handle) => match Pin::new(handle).poll(cx) {
                Poll::Pending => continue,
                Poll::Ready(original) => original,
            },
            _ => continue,
        };
        let SessionSlot::Installed(handle) =
            std::mem::replace(&mut slot.session, SessionSlot::Empty)
        else {
            unreachable!()
        };
        let drain = if external {
            Drain::External
        } else if data.progress().reason.is_some() {
            Drain::PeerEnd
        } else {
            Drain::Live
        };
        slot.session = SessionSlot::Joined {
            handle,
            original,
            drain,
        };
        *pending_ready = Some(index);
        data.update(|state| state.session_joins += 1);
        // Raw original Ready is installed before the observer/classifier can suspend.
        return Poll::Ready(index);
    }
    Poll::Pending
}

fn poll_workers(slots: &mut [Slot], cx: &mut Context<'_>) -> Poll<()> {
    for slot in slots {
        if let Some(loan) = &mut slot.loan
            && !loan.is_empty()
            && matches!(loan.poll_next(cx), Poll::Ready(Some(_)))
        {
            return Poll::Ready(());
        }
    }
    Poll::Pending
}

fn poll_owned(
    slots: &mut [Slot],
    data: &Data,
    external: bool,
    pending_ready: &mut Option<usize>,
    cx: &mut Context<'_>,
) -> Poll<Step> {
    if let Poll::Ready(index) = poll_admissions(slots, cx) {
        return Poll::Ready(Step::Admission(index));
    }
    if let Poll::Ready(index) = poll_sessions(slots, data, external, pending_ready, cx) {
        return Poll::Ready(Step::Session(index));
    }
    if poll_workers(slots, cx).is_ready() {
        return Poll::Ready(Step::Worker);
    }
    Poll::Pending
}

async fn select_step<B: NativeAtomicBroker>(
    slots: &mut [Slot],
    collector: &mut Option<Collector<B>>,
    incoming: &mut mpsc::Receiver<Event>,
    tick: &mut tokio::time::Interval,
    data: &Arc<Data>,
    external: bool,
    pending_ready: &mut Option<usize>,
) -> Step {
    let closed = data.progress().authority_closed;
    let operation = async {
        if let Some(collector) = collector {
            collector.owner.next_operation().await
        } else {
            pending().await
        }
    };
    tokio::select! {
        ready = poll_fn(|cx| poll_owned(slots, data, external, pending_ready, cx)) => ready,
        event = incoming.recv() => Step::Event(event),
        Some(operation) = operation, if !closed => Step::Operation(operation),
        _ = tick.tick() => Step::Tick,
        _ = data.changed.notified() => Step::Changed,
    }
}

impl<A, B: NativeAtomicBroker> Root<A, B> {
    fn close_authority(&mut self) {
        self.admissions_budget.seal();
        self.workers.seal();
        self.data.update(|state| state.sealed = true);
        if let Some(collector) = &mut self.collector {
            collector.owner.close();
        }
        for slot in &mut self.slots {
            if let Some(record) = &mut slot.record {
                record.ticket.take();
            }
        }
        // Original Owner registry/rows are closed before any native stop or acknowledgment.
        self.data.update(|state| state.authority_closed = true);
    }

    fn stop(&mut self) {
        self.external = true;
        self.data.update(|state| state.stop_requested = true);
        self.close_authority();
        for slot in &mut self.slots {
            if let SessionSlot::Installed(handle) = &slot.session
                && !slot.abort_requested
            {
                slot.abort_requested = true;
                handle.abort();
            }
            if let Some(loan) = &mut slot.loan {
                loan.external_drain();
            }
        }
        // This existing socket stop is cooperative, not an Actor/Reader/Wrapper abort fact.
        self.socket.stop();
    }

    async fn bind(&mut self) {
        if self.collector.is_some() || self.data.progress().authority_closed {
            return;
        }
        if self.open_pending.is_none() {
            self.open_pending = self.exchange.take_open();
        }
        if self.open_pending.is_none() {
            return;
        }
        #[cfg(test)]
        self.hooks.binding.hold().await;
        if self.data.progress().authority_closed {
            return;
        }
        let initial_expired = if let Some(open) = &self.open_pending {
            if let (Some(authorization), Some(deadline)) = (&open.authorization, open.deadline) {
                tokio::time::Instant::now() >= deadline
                    && matches!(
                        authorization.initial_control_state().await,
                        crate::authorization::InitialControlState::InitialExpired
                    )
            } else {
                false
            }
        } else {
            false
        };
        if initial_expired {
            self.data.natural(Reason::Expired);
            self.close_authority();
            return;
        }
        // Arbitrary Broker::clone may unwind; keep the exact Open rooted until it returns.
        let owner = Collector::prepare_owner(
            self.open_pending
                .as_ref()
                .expect("rooted original Open context"),
        );
        let open = self
            .open_pending
            .take()
            .expect("one retained original Open context");
        self.collector = Some(Collector::new(
            open,
            owner,
            self.sender.as_ref().expect("original event sender").clone(),
            Arc::clone(&self.links),
        ));
        self.data.update(|state| state.bound = true);
    }

    fn transfer(&mut self) {
        for (slot, cell) in self.slots.iter_mut().zip(&self.exchange.cells) {
            if slot.record.is_none()
                && slot.admission.is_none()
                && let Some(mut record) = cell.take_record()
            {
                if self.data.progress().sealed {
                    record.ticket.take();
                }
                slot.record = Some(record);
            }
        }
    }

    fn accepted(&mut self, index: usize) {
        // Clone launch inputs before moving any original native Ready result out of its slot.
        let launch = if !self.data.progress().sealed
            && self.slots[index]
                .record
                .as_ref()
                .and_then(|record| record.result.as_ref())
                .is_some_and(Result::is_ok)
        {
            Some(
                self.collector
                    .as_ref()
                    .expect("bound original Owner before native admission")
                    .prepare_launch(),
            )
        } else {
            None
        };
        let mut record = self.slots[index]
            .record
            .take()
            .expect("rooted original acceptance");
        let result = record
            .result
            .take()
            .expect("actual native acceptance Ready");
        let mut error = None;
        let mut unlaunched = None;
        let mut launched = None;
        let mut close_after_install = false;
        match result {
            Err(original) => {
                let detached = matches!(original, amqp::EngineError::RemoteDetached);
                error = Some(original);
                record.ticket.take();
                close_after_install = !detached && !self.external;
            }
            Ok(session) => {
                let claim = if self.data.progress().sealed {
                    None
                } else {
                    record.ticket.take().and_then(|ticket| ticket.claim().ok())
                };
                if let Some(claim) = claim {
                    let loan = self.slots[index]
                        .cell
                        .loan(
                            Arc::clone(&self.workers),
                            self.runtime.clone(),
                            Arc::clone(&self.data),
                            #[cfg(test)]
                            Arc::clone(&self.hooks),
                        )
                        .expect("original unborrowed worker packet before Session launch");
                    let future = launch
                        .expect("prepared original launch context before native Ready move")
                        .run(session, loan);
                    let handle = self.runtime.spawn(future);
                    let id = handle.id();
                    self.slots[index].session = SessionSlot::Installed(handle);
                    let ordinal = claim.commit();
                    self.slots[index].session_ordinal = Some(ordinal);
                    launched = Some((id, ordinal));
                } else {
                    record.ticket.take();
                    unlaunched = Some(session);
                }
            }
        }
        self.slots[index].admission = Some(record.into_outcome(error, launched, unlaunched));
        self.pending_admission_ready = Some(index);
        // Owner closure can run backend destructors; every disposition is already rooted.
        if close_after_install {
            self.data.natural(Reason::Worker);
            self.close_authority();
        }
    }

    async fn classify_pending(&mut self) {
        if self.pending_admission_ready.is_some() {
            #[cfg(test)]
            self.hooks.admission_ready.hold().await;
            self.pending_admission_ready = None;
        }
        if let Some(index) = self.pending_session_ready {
            #[cfg(test)]
            self.hooks.session_ready.hold().await;
            let failed = matches!(&self.slots[index].session, SessionSlot::Joined { original, .. }
                if !matches!(original, Ok(SessionExit::Completed)));
            if failed && !self.external {
                self.data.natural(Reason::Worker);
                self.close_authority();
            }
            // Clear only after the saved original callback/classification actually returns.
            self.pending_session_ready = None;
        }
    }

    fn acquire_drains(&mut self) {
        for slot in &mut self.slots {
            if slot.loan.is_none()
                && slot.packet.is_none()
                && (self.external || matches!(slot.session, SessionSlot::Joined { .. }))
                && !matches!(slot.session, SessionSlot::Installed(_))
            {
                slot.loan = slot.cell.loan(
                    Arc::clone(&self.workers),
                    self.runtime.clone(),
                    Arc::clone(&self.data),
                    #[cfg(test)]
                    Arc::clone(&self.hooks),
                );
                if self.external
                    && let Some(loan) = &mut slot.loan
                {
                    loan.external_drain();
                }
            }
            if slot.loan.as_ref().is_some_and(WorkerTasks::is_empty) {
                slot.packet = Some(
                    slot.loan
                        .take()
                        .expect("joined original worker loan")
                        .finish(),
                );
            }
        }
    }

    fn extract_creator(&mut self) {
        if self.creator_extracted || self.socket_report.is_none() {
            return;
        }
        self.transfer();
        for (index, cell) in self.exchange.cells.iter().enumerate() {
            match cell.take_after_creator().payload {
                Payload::Empty | Payload::Transferred => {}
                Payload::Incoming(incoming) => {
                    self.slots[index].admission = Some(AdmissionOutcome {
                        ordinal: index,
                        error: None,
                        launched: None,
                        _original: None,
                        _incoming: Some(incoming),
                        _unlaunched: None,
                    });
                }
                Payload::Admission(mut record) => {
                    record.ticket.take();
                    assert!(
                        self.slots[index].record.is_none(),
                        "one exact native admission handoff"
                    );
                    self.slots[index].record = Some(record);
                }
            }
        }
        if let Some(incoming) = locked(&self.exchange.overflow).take() {
            self.output_admissions.push(AdmissionOutcome {
                ordinal: self.slots.len(),
                error: None,
                launched: None,
                _original: None,
                _incoming: Some(incoming),
                _unlaunched: None,
            });
        }
        self.creator_extracted = true;
    }

    async fn prepare(&mut self) {
        self.bind().await;
        self.transfer();
        let state = self.data.progress();
        if state.stop_requested && !self.external {
            self.stop();
        } else if state.reason.is_some() && !state.authority_closed {
            self.close_authority();
        }
        self.extract_creator();
        self.classify_pending().await;
        self.acquire_drains();
    }

    async fn step(&mut self) {
        self.prepare().await;
        let ready = select_step(
            &mut self.slots,
            &mut self.collector,
            &mut self.incoming,
            &mut self.tick,
            &self.data,
            self.external,
            &mut self.pending_session_ready,
        )
        .await;
        self.apply(ready).await;
    }

    async fn apply(&mut self, ready: Step) {
        match ready {
            Step::Admission(index) => self.accepted(index),
            Step::Session(index) => assert_eq!(
                self.pending_session_ready,
                Some(index),
                "classification rooted with original Ready"
            ),
            Step::Worker | Step::Changed | Step::Socket => {}
            Step::Event(Some(Event::StopConnection { reply })) => {
                if !self.external {
                    self.data.natural(Reason::Worker);
                }
                self.close_authority();
                let _ = reply.send(());
            }
            Step::Event(Some(event)) => {
                if let Some(collector) = &mut self.collector {
                    collector.owner.process(event);
                }
            }
            Step::Event(None) => {}
            Step::Operation(operation) => {
                self.collector
                    .as_mut()
                    .expect("original bound Owner operation")
                    .owner
                    .accept_completion(operation);
            }
            Step::Tick => {
                if !self.data.progress().authority_closed
                    && let Some(collector) = &mut self.collector
                {
                    collector.owner.tick();
                }
            }
        }
        self.prepare().await;
    }

    fn complete(&self) -> bool {
        self.socket_report.is_some()
            && self.creator_extracted
            && self.pending_session_ready.is_none()
            && self.pending_admission_ready.is_none()
            && self.slots.iter().all(|slot| {
                slot.record.is_none()
                    && !matches!(slot.session, SessionSlot::Installed(_))
                    && slot.packet.is_some()
            })
    }

    fn report(&mut self) -> Report<A> {
        assert!(
            self.complete(),
            "all actual covered original roles joined before report publication"
        );
        self.incoming.close();
        self.sender.take();
        self.collector.take();
        while let Ok(event) = self.incoming.try_recv() {
            drop(event);
        }
        for slot in &mut self.slots {
            if let Some(admission) = slot.admission.take() {
                self.output_admissions.push(admission);
            }
            let session = std::mem::replace(&mut slot.session, SessionSlot::Empty);
            if let SessionSlot::Joined {
                handle,
                original,
                drain,
            } = session
            {
                self.output_sessions.push(SessionOutcome {
                    id: handle.id(),
                    ordinal: slot.session_ordinal.expect("original session ordinal"),
                    abort_requested: slot.abort_requested,
                    drain,
                    original,
                });
                drop(handle);
            }
            let packet = slot
                .packet
                .take()
                .expect("actual joined original worker packet");
            assert_eq!(
                packet.rows.len(),
                packet.launches.len(),
                "every original worker token has one raw returned row"
            );
            self.output_packets.push(packet);
        }
        self.output_admissions
            .sort_unstable_by_key(AdmissionOutcome::ordinal);
        self.data.update(|state| state.reported = true);
        Report {
            socket: self
                .socket_report
                .take()
                .expect("original socket join report"),
            anchor: self.anchor.take().expect("original one-shot caller anchor"),
            admissions: std::mem::take(&mut self.output_admissions),
            sessions: std::mem::take(&mut self.output_sessions),
            packets: std::mem::take(&mut self.output_packets),
            close: Arc::clone(&self.exchange.close),
            attempts: self.data.progress().session_attempts,
        }
    }
}

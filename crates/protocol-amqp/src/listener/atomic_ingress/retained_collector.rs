use std::{
    future::poll_fn,
    sync::{Arc, atomic::Ordering},
    task::{Context, Poll},
    time::Duration,
};

use amqp::{EngineError, IncomingSession, NativeConnectionIdentity, ServerConnection};
use domain::NamespaceName;
use tokio::{
    runtime::Handle,
    sync::{Semaphore, mpsc, oneshot},
    task::{Id, JoinError},
    time::MissedTickBehavior,
};

use super::{
    EVENT_CAPACITY, Event, IngressMode, MAX_LINKS,
    operations::Operation,
    owner::Owner,
    retained_session::{
        self, SessionExit, SessionSlot,
        budget::{Budget, Closed},
        packet::{Cell, Loan, Packet},
    },
    routing::WorkerTasks,
};
use crate::{NativeAtomicBroker, authorization::ConnectionAuthorization};

mod admissions;
mod controls;
mod tests;

use admissions::{Outcome, Record};
use controls::Controls;

pub(super) struct Settings<B> {
    pub(super) identity: NativeConnectionIdentity,
    pub(super) namespace: NamespaceName,
    pub(super) broker: B,
    pub(super) authorization: Option<Arc<ConnectionAuthorization>>,
    pub(super) mode: IngressMode,
    pub(super) runtime: Handle,
    controls: Arc<Controls>,
}

pub(super) struct Refused<A, B> {
    pub(super) settings: Settings<B>,
    pub(super) anchor: A,
    pub(super) limit: usize,
}
pub(super) struct OfferRefused {
    pub(super) incoming: Box<IncomingSession>,
    pub(super) cause: Closed,
}
pub(super) struct SessionReport {
    pub(super) id: Id,
    pub(super) original: Result<SessionExit, JoinError>,
}
pub(super) struct Report<A> {
    admissions: [Option<Record>; 2],
    pub(super) sessions: [Option<SessionReport>; 2],
    pub(super) packets: [Packet; 2],
    pub(super) attempts: usize,
    pub(super) session_counts: (usize, usize, bool),
    pub(super) worker_counts: (usize, usize, bool),
    pub(super) anchor: A,
}

// Private one-shot root. Caller retains/drives root and captured Runtime A.
pub(super) struct Root<A, B: NativeAtomicBroker> {
    settings: Settings<B>,
    admissions: [Option<Record>; 2],
    sessions: [SessionSlot; 2],
    cells: [Arc<Cell>; 2],
    session_budget: Arc<Budget>,
    worker_budget: Arc<Budget>,
    links: Arc<Semaphore>,
    attempts: usize,
    closed: bool,
    owner: Option<Owner<B>>,
    incoming: Option<mpsc::Receiver<Event>>,
    sender: Option<mpsc::Sender<Event>>,
    acknowledgments: [Option<oneshot::Sender<()>>; 2],
    anchor: Option<A>,
}

impl<A, B: NativeAtomicBroker> Root<A, B> {
    pub(super) fn new(
        settings: Settings<B>,
        limit: usize,
        anchor: A,
    ) -> Result<Self, Box<Refused<A, B>>> {
        if !(1..=MAX_LINKS).contains(&limit) {
            return Err(Box::new(Refused {
                settings,
                anchor,
                limit,
            }));
        }
        let owner = Owner::new(settings.identity.clone(), settings.broker.clone());
        let (sender, incoming) = mpsc::channel(EVENT_CAPACITY);
        Ok(Self {
            settings,
            admissions: [None, None],
            sessions: [SessionSlot::NotLaunched, SessionSlot::NotLaunched],
            cells: [Cell::new(limit), Cell::new(limit)],
            session_budget: Budget::new(2),
            worker_budget: Budget::new(limit),
            links: Arc::new(Semaphore::new(MAX_LINKS)),
            attempts: 0,
            closed: false,
            owner: Some(owner),
            incoming: Some(incoming),
            sender: Some(sender),
            acknowledgments: [None, None],
            anchor: Some(anchor),
        })
    }
    pub(super) fn counts(&self) -> (usize, (usize, usize, bool), (usize, usize, bool)) {
        (
            self.attempts,
            self.session_budget.counts(),
            self.worker_budget.counts(),
        )
    }
    pub(super) fn offer(
        &mut self,
        connection: &ServerConnection,
        incoming: IncomingSession,
    ) -> Result<usize, OfferRefused> {
        self.offer_with(connection, incoming, |future| future)
    }
    pub(super) fn offer_with<F>(
        &mut self,
        connection: &ServerConnection,
        incoming: IncomingSession,
        wrap: F,
    ) -> Result<usize, OfferRefused>
    where
        F: FnOnce(admissions::Original) -> admissions::Original,
    {
        if self.closed || self.attempts == 2 {
            return Err(OfferRefused {
                incoming: Box::new(incoming),
                cause: if self.closed {
                    Closed::Sealed
                } else {
                    Closed::Budget
                },
            });
        }
        let ticket = match self.session_budget.reserve() {
            Ok(ticket) => ticket,
            Err(cause) => {
                return Err(OfferRefused {
                    incoming: Box::new(incoming),
                    cause,
                });
            }
        };
        let index = self.attempts;
        let original = Box::pin(connection.accept_session(incoming));
        self.admissions[index] = Some(Record::new(wrap(original), ticket));
        self.attempts += 1;
        Ok(index)
    }
    pub(super) fn stop(&mut self) {
        self.close_authority();
        for slot in &self.sessions {
            if let SessionSlot::Installed(handle) = slot {
                handle.abort();
            }
        }
    }
    fn close_authority(&mut self) {
        self.session_budget.seal();
        self.worker_budget.seal();
        self.closed = true;
        if let Some(owner) = &mut self.owner {
            owner.close();
        }
        self.settings
            .controls
            .authority_closed
            .store(true, Ordering::SeqCst);
        for controls in &self.settings.controls.sessions {
            controls.authority_closed.store(true, Ordering::SeqCst);
        }
        for record in self.admissions.iter_mut().flatten() {
            drop(record.ticket.take());
        }
    }
    fn poll_admissions(&mut self, cx: &mut Context<'_>) -> Poll<usize> {
        poll_admission_records(&mut self.admissions, self.closed, cx)
    }
    async fn accepted(&mut self, index: usize) {
        self.settings.controls.admission_ready[index].hold().await;
        let record = self.admissions[index]
            .as_mut()
            .expect("installed admission");
        if self.closed {
            record.classified = true;
            return;
        }
        if let Outcome::Observed(Err(error)) = &record.outcome {
            let detached = matches!(error, EngineError::RemoteDetached);
            record.classified = true;
            drop(record.ticket.take());
            if !detached {
                self.close_authority();
            }
            return;
        }
        let claim = match record
            .ticket
            .take()
            .expect("reserved session ticket")
            .claim()
        {
            Ok(claim) => claim,
            Err(_) => {
                record.classified = true;
                return;
            }
        };
        let original = std::mem::replace(&mut record.outcome, Outcome::Pending);
        let Outcome::Observed(Ok(session)) = original else {
            unreachable!("accepted session")
        };
        let packet = self.cells[index].loan(
            self.worker_budget.clone(),
            self.settings.runtime.clone(),
            self.settings.controls.sessions[index].clone(),
        );
        let future = retained_session::scoped_session(
            session,
            self.settings.namespace.clone(),
            self.settings.broker.clone(),
            self.settings.authorization.clone(),
            self.sender.as_ref().expect("retained sender").clone(),
            self.links.clone(),
            self.settings.mode,
            packet,
            self.settings.controls.sessions[index].clone(),
        );
        // No await/callback between accepted claim, normally returned spawn and installation.
        let handle = self.settings.runtime.spawn(future);
        let id = handle.id();
        self.sessions[index] = SessionSlot::Installed(handle);
        let ordinal = claim.commit();
        record.outcome = Outcome::Launched { id, ordinal };
        record.classified = true;
    }
    async fn session_ready(&mut self, index: usize) {
        self.settings.controls.session_ready[index].hold().await;
        let failed = matches!(&self.sessions[index], SessionSlot::Joined { original, .. }
            if !matches!(original, Ok(SessionExit::Completed)));
        if failed {
            self.close_authority();
        }
    }
    async fn flush_acknowledgments(&mut self) {
        if self.acknowledgments.iter().any(Option::is_some) {
            self.settings.controls.close_ack.hold().await;
            for slot in &mut self.acknowledgments {
                if let Some(reply) = slot.take() {
                    let _ = reply.send(());
                }
            }
        }
    }
    async fn process(&mut self, event: Event) {
        if let Event::StopConnection { reply } = event {
            self.close_authority();
            let slot = self
                .acknowledgments
                .iter_mut()
                .find(|slot| slot.is_none())
                .expect("two fixed stop acknowledgments");
            *slot = Some(reply);
            self.flush_acknowledgments().await;
        } else if let Some(owner) = &mut self.owner {
            if matches!(&event, Event::WorkerStopped { .. }) {
                self.settings
                    .controls
                    .worker_stopped
                    .fetch_add(1, Ordering::SeqCst);
            }
            owner.process(event);
        }
    }
    // Active operation, not production run_driver's discovery/Close/deadline policy.
    pub(super) async fn drive(&mut self) {
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            self.step(&mut tick, true).await;
        }
    }
    async fn step(&mut self, tick: &mut tokio::time::Interval, admissions: bool) {
        self.flush_acknowledgments().await;
        enum Ready {
            Admission(usize),
            Session(usize),
            Event(Option<Event>),
            Operation(Operation),
            Tick,
        }
        let ready = {
            let Self {
                sessions,
                settings,
                incoming,
                owner,
                closed,
                admissions: records,
                ..
            } = self;
            // Split fields to keep original socket futures and tokens in the root.
            tokio::select! {
                index = poll_fn(|cx| {
                    for (index, slot) in sessions.iter_mut().enumerate() {
                        if matches!(slot, SessionSlot::Installed(_)) {
                            if retained_session::poll_original(slot, cx).is_ready() {
                                settings.controls.observed_sessions.fetch_add(1, Ordering::SeqCst);
                                return Poll::Ready(index);
                            }
                            if !admissions { return Poll::Pending; }
                        }
                    }
                    Poll::Pending
                }) => Ready::Session(index),
                index = poll_fn(|cx| poll_admission_records(records, !admissions || *closed, cx)) => Ready::Admission(index),
                event = incoming.as_mut().expect("retained receiver").recv() => Ready::Event(event),
                Some(operation) = owner.as_mut().expect("retained owner").next_operation(), if !*closed => Ready::Operation(operation),
                _ = tick.tick() => Ready::Tick,
            }
        };
        match ready {
            Ready::Admission(index) => self.accepted(index).await,
            Ready::Session(index) => self.session_ready(index).await,
            Ready::Event(Some(event)) => self.process(event).await,
            Ready::Event(None) => {}
            Ready::Operation(operation) => self
                .owner
                .as_mut()
                .expect("retained owner")
                .accept_completion(operation),
            Ready::Tick => {
                if !self.closed {
                    self.owner.as_mut().expect("retained owner").tick();
                }
            }
        }
    }
    pub(super) async fn finish(&mut self) -> Report<A> {
        self.stop();
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        while self
            .sessions
            .iter()
            .any(|slot| matches!(slot, SessionSlot::Installed(_)))
        {
            self.step(&mut tick, false).await;
        }
        self.flush_acknowledgments().await;
        let mut packets = std::array::from_fn::<_, 2, _>(|index| {
            self.cells[index].loan(
                self.worker_budget.clone(),
                self.settings.runtime.clone(),
                self.settings.controls.sessions[index].clone(),
            )
        });
        for packet in &mut packets {
            packet.external_drain();
            packet.packet_mut().set.abort_all();
        }
        while packets.iter().any(|packet| !packet.is_empty()) {
            tokio::select! {
                () = poll_fn(|cx| poll_packets(&mut packets, cx)) => {
                    self.settings.controls.observed_rows.fetch_add(1, Ordering::SeqCst);
                    if self.settings.controls.row_panic.swap(false, Ordering::SeqCst) {
                        panic!("controlled collector unwind after rooted worker Ready");
                    }
                    self.settings.controls.row_ready.hold().await;
                },
                event = self.incoming.as_mut().expect("retained receiver").recv() => {
                    if let Some(event) = event { self.process(event).await; }
                },
                _ = tick.tick() => {},
            }
        }
        let rows: usize = packets.iter().map(|p| p.packet().rows.len()).sum();
        assert_eq!(
            rows,
            self.worker_budget.counts().1,
            "every returned worker has an original row"
        );
        for packet in &packets {
            assert_eq!(packet.packet().rows.len(), packet.packet().launches.len());
        }
        assert_eq!(self.worker_budget.counts().0, 0);
        assert_eq!(self.session_budget.counts().0, 0);
        self.validate_identities(&packets);
        self.incoming.as_mut().expect("retained receiver").close();
        drop(self.sender.take());
        while let Some(event) = self
            .incoming
            .as_mut()
            .expect("retained receiver")
            .recv()
            .await
        {
            self.owner
                .as_mut()
                .expect("closed retained owner")
                .process(event);
        }
        drop(self.incoming.take());
        self.settings
            .controls
            .receiver_torn_down
            .store(true, Ordering::SeqCst);
        drop(self.owner.take());
        self.settings
            .controls
            .owner_torn_down
            .store(true, Ordering::SeqCst);
        let sessions = std::array::from_fn(|index| {
            match std::mem::replace(&mut self.sessions[index], SessionSlot::NotLaunched) {
                SessionSlot::Joined { handle, original } => Some(SessionReport {
                    id: handle.id(),
                    original,
                }),
                SessionSlot::NotLaunched => None,
                SessionSlot::Installed(_) => unreachable!("both actual Session barriers"),
            }
        });
        let [first, second] = packets;
        Report {
            admissions: std::mem::take(&mut self.admissions),
            sessions,
            packets: [first.finish(), second.finish()],
            attempts: self.attempts,
            session_counts: self.session_budget.counts(),
            worker_counts: self.worker_budget.counts(),
            anchor: self.anchor.take().expect("one-shot external anchor"),
        }
    }
    fn validate_identities(&self, packets: &[Loan; 2]) {
        let mut worker_ids = Vec::new();
        let mut ordinals = Vec::new();
        for packet in packets {
            for launch in &packet.packet().launches {
                assert!(
                    !worker_ids.contains(&launch.id),
                    "unique original worker identity"
                );
                assert!(
                    !ordinals.contains(&launch.ordinal),
                    "unique shared commit ordinal"
                );
                assert!(launch.ordinal < self.worker_budget.counts().1);
                assert_eq!(
                    packet
                        .packet()
                        .rows
                        .iter()
                        .filter(|row| row.id == launch.id)
                        .count(),
                    1
                );
                worker_ids.push(launch.id);
                ordinals.push(launch.ordinal);
            }
        }
        let mut session_ids = Vec::new();
        let mut session_ordinals = Vec::new();
        for (index, slot) in self.sessions.iter().enumerate() {
            if let SessionSlot::Joined { handle, .. } = slot {
                let id = handle.id();
                assert!(!session_ids.contains(&id) && !worker_ids.contains(&id));
                let Some(record) = &self.admissions[index] else {
                    unreachable!("launched original admission")
                };
                let Outcome::Launched {
                    id: original,
                    ordinal,
                } = &record.outcome
                else {
                    unreachable!("unique admission handoff")
                };
                assert_eq!(*original, id);
                assert!(
                    !session_ordinals.contains(ordinal)
                        && *ordinal < self.session_budget.counts().1
                );
                session_ids.push(id);
                session_ordinals.push(*ordinal);
            }
        }
        assert_eq!(session_ids.len(), self.session_budget.counts().1);
    }
}

fn poll_admission_records(
    records: &mut [Option<Record>; 2],
    closed: bool,
    cx: &mut Context<'_>,
) -> Poll<usize> {
    if closed {
        return Poll::Pending;
    }
    for (index, record) in records.iter_mut().enumerate() {
        if let Some(record) = record
            && !record.classified
            && (matches!(record.outcome, Outcome::Observed(_))
                || record.poll_original(cx).is_ready())
        {
            return Poll::Ready(index);
        }
    }
    Poll::Pending
}

fn poll_packets(packets: &mut [Loan; 2], cx: &mut Context<'_>) -> Poll<()> {
    for packet in packets {
        if !packet.is_empty() && matches!(packet.poll_next(cx), Poll::Ready(Some(_))) {
            return Poll::Ready(());
        }
    }
    Poll::Pending
}

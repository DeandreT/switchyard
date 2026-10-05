use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use amqp::{NativeConnectionIdentity, ServerSession};
use domain::NamespaceName;
use tokio::{
    runtime::Handle,
    sync::{Semaphore, mpsc},
    task::{JoinError, JoinHandle},
    time::MissedTickBehavior,
};

use super::{
    EVENT_CAPACITY, Event, IngressError, IngressMode, MAX_LINKS,
    owner::Owner,
    routing::{self, RouteError, WorkerTasks},
};
use crate::{NativeAtomicBroker, authorization::ConnectionAuthorization};

mod budget;
pub(super) mod controls;
mod packet;
mod tests;

use budget::{Budget, Closed};
use controls::Controls;
use packet::{Cell, Launch, Row};

enum SessionExit {
    Completed,
    Routing(IngressError),
    WorkerFailed(usize),
    Closed(Closed),
}

enum SessionSlot {
    NotLaunched,
    Installed(JoinHandle<SessionExit>),
    Joined {
        handle: JoinHandle<SessionExit>,
        original: Result<SessionExit, JoinError>,
    },
}

pub(super) struct Refused<A> {
    pub(super) session: ServerSession,
    pub(super) anchor: A,
}
pub(super) struct Report<A> {
    session: Result<SessionExit, JoinError>,
    launches: Vec<Launch>,
    rows: Vec<Row>,
    anchor: A,
}

pub(super) struct Root<A, B: NativeAtomicBroker> {
    session: SessionSlot,
    owner: Option<Owner<B>>,
    incoming: Option<mpsc::Receiver<Event>>,
    sender: Option<mpsc::Sender<Event>>,
    cell: Arc<Cell>,
    budget: Arc<Budget>,
    runtime: Handle,
    closed: bool,
    anchor: Option<A>,
    pub(super) controls: Arc<Controls>,
}

// Private experiment: caller MUST retain/drive root on live A until finish.
#[allow(clippy::too_many_arguments)]
pub(super) fn launch<A, B: NativeAtomicBroker>(
    session: ServerSession,
    connection: NativeConnectionIdentity,
    namespace: NamespaceName,
    broker: B,
    authorization: Option<Arc<ConnectionAuthorization>>,
    mode: IngressMode,
    limit: usize,
    runtime: Handle,
    anchor: A,
    controls: Arc<Controls>,
) -> Result<Root<A, B>, Refused<A>> {
    if !(1..=MAX_LINKS).contains(&limit) {
        return Err(Refused { session, anchor });
    }
    let budget = Budget::new(limit);
    let cell = Cell::new(limit);
    let (sender, incoming) = mpsc::channel(EVENT_CAPACITY);
    let owner = Owner::new(connection, broker.clone());
    let mut root = Root {
        session: SessionSlot::NotLaunched,
        owner: Some(owner),
        incoming: Some(incoming),
        sender: Some(sender.clone()),
        cell: Arc::clone(&cell),
        budget: Arc::clone(&budget),
        runtime: runtime.clone(),
        closed: false,
        anchor: Some(anchor),
        controls: Arc::clone(&controls),
    };
    let mut packet = cell.loan(budget, runtime.clone(), Arc::clone(&controls));
    let handle = runtime.spawn(async move {
        controls.session_start.hold().await;
        if let Some(fault) = controls.take_session_fault() {
            match fault {
                controls::Fault::Error(error) => return SessionExit::Routing(error),
                controls::Fault::Panic(payload) => std::panic::resume_unwind(payload),
            }
        }
        let mut session = session;
        let result = routing::route_session(
            &mut session,
            &namespace,
            &broker,
            &authorization,
            &sender,
            &Arc::new(Semaphore::new(MAX_LINKS)),
            mode,
            &mut packet,
        )
        .await;
        let acknowledged = matches!(
            &result,
            Err(RouteError::Closed {
                acknowledged: true,
                ..
            })
        );
        if result.is_err() && !acknowledged {
            routing::stop_connection(&sender).await;
        }
        match result {
            Ok(()) => SessionExit::Completed,
            Err(RouteError::Routing(error)) => SessionExit::Routing(error),
            Err(RouteError::Worker(row)) => SessionExit::WorkerFailed(row),
            Err(RouteError::Closed { reason, .. }) => SessionExit::Closed(reason),
        }
    });
    root.session = SessionSlot::Installed(handle);
    Ok(root)
}

impl<A, B: NativeAtomicBroker> Root<A, B> {
    pub(super) fn seal_launches(&self) {
        self.budget.seal();
    }
    pub(super) fn budget_counts(&self) -> (usize, usize, bool) {
        self.budget.counts()
    }
    pub(super) fn stop(&mut self) {
        self.close_authority();
        if let SessionSlot::Installed(handle) = &self.session {
            handle.abort();
        }
    }
    fn close_authority(&mut self) {
        self.budget.seal();
        self.closed = true;
        if let Some(owner) = &mut self.owner {
            owner.close();
        }
        self.controls
            .authority_closed
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
    async fn process(&mut self, event: Event) {
        if let Event::StopConnection { reply } = event {
            self.close_authority();
            self.controls.close_ack.hold().await;
            let _ = reply.send(());
        } else if let Some(owner) = &mut self.owner {
            if matches!(&event, Event::WorkerStopped { .. }) {
                self.controls
                    .worker_stopped_observed
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            owner.process(event);
        }
    }
    // One-shot after report extraction; canceled borrowed observations may retry.
    pub(super) async fn finish(&mut self) -> Report<A> {
        let mut tick = tokio::time::interval(Duration::from_millis(100));
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        while !matches!(self.session, SessionSlot::Joined { .. }) {
            enum Ready {
                Session,
                Event(Option<Event>),
                Operation(super::operations::Operation),
                Tick,
            }
            let ready = {
                let Self {
                    session,
                    incoming,
                    owner,
                    closed,
                    ..
                } = self;
                tokio::select! {
                    () = poll_fn(|cx| poll_original(session, cx)) => Ready::Session,
                    event = incoming.as_mut().expect("retained receiver").recv() => Ready::Event(event),
                    Some(operation) = owner.as_mut().expect("retained owner").next_operation(), if !*closed => Ready::Operation(operation),
                    _ = tick.tick() => Ready::Tick,
                }
            };
            match ready {
                Ready::Session => {
                    self.controls.session_ready.hold().await;
                }
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
        // Original Session Ready was rooted before any close/checkpoint callback.
        self.close_authority();
        let mut packet = self.cell.loan(
            Arc::clone(&self.budget),
            self.runtime.clone(),
            Arc::clone(&self.controls),
        );
        packet.external_drain();
        packet.packet_mut().set.abort_all();
        while !packet.is_empty() {
            tokio::select! {
                result = poll_fn(|cx| packet.poll_next(cx)) => {
                    let _ = result;
                    if self
                        .controls
                        .row_panic
                        .swap(false, std::sync::atomic::Ordering::SeqCst)
                    {
                        panic!("controlled finish unwind after rooted row");
                    }
                    self.controls.finish_after_row.hold().await;
                },
                event = self.incoming.as_mut().expect("retained receiver").recv() => {
                    if let Some(event) = event {
                        self.process(event).await;
                    }
                },
                _ = tick.tick() => {},
            }
        }
        assert_eq!(
            packet.packet().launches.len(),
            packet.packet().rows.len(),
            "every original worker joined"
        );
        let (reserved, committed, _) = self.budget.counts();
        assert_eq!(reserved, 0, "no uncompleted installation obligation");
        assert_eq!(
            committed,
            packet.packet().rows.len(),
            "every returned launch has its original row"
        );
        if let Some(incoming) = &mut self.incoming {
            incoming.close();
        }
        drop(self.sender.take());
        if let Some(incoming) = &mut self.incoming {
            while let Some(event) = incoming.recv().await {
                if let Some(owner) = &mut self.owner {
                    owner.process(event);
                }
            }
        }
        drop(self.incoming.take());
        self.controls
            .receiver_torn_down
            .store(true, std::sync::atomic::Ordering::SeqCst);
        drop(self.owner.take());
        self.controls
            .owner_torn_down
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let packet = packet.finish();
        let slot = std::mem::replace(&mut self.session, SessionSlot::NotLaunched);
        let SessionSlot::Joined { handle, original } = slot else {
            unreachable!("actual Session barrier")
        };
        drop(handle);
        Report {
            session: original,
            launches: packet.launches,
            rows: packet.rows,
            anchor: self.anchor.take().expect("unique external anchor"),
        }
    }
}

fn poll_original(slot: &mut SessionSlot, cx: &mut Context<'_>) -> Poll<()> {
    let result = match slot {
        SessionSlot::Installed(handle) => match Pin::new(handle).poll(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(original) => original,
        },
        SessionSlot::Joined { .. } => return Poll::Ready(()),
        SessionSlot::NotLaunched => return Poll::Pending,
    };
    let current = std::mem::replace(slot, SessionSlot::NotLaunched);
    let SessionSlot::Installed(handle) = current else {
        unreachable!("installed original session")
    };
    *slot = SessionSlot::Joined {
        handle,
        original: result,
    };
    Poll::Ready(())
}

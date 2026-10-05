use std::{
    future::Future,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use tokio::{
    runtime::Handle,
    task::{Id, JoinError, JoinSet},
};

use super::super::{
    IngressError,
    routing::{Branch, WorkerTasks},
};
use super::{
    budget::{Budget, Closed, Ticket},
    controls::{Controls, Fault, WorkerDrop},
};

pub(in crate::listener::atomic_ingress) struct Launch {
    pub(in crate::listener::atomic_ingress) id: Id,
    pub(in crate::listener::atomic_ingress) ordinal: usize,
    pub(in crate::listener::atomic_ingress) branch: Branch,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::listener::atomic_ingress) enum Drain {
    Live,
    PeerEnd,
    External,
}
pub(in crate::listener::atomic_ingress) struct Row {
    pub(in crate::listener::atomic_ingress) id: Id,
    pub(in crate::listener::atomic_ingress) drain: Drain,
    pub(in crate::listener::atomic_ingress) original: Result<Result<(), IngressError>, JoinError>,
}

pub(in crate::listener::atomic_ingress) struct Packet {
    pub(in crate::listener::atomic_ingress) set: JoinSet<Result<(), IngressError>>,
    pub(in crate::listener::atomic_ingress) launches: Vec<Launch>,
    pub(in crate::listener::atomic_ingress) rows: Vec<Row>,
}

pub(in crate::listener::atomic_ingress) struct Cell(Mutex<Option<Packet>>);
pub(in crate::listener::atomic_ingress) struct Loan {
    cell: Arc<Cell>,
    packet: Option<Packet>,
    pub(in crate::listener::atomic_ingress) budget: Arc<Budget>,
    runtime: Handle,
    controls: Arc<Controls>,
    drain: Drain,
}

impl Cell {
    pub(in crate::listener::atomic_ingress) fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self(Mutex::new(Some(Packet {
            set: JoinSet::new(),
            launches: Vec::with_capacity(limit),
            rows: Vec::with_capacity(limit),
        }))))
    }
    pub(in crate::listener::atomic_ingress) fn loan(
        self: &Arc<Self>,
        budget: Arc<Budget>,
        runtime: Handle,
        controls: Arc<Controls>,
    ) -> Loan {
        let packet = self.0.lock().unwrap_or_else(|e| e.into_inner()).take();
        Loan {
            cell: Arc::clone(self),
            packet: Some(packet.expect("unique packet loan")),
            budget,
            runtime,
            controls,
            drain: Drain::Live,
        }
    }
}

impl Loan {
    pub(in crate::listener::atomic_ingress) fn packet(&self) -> &Packet {
        self.packet.as_ref().expect("armed packet loan")
    }
    pub(in crate::listener::atomic_ingress) fn packet_mut(&mut self) -> &mut Packet {
        self.packet.as_mut().expect("armed packet loan")
    }
    pub(in crate::listener::atomic_ingress) fn finish(mut self) -> Packet {
        self.packet.take().expect("joined packet")
    }
    pub(in crate::listener::atomic_ingress) fn external_drain(&mut self) {
        self.drain = Drain::External;
    }
}

impl Drop for Loan {
    fn drop(&mut self) {
        if let Some(packet) = self.packet.take() {
            let previous = {
                let mut slot = self.cell.0.lock().unwrap_or_else(|e| e.into_inner());
                slot.replace(packet)
            };
            assert!(previous.is_none(), "unique whole-packet restoration");
        }
    }
}

impl WorkerTasks for Loan {
    type Ticket = Ticket;
    type Failure = usize;
    type Closed = Closed;
    fn is_empty(&self) -> bool {
        self.packet().set.is_empty()
    }
    fn reserve(&mut self) -> Result<Ticket, Closed> {
        self.budget.reserve()
    }
    fn begin_peer_end_drain(&mut self) {
        self.drain = Drain::PeerEnd;
        self.controls
            .peer_end_entered
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<(), usize>>> {
        let result = self.packet_mut().set.poll_join_next_with_id(cx);
        match result {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(original)) => {
                let drain = self.drain;
                let packet = self.packet_mut();
                let row = packet.rows.len();
                match original {
                    Ok((id, value)) => packet.rows.push(Row {
                        id,
                        drain,
                        original: Ok(value),
                    }),
                    Err(error) => packet.rows.push(Row {
                        id: error.id(),
                        drain,
                        original: Err(error),
                    }),
                }
                // No callback, await, classification or observer Drop precedes rooting.
                let failed = !matches!(packet.rows[row].original, Ok(Ok(())));
                self.controls
                    .joined_rows
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Poll::Ready(Some(if failed { Err(row) } else { Ok(()) }))
            }
        }
    }
    fn launch<F>(&mut self, ticket: Ticket, future: F, branch: Branch) -> Result<(), (Closed, F)>
    where
        F: Future<Output = Result<(), IngressError>> + Send + 'static,
    {
        let claim = match ticket.claim() {
            Ok(claim) => claim,
            Err(reason) => return Err((reason, future)),
        };
        let controls = Arc::clone(&self.controls);
        let witness = WorkerDrop(Arc::clone(&controls));
        let runtime = self.runtime.clone();
        let handle = self.packet_mut().set.spawn_on(
            async move {
                // Counter observation is post-join only; capture Drop order is not a barrier.
                let _witness = witness;
                if controls.blocks_worker(branch) {
                    controls.worker_start.hold().await;
                }
                let original = future.await;
                controls.worker_final.hold().await;
                if original.is_ok() {
                    match controls.take_fault() {
                        Some(Fault::Error(error)) => return Err(error),
                        Some(Fault::Panic(payload)) => std::panic::resume_unwind(payload),
                        None => {}
                    }
                }
                original
            },
            &runtime,
        );
        let ordinal = claim.commit();
        self.packet_mut().launches.push(Launch {
            id: handle.id(),
            ordinal,
            branch,
        });
        if self
            .controls
            .session_unwind_after_launch
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            panic!("controlled actual session unwind after original worker installation");
        }
        Ok(())
    }
    fn controls(&self) -> Option<Arc<Controls>> {
        Some(Arc::clone(&self.controls))
    }
}

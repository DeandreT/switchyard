use std::{
    collections::TryReserveError,
    error::Error,
    fmt,
    future::Future,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use tokio::{
    runtime::Handle,
    task::{AbortHandle, Id, JoinSet},
};

use super::super::{
    IngressError,
    routing::{Branch, WorkerTasks},
};
use super::outcomes::{
    Data, RetainedAtomicMessagingDrain as Drain, RetainedAtomicMessagingWorkerOutcome as Row,
    locked,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Closed {
    Sealed,
    History,
}
impl fmt::Display for Closed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Sealed => "retained admission is sealed",
            Self::History => "retained worker history is exhausted",
        })
    }
}
impl Error for Closed {}

struct Counts {
    launched: usize,
    reserved: usize,
    sealed: bool,
}
pub(super) struct Budget {
    limit: usize,
    state: Mutex<Counts>,
}
pub(super) struct Ticket {
    budget: Arc<Budget>,
    armed: bool,
}
pub(super) struct Claim {
    budget: Arc<Budget>,
    armed: bool,
}

impl Budget {
    pub(super) fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            state: Mutex::new(Counts {
                launched: 0,
                reserved: 0,
                sealed: false,
            }),
        })
    }
    pub(super) fn seal(&self) {
        locked(&self.state).sealed = true;
    }
    pub(super) fn reserve(self: &Arc<Self>) -> Result<Ticket, Closed> {
        let mut state = locked(&self.state);
        if state.sealed {
            return Err(Closed::Sealed);
        }
        if state.launched + state.reserved >= self.limit {
            return Err(Closed::History);
        }
        state.reserved += 1;
        Ok(Ticket {
            budget: Arc::clone(self),
            armed: true,
        })
    }
    #[cfg(test)]
    pub(super) fn counts(&self) -> (usize, usize, bool) {
        let state = locked(&self.state);
        (state.launched, state.reserved, state.sealed)
    }
}
impl Ticket {
    pub(super) fn claim(mut self) -> Result<Claim, Closed> {
        if locked(&self.budget.state).sealed {
            return Err(Closed::Sealed);
        }
        self.armed = false;
        Ok(Claim {
            budget: Arc::clone(&self.budget),
            armed: true,
        })
    }
}
impl Drop for Ticket {
    fn drop(&mut self) {
        if self.armed {
            locked(&self.budget.state).reserved -= 1;
        }
    }
}
impl Claim {
    pub(super) fn commit(mut self) -> usize {
        let mut state = locked(&self.budget.state);
        state.reserved -= 1;
        let ordinal = state.launched;
        state.launched += 1;
        self.armed = false;
        ordinal
    }
}
impl Drop for Claim {
    fn drop(&mut self) {
        if self.armed {
            locked(&self.budget.state).reserved -= 1;
        }
    }
}

pub(super) struct Launch {
    pub(super) token: AbortHandle,
    pub(super) id: Id,
    pub(super) ordinal: usize,
    pub(super) branch: Branch,
    pub(super) abort_requested: bool,
}
pub(super) struct Packet {
    pub(super) set: JoinSet<Result<(), IngressError>>,
    pub(super) launches: Vec<Launch>,
    pub(super) rows: Vec<Row>,
    pending_row: Option<usize>,
}
pub(super) struct Cell(Mutex<Option<Packet>>);
pub(super) struct Loan {
    cell: Arc<Cell>,
    packet: Option<Packet>,
    budget: Arc<Budget>,
    runtime: Handle,
    data: Arc<Data>,
    drain: Drain,
    #[cfg(test)]
    hooks: Arc<super::outcomes::Hooks>,
}

impl Packet {
    pub(super) fn try_new(limit: usize) -> Result<Self, TryReserveError> {
        let mut launches = Vec::new();
        launches.try_reserve_exact(limit)?;
        let mut rows = Vec::new();
        rows.try_reserve_exact(limit)?;
        Ok(Self {
            set: JoinSet::new(),
            launches,
            rows,
            pending_row: None,
        })
    }
    fn abort_unobserved(&mut self) {
        for launch in &mut self.launches {
            if !launch.abort_requested && !self.rows.iter().any(|row| row.id == launch.id) {
                // This fact belongs to this original token, immediately before abort.
                launch.abort_requested = true;
                launch.token.abort();
            }
        }
    }
}
impl Cell {
    pub(super) fn new(packet: Packet) -> Arc<Self> {
        Arc::new(Self(Mutex::new(Some(packet))))
    }
    pub(super) fn loan(
        self: &Arc<Self>,
        budget: Arc<Budget>,
        runtime: Handle,
        data: Arc<Data>,
        #[cfg(test)] hooks: Arc<super::outcomes::Hooks>,
    ) -> Option<Loan> {
        let packet = locked(&self.0).take()?;
        Some(Loan {
            cell: Arc::clone(self),
            packet: Some(packet),
            budget,
            runtime,
            data,
            drain: Drain::Live,
            #[cfg(test)]
            hooks,
        })
    }
    #[cfg(test)]
    pub(super) fn original_ids(&self) -> Vec<Id> {
        locked(&self.0)
            .as_ref()
            .map(|packet| packet.launches.iter().map(|launch| launch.id).collect())
            .unwrap_or_default()
    }
}
impl Loan {
    pub(super) fn packet(&self) -> &Packet {
        self.packet.as_ref().expect("armed whole worker packet")
    }
    fn packet_mut(&mut self) -> &mut Packet {
        self.packet.as_mut().expect("armed whole worker packet")
    }
    pub(super) fn external_drain(&mut self) {
        self.drain = Drain::External;
        self.packet_mut().abort_unobserved();
    }
    pub(super) fn finish(mut self) -> Packet {
        assert!(
            self.packet().set.is_empty() && self.packet().pending_row.is_none(),
            "actual complete worker joins"
        );
        self.packet.take().expect("joined original packet")
    }
    #[cfg(test)]
    pub(super) fn session_fault(&self) -> Option<IngressError> {
        locked(&self.hooks.session_fault).take()
    }
    fn classified(&mut self) -> Poll<Option<Result<(), usize>>> {
        let Some(index) = self.packet().pending_row else {
            return Poll::Ready(None);
        };
        let row = &self.packet().rows[index];
        let failed = !matches!(row.original, Ok(Ok(())));
        let ordinal = row.ordinal;
        self.packet_mut().pending_row = None;
        Poll::Ready(Some(if failed { Err(ordinal) } else { Ok(()) }))
    }
}
impl Drop for Loan {
    fn drop(&mut self) {
        if let Some(packet) = self.packet.take() {
            let previous = locked(&self.cell.0).replace(packet);
            assert!(previous.is_none(), "unique whole worker packet restoration");
        }
    }
}

impl WorkerTasks for Loan {
    type Ticket = Ticket;
    type Failure = usize;
    type Closed = Closed;
    fn is_empty(&self) -> bool {
        self.packet().set.is_empty() && self.packet().pending_row.is_none()
    }
    fn reserve(&mut self) -> Result<Ticket, Closed> {
        self.budget.reserve()
    }
    fn begin_peer_end_drain(&mut self) {
        self.drain = Drain::PeerEnd;
    }
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<(), usize>>> {
        if self.packet().pending_row.is_some() {
            #[cfg(test)]
            if self.hooks.worker_ready.poll(cx).is_pending() {
                return Poll::Pending;
            }
            return self.classified();
        }
        let original = match self.packet_mut().set.poll_join_next_with_id(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Ready(Some(original)) => original,
        };
        let id = match &original {
            Ok((id, _)) => *id,
            Err(error) => error.id(),
        };
        let launch = self
            .packet()
            .launches
            .iter()
            .find(|launch| launch.id == id)
            .expect("original returned worker token");
        let (ordinal, branch, abort_requested) =
            (launch.ordinal, launch.branch.into(), launch.abort_requested);
        let original = original.map(|(_, result)| result);
        let drain = self.drain;
        let packet = self.packet_mut();
        let index = packet.rows.len();
        packet.rows.push(Row {
            id,
            ordinal,
            branch,
            abort_requested,
            drain,
            original,
        });
        packet.pending_row = Some(index);
        // The original Ready value is stored before observers or classification.
        self.data.update(|state| state.worker_joins += 1);
        #[cfg(test)]
        if self.hooks.worker_ready.poll(cx).is_pending() {
            return Poll::Pending;
        }
        self.classified()
    }
    fn launch<F>(&mut self, ticket: Ticket, future: F, branch: Branch) -> Result<(), (Closed, F)>
    where
        F: Future<Output = Result<(), IngressError>> + Send + 'static,
    {
        let claim = match ticket.claim() {
            Ok(claim) => claim,
            Err(reason) => return Err((reason, future)),
        };
        let runtime = self.runtime.clone();
        #[cfg(test)]
        let hooks = Arc::clone(&self.hooks);
        let token = self.packet_mut().set.spawn_on(
            async move {
                #[cfg(test)]
                hooks.worker_start.hold().await;
                let result = future.await;
                #[cfg(test)]
                if result.is_ok() {
                    let fault = locked(&hooks.worker_fault).take();
                    if let Some(fault) = fault {
                        return Err(fault);
                    }
                    let panic = locked(&hooks.worker_panic).take();
                    if let Some(panic) = panic {
                        std::panic::resume_unwind(panic);
                    }
                }
                result
            },
            &runtime,
        );
        let id = token.id();
        let ordinal = claim.commit();
        self.packet_mut().launches.push(Launch {
            token,
            id,
            ordinal,
            branch,
            abort_requested: false,
        });
        #[cfg(test)]
        locked(&self.hooks.worker_ids).push(id);
        self.data.update(|state| state.worker_launches += 1);
        #[cfg(test)]
        if self
            .hooks
            .session_panic
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            panic!("controlled original session unwind after worker installation");
        }
        Ok(())
    }
}

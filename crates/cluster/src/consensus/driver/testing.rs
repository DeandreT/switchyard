//! Explicit local test-only network faults, not voter trust or a new wire
//! protocol. Delivered/released packets still pass the original #359 TCP path.
//! Fixed maxima: eight held/released packets capped at 64 MiB, plus one captured
//! packet bounded by the configured production frame limit.
use protobuf::Message as _;
use raft::eraftpb::{Message, MessageType};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, Ordering},
    },
};
use tokio::sync::{Notify, Semaphore};

use super::{
    DriverHandle,
    input::{Event, Operation},
};
use crate::{
    consensus::ConsensusStore,
    transport::{PeerHandler, TransportError},
};

/// A fixed-size observation AFTER the original peer event has stepped the core
/// and completed Ready persistence. These are the core's actual progress values,
/// not a reconstructed quorum or a writable progress/test admission interface.
#[derive(Clone, Copy, Debug)]
pub struct ProcessedAppend {
    pub from: u64,
    pub term: u64,
    pub index: u64,
    pub reject: bool,
    pub core_term: u64,
    pub matched: [u64; 3],
    pub voters: [u64; 3],
    pub commit: u64,
    pub rejected_before_success: bool,
}

struct DispatchState {
    // Single-use states: idle, armed, held, released, actually returned.
    phase: AtomicU8,
    release: Semaphore,
}
impl Default for DispatchState {
    fn default() -> Self {
        Self {
            phase: AtomicU8::new(0),
            release: Semaphore::new(0),
        }
    }
}

/// One selected genuine PeerHandler original pauses AFTER its admitted driver
/// work completes, before returning to the transport. Retirement does not release
/// it. There is one slot/permit, no timer, detached task or unrelated pending work.
#[derive(Clone, Default)]
pub struct DispatchGate {
    inner: Arc<DispatchState>,
}
impl DispatchGate {
    pub fn arm(&self) -> bool {
        self.inner
            .phase
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }
    pub fn held(&self) -> bool {
        self.inner.phase.load(Ordering::SeqCst) == 2
    }
    pub fn returned(&self) -> bool {
        self.inner.phase.load(Ordering::SeqCst) == 4
    }
    pub fn release(&self) -> bool {
        if self
            .inner
            .phase
            .compare_exchange(2, 3, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return false;
        }
        self.inner.release.add_permits(1);
        true
    }
    async fn pause_original(&self) {
        if self
            .inner
            .phase
            .compare_exchange(1, 2, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        self.inner
            .release
            .acquire()
            .await
            .expect("test dispatch gate")
            .forget();
        self.inner.phase.store(4, Ordering::SeqCst);
    }
}
pub(super) struct Handler {
    pub handle: DriverHandle,
    pub gate: DispatchGate,
}
impl PeerHandler for Handler {
    async fn dispatch(&self, message: Message) -> Result<(), TransportError> {
        let result = self.handle.dispatch(message).await;
        if result.is_ok() {
            self.gate.pause_original().await;
        }
        result
    }
}

pub(super) fn append_metadata(event: &Event) -> Option<(u64, u64, u64, bool)> {
    match &event.op {
        Operation::Peer(m, _) if m.get_msg_type() == MessageType::MsgAppendResponse => {
            Some((m.from, m.term, m.index, m.reject))
        }
        _ => None,
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Rule {
    pub peer: u64,
    pub kind: MessageType,
    pub min_index: u64,
}
impl Rule {
    fn matches(self, message: &Message) -> bool {
        self.peer == message.to
            && self.kind == message.get_msg_type()
            && message.index >= self.min_index
    }
}
#[derive(Default)]
struct State {
    blocked: [u64; 3],
    hold: Option<Rule>,
    capture: Option<Rule>,
    captured: Option<Message>,
    held: VecDeque<Message>,
    released: VecDeque<Message>,
    bytes: usize,
    observe: Option<(u64, u64)>,
    rejected_before_success: bool,
    processed: Option<ProcessedAppend>,
}
#[derive(Clone, Default)]
pub struct NetworkControl {
    state: Arc<Mutex<State>>,
    notify: Arc<Notify>,
}
pub(super) enum Disposition {
    Deliver(Message),
    Held,
    Lost,
}
impl NetworkControl {
    pub fn observe_append(&self, from: u64, index: u64) {
        let mut state = self.state.lock().expect("test links");
        state.observe = Some((from, index));
        state.rejected_before_success = false;
        state.processed = None;
    }
    pub fn processed_append(&self) -> Option<ProcessedAppend> {
        self.state.lock().expect("test links").processed
    }
    pub(super) fn observe_processed<B: storage::ConsensusBackend>(
        &self,
        metadata: Option<(u64, u64, u64, bool)>,
        core: &raft::RawNode<ConsensusStore<B>>,
    ) {
        let Some((from, term, index, reject)) = metadata else {
            return;
        };
        let mut state = self.state.lock().expect("test links");
        if state.observe != Some((from, index))
            || term != core.raft.term
            || core.raft.state != raft::StateRole::Leader
        {
            return;
        }
        if reject {
            state.rejected_before_success = true;
            return;
        }
        if state.processed.is_some() {
            return;
        }
        let voters = core.store().identity().voters().each_ref().map(|v| v.id);
        state.processed = Some(ProcessedAppend {
            from,
            term,
            index,
            reject,
            core_term: core.raft.term,
            matched: voters.map(|id| core.raft.prs().get(id).expect("fixed core voter").matched),
            voters,
            commit: core
                .store()
                .hard_state()
                .expect("usable observed store")
                .commit,
            rejected_before_success: state.rejected_before_success,
        });
    }
    pub fn block(&self, peers: [u64; 3]) {
        self.state.lock().expect("test links").blocked = peers;
    }
    pub fn hold(&self, rule: Option<Rule>) {
        self.state.lock().expect("test links").hold = rule;
    }
    pub fn capture(&self, rule: Option<Rule>) {
        let mut s = self.state.lock().expect("test links");
        s.capture = rule;
        s.captured = None;
    }
    pub fn captured(&self) -> Option<Message> {
        self.state.lock().expect("test links").captured.clone()
    }
    pub fn held(&self) -> usize {
        self.state.lock().expect("test links").held.len()
    }
    pub fn release(&self) {
        let mut s = self.state.lock().expect("test links");
        s.hold = None;
        while let Some(m) = s.held.pop_front() {
            s.released.push_back(m);
        }
        drop(s);
        self.notify.notify_one();
    }
    pub fn replay(&self, message: Message) -> bool {
        let mut s = self.state.lock().expect("test links");
        let bytes = message.compute_size() as usize;
        if s.held.len() + s.released.len() >= 8 || s.bytes + bytes > 64 * 1024 * 1024 {
            return false;
        }
        s.bytes += bytes;
        s.released.push_back(message);
        drop(s);
        self.notify.notify_one();
        true
    }
    pub(super) fn intercept(&self, message: Message) -> Disposition {
        let mut s = self.state.lock().expect("test links");
        if s.captured.is_none() && s.capture.is_some_and(|r| r.matches(&message)) {
            s.captured = Some(message.clone());
        }
        if s.blocked.contains(&message.to) {
            return Disposition::Lost;
        }
        if s.hold.is_some_and(|r| r.matches(&message)) {
            let bytes = message.compute_size() as usize;
            if s.held.len() + s.released.len() >= 8 || s.bytes + bytes > 64 * 1024 * 1024 {
                return Disposition::Lost;
            }
            s.bytes += bytes;
            s.held.push_back(message);
            Disposition::Held
        } else {
            Disposition::Deliver(message)
        }
    }
    pub(super) async fn changed(&self) {
        self.notify.notified().await;
    }
    pub(super) fn take_released(&self) -> VecDeque<Message> {
        let mut s = self.state.lock().expect("test links");
        let result = std::mem::take(&mut s.released);
        s.bytes -= result
            .iter()
            .map(|m| m.compute_size() as usize)
            .sum::<usize>();
        result
    }
    pub(super) fn clear(&self) {
        *self.state.lock().expect("test links") = State::default();
    }
}

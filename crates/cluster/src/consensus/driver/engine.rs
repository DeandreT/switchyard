use std::{
    any::Any,
    collections::VecDeque,
    panic::{AssertUnwindSafe, catch_unwind},
};

use protobuf::Message as _;
use protobuf::ProtobufEnum;
use raft::{LightReady, RawNode, Ready, StateRole, eraftpb::Message};
use storage::ConsensusBackend;

use super::{
    AdmissionError, DriverError, DriverOptions, DriverStatus, Failure, ProposalAdmission,
    input::{Event, Operation},
};
use crate::{consensus::ConsensusStore, transport::FrameCodec};

pub(super) struct Engine<B: ConsensusBackend> {
    pub core: RawNode<ConsensusStore<B>>,
    pub ready: Option<Ready>,
    pub light: Option<LightReady>,
    pub outbox: VecDeque<Message>,
    pub outbox_bytes: usize,
    pub status: DriverStatus,
    pub error: Option<DriverError>,
    pub panic: Option<Box<dyn Any + Send>>,
    options: DriverOptions,
    codec: FrameCodec,
}
impl<B: ConsensusBackend> Engine<B> {
    pub fn new(
        store: ConsensusStore<B>,
        options: DriverOptions,
        codec: FrameCodec,
    ) -> Result<Self, DriverError> {
        let core = RawNode::new(
            &options.core_config(store.local_id()),
            store,
            &slog::Logger::root(slog::Discard, slog::o!()),
        )?;
        let mut engine = Self {
            core,
            ready: None,
            light: None,
            outbox: VecDeque::new(),
            outbox_bytes: 0,
            status: DriverStatus::default(),
            error: None,
            panic: None,
            options,
            codec,
        };
        engine.observe()?;
        Ok(engine)
    }
    fn observe(&mut self) -> Result<(), DriverError> {
        let hs = self.core.store().hard_state()?;
        self.status.observed_vote = hs.vote;
        self.status.observed_commit = hs.commit;
        self.status.observed_last = self.core.store().last_index()?;
        self.status.term = self.core.raft.term;
        self.status.role = self.core.raft.state.into();
        self.status.leader_id = self.core.raft.leader_id;
        self.status.accepting_proposals =
            self.core.raft.state == StateRole::Leader && !self.status.retired;
        // Actual raft_log.applied is never advanced by the driver.
        self.status.applied = self.core.raft.raft_log.applied;
        Ok(())
    }
    pub fn guarded<T>(
        &mut self,
        work: impl FnOnce(&mut Self) -> Result<T, DriverError>,
    ) -> Result<T, AdmissionError> {
        if self.status.retired {
            return Err(AdmissionError::Retired);
        }
        match catch_unwind(AssertUnwindSafe(|| work(self))) {
            Ok(Ok(value)) => Ok(value),
            result => {
                let error = match result {
                    Ok(Err(e)) => e,
                    Err(payload) => {
                        self.panic = Some(payload);
                        DriverError::Panic
                    }
                    _ => unreachable!(),
                };
                self.status.failure = Some(match &error {
                    DriverError::Persistence(_) | DriverError::Backend(_) => Failure::Persistence,
                    DriverError::Panic => Failure::Panic,
                    DriverError::Bound(_) => Failure::Bounds,
                    _ => Failure::Core,
                });
                self.status.storage_uncertain = self.core.store().is_retired();
                self.status.retired = true;
                self.status.accepting_proposals = false;
                self.error = Some(error);
                Err(AdmissionError::Failed)
            }
        }
    }
    pub fn process(&mut self, event: Event) {
        // No yield from first core mutation through both persistence stages.
        // The original caller's cancellation cannot interrupt an atomic apply.
        let _credit = event._credit;
        match event.op {
            Operation::Propose {
                data,
                context,
                term,
                reply,
            } => {
                let result = self
                    .guarded(|engine| {
                        if engine.core.raft.state != StateRole::Leader
                            || engine.core.raft.term != term
                        {
                            return Ok(Err(AdmissionError::NotLeader));
                        }
                        let before = engine.core.raft.raft_log.last_index();
                        match engine.core.propose(context, data) {
                            Ok(()) => {}
                            Err(raft::Error::ProposalDropped) => {
                                return Ok(Err(AdmissionError::Declined));
                            }
                            Err(e) => return Err(e.into()),
                        }
                        let index = engine.core.raft.raft_log.last_index();
                        if index != before + 1 {
                            return Ok(Err(AdmissionError::Declined));
                        }
                        engine.persist_ready()?;
                        Ok(Ok(ProposalAdmission {
                            term,
                            raft_index: index,
                        }))
                    })
                    .and_then(|v| v);
                let _ = reply.send(result);
            }
            Operation::Peer(message, reply) => {
                let result = self.guarded(|engine| {
                    engine.status.peer_events = engine.status.peer_events.saturating_add(1);
                    engine.status.last_peer_from = message.from;
                    engine.status.last_peer_term = message.term;
                    engine.status.last_peer_index = message.index;
                    engine.status.last_peer_kind = message.msg_type.value();
                    engine.core.step(message)?;
                    // All later local admissions see the actual core's new role
                    // and term; no cached leader capability survives this step.
                    engine.persist_ready()
                });
                let _ = reply.send(result);
            }
            Operation::Campaign(reply) => {
                let result = self.guarded(|e| {
                    e.core.campaign()?;
                    e.persist_ready()
                });
                let _ = reply.send(result);
            }
            Operation::Tick(reply) => {
                let result = self.tick();
                let _ = reply.send(result);
            }
        }
    }
    pub fn tick(&mut self) -> Result<(), AdmissionError> {
        self.guarded(|e| {
            e.status.ticks = e.status.ticks.saturating_add(1);
            e.core.tick();
            e.persist_ready()
        })
    }
    pub fn unreachable(&mut self, peer: u64) {
        let _ = self.guarded(|e| {
            e.core.report_unreachable(peer);
            e.persist_ready()
        });
    }
    pub fn persist_ready(&mut self) -> Result<(), DriverError> {
        if self.ready.is_some() || self.light.is_some() {
            return Err(DriverError::Bound("previous Ready retained"));
        }
        while self.core.has_ready() {
            self.ready = Some(self.core.ready());
            let ready = self.ready.as_ref().expect("original Ready");
            if !ready.snapshot().is_empty() || !ready.read_states().is_empty() {
                return Err(DriverError::Bound("snapshot/read states unsupported"));
            }
            // Even leader messages permitted before fsync are held here. The
            // store performs its real atomic SyncAll, irrespective of must_sync.
            self.core.mut_store().persist(ready.entries(), ready.hs())?;
            self.status.ready_persisted = self.status.ready_persisted.saturating_add(1);
            let ready = self.ready.as_mut().expect("persisted Ready");
            let mut messages = ready.take_messages();
            messages.extend(ready.take_persisted_messages());
            self.stage(messages)?;
            // NEVER convenience advance(), async advance or fictional apply.
            self.light = Some(
                self.core
                    .advance_append(self.ready.take().expect("persisted original")),
            );
            self.status.advanced = self.status.advanced.saturating_add(1);
            if let Some(commit) = self.light.as_ref().expect("LightReady").commit_index() {
                let mut hs = self.core.store().hard_state()?.clone();
                hs.commit = commit;
                self.core.mut_store().persist(&[], Some(&hs))?;
                self.status.light_commits_persisted =
                    self.status.light_commits_persisted.saturating_add(1);
            }
            let messages = self
                .light
                .as_mut()
                .expect("persisted LightReady")
                .take_messages();
            self.stage(messages)?;
            self.light = None;
            self.observe()?;
        }
        self.observe()
    }
    fn stage(&mut self, messages: Vec<Message>) -> Result<(), DriverError> {
        for message in messages {
            // Public codec performs the frozen transport's full validation.
            // Its one bounded temporary frame is dropped before retaining the
            // message; no extra store/frame hook or custom codec is required.
            self.codec.encode(&message)?;
            let size = message.compute_size() as usize + 128;
            if self.outbox.len() >= self.options.max_outbound
                || self.outbox_bytes + size > self.options.max_outbound_bytes
            {
                return Err(DriverError::Bound("Ready outbox"));
            }
            self.outbox_bytes += size;
            self.outbox.push_back(message);
        }
        Ok(())
    }
    #[cfg(feature = "consensus-driver-testkit")]
    pub fn stage_for_test(&mut self, message: Message) -> Result<(), DriverError> {
        self.stage(vec![message])
    }
    pub fn pop(&mut self) -> Option<Message> {
        let message = self.outbox.pop_front()?;
        self.outbox_bytes -= message.compute_size() as usize + 128;
        Some(message)
    }
}

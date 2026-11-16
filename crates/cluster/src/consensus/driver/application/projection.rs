use super::{
    ApplicationError,
    composition::{F2View, Hub, View},
    original::Shared,
};
use crate::{
    Journal,
    consensus::{ConsensusStore, EntryKind, entry_kind},
};
use domain::{DurableProposal, IndexedApplyOutcome, IndexedWriter};
use raft::eraftpb::Entry;
use std::sync::Arc;
use storage::StateStore;

enum Stage {
    Append(Entry, u64),
    Frontier(Entry, u64),
    Apply(Entry, u64),
}
/// Sealed policy: only real successful stages produce an actual-applied prefix.
pub(in crate::consensus::driver) enum Policy {
    NoApplication,
    Committed(Box<Projection>),
}
pub(in crate::consensus::driver) struct AppliedPrefix {
    pub raft: u64,
    completion: Option<(Entry, u64, IndexedApplyOutcome)>,
}
#[cfg(test)]
impl AppliedPrefix {
    pub(super) fn test_outcome(self) -> Option<IndexedApplyOutcome> {
        self.completion.map(|(_, _, outcome)| outcome)
    }
}
pub(in crate::consensus::driver) struct Projection {
    pub(super) hub: Hub,
    pub(super) shared: Arc<Shared>,
    pub(super) journal: Journal<View>,
    pub(super) writer: IndexedWriter<View>,
    stage: Option<Stage>,
    pub(super) a: u64,
    pub(super) r: u64,
}
impl Projection {
    pub(super) fn new(
        validated: super::validation::Validated,
        shared: Arc<Shared>,
    ) -> Result<(ConsensusStore<F2View>, Self), ApplicationError> {
        let (store, hub, r) = validated.consume();
        let journal = Journal::open(hub.journal())?;
        let writer = IndexedWriter::open(hub.indexed())?;
        let a = writer.applied_index()?;
        let projection = Self {
            hub,
            shared,
            journal,
            writer,
            stage: None,
            a,
            r,
        };
        projection.publish(store.hard_state()?.commit)?;
        Ok((store, projection))
    }
    pub fn actual(&self) -> u64 {
        self.r
    }
    pub fn turn<B: storage::ConsensusBackend>(
        &mut self,
        store: &ConsensusStore<B>,
        term: u64,
        leader: bool,
    ) -> Result<Option<AppliedPrefix>, ApplicationError> {
        self.shared.fence(term, leader);
        let commit = store.hard_state()?.commit;
        match self.stage.as_ref() {
            None if self.r < commit => {
                let entry = store
                    .read_entries(self.r + 1, 1)?
                    .pop()
                    .ok_or(ApplicationError::Corrupt("missing committed original"))?;
                match entry_kind(&entry) {
                    EntryKind::Blank => {
                        self.r = entry.index;
                        self.publish(commit)?;
                        return Ok(Some(AppliedPrefix {
                            raft: self.r,
                            completion: None,
                        }));
                    }
                    EntryKind::Proposal => self.stage = Some(Stage::Append(entry, self.a + 1)),
                    EntryKind::Configuration => {
                        return Err(ApplicationError::Corrupt(
                            "configuration cannot become application",
                        ));
                    }
                }
            }
            None => {}
            Some(Stage::Append(entry, n)) => {
                if self.journal.last_appended_index()? >= *n {
                    let key = crate::journal::entry_key(*n);
                    let raw = self
                        .hub
                        .readonly()
                        .get(&key)?
                        .ok_or(ApplicationError::Corrupt("missing projected entry"))?;
                    if crate::journal::decode_entry(&key, &raw)?.payload() != entry.data.as_ref() {
                        return Err(ApplicationError::Corrupt("conflicting exact projection"));
                    }
                } else {
                    self.journal.append(*n, &entry.data)?;
                }
                self.stage = Some(Stage::Frontier(entry.clone(), *n));
            }
            Some(Stage::Frontier(entry, n)) => {
                if self.journal.committed_index()? < *n {
                    self.journal.commit(*n)?;
                }
                self.stage = Some(Stage::Apply(entry.clone(), *n));
            }
            Some(Stage::Apply(entry, n)) => {
                let proposal = DurableProposal::decode(&entry.data)?;
                let outcome = self.writer.apply(*n, &proposal)?;
                if matches!(outcome, IndexedApplyOutcome::AlreadyApplied) {
                    return Err(ApplicationError::Corrupt(
                        "next original was already applied",
                    ));
                }
                self.a = *n;
                self.r = entry.index;
                // Same serialized owner, no core mutation/yield between the
                // actual atomic result and its term-fenced publication.
                let proof = AppliedPrefix {
                    raft: self.r,
                    completion: Some((entry.clone(), *n, outcome)),
                };
                self.stage = None;
                self.publish(commit)?;
                return Ok(Some(proof));
            }
        }
        self.publish(commit)?;
        Ok(None)
    }
    pub fn work(&self, commit: u64) -> bool {
        self.stage.is_some() || self.r < commit
    }
    fn publish(&self, commit: u64) -> Result<(), ApplicationError> {
        let mut s = self.shared.status.lock().expect("application status");
        s.applied_ordinal = self.a;
        s.applied_raft = self.r;
        s.committed_raft = commit;
        s.projected = self.journal.last_appended_index()?;
        s.projected_commit = self.journal.committed_index()?;
        Ok(())
    }
    pub(super) fn recover(
        &mut self,
        store: &ConsensusStore<F2View>,
    ) -> Result<(), ApplicationError> {
        while self.work(store.hard_state()?.commit) {
            self.turn(store, 0, false)?;
        }
        Ok(())
    }
    pub fn retire(&self) {
        self.hub.poison();
        self.shared.retire();
        let mut s = self.shared.status.lock().expect("application status");
        s.retired = true;
        s.uncertain = self.hub.uncertain();
    }
}
impl Policy {
    pub fn publish(&self, proof: AppliedPrefix, term: u64, leader: bool) {
        if let Self::Committed(p) = self {
            if let Some((entry, n, outcome)) = proof.completion {
                p.shared.complete(&entry, n, term, leader, outcome);
            }
            p.shared.advance(proof.raft);
        }
    }
    pub fn track(
        &self,
        o: Arc<super::original::Original>,
        index: u64,
    ) -> Result<(), ApplicationError> {
        match self {
            Self::Committed(p) => {
                p.shared.track(o, index);
                Ok(())
            }
            Self::NoApplication => Err(ApplicationError::Corrupt(
                "application submission to F2-only driver",
            )),
        }
    }
    pub fn work(&self, commit: u64) -> bool {
        match self {
            Self::Committed(p) => p.work(commit),
            Self::NoApplication => false,
        }
    }
    pub fn turn<B: storage::ConsensusBackend>(
        &mut self,
        store: &ConsensusStore<B>,
        term: u64,
        leader: bool,
    ) -> Result<Option<AppliedPrefix>, ApplicationError> {
        match self {
            Self::Committed(p) => p.turn(store, term, leader),
            Self::NoApplication => Ok(None),
        }
    }
    pub fn stop(&self) {
        if let Self::Committed(p) = self {
            p.shared.close();
            p.shared.status.lock().expect("application status").retired = true;
        }
    }
    pub fn fence(&self, term: u64, leader: bool) {
        if let Self::Committed(p) = self {
            p.shared.fence(term, leader);
        }
    }
    pub fn retire(&self) {
        if let Self::Committed(p) = self {
            p.retire();
        }
    }
    pub fn uncertain(&self) -> bool {
        match self {
            Self::Committed(p) => p.hub.uncertain(),
            Self::NoApplication => false,
        }
    }
    pub fn actual(&self) -> u64 {
        match self {
            Self::Committed(p) => p.actual(),
            Self::NoApplication => 0,
        }
    }
}

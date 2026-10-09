//! Opt-in replay of original proposals through a captured committed frontier.
//!
//! Callers must exclusively serialize ALL journal, indexed ownership and domain
//! writes, including raw/cloned stores and unindexed state machines. One shared
//! store identity and `&mut self` do not provide CAS or concurrent-writer safety.
//! No journal mutation, frontier refresh, runtime activation or outcome recovery
//! is exposed here. Proposal bytes establish persistence shape, not authenticated
//! authority. A structurally valid uncommitted tail remains opaque and unapplied.

use std::sync::Arc;

use domain::{
    DurableProposal, DurableProposalError, IndexedApplyError, IndexedApplyOutcome, IndexedWriter,
};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use thiserror::Error;

use crate::journal::{
    JOURNAL_VALIDATION_PAGE_ENTRIES, Journal, JournalError, MAX_JOURNAL_READ_ENTRIES,
};

/// Progress only; no previous command outcome or acknowledgement is retained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplayProgress {
    pub applied_index: u64,
    pub committed_index: u64,
    pub processed: usize,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ReplayError {
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error(transparent)]
    Indexed(#[from] IndexedApplyError),
    #[error("committed proposal at index {index} is invalid: {source}")]
    Proposal {
        index: u64,
        #[source]
        source: DurableProposalError,
    },
    #[error("applied index {applied} exceeds committed index {committed}")]
    AppliedAhead { applied: u64, committed: u64 },
    #[error("replay batch limit {requested} is outside 1..={maximum}")]
    InvalidBatchLimit { requested: usize, maximum: usize },
    #[error("the replay owner must be reopened after a fatal replay failure")]
    Unusable,
    #[error("replay ownership is corrupt: {detail}")]
    Corrupt { detail: &'static str },
}

/// A non-Clone replay owner over one original store moved into an internal Arc.
///
/// The committed and appended frontiers are captured at open. To observe later
/// journal commitment, drop this owner and reopen under the same external write
/// exclusivity. A fatal batch error or explicitly caught unwind retires every
/// retained API until reopen. No panic is caught or replaced here.
#[derive(Debug)]
pub struct CommittedReplay<S> {
    journal: Journal<SharedStore<S>>,
    writer: IndexedWriter<SharedStore<S>>,
    applied: u64,
    committed: u64,
    last_appended: u64,
    usable: bool,
}

impl<S: StateStore> CommittedReplay<S> {
    /// Validates the complete journal and every committed proposal without writes.
    ///
    /// Journal validation includes its structural tail; only committed payloads
    /// are proposal-decoded. All committed schemas, including already applied
    /// history, are checked before the read-free latest checkpoint identity probe.
    /// Pages are bounded to 32 entries, but total open work is linear in journal
    /// length. StateStore materializes raw values before validation, and proposal
    /// DTO/container overhead is extra; this is not a hostile-allocation bound.
    pub fn open(store: S) -> Result<Self, ReplayError> {
        let shared = SharedStore(Arc::new(store));
        let journal = Journal::open(shared.clone())?;
        let mut writer = IndexedWriter::open(shared)?;
        let applied = writer.applied_index()?;
        let committed = journal.committed_index()?;
        let last_appended = journal.last_appended_index()?;
        if applied > committed {
            return Err(ReplayError::AppliedAhead { applied, committed });
        }
        let latest = validate_committed(&journal, applied, committed)?;
        if applied != 0 {
            let proposal =
                latest.ok_or_else(|| corrupt("the latest applied proposal is missing"))?;
            if writer.apply(applied, &proposal)? != IndexedApplyOutcome::AlreadyApplied {
                return Err(corrupt("the latest checkpoint was not an exact duplicate"));
            }
        }
        Ok(Self {
            journal,
            writer,
            applied,
            committed,
            last_appended,
            usable: true,
        })
    }

    pub fn applied_index(&self) -> Result<u64, ReplayError> {
        self.require_usable()?;
        Ok(self.applied)
    }

    pub fn committed_index(&self) -> Result<u64, ReplayError> {
        self.require_usable()?;
        Ok(self.committed)
    }

    pub fn last_appended_index(&self) -> Result<u64, ReplayError> {
        self.require_usable()?;
        Ok(self.last_appended)
    }

    /// Applies up to 64 next originals with their recorded time and authority.
    ///
    /// Invalid limits leave a usable owner unchanged; caught-up replay performs
    /// no store IO. Otherwise this owner retires before the first external read
    /// or apply and restores usability only after the whole batch succeeds.
    /// A successful prefix may persist before a later fatal failure. Reopen
    /// resolves the actual indexed checkpoint without recovering old outcomes.
    pub fn replay_batch(&mut self, limit: usize) -> Result<ReplayProgress, ReplayError> {
        self.require_usable()?;
        if limit == 0 || limit > MAX_JOURNAL_READ_ENTRIES {
            return Err(ReplayError::InvalidBatchLimit {
                requested: limit,
                maximum: MAX_JOURNAL_READ_ENTRIES,
            });
        }
        if self.applied == self.committed {
            return Ok(self.progress(0));
        }

        self.usable = false;
        let next = self
            .applied
            .checked_add(1)
            .ok_or_else(|| corrupt("the next applied index overflows"))?;
        let entries = self.journal.read_committed(next, limit)?;
        if entries.is_empty() {
            return Err(corrupt("the next committed page is empty"));
        }
        let mut processed = 0;
        for entry in entries {
            let proposal = decode_proposal(entry.index(), entry.payload())?;
            match self.writer.apply(entry.index(), &proposal)? {
                IndexedApplyOutcome::Applied(_) | IndexedApplyOutcome::Refused(_) => {}
                IndexedApplyOutcome::AlreadyApplied => {
                    return Err(corrupt("a next replay index was already applied"));
                }
            }
            self.applied = entry.index();
            processed += 1;
        }
        self.usable = true;
        Ok(self.progress(processed))
    }

    fn require_usable(&self) -> Result<(), ReplayError> {
        if self.usable {
            Ok(())
        } else {
            Err(ReplayError::Unusable)
        }
    }

    fn progress(&self, processed: usize) -> ReplayProgress {
        ReplayProgress {
            applied_index: self.applied,
            committed_index: self.committed,
            processed,
        }
    }
}

fn validate_committed<S: StateStore>(
    journal: &Journal<S>,
    applied: u64,
    committed: u64,
) -> Result<Option<DurableProposal>, ReplayError> {
    let mut validated = 0_u64;
    let mut latest = None;
    while validated < committed {
        let next = validated
            .checked_add(1)
            .ok_or_else(|| corrupt("the validation cursor overflows"))?;
        let entries = journal.read_committed(next, JOURNAL_VALIDATION_PAGE_ENTRIES)?;
        if entries.is_empty() {
            return Err(corrupt("a committed validation page is empty"));
        }
        for entry in entries {
            let proposal = decode_proposal(entry.index(), entry.payload())?;
            if entry.index() == applied {
                latest = Some(proposal);
            }
            validated = entry.index();
        }
    }
    Ok(latest)
}

fn decode_proposal(index: u64, bytes: &[u8]) -> Result<DurableProposal, ReplayError> {
    DurableProposal::decode(bytes).map_err(|source| ReplayError::Proposal { index, source })
}

fn corrupt(detail: &'static str) -> ReplayError {
    ReplayError::Corrupt { detail }
}

#[derive(Debug)]
struct SharedStore<S>(Arc<S>);

impl<S> Clone for SharedStore<S> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<S: StateStore> StateStore for SharedStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.0.get(key)
    }

    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.0.apply(batch)
    }

    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.0.snapshot()
    }

    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.0.scan_from(prefix, start, limit)
    }

    fn scan_prefix(&self, prefix: &[u8], limit: usize) -> Result<Vec<(Key, Value)>, StorageError> {
        self.0.scan_prefix(prefix, limit)
    }
}

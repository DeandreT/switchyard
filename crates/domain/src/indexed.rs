//! Opt-in indexed effects over an externally exclusive domain store owner.
//!
//! Callers must serialize ALL domain and `0xF1` writes, including unindexed
//! state machines and cloned/raw stores. `&mut self` is not a database CAS.
//! The disjoint `0xF0` journal may share this database; no replay is activated.

use sha2::{Digest, Sha256};
use storage::{StateStore, StorageError, WriteBatch};
use thiserror::Error;

use crate::{
    BrokerError, CommandKind, CommandOutcome, DurableProposal, DurableProposalAuthority,
    DurableProposalError, StateMachine,
};

const PREFIX: &[u8] = b"\xF1switchyard/replay\0";
const OWNER_TAG: u8 = 0;
const CHECKPOINT_TAG: u8 = 1;
const VERSION: u32 = 1;
const OWNER_MAGIC: &[u8; 4] = b"SWIA";
const OWNER_BYTES: usize = 40;
const CHECKPOINT_BYTES: usize = 72;
const OWNER_HASH_SCOPE: &[u8] = b"switchyard indexed owner v1\0";
const CHECKPOINT_HASH_SCOPE: &[u8] = b"switchyard indexed checkpoint v1\0";
const PROPOSAL_HASH_SCOPE: &[u8] = b"switchyard indexed proposal v1\0";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IndexedApplyOutcome {
    Applied(CommandOutcome),
    Refused(BrokerError),
    /// Only the latest index and proposal identity are retained, not its result.
    AlreadyApplied,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum IndexedApplyError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Proposal(#[from] DurableProposalError),
    #[error("domain preparation failed without recording a checkpoint: {0}")]
    Domain(BrokerError),
    #[error("the indexed writer must be reopened after an ambiguous failed write")]
    Unusable,
    #[error("indexed owner format {found} is unsupported; expected {expected}")]
    UnsupportedVersion { found: u32, expected: u32 },
    #[error("indexed ownership is corrupt: {detail}")]
    Corrupt { detail: &'static str },
    #[error("populated unindexed domain state cannot be adopted")]
    PopulatedStore,
    #[error("an applied index must be nonzero")]
    ZeroIndex,
    #[error("applied index {requested} is not the next index {expected}")]
    UnexpectedIndex { requested: u64, expected: u64 },
    #[error("proposal at the latest applied index {index} does not match")]
    ConflictingProposal { index: u64 },
    #[error("applied index space is exhausted")]
    IndexExhausted,
}

/// One opt-in writer whose checkpoint is atomic with domain effects.
///
/// Deliberately not `Clone`. Other store handles and unindexed application can
/// bypass it, so external exclusive write ownership is a required precondition.
/// An attempted storage apply retires the writer until success and cache update.
/// A returned error or explicitly caught unwind requires reopening.
/// No panic is caught here.
/// This is not quorum, fsync-fault certification, a cached acknowledgement or replay.
#[derive(Debug)]
pub struct IndexedWriter<S> {
    machine: StateMachine<S>,
    checkpoint: Option<Checkpoint>,
    usable: bool,
}

#[derive(Clone, Copy, Debug)]
struct Checkpoint {
    index: u64,
    proposal_hash: [u8; 32],
}

impl<S: StateStore> IndexedWriter<S> {
    /// Checks reserved ownership without writing or adopting existing effects.
    /// Only two fixed-size records are valid anywhere beneath the `0xF1` tag.
    /// StateStore still materializes raw values before checking their lengths.
    pub fn open(store: S) -> Result<Self, IndexedApplyError> {
        let entries = store.scan_prefix(&[0xF1], 3)?;
        if entries.len() > 2 {
            return Err(corrupt("unexpected ownership rows"));
        }
        let checkpoint = if entries.is_empty() {
            for tag in 0x00..=0x11 {
                if !store.scan_prefix(&[tag], 1)?.is_empty() {
                    return Err(IndexedApplyError::PopulatedStore);
                }
            }
            None
        } else {
            if entries.len() != 2
                || entries[0].0 != key(OWNER_TAG)
                || entries[1].0 != key(CHECKPOINT_TAG)
            {
                return Err(corrupt("ownership keys are incomplete or noncanonical"));
            }
            decode_owner(&entries[0].1)?;
            Some(decode_checkpoint(&entries[1].1)?)
        };
        Ok(Self {
            machine: StateMachine::new(store),
            checkpoint,
            usable: true,
        })
    }

    pub fn applied_index(&self) -> Result<u64, IndexedApplyError> {
        self.require_usable()?;
        Ok(self.checkpoint.map_or(0, |checkpoint| checkpoint.index))
    }

    /// Hashes the canonical persistence proposal, then prepares one next index.
    /// A latest duplicate never reads domain state, authority or Clock, and
    /// returns no previous outcome. Failed preparation drops all staged effects.
    pub fn apply(
        &mut self,
        index: u64,
        proposal: &DurableProposal,
    ) -> Result<IndexedApplyOutcome, IndexedApplyError> {
        self.require_usable()?;
        if index == 0 {
            return Err(IndexedApplyError::ZeroIndex);
        }
        let proposal_hash = checksum(PROPOSAL_HASH_SCOPE, &proposal.encode()?);
        if let Some(latest) = self.checkpoint
            && index == latest.index
        {
            return if proposal_hash == latest.proposal_hash {
                Ok(IndexedApplyOutcome::AlreadyApplied)
            } else {
                Err(IndexedApplyError::ConflictingProposal { index })
            };
        }
        let expected = self
            .checkpoint
            .map_or(Some(1), |checkpoint| checkpoint.index.checked_add(1))
            .ok_or(IndexedApplyError::IndexExhausted)?;
        if index != expected {
            return Err(IndexedApplyError::UnexpectedIndex {
                requested: index,
                expected,
            });
        }

        let command = proposal.command();
        let prepared = match proposal.authority() {
            DurableProposalAuthority::Unbound => self.machine.prepare(command),
            DurableProposalAuthority::Bound(binding) => {
                self.machine.prepare_bound(command, binding)
            }
        };
        let (result, mut batch) = match prepared {
            Ok((outcome, batch)) => (IndexedApplyOutcome::Applied(outcome), batch),
            Err(error) if deterministic_refusal(&command.kind, &error) => {
                (IndexedApplyOutcome::Refused(error), WriteBatch::default())
            }
            Err(error) => return Err(IndexedApplyError::Domain(error)),
        };
        let checkpoint = Checkpoint {
            index,
            proposal_hash,
        };
        if self.checkpoint.is_none() {
            batch.push_put(key(OWNER_TAG), encode_owner());
        }
        batch.push_put(key(CHECKPOINT_TAG), encode_checkpoint(checkpoint));
        self.usable = false;
        if let Err(error) = self.machine.store().apply(batch) {
            return Err(IndexedApplyError::Storage(error));
        }
        self.checkpoint = Some(checkpoint);
        self.usable = true;
        Ok(result)
    }

    fn require_usable(&self) -> Result<(), IndexedApplyError> {
        if self.usable {
            Ok(())
        } else {
            Err(IndexedApplyError::Unusable)
        }
    }
}

fn deterministic_refusal(kind: &CommandKind, error: &BrokerError) -> bool {
    use BrokerError as E;
    use CommandKind as C;
    match error {
        E::StaleEntityBinding
        | E::ClockRegression { .. }
        | E::QueueNotFound
        | E::QueueAlreadyExists
        | E::TopicNotFound
        | E::TopicAlreadyExists
        | E::EntityAlreadyExists
        | E::EntityPathReserved
        | E::SubscriptionAlreadyExists
        | E::SubscriptionNotFound
        | E::RuleAlreadyExists { .. }
        | E::RuleNotFound { .. }
        | E::EmptyRulePage
        | E::RulePageTooLarge { .. }
        | E::SubscriptionSendNotAllowed
        | E::TopicReceiveNotSupported
        | E::TopicSessionNotSupported
        | E::MessageNotFound { .. }
        | E::LockTokenMismatch { .. }
        | E::LockExpired { .. }
        | E::MessageTooLarge { .. }
        | E::MessageIdTooLong { .. }
        | E::EmptyMessageBatch
        | E::MessageBatchSessionMismatch
        | E::EmptyPeek
        | E::EmptyDeferredReceive
        | E::DeferredReceiveBatchTooLarge { .. }
        | E::DuplicateDeferredSequence { .. }
        | E::MessageNotDeferred { .. }
        | E::EmptyScheduledCancellation
        | E::DuplicateScheduledSequence { .. }
        | E::DeferredMessageSessionMismatch { .. }
        | E::DeadLetterQueueIsReserved
        | E::SessionRequired
        | E::SessionNotSupported
        | E::SessionAlreadyLocked { .. }
        | E::SessionLockNotHeld { .. }
        | E::SessionLockExpired { .. } => true,
        E::QueueConfig(_) => matches!(
            kind,
            C::CreateQueue { .. } | C::UpdateQueue { .. } | C::CreateSubscription { .. }
        ),
        E::TopicConfig(_) => matches!(kind, C::CreateTopic { .. }),
        E::SubscriptionConfig(_) => matches!(kind, C::CreateSubscription { .. }),
        E::RuleConfig(_) => matches!(
            kind,
            C::CreateRule { .. } | C::Send { .. } | C::SendBatch { .. }
        ),
        E::MessageNotLocked { .. } => matches!(
            kind,
            C::Complete { .. }
                | C::Abandon { .. }
                | C::Defer { .. }
                | C::DeadLetter { .. }
                | C::RenewLock { .. }
        ),
        E::MessageNotScheduled { .. } => matches!(kind, C::CancelScheduled { .. }),
        // Catalog caps conflate valid full catalogs with corrupt oversized ones.
        // Unmatched errors, including future variants, never advance the index.
        _ => false,
    }
}

fn key(tag: u8) -> Vec<u8> {
    let mut key = PREFIX.to_vec();
    key.push(tag);
    key
}

fn checksum(scope: &[u8], bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(scope);
    hash.update(bytes);
    hash.finalize().into()
}

fn encode_owner() -> Vec<u8> {
    let mut bytes = OWNER_MAGIC.to_vec();
    bytes.extend_from_slice(&VERSION.to_be_bytes());
    bytes.extend_from_slice(&checksum(OWNER_HASH_SCOPE, &bytes));
    bytes
}

fn decode_owner(bytes: &[u8]) -> Result<(), IndexedApplyError> {
    if bytes.len() != OWNER_BYTES || &bytes[..4] != OWNER_MAGIC {
        return Err(corrupt("owner framing is malformed"));
    }
    if bytes[8..] != checksum(OWNER_HASH_SCOPE, &bytes[..8]) {
        return Err(corrupt("owner checksum does not match"));
    }
    let version = u32::from_be_bytes(bytes[4..8].try_into().expect("four version bytes"));
    if version != VERSION {
        return Err(IndexedApplyError::UnsupportedVersion {
            found: version,
            expected: VERSION,
        });
    }
    Ok(())
}

fn encode_checkpoint(checkpoint: Checkpoint) -> Vec<u8> {
    let mut bytes = checkpoint.index.to_be_bytes().to_vec();
    bytes.extend_from_slice(&checkpoint.proposal_hash);
    bytes.extend_from_slice(&checksum(CHECKPOINT_HASH_SCOPE, &bytes));
    bytes
}

fn decode_checkpoint(bytes: &[u8]) -> Result<Checkpoint, IndexedApplyError> {
    if bytes.len() != CHECKPOINT_BYTES {
        return Err(corrupt("checkpoint framing is malformed"));
    }
    if bytes[40..] != checksum(CHECKPOINT_HASH_SCOPE, &bytes[..40]) {
        return Err(corrupt("checkpoint checksum does not match"));
    }
    let index = u64::from_be_bytes(bytes[..8].try_into().expect("eight index bytes"));
    if index == 0 {
        return Err(corrupt("persisted checkpoint index is zero"));
    }
    Ok(Checkpoint {
        index,
        proposal_hash: bytes[8..40].try_into().expect("32 digest bytes"),
    })
}

fn corrupt(detail: &'static str) -> IndexedApplyError {
    IndexedApplyError::Corrupt { detail }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_owner_and_checkpoint_v1_bytes_are_frozen() {
        let owner_golden = vec![
            0x53, 0x57, 0x49, 0x41, 0, 0, 0, 1, 0x14, 0xf8, 0xfc, 0x73, 0x11, 0xc2, 0xb3, 0x4b,
            0xaa, 0x94, 0xec, 0x5e, 0xa3, 0xe5, 0x63, 0x46, 0x8c, 0xa9, 0x1d, 0xc1, 0x1f, 0xd6,
            0xa9, 0x67, 0x15, 0x03, 0x0e, 0xe7, 0x86, 0x77, 0xc6, 0x57,
        ];
        assert_eq!(encode_owner(), owner_golden);
        decode_owner(&owner_golden).unwrap();
        let mut checkpoint_golden = vec![0, 0, 0, 0, 0, 0, 0, 1];
        checkpoint_golden.extend_from_slice(&[0x42; 32]);
        checkpoint_golden.extend_from_slice(&[
            0x9f, 0xd2, 0xbf, 0x09, 0x6d, 0xb7, 0xe3, 0x19, 0x68, 0x3b, 0x29, 0xb5, 0x1b, 0xfb,
            0x0d, 0x79, 0x4e, 0x6c, 0xfd, 0x85, 0xd0, 0x45, 0x00, 0xcc, 0x41, 0x2a, 0x72, 0x52,
            0x91, 0x75, 0xad, 0x57,
        ]);
        assert_eq!(
            encode_checkpoint(Checkpoint {
                index: 1,
                proposal_hash: [0x42; 32]
            }),
            checkpoint_golden
        );
        let decoded = decode_checkpoint(&checkpoint_golden).unwrap();
        assert_eq!(decoded.index, 1);
        assert_eq!(decoded.proposal_hash, [0x42; 32]);
        assert_eq!(key(OWNER_TAG), b"\xF1switchyard/replay\0\0");
        assert_eq!(key(CHECKPOINT_TAG), b"\xF1switchyard/replay\0\x01");
    }
}

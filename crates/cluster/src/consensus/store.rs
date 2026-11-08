use protobuf::Message;
use raft::{
    GetEntriesContext, RaftState,
    eraftpb::{ConfState, Entry, EntryType, HardState, Snapshot},
};
use storage::{ConsensusBackend, WriteBatch};

use super::{
    ConsensusError, ConsensusIdentity, ENTRY, HARD_STATE, MAX_CONSENSUS_CONTEXT_BYTES,
    MAX_CONSENSUS_ENTRY_BYTES, MAX_CONSENSUS_PAGE_ENTRIES, MAX_CONSENSUS_PROPOSAL_BYTES, PREFIX,
    ROOT, codec, entry_key, key,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EntryKind {
    Blank,
    Proposal,
    Configuration,
}

/// Classify without treating a consensus control entry as a domain command.
/// Validation at admission/open additionally fences unsupported configurations.
pub fn entry_kind(entry: &Entry) -> EntryKind {
    match entry.get_entry_type() {
        EntryType::EntryNormal if entry.data.is_empty() => EntryKind::Blank,
        EntryType::EntryNormal => EntryKind::Proposal,
        _ => EntryKind::Configuration,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetainedLogLimits {
    pub max_entries: u64,
    /// Logical entry keys plus framed values, not disk usage/allocator overhead.
    pub max_bytes: u64,
}

impl Default for RetainedLogLimits {
    fn default() -> Self {
        Self {
            max_entries: 4096,
            max_bytes: 256 * 1024 * 1024,
        }
    }
}

impl RetainedLogLimits {
    fn validate(self) -> Result<Self, ConsensusError> {
        if self.max_entries == 0
            || self.max_entries > u64::from(u32::MAX / 2)
            || self.max_bytes < (MAX_CONSENSUS_ENTRY_BYTES + 128) as u64
        {
            return Err(ConsensusError::Limit(
                "retention limits must fit a full-size entry and bounded native batch",
            ));
        }
        Ok(self)
    }
}

/// One exclusive, non-Clone persistence owner. No backend reference, Clone or
/// unindexed writer escapes. Custom backends must honor ConsensusBackend's
/// external exclusion contract. This is not authenticated commit authority.
///
/// ```compile_fail
/// use cluster::consensus::ConsensusStore;
/// fn duplicate<B: storage::ConsensusBackend>(owner: ConsensusStore<B>) {
///     let alias = owner.clone();
/// }
/// ```
#[derive(Debug)]
pub struct ConsensusStore<B> {
    backend: B,
    identity: ConsensusIdentity,
    local_id: u64,
    limits: RetainedLogLimits,
    hard_state: HardState,
    last: u64,
    bytes: u64,
    usable: bool,
}

impl<B: ConsensusBackend> ConsensusStore<B> {
    /// Only an entirely empty record space is eligible. Writes root and empty
    /// hard state in one batch; never adopts F0/F1, domain rows or partial F2.
    pub fn bootstrap(
        mut backend: B,
        identity: ConsensusIdentity,
        local_id: u64,
        limits: RetainedLogLimits,
    ) -> Result<Self, ConsensusError> {
        let limits = limits.validate()?;
        let root = identity.replica_bytes(local_id)?;
        if !backend.scan_from(b"", b"", 1)?.is_empty() {
            return Err(ConsensusError::NotFresh);
        }
        let hs = HardState::default();
        // No owner escapes if this write fails or unwinds. Reopen the original
        // backend/directory to discover whether the bootstrap batch persisted.
        backend.apply(
            WriteBatch::default()
                .put(key(ROOT), codec::frame(ROOT, &root))
                .put(
                    key(HARD_STATE),
                    codec::frame(HARD_STATE, &hs.write_to_bytes()?),
                ),
        )?;
        Ok(Self {
            backend,
            identity,
            local_id,
            limits,
            hard_state: hs,
            last: 0,
            bytes: 0,
            usable: true,
        })
    }

    /// Read-only full validation using bounded pages, with O(retained log) work.
    /// Backend value allocation precedes validation; limits are logical, not a
    /// hostile allocator or physical disk bound. No repair/truncation occurs.
    pub fn open(
        backend: B,
        identity: ConsensusIdentity,
        local_id: u64,
        limits: RetainedLogLimits,
    ) -> Result<Self, ConsensusError> {
        let limits = limits.validate()?;
        let expected = identity.replica_bytes(local_id)?;
        let mut cursor = Vec::new();
        let mut previous: Option<Vec<u8>> = None;
        let mut root = false;
        let mut hard_state = None;
        let mut last = 0;
        let mut previous_term = 0;
        let mut bytes = 0u64;
        loop {
            let page = backend.scan_from(b"", &cursor, MAX_CONSENSUS_PAGE_ENTRIES)?;
            if page.len() > MAX_CONSENSUS_PAGE_ENTRIES {
                return Err(ConsensusError::Corrupt("oversized scan page"));
            }
            if page.is_empty() {
                break;
            }
            for (k, value) in page {
                if k < cursor
                    || previous.as_ref().is_some_and(|p| k <= *p)
                    || !k.starts_with(PREFIX)
                {
                    return Err(ConsensusError::Corrupt(
                        "foreign namespace or noncanonical scan",
                    ));
                }
                let suffix = &k[PREFIX.len()..];
                match suffix {
                    [ROOT] => {
                        if codec::unframe(ROOT, &value, 2048)? != expected {
                            return Err(ConsensusError::ForeignIdentity);
                        }
                        root = true;
                    }
                    [HARD_STATE] => {
                        hard_state = Some(codec::decode_hard_state(&value)?);
                    }
                    [ENTRY, index @ ..] if index.len() == 8 => {
                        let hs = hard_state.as_ref().ok_or(ConsensusError::MissingMetadata)?;
                        let entry = codec::decode_entry(&k, &value)?;
                        validate_entry(&entry, &identity)?;
                        if !root
                            || entry.index != last + 1
                            || entry.term < previous_term
                            || entry.term > hs.term
                        {
                            return Err(ConsensusError::Corrupt(
                                "log hole, term regression or incomplete metadata",
                            ));
                        }
                        last = entry.index;
                        previous_term = entry.term;
                        bytes = bytes
                            .checked_add((k.len() + value.len()) as u64)
                            .ok_or(ConsensusError::Limit("entry bytes overflow"))?;
                        if last > limits.max_entries || bytes > limits.max_bytes {
                            return Err(ConsensusError::Limit("retained log"));
                        }
                    }
                    _ => return Err(ConsensusError::Corrupt("unknown consensus record")),
                }
                previous = Some(k);
            }
            cursor = previous.as_ref().expect("nonempty page").clone();
            cursor.push(0);
        }
        let hs = hard_state
            .filter(|_| root)
            .ok_or(ConsensusError::MissingMetadata)?;
        validate_hard_state(&identity, &HardState::default(), &hs, last)?;
        Ok(Self {
            backend,
            identity,
            local_id,
            limits,
            hard_state: hs,
            last,
            bytes,
            usable: true,
        })
    }

    /// Consumes even a retired owner and revalidates its original backend. No
    /// old cached frontier or outcome is reused, and no raw handle is returned.
    pub fn reopen(self) -> Result<Self, ConsensusError> {
        Self::open(self.backend, self.identity, self.local_id, self.limits)
    }

    pub fn is_retired(&self) -> bool {
        !self.usable
    }
    pub fn identity(&self) -> &ConsensusIdentity {
        &self.identity
    }
    pub fn local_id(&self) -> u64 {
        self.local_id
    }
    pub fn hard_state(&self) -> Result<&HardState, ConsensusError> {
        self.require_usable()?;
        Ok(&self.hard_state)
    }
    pub fn last_index(&self) -> Result<u64, ConsensusError> {
        self.require_usable()?;
        Ok(self.last)
    }
    pub fn retained_bytes(&self) -> Result<u64, ConsensusError> {
        self.require_usable()?;
        Ok(self.bytes)
    }

    fn require_usable(&self) -> Result<(), ConsensusError> {
        if self.usable {
            Ok(())
        } else {
            Err(ConsensusError::Retired)
        }
    }

    fn read_entry(&self, index: u64) -> Result<Entry, ConsensusError> {
        let k = entry_key(index);
        let raw = self
            .backend
            .get(&k)?
            .ok_or(ConsensusError::Corrupt("missing retained entry"))?;
        let entry = codec::decode_entry(&k, &raw)?;
        validate_entry(&entry, &self.identity)?;
        Ok(entry)
    }

    /// Bounded tail access, including uncommitted entries, without promotion.
    pub fn read_entries(&self, from: u64, limit: usize) -> Result<Vec<Entry>, ConsensusError> {
        self.require_usable()?;
        if from == 0 || limit == 0 || limit > MAX_CONSENSUS_PAGE_ENTRIES {
            return Err(ConsensusError::Limit(
                "nonzero start and 1..=64 entries required",
            ));
        }
        let count = self
            .last
            .saturating_add(1)
            .saturating_sub(from)
            .min(limit as u64);
        (from..from + count).map(|i| self.read_entry(i)).collect()
    }

    /// Return the 1-based application ordinal ONLY for a committed proposal.
    /// Control entries return None. Ordinals count proposal entries in [1,index],
    /// never Raft blanks/configuration records. No log compaction exists, so the
    /// mapping is fully derivable and survives restart without a second clock or
    /// outcome record. An uncommitted ordinal must not be used for projection.
    pub fn application_ordinal(&self, index: u64) -> Result<Option<u64>, ConsensusError> {
        self.require_usable()?;
        if index == 0 || index > self.hard_state.commit {
            return Err(ConsensusError::InvalidUpdate(
                "application mapping requires a committed index",
            ));
        }
        let mut ordinal = 0;
        for i in 1..=index {
            let kind = entry_kind(&self.read_entry(i)?);
            if kind == EntryKind::Proposal {
                ordinal += 1;
            }
            if i == index {
                return Ok((kind == EntryKind::Proposal).then_some(ordinal));
            }
        }
        unreachable!("nonzero index")
    }

    /// Persist one trusted core update atomically. Does NOT certify quorum.
    /// Input entries are contiguous; duplicate matching prefixes are no-ops.
    /// Only the first actual uncommitted conflict discards an old suffix. A
    /// matching shorter append never discards its later history. Term/vote and
    /// commit cannot regress; same-term byte conflicts and committed conflicts
    /// refuse without mutation. All validation/bounds precede the single batch.
    pub fn persist(
        &mut self,
        entries: &[Entry],
        hard_state: Option<&HardState>,
    ) -> Result<(), ConsensusError> {
        self.require_usable()?;
        if entries.len() as u64 > self.limits.max_entries {
            return Err(ConsensusError::Limit("persistence batch entries"));
        }
        let hs = hard_state.unwrap_or(&self.hard_state).clone();
        let mut first_change = None;
        let mut prev_term = if let Some(first) = entries.first() {
            if first.index == 0 || first.index > self.last + 1 {
                return Err(ConsensusError::InvalidUpdate("append would create a hole"));
            }
            if first.index > 1 {
                self.read_entry(first.index - 1)?.term
            } else {
                0
            }
        } else {
            0
        };
        for (offset, entry) in entries.iter().enumerate() {
            validate_entry(entry, &self.identity)?;
            if entry.index != entries[0].index + offset as u64
                || entry.term < prev_term
                || entry.term > hs.term
            {
                return Err(ConsensusError::InvalidUpdate("entry index/term order"));
            }
            prev_term = entry.term;
            // A selected rewrite boundary does not relax validation of later
            // overlaps, including same-term entries with conflicting bytes.
            if entry.index <= self.last {
                let old = self.read_entry(entry.index)?;
                if old == *entry {
                    continue;
                }
                if entry.index <= self.hard_state.commit {
                    return Err(ConsensusError::InvalidUpdate("committed entry conflict"));
                }
                if old.term == entry.term {
                    return Err(ConsensusError::InvalidUpdate(
                        "same-term entry bytes conflict",
                    ));
                }
            }
            if first_change.is_none() {
                first_change = Some(offset);
            }
        }
        let last = first_change.map_or(self.last, |_| entries.last().expect("changed entry").index);
        validate_hard_state(&self.identity, &self.hard_state, &hs, last)?;
        if last > self.limits.max_entries {
            return Err(ConsensusError::Limit("retained log"));
        }
        let mut bytes = self.bytes;
        if let Some(offset) = first_change {
            let from = entries[offset].index;
            // Retention caps bound both deletion count and native batch size.
            for index in from..=self.last {
                let k = entry_key(index);
                let value = self
                    .backend
                    .get(&k)?
                    .ok_or(ConsensusError::Corrupt("missing replacement suffix"))?;
                bytes = bytes
                    .checked_sub((k.len() + value.len()) as u64)
                    .ok_or(ConsensusError::Corrupt("retained byte accounting"))?;
            }
            for entry in &entries[offset..] {
                bytes = bytes
                    .checked_add(
                        (entry_key(entry.index).len() + codec::FRAME_OVERHEAD) as u64
                            + u64::from(entry.compute_size()),
                    )
                    .ok_or(ConsensusError::Limit("entry bytes overflow"))?;
                if bytes > self.limits.max_bytes {
                    return Err(ConsensusError::Limit("retained log"));
                }
            }
        }
        // Bounds precede serialization/copying of the replacement values.
        let mut batch = WriteBatch::default();
        if let Some(offset) = first_change {
            for index in entries[offset].index..=self.last {
                batch.push_delete(entry_key(index));
            }
            for entry in &entries[offset..] {
                batch.push_put(entry_key(entry.index), codec::encode_entry(entry)?);
            }
        }
        if hs != self.hard_state {
            batch.push_put(
                key(HARD_STATE),
                codec::frame(HARD_STATE, &hs.write_to_bytes()?),
            );
        }
        if batch.is_empty() {
            return Ok(());
        }
        // Retire BEFORE invoking external persistence. No catch/retry/rollback:
        // an error or escaping panic may follow a fully durable original batch.
        self.usable = false;
        self.backend.apply(batch)?;
        self.hard_state = hs;
        self.last = last;
        self.bytes = bytes;
        self.usable = true;
        Ok(())
    }
}

fn validate_entry(entry: &Entry, identity: &ConsensusIdentity) -> Result<(), ConsensusError> {
    if entry.index == 0
        || entry.index == u64::MAX
        || entry.term == 0
        || entry.term == u64::MAX
        || entry.unknown_fields.iter().next().is_some()
    {
        return Err(ConsensusError::InvalidUpdate(
            "entry index/term or unknown fields",
        ));
    }
    if entry.data.len() > MAX_CONSENSUS_PROPOSAL_BYTES
        || entry.context.len() > MAX_CONSENSUS_CONTEXT_BYTES
        || entry.compute_size() as usize > MAX_CONSENSUS_ENTRY_BYTES
    {
        return Err(ConsensusError::Limit("entry payload/context"));
    }
    match entry.get_entry_type() {
        EntryType::EntryNormal if !entry.data.is_empty() => {
            let proposal = domain::DurableProposal::decode(&entry.data)?;
            if &proposal.command().namespace != identity.namespace() {
                return Err(ConsensusError::ForeignIdentity);
            }
        }
        EntryType::EntryNormal => {}
        // Static bootstrap supplies ConfState, not configuration log records.
        // Even an empty V2 means leave-joint in this core, not a harmless blank
        // in our non-joint group. Never admit it as a synthetic domain no-op.
        EntryType::EntryConfChange | EntryType::EntryConfChangeV2 => {
            return Err(ConsensusError::MembershipChange);
        }
    }
    Ok(())
}

fn validate_hard_state(
    identity: &ConsensusIdentity,
    old: &HardState,
    hs: &HardState,
    last: u64,
) -> Result<(), ConsensusError> {
    if hs.unknown_fields.iter().next().is_some()
        || hs.term == u64::MAX
        || hs.term < old.term
        || hs.commit < old.commit
        || hs.commit > last
        || (hs.term == old.term && old.vote != 0 && hs.vote != old.vote)
        || (hs.vote != 0 && !identity.contains(hs.vote))
        || (hs.term == 0 && (hs.vote != 0 || hs.commit != 0 || last != 0))
    {
        return Err(ConsensusError::InvalidUpdate(
            "term, vote or commit would violate persisted state",
        ));
    }
    Ok(())
}

fn raft_error(error: ConsensusError) -> raft::Error {
    raft::Error::Store(raft::StorageError::Other(Box::new(error)))
}

impl<B: ConsensusBackend> raft::Storage for ConsensusStore<B> {
    fn initial_state(&self) -> raft::Result<RaftState> {
        self.require_usable().map_err(raft_error)?;
        let conf = ConfState {
            voters: self.identity.voters().iter().map(|v| v.id).collect(),
            ..Default::default()
        };
        Ok(RaftState::new(self.hard_state.clone(), conf))
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        self.require_usable().map_err(raft_error)?;
        if low == 0 {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }
        if low > high || high > self.last + 1 {
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }
        let max = max_size.into().unwrap_or(u64::MAX);
        let mut total = 0u64;
        let mut result = Vec::new();
        // The full retained log is caller-capped. Respect the core contract:
        // return at least one entry even when max_size is below its size.
        for index in low..high {
            let entry = self.read_entry(index).map_err(raft_error)?;
            let size = u64::from(entry.compute_size());
            if !result.is_empty() && total.saturating_add(size) > max {
                break;
            }
            total = total.saturating_add(size);
            result.push(entry);
        }
        Ok(result)
    }

    fn term(&self, index: u64) -> raft::Result<u64> {
        self.require_usable().map_err(raft_error)?;
        if index == 0 {
            return Ok(0);
        }
        if index > self.last {
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }
        Ok(self.read_entry(index).map_err(raft_error)?.term)
    }
    fn first_index(&self) -> raft::Result<u64> {
        self.require_usable().map_err(raft_error)?;
        Ok(1)
    }
    fn last_index(&self) -> raft::Result<u64> {
        self.last_index().map_err(raft_error)
    }
    fn snapshot(&self, _request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        self.require_usable().map_err(raft_error)?;
        Err(raft::Error::Store(
            raft::StorageError::SnapshotTemporarilyUnavailable,
        ))
    }
}

use std::cmp::Ordering;

use domain::{
    CommittedCheckpoint, CommittedCheckpointUpdate, CommittedEntryId, CommittedMembership,
    CommittedQueueWork, Timestamp,
};
use openraft::EntryPayload;
use storage::{CommittedStore, StateStore, WriteBatch};

use super::super::{
    codec as entry_codec,
    types::{
        ENTRY_PREFIX, EncodedAppend, EncodedEntry, LogProgress, PROFILE_KEY, PROGRESS_KEY,
        entry_key,
    },
};
use super::codec::{self, BASELINE_KEY, Baseline, raft_id};
use crate::{
    LogEntry, LogId, LogProfile, LogVote,
    experimental_local_compaction::{
        LocalCompactionError as Error,
        frontier::{Frontier, PairIdentity, PurgePermit, Receipt},
    },
    experimental_state_machine::NativeCheckpointSummary,
};

pub(super) struct StoreState<W: CommittedStore> {
    writer: W,
    reader: W::Reader,
    profile: LogProfile,
    progress: LogProgress,
    baseline: Baseline,
    identity: Option<PairIdentity>,
    poisoned: bool,
}

pub(crate) struct SealReport {
    pub(crate) profile: LogProfile,
    pub(crate) ordinal: u64,
    pub(crate) retained_entries: u64,
    pub(crate) retained_bytes: u64,
    pub(crate) already_compacted: bool,
}

impl<W: CommittedStore> StoreState<W> {
    pub(super) fn create(mut writer: W, profile: LogProfile) -> Result<Self, Error> {
        let encoded_profile = codec::encode_profile(&profile)?;
        if writer.is_initialized().map_err(|_| Error::OwnerFailure)? {
            return Err(Error::InvalidPair);
        }
        let reader = writer.reader();
        if !reader
            .scan_from(&[], &[], 1)
            .map_err(|_| Error::OwnerFailure)?
            .is_empty()
        {
            return Err(Error::InvalidHistory);
        }
        let progress = LogProgress::default();
        let baseline = Baseline::empty(&profile)?;
        let mut batch = WriteBatch::default();
        batch
            .try_reserve_mutations(3)
            .map_err(|_| Error::Allocation)?;
        batch.push_put(PROFILE_KEY, encoded_profile);
        batch.push_put(PROGRESS_KEY, encode_progress(&progress)?);
        batch.push_put(BASELINE_KEY, copy(&baseline.bytes)?);
        writer.commit(batch).map_err(|_| Error::CommitUnknown)?;
        Ok(Self {
            writer,
            reader,
            profile,
            progress,
            baseline,
            identity: None,
            poisoned: false,
        })
    }

    pub(super) fn open(writer: W, profile: LogProfile) -> Result<Self, Error> {
        codec::encode_profile(&profile)?;
        if !writer.is_initialized().map_err(|_| Error::OwnerFailure)? {
            return Err(Error::InvalidPair);
        }
        let reader = writer.reader();
        let records = reader
            .scan_from(&[], &[], crate::MAX_RETAINED_ENTRIES as usize + 4)
            .map_err(|_| Error::OwnerFailure)?;
        let mut profile_seen = false;
        let mut progress = None;
        let mut baseline = None;
        let mut rows = Vec::new();
        let mut previous_key = None;
        for (key, value) in records {
            if previous_key
                .as_ref()
                .is_some_and(|previous| previous >= &key)
            {
                return Err(Error::InvalidHistory);
            }
            previous_key = Some(key.clone());
            if key == PROFILE_KEY {
                codec::check_profile(&value, &profile)?;
                profile_seen = true;
            } else if key == PROGRESS_KEY {
                progress =
                    Some(entry_codec::decode_progress(&value).map_err(|_| Error::InvalidHistory)?);
            } else if key == BASELINE_KEY {
                baseline = Some(Baseline::decode(&profile, value)?);
            } else {
                rows.push(decode_row(key, value)?);
            }
        }
        if !profile_seen {
            return Err(Error::InvalidPair);
        }
        let progress = progress.ok_or(Error::InvalidHistory)?;
        let baseline = baseline.ok_or(Error::InvalidHistory)?;
        validate_rows(&progress, &baseline, &rows)?;
        Ok(Self {
            writer,
            reader,
            profile,
            progress,
            baseline,
            identity: None,
            poisoned: false,
        })
    }

    fn healthy(&self) -> Result<(), Error> {
        if self.poisoned {
            Err(Error::OwnerFailure)
        } else {
            Ok(())
        }
    }
    fn mutable(&self) -> Result<(), Error> {
        self.healthy()?;
        if self.identity.is_some() {
            Err(Error::Closed)
        } else {
            Ok(())
        }
    }

    fn rows(&mut self) -> Result<Vec<EncodedEntry>, Error> {
        self.healthy()?;
        let records = match self.reader.scan_from(
            &[ENTRY_PREFIX],
            &[ENTRY_PREFIX],
            crate::MAX_RETAINED_ENTRIES as usize + 1,
        ) {
            Ok(records) => records,
            Err(_) => {
                self.poisoned = true;
                return Err(Error::OwnerFailure);
            }
        };
        let result = records
            .into_iter()
            .map(|(key, value)| decode_row(key, value))
            .collect::<Result<Vec<_>, _>>()
            .and_then(|rows| {
                validate_rows(&self.progress, &self.baseline, &rows)?;
                Ok(rows)
            });
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    fn refresh(&mut self) -> Result<(), Error> {
        self.healthy()?;
        let result = (|| {
            let profile = self
                .reader
                .get(PROFILE_KEY)
                .map_err(|_| Error::OwnerFailure)?
                .ok_or(Error::InvalidHistory)?;
            codec::check_profile(&profile, &self.profile)?;
            let progress = self
                .reader
                .get(PROGRESS_KEY)
                .map_err(|_| Error::OwnerFailure)?
                .ok_or(Error::InvalidHistory)?;
            let baseline = self
                .reader
                .get(BASELINE_KEY)
                .map_err(|_| Error::OwnerFailure)?
                .ok_or(Error::InvalidHistory)?;
            if progress != encode_progress(&self.progress)? || baseline != self.baseline.bytes {
                return Err(Error::InvalidHistory);
            }
            Ok(())
        })();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    pub(super) fn append(&mut self, packet: EncodedAppend) -> Result<(), Error> {
        self.mutable()?;
        let rows = self.rows()?;
        let mut progress = self.progress.clone();
        let mut batch = WriteBatch::default();
        batch
            .try_reserve_mutations(packet.entries().len() + 1)
            .map_err(|_| Error::Allocation)?;
        for row in packet.entries() {
            let id = row.id();
            if self
                .baseline
                .through()
                .is_some_and(|base| id.index <= base.index)
            {
                return Err(Error::InvalidHistory);
            }
            if let Some(existing) = rows.iter().find(|existing| existing.id().index == id.index) {
                if existing.id() != id || existing.bytes() != row.bytes() {
                    return Err(Error::InvalidHistory);
                }
                continue;
            }
            match progress.last_present.or(progress.last_purged) {
                Some(previous) if !successor(previous, id) => return Err(Error::InvalidHistory),
                None if id.index != 0 => return Err(Error::InvalidHistory),
                _ => {}
            }
            progress.retained_entries = progress
                .retained_entries
                .checked_add(1)
                .ok_or(Error::LimitExceeded)?;
            progress.retained_bytes = progress
                .retained_bytes
                .checked_add(row.encoded_len() as u64)
                .ok_or(Error::LimitExceeded)?;
            if progress.retained_entries > crate::MAX_RETAINED_ENTRIES
                || progress.retained_bytes > crate::MAX_RETAINED_BYTES
            {
                return Err(Error::LimitExceeded);
            }
            progress.last_present = Some(id);
            batch.push_put(entry_key(id.index), copy(row.bytes())?);
        }
        if progress.last_present == self.progress.last_present {
            return Ok(());
        }
        batch.push_put(PROGRESS_KEY, encode_progress(&progress)?);
        self.commit(batch)?;
        self.progress = progress;
        Ok(())
    }

    pub(super) fn read(&mut self, start: u64, through: u64) -> Result<Vec<LogEntry>, Error> {
        self.healthy()?;
        let rows = self.rows()?;
        let mut result = Vec::new();
        let mut bytes = 0usize;
        for row in rows {
            if row.id().index < start {
                continue;
            }
            if row.id().index > through {
                break;
            }
            let next = bytes
                .checked_add(row.encoded_len())
                .ok_or(Error::LimitExceeded)?;
            if result.len() == crate::MAX_LIMITED_ENTRIES || next > crate::MAX_LIMITED_BYTES {
                break;
            }
            bytes = next;
            result.push(entry_codec::decode_entry(row.bytes()).map_err(|_| Error::InvalidHistory)?);
        }
        Ok(result)
    }

    pub(super) fn vote(&mut self) -> Result<Option<LogVote>, Error> {
        self.refresh()?;
        Ok(self.progress.vote)
    }
    pub(super) fn save_vote(&mut self, vote: LogVote) -> Result<(), Error> {
        self.mutable()?;
        if let Some(previous) = self.progress.vote {
            match vote.partial_cmp(&previous) {
                Some(Ordering::Equal) => return Ok(()),
                Some(Ordering::Greater) => {}
                _ => return Err(Error::InvalidHistory),
            }
        }
        let mut progress = self.progress.clone();
        progress.vote = Some(vote);
        let mut batch = WriteBatch::default();
        batch
            .try_reserve_mutations(1)
            .map_err(|_| Error::Allocation)?;
        batch.push_put(PROGRESS_KEY, encode_progress(&progress)?);
        self.commit(batch)?;
        self.progress = progress;
        Ok(())
    }

    pub(super) fn seal(
        &mut self,
        identity: PairIdentity,
        expected: &CommittedCheckpoint,
        catalog: Option<&[u8]>,
    ) -> Result<SealReport, Error> {
        self.mutable()?;
        self.refresh()?;
        if expected.stream() != self.profile.stream() {
            return Err(Error::InvalidPair);
        }
        if expected.last().is_none() {
            return Err(Error::NoAppliedEntry);
        }
        let rows = self.rows()?;
        let catalog = catalog
            .map(NativeCheckpointSummary::decode)
            .transpose()
            .map_err(|_| Error::InvalidHistory)?;
        prove(
            &self.profile,
            &self.baseline,
            &rows,
            expected,
            catalog.as_ref(),
        )?;
        let tail = self
            .progress
            .last_present
            .or(self.progress.last_purged)
            .ok_or(Error::InvalidHistory)?;
        let minimum = LogVote::new_committed(tail.leader_id.term, tail.leader_id.node_id);
        if !self.progress.vote.is_some_and(|vote| {
            matches!(
                vote.partial_cmp(&minimum),
                Some(Ordering::Equal | Ordering::Greater)
            )
        }) {
            return Err(Error::InvalidHistory);
        }
        let already_compacted = match (&self.baseline.summary, &catalog) {
            (Some(base), Some(catalog)) => base.matches(expected) && base == catalog,
            _ => false,
        };
        self.identity = Some(identity);
        Ok(SealReport {
            profile: self.profile.clone(),
            ordinal: self.baseline.ordinal,
            retained_entries: self.progress.retained_entries,
            retained_bytes: self.progress.retained_bytes,
            already_compacted,
        })
    }

    pub(super) fn permit(
        &mut self,
        frontier: Frontier,
        receipt: Receipt,
    ) -> Result<PurgePermit, Error> {
        self.refresh()?;
        let rows = self.rows()?;
        let summary = NativeCheckpointSummary::decode(receipt.metadata.as_bytes())
            .map_err(|_| Error::InvalidHistory)?;
        if !summary.matches(&receipt.checkpoint) {
            return Err(Error::InvalidHistory);
        }
        prove(
            &self.profile,
            &self.baseline,
            &rows,
            &receipt.checkpoint,
            Some(&summary),
        )?;
        frontier.permit(
            receipt,
            self.baseline.ordinal,
            self.baseline.checksum(),
            encode_progress(&self.progress)?,
        )
    }

    pub(super) fn compact(&mut self, permit: PurgePermit) -> Result<SealReport, Error> {
        self.refresh()?;
        let identity = self.identity.as_ref().ok_or(Error::InvalidPair)?.clone();
        let through = permit
            .receipt()
            .checkpoint
            .last()
            .map(|mark| raft_id(mark.id))
            .ok_or(Error::NoAppliedEntry)?;
        let rows = self.rows()?;
        let summary = NativeCheckpointSummary::decode(permit.receipt().metadata.as_bytes())
            .map_err(|_| Error::InvalidHistory)?;
        if !summary.matches(&permit.receipt().checkpoint) {
            return Err(Error::InvalidHistory);
        }
        prove(
            &self.profile,
            &self.baseline,
            &rows,
            &permit.receipt().checkpoint,
            Some(&summary),
        )?;
        let mut progress = self.progress.clone();
        let Some(ordinal) = self.baseline.ordinal.checked_add(1) else {
            permit.exhausted();
            return Err(Error::Exhausted);
        };
        let baseline =
            Baseline::make(&self.profile, ordinal, permit.receipt().metadata.as_bytes())?;
        let mut batch = WriteBatch::default();
        batch
            .try_reserve_mutations(rows.len() + 2)
            .map_err(|_| Error::Allocation)?;
        for row in &rows {
            if row.id().index <= through.index {
                progress.retained_entries = progress
                    .retained_entries
                    .checked_sub(1)
                    .ok_or(Error::InvalidHistory)?;
                progress.retained_bytes = progress
                    .retained_bytes
                    .checked_sub(row.encoded_len() as u64)
                    .ok_or(Error::InvalidHistory)?;
                batch.push_delete(entry_key(row.id().index));
            }
        }
        progress.last_purged = Some(through);
        if progress
            .last_present
            .is_some_and(|tail| tail.index <= through.index)
        {
            progress.last_present = None;
        }
        batch.push_put(BASELINE_KEY, copy(&baseline.bytes)?);
        batch.push_put(PROGRESS_KEY, encode_progress(&progress)?);
        let _receipt = permit.claim(
            &identity,
            self.profile.stream(),
            self.profile.node_id(),
            self.baseline.ordinal,
            self.baseline.checksum(),
            &encode_progress(&self.progress)?,
        )?;
        // The claim lock is released; later close cannot revoke this admitted write.
        self.commit(batch)?;
        self.progress = progress;
        self.baseline = baseline;
        Ok(SealReport {
            profile: self.profile.clone(),
            ordinal,
            retained_entries: self.progress.retained_entries,
            retained_bytes: self.progress.retained_bytes,
            already_compacted: true,
        })
    }

    fn commit(&mut self, batch: WriteBatch) -> Result<(), Error> {
        if self.writer.commit(batch).is_err() {
            self.poisoned = true;
            return Err(Error::CommitUnknown);
        }
        Ok(())
    }
}

fn prove(
    profile: &LogProfile,
    baseline: &Baseline,
    rows: &[EncodedEntry],
    current: &CommittedCheckpoint,
    catalog: Option<&NativeCheckpointSummary>,
) -> Result<(), Error> {
    let mut last = baseline.summary.as_ref().and_then(|s| s.last);
    let mut previous = baseline.summary.as_ref().and_then(|s| s.previous);
    let mut watermark = baseline
        .summary
        .as_ref()
        .map_or(Timestamp::UNIX_EPOCH, |s| s.highest_timestamp);
    let mut membership = baseline.summary.as_ref().and_then(|s| s.membership.clone());
    let matches_current = |last, previous, watermark, membership: &Option<CommittedMembership>| {
        current.last() == last
            && current.previous() == previous
            && current.highest_timestamp() == watermark
            && current.membership() == membership.as_ref()
    };
    let matches_catalog = |last, previous, watermark, membership: &Option<CommittedMembership>| {
        catalog.is_some_and(|c| {
            c.stream == profile.stream()
                && c.last == last
                && c.previous == previous
                && c.highest_timestamp == watermark
                && c.membership == *membership
        })
    };
    let mut current_seen = matches_current(last, previous, watermark, &membership);
    let mut catalog_seen =
        catalog.is_none() || matches_catalog(last, previous, watermark, &membership);
    if let Some(base) = &baseline.summary {
        base.recover().map_err(|_| Error::InvalidHistory)?;
        let catalog = catalog.ok_or(Error::InvalidHistory)?;
        let base_last = base.last.ok_or(Error::InvalidHistory)?;
        let cat_last = catalog.last.ok_or(Error::InvalidHistory)?;
        if cat_last.id.index < base_last.id.index
            || (catalog.same_checkpoint(base) && catalog != base)
        {
            return Err(Error::InvalidHistory);
        }
    }
    if catalog.is_some_and(|c| c.last.map(|m| m.id.index) > current.last().map(|m| m.id.index)) {
        return Err(Error::InvalidHistory);
    }
    for row in rows {
        let entry = entry_codec::decode_entry(row.bytes()).map_err(|_| Error::InvalidHistory)?;
        if last.is_none()
            && (entry.log_id != LogId::default()
                || !matches!(entry.payload, EntryPayload::Membership(_)))
        {
            return Err(Error::InvalidHistory);
        }
        let id = CommittedEntryId {
            term: entry.log_id.leader_id.term,
            node_id: entry.log_id.leader_id.node_id,
            index: entry.log_id.index,
        };
        let work = match entry.payload {
            EntryPayload::Blank => CommittedQueueWork::Blank,
            EntryPayload::Membership(value) => {
                let payload = crate::experimental_log::encode_membership(&value)
                    .map_err(|_| Error::InvalidHistory)?;
                membership = Some(CommittedMembership {
                    source: id,
                    schema_version: crate::experimental_log::MEMBERSHIP_SCHEMA_VERSION,
                    payload: payload.clone(),
                });
                CommittedQueueWork::Membership {
                    schema_version: crate::experimental_log::MEMBERSHIP_SCHEMA_VERSION,
                    payload,
                }
            }
            EntryPayload::Normal(command) => {
                watermark =
                    watermark.max(crate::experimental_log::queue_command_timestamp(&command));
                command.into_committed_work()
            }
        };
        previous = last;
        last = Some(
            work.entry_mark(&CommittedCheckpointUpdate {
                stream: profile.stream(),
                expected_previous: previous,
                entry: id,
            })
            .map_err(|_| Error::InvalidHistory)?,
        );
        current_seen |= matches_current(last, previous, watermark, &membership);
        catalog_seen |= matches_catalog(last, previous, watermark, &membership);
    }
    if !current_seen || !catalog_seen {
        return Err(Error::InvalidHistory);
    }
    Ok(())
}

fn validate_rows(
    progress: &LogProgress,
    baseline: &Baseline,
    rows: &[EncodedEntry],
) -> Result<(), Error> {
    encode_progress(progress)?;
    if progress.last_purged != baseline.through()
        || rows.len() as u64 != progress.retained_entries
        || rows.len() as u64 > crate::MAX_RETAINED_ENTRIES
    {
        return Err(Error::InvalidHistory);
    }
    let mut previous = baseline.through();
    let mut bytes = 0u64;
    for row in rows {
        match previous {
            Some(id) if !successor(id, row.id()) => return Err(Error::InvalidHistory),
            None if row.id().index != 0 => return Err(Error::InvalidHistory),
            _ => {}
        }
        previous = Some(row.id());
        bytes = bytes
            .checked_add(row.encoded_len() as u64)
            .filter(|bytes| *bytes <= crate::MAX_RETAINED_BYTES)
            .ok_or(Error::InvalidHistory)?;
    }
    if bytes != progress.retained_bytes
        || rows.last().map(EncodedEntry::id) != progress.last_present
    {
        return Err(Error::InvalidHistory);
    }
    Ok(())
}
fn successor(previous: LogId, next: LogId) -> bool {
    previous.index.checked_add(1) == Some(next.index) && next > previous
}
fn decode_row(key: Vec<u8>, value: Vec<u8>) -> Result<EncodedEntry, Error> {
    if key.len() != 9 || key[0] != ENTRY_PREFIX {
        return Err(Error::InvalidHistory);
    }
    let index = u64::from_be_bytes(key[1..].try_into().map_err(|_| Error::InvalidHistory)?);
    let row = entry_codec::validate_encoded_entry(value).map_err(|_| Error::InvalidHistory)?;
    if row.id().index != index {
        return Err(Error::InvalidHistory);
    }
    Ok(row)
}
fn encode_progress(progress: &LogProgress) -> Result<Vec<u8>, Error> {
    entry_codec::encode_progress(progress).map_err(|_| Error::InvalidHistory)
}
fn copy(bytes: &[u8]) -> Result<Vec<u8>, Error> {
    let mut result = Vec::new();
    result
        .try_reserve_exact(bytes.len())
        .map_err(|_| Error::Allocation)?;
    result.extend_from_slice(bytes);
    Ok(result)
}

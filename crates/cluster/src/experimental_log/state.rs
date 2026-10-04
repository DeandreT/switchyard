use std::{
    cell::Cell,
    cmp::Ordering,
    ops::{Bound, RangeBounds},
};

use storage::{CommittedStore, StateStore, StorageError, WriteBatch};
use thiserror::Error;

use super::{
    LogRetention, codec,
    types::{
        ENTRY_PREFIX, EncodedAppend, EncodedEntry, LogEntry, LogId, LogProfile, LogProgress,
        LogTypes, LogVote, MAX_APPEND_BYTES, MAX_APPEND_ENTRIES, MAX_LIMITED_BYTES,
        MAX_LIMITED_ENTRIES, MAX_RETAINED_BYTES, MAX_RETAINED_ENTRIES, PROFILE_KEY, PROGRESS_KEY,
        entry_key,
    },
};

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub(super) enum LogStateError {
    #[error("log storage is poisoned")]
    Poisoned,
    #[error("log storage I/O failed")]
    Storage,
    #[error("log storage profile does not match")]
    InvalidProfile,
    #[error("log storage records are inconsistent")]
    Corrupt,
    #[error("log append is invalid")]
    InvalidAppend,
    #[error("log append conflicts with retained history")]
    Conflict,
    #[error("log storage capacity is exhausted")]
    Capacity,
    #[error("log read range is unavailable")]
    InvalidRange,
    #[error("log vote cannot move backward")]
    VoteRegression,
    #[error("log purge boundary is invalid")]
    InvalidPurge,
    #[error("log truncation crosses the purged boundary")]
    InvalidTruncate,
    #[error("log index is exhausted")]
    IndexExhausted,
}

#[derive(Clone, Debug)]
pub(super) struct OwnedLogRange {
    pub(super) start: Bound<u64>,
    pub(super) end: Bound<u64>,
}

impl OwnedLogRange {
    pub(super) fn from_range<R: RangeBounds<u64>>(range: R) -> Self {
        Self {
            start: range.start_bound().cloned(),
            end: range.end_bound().cloned(),
        }
    }

    fn first(&self) -> Option<u64> {
        match self.start {
            Bound::Included(index) => Some(index),
            Bound::Excluded(index) => index.checked_add(1),
            Bound::Unbounded => Some(0),
        }
    }

    fn contains_end(&self, index: u64) -> bool {
        match self.end {
            Bound::Included(end) => index <= end,
            Bound::Excluded(end) => index < end,
            Bound::Unbounded => true,
        }
    }
}

pub(super) struct StoreState<W: CommittedStore> {
    writer: W,
    reader: W::Reader,
    profile: LogProfile,
    progress: LogProgress,
    poisoned: Cell<bool>,
}

impl<W: CommittedStore> StoreState<W> {
    pub(super) fn create(mut writer: W, profile: LogProfile) -> Result<Self, LogStateError> {
        profile
            .validate()
            .map_err(|_| LogStateError::InvalidProfile)?;
        if writer
            .is_initialized()
            .map_err(|_| LogStateError::Storage)?
        {
            return Err(LogStateError::InvalidProfile);
        }
        let reader = writer.reader();
        if !reader
            .scan_from(&[], &[], 1)
            .map_err(|_| LogStateError::Storage)?
            .is_empty()
        {
            return Err(LogStateError::Corrupt);
        }
        let progress = LogProgress::default();
        let batch = WriteBatch::default()
            .put(
                PROFILE_KEY,
                codec::encode_profile(&profile).map_err(|_| LogStateError::InvalidProfile)?,
            )
            .put(
                PROGRESS_KEY,
                codec::encode_progress(&progress).map_err(|_| LogStateError::Corrupt)?,
            );
        writer.commit(batch).map_err(|_| LogStateError::Storage)?;
        Ok(Self {
            writer,
            reader,
            profile,
            progress,
            poisoned: Cell::new(false),
        })
    }

    pub(super) fn open(writer: W, profile: LogProfile) -> Result<Self, LogStateError> {
        profile
            .validate()
            .map_err(|_| LogStateError::InvalidProfile)?;
        if !writer
            .is_initialized()
            .map_err(|_| LogStateError::Storage)?
        {
            return Err(LogStateError::InvalidProfile);
        }
        let reader = writer.reader();
        let entries = reader
            .scan_from(&[], &[], MAX_RETAINED_ENTRIES as usize + 3)
            .map_err(|_| LogStateError::Storage)?;
        let mut recorded_profile = None;
        let mut progress = None;
        let mut rows = Vec::new();
        let mut previous_key: Option<Vec<u8>> = None;
        for (key, value) in entries {
            if previous_key
                .as_ref()
                .is_some_and(|previous| previous >= &key)
            {
                return Err(LogStateError::Corrupt);
            }
            previous_key = Some(key.clone());
            if key == PROFILE_KEY {
                recorded_profile =
                    Some(codec::decode_profile(&value).map_err(|_| LogStateError::Corrupt)?);
            } else if key == PROGRESS_KEY {
                progress =
                    Some(codec::decode_progress(&value).map_err(|_| LogStateError::Corrupt)?);
            } else {
                rows.push(decode_row(key, value)?);
                if rows.len() > MAX_RETAINED_ENTRIES as usize {
                    return Err(LogStateError::Corrupt);
                }
            }
        }
        let recorded_profile = recorded_profile.ok_or(LogStateError::Corrupt)?;
        if recorded_profile != profile {
            return Err(LogStateError::InvalidProfile);
        }
        let progress = progress.ok_or(LogStateError::Corrupt)?;
        validate_rows(&progress, &rows)?;
        Ok(Self {
            writer,
            reader,
            profile: recorded_profile,
            progress,
            poisoned: Cell::new(false),
        })
    }

    pub(super) fn append(&mut self, append: &EncodedAppend) -> Result<(), LogStateError> {
        self.ensure_healthy()?;
        if append.entries().len() > MAX_APPEND_ENTRIES || append.encoded_bytes() > MAX_APPEND_BYTES
        {
            return Err(LogStateError::Capacity);
        }
        let mut previous_input = None;
        let mut new_rows = Vec::new();
        let mut progress = self.progress.clone();
        let mut encoded_bytes = 0usize;
        for row in append.entries() {
            let id = row.id();
            if previous_input.is_some_and(|previous| !successor(previous, id)) {
                return Err(LogStateError::InvalidAppend);
            }
            previous_input = Some(id);
            encoded_bytes = encoded_bytes
                .checked_add(row.encoded_len())
                .ok_or(LogStateError::Capacity)?;
            if self
                .progress
                .last_purged
                .is_some_and(|purged| id.index <= purged.index)
            {
                return Err(LogStateError::Conflict);
            }
            if self
                .progress
                .last_present
                .is_some_and(|last| id.index <= last.index)
            {
                let stored = self.require(self.io(self.reader.get(&entry_key(id.index)))?)?;
                let stored = self.stored_row(entry_key(id.index), stored)?;
                if stored.id() != id || stored.bytes() != row.bytes() {
                    return Err(LogStateError::Conflict);
                }
                continue;
            }
            let predecessor = progress.last_present.or(progress.last_purged);
            match predecessor {
                Some(previous) if previous.index == u64::MAX => {
                    return Err(LogStateError::IndexExhausted);
                }
                Some(previous) if !successor(previous, id) => {
                    return Err(LogStateError::InvalidAppend);
                }
                None if id.index != 0 => return Err(LogStateError::InvalidAppend),
                _ => {}
            }
            progress.retained_entries = progress
                .retained_entries
                .checked_add(1)
                .ok_or(LogStateError::Capacity)?;
            progress.retained_bytes = progress
                .retained_bytes
                .checked_add(row.encoded_len() as u64)
                .ok_or(LogStateError::Capacity)?;
            if progress.retained_entries > MAX_RETAINED_ENTRIES
                || progress.retained_bytes > MAX_RETAINED_BYTES
            {
                return Err(LogStateError::Capacity);
            }
            progress.last_present = Some(id);
            new_rows.push(row);
        }
        if encoded_bytes != append.encoded_bytes() {
            return Err(LogStateError::InvalidAppend);
        }
        if new_rows.is_empty() {
            return Ok(());
        }
        let mut batch = WriteBatch::default();
        for row in new_rows {
            batch.push_put(entry_key(row.id().index), row.bytes().to_vec());
        }
        self.commit(batch, progress)
    }

    pub(super) fn read_full(&self, range: OwnedLogRange) -> Result<Vec<LogEntry>, LogStateError> {
        self.ensure_healthy()?;
        let Some(start) = range.first() else {
            return Ok(Vec::new());
        };
        let Some(start) = self.read_start(start) else {
            return Ok(Vec::new());
        };
        if !range.contains_end(start) {
            return Ok(Vec::new());
        }
        let rows = self.io(self.reader.scan_from(
            &[ENTRY_PREFIX],
            &entry_key(start),
            MAX_RETAINED_ENTRIES as usize + 1,
        ))?;
        let mut result = Vec::new();
        let mut expected = start;
        let mut previous = self.predecessor_at(start)?;
        let tail = self.require(self.progress.last_present)?;
        let mut consumed_tail = false;
        let mut bytes = 0u64;
        for (key, value) in rows {
            let row = self.stored_row(key, value)?;
            if !range.contains_end(row.id().index) {
                break;
            }
            if row.id().index != expected || previous.is_some_and(|id| !successor(id, row.id())) {
                return self.corrupt();
            }
            if row.id().index > tail.index || (row.id().index == tail.index && row.id() != tail) {
                return self.corrupt();
            }
            bytes = self.require(bytes.checked_add(row.encoded_len() as u64))?;
            if bytes > MAX_RETAINED_BYTES {
                return self.corrupt();
            }
            consumed_tail = row.id() == tail;
            previous = Some(row.id());
            result.push(self.decode_stored(row.bytes())?);
            if result.len() > MAX_RETAINED_ENTRIES as usize {
                return self.corrupt();
            }
            match expected.checked_add(1) {
                Some(next) => expected = next,
                None => break,
            }
        }
        if !consumed_tail && range.contains_end(expected) && expected <= tail.index {
            return self.corrupt();
        }
        Ok(result)
    }

    pub(super) fn read_limited(
        &self,
        start: u64,
        end: u64,
    ) -> Result<Vec<LogEntry>, LogStateError> {
        self.ensure_healthy()?;
        if start >= end {
            return Ok(Vec::new());
        }
        let start = self.read_start(start).ok_or(LogStateError::InvalidRange)?;
        if start >= end {
            return Err(LogStateError::InvalidRange);
        }
        let mut result = Vec::new();
        let mut bytes = 0usize;
        let mut next = start;
        let mut previous = self.predecessor_at(start)?;
        let tail = self.require(self.progress.last_present)?;
        while next < end && next <= tail.index && result.len() < MAX_LIMITED_ENTRIES {
            let rows = self.io(self.reader.scan_from(&[ENTRY_PREFIX], &entry_key(next), 1))?;
            let Some((key, value)) = rows.into_iter().next() else {
                if self
                    .progress
                    .last_present
                    .is_some_and(|last| next <= last.index)
                {
                    return self.corrupt();
                }
                break;
            };
            let row = self.stored_row(key, value)?;
            if row.id().index != next || previous.is_some_and(|id| !successor(id, row.id())) {
                return self.corrupt();
            }
            if row.id().index == tail.index && row.id() != tail {
                return self.corrupt();
            }
            let cost = self.require(bytes.checked_add(row.encoded_len()))?;
            if cost > MAX_LIMITED_BYTES {
                if result.is_empty() {
                    return self.corrupt();
                }
                break;
            }
            bytes = cost;
            previous = Some(row.id());
            result.push(self.decode_stored(row.bytes())?);
            match next.checked_add(1) {
                Some(index) => next = index,
                None => break,
            }
        }
        if result.is_empty() {
            Err(LogStateError::InvalidRange)
        } else {
            Ok(result)
        }
    }

    pub(super) fn profile(&self) -> Result<LogProfile, LogStateError> {
        self.ensure_healthy()?;
        let bytes = self.require(self.io(self.reader.get(PROFILE_KEY))?)?;
        match codec::decode_profile(&bytes) {
            Ok(profile) if profile == self.profile => Ok(profile),
            _ => self.corrupt(),
        }
    }

    pub(super) fn retention(&self) -> Result<LogRetention, LogStateError> {
        self.ensure_healthy()?;
        let bytes = self.require(self.io(self.reader.get(PROGRESS_KEY))?)?;
        match codec::decode_progress(&bytes) {
            Ok(progress) if progress == self.progress => Ok(LogRetention {
                last_present: progress.last_present,
                last_purged: progress.last_purged,
                retained_entries: progress.retained_entries,
                retained_bytes: progress.retained_bytes,
            }),
            _ => self.corrupt(),
        }
    }

    pub(super) fn log_state(&self) -> Result<openraft::LogState<LogTypes>, LogStateError> {
        self.ensure_healthy()?;
        Ok(openraft::LogState {
            last_purged_log_id: self.progress.last_purged,
            last_log_id: self.progress.last_present.or(self.progress.last_purged),
        })
    }

    pub(super) fn read_vote(&self) -> Result<Option<LogVote>, LogStateError> {
        self.ensure_healthy()?;
        Ok(self.progress.vote)
    }

    pub(super) fn save_vote(&mut self, vote: LogVote) -> Result<(), LogStateError> {
        self.ensure_healthy()?;
        if let Some(previous) = self.progress.vote {
            match vote.partial_cmp(&previous) {
                Some(Ordering::Equal) => return Ok(()),
                Some(Ordering::Greater) => {}
                _ => return Err(LogStateError::VoteRegression),
            }
        }
        let mut progress = self.progress.clone();
        progress.vote = Some(vote);
        self.commit(WriteBatch::default(), progress)
    }

    pub(super) fn truncate(&mut self, since: LogId) -> Result<(), LogStateError> {
        self.ensure_healthy()?;
        if self
            .progress
            .last_purged
            .is_some_and(|purged| since.index <= purged.index)
        {
            return Err(LogStateError::InvalidTruncate);
        }
        if self
            .progress
            .last_present
            .is_none_or(|last| since.index > last.index)
        {
            return Ok(());
        }
        let rows = self.all_rows()?;
        let mut progress = self.progress.clone();
        let mut batch = WriteBatch::default();
        let mut last_present = None;
        for row in rows {
            if row.id().index >= since.index {
                batch.push_delete(entry_key(row.id().index));
                progress.retained_entries -= 1;
                progress.retained_bytes -= row.encoded_len() as u64;
            } else {
                last_present = Some(row.id());
            }
        }
        progress.last_present = last_present;
        self.commit(batch, progress)
    }

    pub(super) fn purge(&mut self, through: LogId) -> Result<(), LogStateError> {
        self.ensure_healthy()?;
        if let Some(previous) = self.progress.last_purged {
            if through.index < previous.index {
                return Ok(());
            }
            if through.index == previous.index {
                return if through == previous {
                    Ok(())
                } else {
                    Err(LogStateError::InvalidPurge)
                };
            }
            if through <= previous {
                return Err(LogStateError::InvalidPurge);
            }
        }
        let rows = self.all_rows()?;
        if let Some(row) = rows.iter().find(|row| row.id().index == through.index) {
            if row.id() != through {
                return Err(LogStateError::InvalidPurge);
            }
        } else if self
            .progress
            .last_present
            .is_some_and(|last| through <= last)
        {
            return Err(LogStateError::InvalidPurge);
        }
        let mut progress = self.progress.clone();
        let mut batch = WriteBatch::default();
        for row in &rows {
            if row.id().index <= through.index {
                batch.push_delete(entry_key(row.id().index));
                progress.retained_entries -= 1;
                progress.retained_bytes -= row.encoded_len() as u64;
            }
        }
        progress.last_purged = Some(through);
        if progress
            .last_present
            .is_some_and(|last| last.index <= through.index)
        {
            progress.last_present = None;
        }
        self.commit(batch, progress)
    }

    fn all_rows(&self) -> Result<Vec<EncodedEntry>, LogStateError> {
        let rows = self.io(self.reader.scan_from(
            &[ENTRY_PREFIX],
            &[ENTRY_PREFIX],
            MAX_RETAINED_ENTRIES as usize + 1,
        ))?;
        let rows = rows
            .into_iter()
            .map(|(key, value)| self.stored_row(key, value))
            .collect::<Result<Vec<_>, _>>()?;
        if validate_rows(&self.progress, &rows).is_err() {
            return self.corrupt();
        }
        Ok(rows)
    }

    fn read_start(&self, start: u64) -> Option<u64> {
        let first = match self.progress.last_purged {
            Some(purged) => purged.index.checked_add(1)?,
            None => 0,
        };
        let start = start.max(first);
        self.progress
            .last_present
            .filter(|last| start <= last.index)
            .map(|_| start)
    }

    fn predecessor_at(&self, start: u64) -> Result<Option<LogId>, LogStateError> {
        if start == 0 {
            return Ok(None);
        }
        let index = start - 1;
        if self
            .progress
            .last_purged
            .is_some_and(|purged| purged.index == index)
        {
            return Ok(self.progress.last_purged);
        }
        let value = self.require(self.io(self.reader.get(&entry_key(index)))?)?;
        Ok(Some(self.stored_row(entry_key(index), value)?.id()))
    }

    fn stored_row(&self, key: Vec<u8>, value: Vec<u8>) -> Result<EncodedEntry, LogStateError> {
        match decode_row(key, value) {
            Ok(row) => Ok(row),
            Err(_) => self.corrupt(),
        }
    }

    fn decode_stored(&self, bytes: &[u8]) -> Result<LogEntry, LogStateError> {
        match codec::decode_entry(bytes) {
            Ok(entry) => Ok(entry),
            Err(_) => self.corrupt(),
        }
    }

    fn require<T>(&self, value: Option<T>) -> Result<T, LogStateError> {
        match value {
            Some(value) => Ok(value),
            None => self.corrupt(),
        }
    }

    fn ensure_healthy(&self) -> Result<(), LogStateError> {
        if self.poisoned.get() {
            Err(LogStateError::Poisoned)
        } else {
            Ok(())
        }
    }

    fn io<T>(&self, result: Result<T, StorageError>) -> Result<T, LogStateError> {
        result.map_err(|_| {
            self.poisoned.set(true);
            LogStateError::Storage
        })
    }

    fn corrupt<T>(&self) -> Result<T, LogStateError> {
        self.poisoned.set(true);
        Err(LogStateError::Corrupt)
    }

    fn commit(
        &mut self,
        mut batch: WriteBatch,
        progress: LogProgress,
    ) -> Result<(), LogStateError> {
        let bytes = match codec::encode_progress(&progress) {
            Ok(bytes) => bytes,
            Err(_) => return self.corrupt(),
        };
        batch.push_put(PROGRESS_KEY, bytes);
        if self.writer.commit(batch).is_err() {
            self.poisoned.set(true);
            return Err(LogStateError::Storage);
        }
        self.progress = progress;
        Ok(())
    }
}

fn decode_row(key: Vec<u8>, value: Vec<u8>) -> Result<EncodedEntry, LogStateError> {
    let [ENTRY_PREFIX, index @ ..] = key.as_slice() else {
        return Err(LogStateError::Corrupt);
    };
    let index = <[u8; 8]>::try_from(index)
        .map(u64::from_be_bytes)
        .map_err(|_| LogStateError::Corrupt)?;
    let row = codec::validate_encoded_entry(value).map_err(|_| LogStateError::Corrupt)?;
    if row.id().index != index {
        return Err(LogStateError::Corrupt);
    }
    Ok(row)
}

fn successor(previous: LogId, next: LogId) -> bool {
    previous.index.checked_add(1) == Some(next.index) && next > previous
}

fn validate_rows(progress: &LogProgress, rows: &[EncodedEntry]) -> Result<(), LogStateError> {
    codec::encode_progress(progress).map_err(|_| LogStateError::Corrupt)?;
    if rows.len() as u64 != progress.retained_entries || rows.len() as u64 > MAX_RETAINED_ENTRIES {
        return Err(LogStateError::Corrupt);
    }
    let mut previous = progress.last_purged;
    let mut bytes = 0u64;
    for row in rows {
        match previous {
            Some(id) if !successor(id, row.id()) => return Err(LogStateError::Corrupt),
            None if row.id().index != 0 => return Err(LogStateError::Corrupt),
            _ => {}
        }
        previous = Some(row.id());
        bytes = bytes
            .checked_add(row.encoded_len() as u64)
            .ok_or(LogStateError::Corrupt)?;
        if bytes > MAX_RETAINED_BYTES {
            return Err(LogStateError::Corrupt);
        }
    }
    if bytes != progress.retained_bytes
        || rows.last().map(EncodedEntry::id) != progress.last_present
    {
        return Err(LogStateError::Corrupt);
    }
    Ok(())
}

#[cfg(test)]
mod retention_tests;
#[cfg(test)]
mod tests;

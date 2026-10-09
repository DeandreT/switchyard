//! A single-writer, indexed journal over the existing database owner.
//!
//! This module reserves `0xF0 || "switchyard/journal" || 0x00` in the record
//! keyspace, outside the domain's current tags `0x00..=0x11`. Callers must
//! exclusively own writes to this prefix, including raw [`StateStore`] writes.
//! Separate journal handles are not serialized: atomic batches are not CAS.
//! Broker writes to disjoint keys may still share the same database owner.
//!
//! Payloads are opaque. A durable committed frontier does not mean effects
//! were applied, a quorum agreed, or a client was acknowledged. This journal
//! does not activate replay, migrate the store format, or open another database.

use sha2::{Digest, Sha256};
use storage::{StateStore, StorageError, WriteBatch};
use thiserror::Error;

pub const JOURNAL_FORMAT_VERSION: u32 = 1;
pub const MAX_JOURNAL_PAYLOAD_BYTES: usize = 1024 * 1024;
pub const MAX_JOURNAL_READ_ENTRIES: usize = 64;
pub const JOURNAL_VALIDATION_PAGE_ENTRIES: usize = 32;

const PREFIX: &[u8] = b"\xF0switchyard/journal\0";
const ROOT_TAG: u8 = 0;
const FRONTIER_TAG: u8 = 1;
const ENTRY_TAG: u8 = 2;
const ENTRY_HEADER_BYTES: usize = 48;
const FRONTIER_BYTES: usize = 44;
const ENTRY_HASH_SCOPE: &[u8] = b"switchyard journal entry v1\0";
const FRONTIER_HASH_SCOPE: &[u8] = b"switchyard journal frontier v1\0";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalEntry {
    index: u64,
    payload: Vec<u8>,
}

impl JournalEntry {
    pub fn index(&self) -> u64 {
        self.index
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum JournalError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("the journal must be reopened after an ambiguous failed write")]
    Unusable,
    #[error("journal format {found} is unsupported; expected {expected}")]
    UnsupportedVersion { found: u32, expected: u32 },
    #[error("journal is corrupt: {detail}")]
    Corrupt { detail: &'static str },
    #[error("journal append index {found} is not the next index {expected}")]
    InvalidAppendIndex { found: u64, expected: u64 },
    #[error("journal index space is exhausted")]
    IndexExhausted,
    #[error("journal payload has {bytes} bytes; maximum is {maximum}")]
    PayloadTooLarge { bytes: usize, maximum: usize },
    #[error("commit index {requested} is outside {committed}..={last_appended}")]
    InvalidCommit {
        requested: u64,
        committed: u64,
        last_appended: u64,
    },
    #[error("journal read must start at a nonzero index")]
    InvalidReadStart,
    #[error("journal read limit {requested} is outside 1..={maximum}")]
    InvalidReadLimit { requested: usize, maximum: usize },
}

/// One externally exclusive writer for the reserved journal prefix.
///
/// This type is deliberately not `Clone`, but that does not prevent another
/// caller constructing a journal over a cloned store. Such concurrent writers,
/// or raw mutations of this prefix, violate the ownership precondition. No
/// database-wide registry or compare-and-swap is implied by `&mut self`.
/// An attempted storage apply retires this owner before invocation; only success
/// and cache updates restore usability. A returned error or explicitly caught
/// unwind requires reopen. No panic is caught here.
#[derive(Debug)]
pub struct Journal<S> {
    store: S,
    initialized: bool,
    committed: u64,
    last_appended: u64,
    usable: bool,
}

impl<S: StateStore> Journal<S> {
    /// Validates the entire reserved prefix without writing any keys.
    ///
    /// Validation reads fixed-size pages, but total work is linear in the
    /// journal length. Normal entries have a bounded encoded size. StateStore
    /// materializes values before validation, so this is not a hard allocator
    /// bound against arbitrarily oversized corrupt raw values.
    ///
    /// A contiguous valid uncommitted tail is retained, never promoted or
    /// deleted. A malformed tail refuses open, just like a corrupt committed
    /// prefix. Missing metadata with existing rows is never adopted.
    pub fn open(store: S) -> Result<Self, JournalError> {
        let mut cursor = PREFIX.to_vec();
        let mut previous: Option<Vec<u8>> = None;
        let mut initialized = false;
        let mut committed = None;
        let mut last_appended = 0;
        loop {
            let page = store.scan_from(PREFIX, &cursor, JOURNAL_VALIDATION_PAGE_ENTRIES)?;
            if page.len() > JOURNAL_VALIDATION_PAGE_ENTRIES {
                return Err(corrupt("a validation page exceeds its requested bound"));
            }
            if page.is_empty() {
                break;
            }
            for (key, value) in page {
                if !key.starts_with(PREFIX)
                    || key < cursor
                    || previous.as_ref().is_some_and(|previous| key <= *previous)
                {
                    return Err(corrupt(
                        "journal scan did not advance in canonical key order",
                    ));
                }
                let suffix = &key[PREFIX.len()..];
                match suffix {
                    [ROOT_TAG] => {
                        require_version(&value)?;
                        initialized = true;
                    }
                    [FRONTIER_TAG] => {
                        if !initialized {
                            return Err(corrupt("frontier has no journal root"));
                        }
                        committed = Some(decode_frontier(&value)?);
                    }
                    [ENTRY_TAG, ..] => {
                        if !initialized || committed.is_none() {
                            return Err(corrupt("entries have incomplete journal metadata"));
                        }
                        let entry = decode_entry(&key, &value)?;
                        if entry.index != next_index(last_appended)? {
                            return Err(corrupt("journal entries contain a gap or regression"));
                        }
                        last_appended = entry.index;
                    }
                    _ => return Err(corrupt("journal key is not canonical")),
                }
                previous = Some(key);
            }
            cursor = previous
                .as_ref()
                .expect("a nonempty page has a last key")
                .clone();
            cursor.push(0);
        }
        let committed = match (initialized, committed, previous.is_some()) {
            (false, None, false) => 0,
            (true, Some(committed), _) => committed,
            _ => return Err(corrupt("journal metadata is incomplete")),
        };
        if committed > last_appended {
            return Err(corrupt("frontier exceeds the appended prefix"));
        }
        Ok(Self {
            store,
            initialized,
            committed,
            last_appended,
            usable: true,
        })
    }

    pub fn committed_index(&self) -> Result<u64, JournalError> {
        self.require_usable()?;
        Ok(self.committed)
    }

    pub fn last_appended_index(&self) -> Result<u64, JournalError> {
        self.require_usable()?;
        Ok(self.last_appended)
    }

    /// Durably appends exactly the next index, without advancing the frontier.
    ///
    /// The first append atomically writes root, frontier zero, and entry one.
    /// A duplicate is refused even if its opaque payload matches. A returned
    /// storage-apply error or explicitly caught unwind requires reopen: neither
    /// proves that the original batch was absent from the durable store.
    pub fn append(&mut self, index: u64, payload: &[u8]) -> Result<(), JournalError> {
        self.require_usable()?;
        let expected = next_index(self.last_appended)?;
        if index != expected {
            return Err(JournalError::InvalidAppendIndex {
                found: index,
                expected,
            });
        }
        if payload.len() > MAX_JOURNAL_PAYLOAD_BYTES {
            return Err(JournalError::PayloadTooLarge {
                bytes: payload.len(),
                maximum: MAX_JOURNAL_PAYLOAD_BYTES,
            });
        }
        let mut batch = WriteBatch::default();
        if !self.initialized {
            batch.push_put(
                metadata_key(ROOT_TAG),
                JOURNAL_FORMAT_VERSION.to_be_bytes().to_vec(),
            );
            batch.push_put(metadata_key(FRONTIER_TAG), encode_frontier(0));
        }
        batch.push_put(entry_key(index), encode_entry(index, payload));
        self.persist(batch)?;
        self.initialized = true;
        self.last_appended = index;
        self.usable = true;
        Ok(())
    }

    /// Durably advances only over already appended contiguous entries.
    ///
    /// Restating the current frontier is a no-op, including an empty journal's
    /// frontier zero. No replay, applied checkpoint, or client acknowledgement
    /// is performed here.
    pub fn commit(&mut self, index: u64) -> Result<(), JournalError> {
        self.require_usable()?;
        if index < self.committed || index > self.last_appended {
            return Err(JournalError::InvalidCommit {
                requested: index,
                committed: self.committed,
                last_appended: self.last_appended,
            });
        }
        if index == self.committed {
            return Ok(());
        }
        self.persist(
            WriteBatch::default().put(metadata_key(FRONTIER_TAG), encode_frontier(index)),
        )?;
        self.committed = index;
        self.usable = true;
        Ok(())
    }

    /// Reads at most `limit` committed entries, starting at nonzero `from`.
    ///
    /// A limit must be 1..=MAX_JOURNAL_READ_ENTRIES. Tail entries are never
    /// returned. This validates the selected page, not the whole journal anew;
    /// external raw prefix mutations are forbidden by the ownership contract.
    pub fn read_committed(
        &self,
        from: u64,
        limit: usize,
    ) -> Result<Vec<JournalEntry>, JournalError> {
        self.require_usable()?;
        if from == 0 {
            return Err(JournalError::InvalidReadStart);
        }
        if limit == 0 || limit > MAX_JOURNAL_READ_ENTRIES {
            return Err(JournalError::InvalidReadLimit {
                requested: limit,
                maximum: MAX_JOURNAL_READ_ENTRIES,
            });
        }
        if from > self.committed {
            return Ok(Vec::new());
        }
        let count = (self.committed - from + 1).min(limit as u64) as usize;
        let prefix = metadata_key(ENTRY_TAG);
        let rows = self.store.scan_from(&prefix, &entry_key(from), count)?;
        if rows.len() != count {
            return Err(corrupt("committed read has a missing entry"));
        }
        let mut entries = Vec::with_capacity(count);
        for (offset, (key, value)) in rows.into_iter().enumerate() {
            let entry = decode_entry(&key, &value)?;
            let expected = from
                .checked_add(offset as u64)
                .ok_or(JournalError::IndexExhausted)?;
            if entry.index != expected {
                return Err(corrupt("committed read is not contiguous"));
            }
            entries.push(entry);
        }
        Ok(entries)
    }

    fn require_usable(&self) -> Result<(), JournalError> {
        if self.usable {
            Ok(())
        } else {
            Err(JournalError::Unusable)
        }
    }

    fn persist(&mut self, batch: WriteBatch) -> Result<(), JournalError> {
        self.usable = false;
        if let Err(error) = self.store.apply(batch) {
            return Err(JournalError::Storage(error));
        }
        Ok(())
    }
}

fn corrupt(detail: &'static str) -> JournalError {
    JournalError::Corrupt { detail }
}

fn next_index(last: u64) -> Result<u64, JournalError> {
    last.checked_add(1).ok_or(JournalError::IndexExhausted)
}

fn metadata_key(tag: u8) -> Vec<u8> {
    let mut key = PREFIX.to_vec();
    key.push(tag);
    key
}

fn entry_key(index: u64) -> Vec<u8> {
    let mut key = metadata_key(ENTRY_TAG);
    key.extend_from_slice(&index.to_be_bytes());
    key
}

fn require_version(bytes: &[u8]) -> Result<(), JournalError> {
    let bytes: [u8; 4] = bytes
        .try_into()
        .map_err(|_| corrupt("version has the wrong length"))?;
    let found = u32::from_be_bytes(bytes);
    if found != JOURNAL_FORMAT_VERSION {
        return Err(JournalError::UnsupportedVersion {
            found,
            expected: JOURNAL_FORMAT_VERSION,
        });
    }
    Ok(())
}

fn hash(scope: &[u8], header: &[u8], payload: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(scope);
    hash.update(header);
    hash.update(payload);
    hash.finalize().into()
}

fn encode_entry(index: u64, payload: &[u8]) -> Vec<u8> {
    let mut value = Vec::with_capacity(ENTRY_HEADER_BYTES + payload.len());
    value.extend_from_slice(&JOURNAL_FORMAT_VERSION.to_be_bytes());
    value.extend_from_slice(&index.to_be_bytes());
    value.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    let checksum = hash(ENTRY_HASH_SCOPE, &value, payload);
    value.extend_from_slice(&checksum);
    value.extend_from_slice(payload);
    value
}

fn decode_entry(key: &[u8], value: &[u8]) -> Result<JournalEntry, JournalError> {
    let prefix = metadata_key(ENTRY_TAG);
    let suffix = key
        .strip_prefix(prefix.as_slice())
        .ok_or_else(|| corrupt("entry has a foreign key"))?;
    let suffix: [u8; 8] = suffix
        .try_into()
        .map_err(|_| corrupt("entry index key has the wrong length"))?;
    let index = u64::from_be_bytes(suffix);
    if index == 0 {
        return Err(corrupt("entry index zero is reserved"));
    }
    if !(ENTRY_HEADER_BYTES..=ENTRY_HEADER_BYTES + MAX_JOURNAL_PAYLOAD_BYTES).contains(&value.len())
    {
        return Err(corrupt("entry encoded size is outside its bound"));
    }
    require_version(&value[..4])?;
    let encoded_index = u64::from_be_bytes(value[4..12].try_into().expect("fixed header index"));
    if encoded_index != index {
        return Err(corrupt("entry key and envelope indices differ"));
    }
    let length =
        u32::from_be_bytes(value[12..16].try_into().expect("fixed header length")) as usize;
    if length > MAX_JOURNAL_PAYLOAD_BYTES
        || ENTRY_HEADER_BYTES.checked_add(length) != Some(value.len())
    {
        return Err(corrupt("entry payload length is not canonical"));
    }
    let payload = &value[ENTRY_HEADER_BYTES..];
    if value[16..ENTRY_HEADER_BYTES] != hash(ENTRY_HASH_SCOPE, &value[..16], payload) {
        return Err(corrupt("entry checksum differs"));
    }
    Ok(JournalEntry {
        index,
        payload: payload.to_vec(),
    })
}

fn encode_frontier(index: u64) -> Vec<u8> {
    let mut value = Vec::with_capacity(FRONTIER_BYTES);
    value.extend_from_slice(&JOURNAL_FORMAT_VERSION.to_be_bytes());
    value.extend_from_slice(&index.to_be_bytes());
    let checksum = hash(FRONTIER_HASH_SCOPE, &value, &[]);
    value.extend_from_slice(&checksum);
    value
}

fn decode_frontier(value: &[u8]) -> Result<u64, JournalError> {
    if value.len() != FRONTIER_BYTES {
        return Err(corrupt("frontier has the wrong length"));
    }
    require_version(&value[..4])?;
    if value[12..] != hash(FRONTIER_HASH_SCOPE, &value[..12], &[]) {
        return Err(corrupt("frontier checksum differs"));
    }
    Ok(u64::from_be_bytes(
        value[4..12].try_into().expect("fixed frontier index"),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn journal_index_arithmetic_refuses_wraparound() {
        assert_eq!(next_index(0), Ok(1));
        assert_eq!(next_index(u64::MAX - 1), Ok(u64::MAX));
        assert_eq!(next_index(u64::MAX), Err(JournalError::IndexExhausted));
        let payload = b"opaque maximum-index envelope";
        let decoded = decode_entry(&entry_key(u64::MAX), &encode_entry(u64::MAX, payload)).unwrap();
        assert_eq!(decoded.index(), u64::MAX);
        assert_eq!(decoded.payload(), payload);
    }
}

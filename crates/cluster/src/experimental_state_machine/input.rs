use crate::{LogEntry, experimental_log::validated_entry_len};

use super::{MAX_APPLY_BYTES, MAX_APPLY_ENTRIES, StateMachineError};

pub(super) struct PreparedApply {
    entries: Vec<LogEntry>,
    encoded_bytes: usize,
}

#[cfg(test)]
mod tests;

impl PreparedApply {
    pub(super) fn from_entries<I>(entries: I) -> Result<Self, StateMachineError>
    where
        I: IntoIterator<Item = LogEntry>,
    {
        let mut prepared = Self {
            entries: Vec::new(),
            encoded_bytes: 0,
        };
        let mut previous: Option<crate::LogId> = None;
        for entry in entries {
            if prepared.entries.len() == MAX_APPLY_ENTRIES {
                return Err(StateMachineError::Capacity);
            }
            if previous.is_some_and(|id| {
                id.index.checked_add(1) != Some(entry.log_id.index) || entry.log_id <= id
            }) {
                return Err(StateMachineError::InvalidApply);
            }
            let bytes = validated_entry_len(&entry).map_err(|_| StateMachineError::Codec)?;
            prepared.encoded_bytes = prepared
                .encoded_bytes
                .checked_add(bytes)
                .filter(|bytes| *bytes <= MAX_APPLY_BYTES)
                .ok_or(StateMachineError::Capacity)?;
            previous = Some(entry.log_id);
            prepared.entries.push(entry);
        }
        Ok(prepared)
    }

    pub(super) fn encoded_bytes(&self) -> usize {
        self.encoded_bytes
    }

    pub(super) fn into_entries(self) -> Vec<LogEntry> {
        self.entries
    }
}

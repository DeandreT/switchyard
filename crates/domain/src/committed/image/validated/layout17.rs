//! Borrowed consistency proof for the closed layout17 NonFinite queue profile.

use std::{collections::BTreeMap, fmt};

use crate::{
    CommittedCheckpoint, CommittedStreamId,
    queue_capacity::{QueueCapacityError, QueueCapacityMode},
};

use super::{
    CommittedImageRole, CommittedImageRows, CommittedImageValidationError as Error,
    DecodedCommittedImage, Result, Rows,
    keys::{self, Key, Scope},
    relations,
};

#[cfg(test)]
mod tests;

// This role freezes layout17's exact mode family, not an arbitrary current tag.
const MODE_TAG: u8 = 0x16;

/// Immutable checked view of the declared CreateSendLayout17V1 business profile.
///
/// The old closed Create/Send record and relation rules remain in force, plus
/// exactly one canonical NonFinite mode for each generation-one primary queue.
/// Finite owners and usage/charge rows are outside this profile. Bodies and
/// names stay borrowed; bounded temporary collections and small checkpoint or
/// mode codecs are not an RSS guarantee. This is not source authenticity,
/// committed history, current-store admission, or install/export authority.
///
/// ```compile_fail
/// fn duplicate(image: domain::ValidatedCreateSendLayout17Image<'_>) {
///     let _ = image.clone();
/// }
/// ```
///
/// ```compile_fail
/// fn writer(image: domain::ValidatedCreateSendLayout17Image<'_>) -> storage::WriteBatch {
///     image.into()
/// }
/// ```
///
/// ```compile_fail
/// fn construct<'a>(image: domain::DecodedCommittedImage<'a>) -> domain::ValidatedCreateSendLayout17Image<'a> {
///     domain::ValidatedCreateSendLayout17Image { image, primary_queues: 0, messages: 0 }
/// }
/// ```
pub struct ValidatedCreateSendLayout17Image<'a> {
    image: DecodedCommittedImage<'a>,
    primary_queues: usize,
    messages: usize,
}

impl<'a> ValidatedCreateSendLayout17Image<'a> {
    pub fn validate(image: DecodedCommittedImage<'a>) -> Result<Self> {
        if image.role() != CommittedImageRole::CreateSendLayout17V1 {
            return Err(Error::UnsupportedProfile);
        }
        let mut rows = Rows::default();
        let mut modes = BTreeMap::new();
        for row in image.rows() {
            let (&tag, rest) = row.key().split_first().ok_or(Error::InvalidKey)?;
            if tag == MODE_TAG {
                let scope = mode_scope(rest)?;
                if modes.insert(scope, row.value()).is_some() {
                    return Err(Error::InconsistentMetadata);
                }
            } else {
                rows.insert(Key::parse(row.key())?, row.value())?;
            }
        }
        let primary_queues = relations::validate(&rows, image.checkpoint())?;
        if modes.len() != primary_queues {
            return Err(Error::InconsistentMetadata);
        }
        for (&scope, &value) in &modes {
            if !rows.configs.contains_key(&scope) {
                return Err(Error::InconsistentMetadata);
            }
            mode(value)?;
        }
        for &scope in rows.configs.keys() {
            if scope.is_primary() && !modes.contains_key(&scope) {
                return Err(Error::InconsistentMetadata);
            }
        }
        Ok(Self {
            image,
            primary_queues,
            messages: rows.messages.len(),
        })
    }

    pub fn role(&self) -> CommittedImageRole {
        self.image.role()
    }
    pub fn checkpoint(&self) -> &CommittedCheckpoint {
        self.image.checkpoint()
    }
    pub fn stream(&self) -> CommittedStreamId {
        self.image.stream()
    }
    pub fn row_count(&self) -> usize {
        self.image.row_count()
    }
    pub fn queue_count(&self) -> usize {
        self.primary_queues
    }
    pub fn message_count(&self) -> usize {
        self.messages
    }
    pub fn rows(&self) -> CommittedImageRows<'a> {
        self.image.rows()
    }
}

impl fmt::Debug for ValidatedCreateSendLayout17Image<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ValidatedCreateSendLayout17Image")
            .field("rows", &self.row_count())
            .field("primary_queues", &self.primary_queues)
            .field("messages", &self.messages)
            .finish_non_exhaustive()
    }
}

fn mode_scope(rest: &[u8]) -> Result<Scope<'_>> {
    let (scope, tail) = keys::scope(rest)?;
    if !tail.is_empty() {
        return Err(Error::InvalidKey);
    }
    if !scope.is_primary() {
        return Err(Error::InconsistentMetadata);
    }
    Ok(scope)
}

fn mode(value: &[u8]) -> Result<()> {
    let mode = QueueCapacityMode::decode(value, 1).map_err(|error| match error {
        QueueCapacityError::InvalidSchema
        | QueueCapacityError::UnsupportedEnvelope
        | QueueCapacityError::Codec(crate::CodecError::UnsupportedVersion { .. }) => {
            Error::UnsupportedProfile
        }
        QueueCapacityError::GenerationMismatch => Error::InconsistentMetadata,
        _ => Error::InvalidRecord,
    })?;
    if mode.limit_bytes().is_some() {
        return Err(Error::UnsupportedProfile);
    }
    Ok(())
}

//! Logical queue reservations, separate from physical storage or wire sizes.

use std::num::NonZeroU64;

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use thiserror::Error;

use crate::{
    CodecError, MAX_SESSION_ID_BYTES, MessageEnvelope, MessageRecord, QueueConfig, SequenceNumber,
    SessionId, codec,
};

pub(crate) const QUEUE_CAPACITY_SCHEMA: u8 = 1;
pub(crate) const QUEUE_CAPACITY_MODEL: u8 = 1;
pub(crate) const MAX_CAPACITY_RECORD_BYTES: usize = 64;
const VARIABLE_BYTES: u64 = 5;
const BASE_RESERVE_BYTES: u64 = 256;
const DEAD_LETTER_RESERVE_BYTES: u64 = 256;
const DEAD_LETTER_PROJECTION_BYTES: u64 = 81;
const MIN_CHARGED_BYTES: u64 = VARIABLE_BYTES + BASE_RESERVE_BYTES + DEAD_LETTER_RESERVE_BYTES;
const MAX_ORIGINAL_SESSION_BYTES: u64 = VARIABLE_BYTES + MAX_SESSION_ID_BYTES as u64;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
enum CapacityMode {
    NonFinite,
    FiniteV1 { limit: u64 },
}

/// Mandatory metadata for one live primary queue owner. Its existence and
/// ownership are checked by the state machine, not by this value type.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct QueueCapacityMode {
    schema: u8,
    generation: u64,
    mode: CapacityMode,
}

impl QueueCapacityMode {
    pub(crate) fn non_finite(generation: u64) -> Result<Self, QueueCapacityError> {
        let value = Self {
            schema: QUEUE_CAPACITY_SCHEMA,
            generation,
            mode: CapacityMode::NonFinite,
        };
        value.validate_generation(generation)?;
        Ok(value)
    }

    pub(crate) fn finite_v1(
        generation: u64,
        limit: NonZeroU64,
    ) -> Result<Self, QueueCapacityError> {
        let value = Self {
            schema: QUEUE_CAPACITY_SCHEMA,
            generation,
            mode: CapacityMode::FiniteV1 { limit: limit.get() },
        };
        value.validate_generation(generation)?;
        Ok(value)
    }

    #[cfg(test)]
    pub(crate) const fn generation(self) -> u64 {
        self.generation
    }

    pub(crate) fn limit_bytes(self) -> Option<NonZeroU64> {
        match self.mode {
            CapacityMode::NonFinite => None,
            CapacityMode::FiniteV1 { limit } => NonZeroU64::new(limit),
        }
    }

    pub(crate) fn validate_generation(self, expected: u64) -> Result<(), QueueCapacityError> {
        validate_identity(self.schema, self.generation, expected)?;
        if matches!(self.mode, CapacityMode::FiniteV1 { limit: 0 }) {
            return Err(QueueCapacityError::InvalidLimit);
        }
        Ok(())
    }

    /// Existing queue validation and creation-only flag checks stay at their
    /// ordinary call sites; this checks only the supported finite profile.
    pub(crate) fn validate_config(self, config: &QueueConfig) -> Result<(), QueueCapacityError> {
        self.validate_generation(self.generation)?;
        if self.limit_bytes().is_some()
            && (config.requires_session || config.requires_duplicate_detection)
        {
            return Err(QueueCapacityError::UnsupportedFiniteQueue);
        }
        Ok(())
    }

    pub(crate) fn encode(self) -> Result<Vec<u8>, QueueCapacityError> {
        self.validate_generation(self.generation)?;
        encode_record(&self)
    }

    pub(crate) fn decode(envelope: &[u8], expected: u64) -> Result<Self, QueueCapacityError> {
        let value: Self = decode_record(envelope)?;
        value.validate_generation(expected)?;
        Ok(value)
    }
}

/// Aggregate reservations across a finite primary queue and its DLQ shadow.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct QueueCapacityUsage {
    schema: u8,
    generation: u64,
    model: u8,
    reserved_bytes: u64,
    message_count: u64,
}

impl QueueCapacityUsage {
    pub(crate) fn new(
        generation: u64,
        reserved_bytes: u64,
        message_count: u64,
    ) -> Result<Self, QueueCapacityError> {
        let value = Self {
            schema: QUEUE_CAPACITY_SCHEMA,
            generation,
            model: QUEUE_CAPACITY_MODEL,
            reserved_bytes,
            message_count,
        };
        value.validate_generation(generation)?;
        Ok(value)
    }

    pub(crate) const fn generation(self) -> u64 {
        self.generation
    }
    pub(crate) const fn reserved_bytes(self) -> u64 {
        self.reserved_bytes
    }
    pub(crate) const fn message_count(self) -> u64 {
        self.message_count
    }

    pub(crate) fn validate_generation(self, expected: u64) -> Result<(), QueueCapacityError> {
        validate_identity(self.schema, self.generation, expected)?;
        validate_model(self.model)?;
        if (self.message_count == 0) != (self.reserved_bytes == 0)
            || self.message_count > self.reserved_bytes / MIN_CHARGED_BYTES
        {
            return Err(QueueCapacityError::InvalidUsage);
        }
        Ok(())
    }

    pub(crate) fn validate_mode(self, mode: QueueCapacityMode) -> Result<(), QueueCapacityError> {
        mode.validate_generation(self.generation)?;
        self.validate_generation(mode.generation)?;
        let limit = mode
            .limit_bytes()
            .ok_or(QueueCapacityError::UnexpectedUsage)?;
        if self.reserved_bytes > limit.get() {
            return Err(QueueCapacityError::InvalidUsage);
        }
        Ok(())
    }

    pub(crate) fn reserve(
        self,
        mode: QueueCapacityMode,
        charge: MessageCharge,
    ) -> Result<Self, QueueCapacityError> {
        self.validate_mode(mode)?;
        charge.validate_generation(self.generation)?;
        let bytes = self
            .reserved_bytes
            .checked_add(charge.charged_bytes)
            .ok_or(QueueCapacityError::ArithmeticOverflow)?;
        let count = self
            .message_count
            .checked_add(1)
            .ok_or(QueueCapacityError::ArithmeticOverflow)?;
        let value = Self::new(self.generation, bytes, count)?;
        if bytes
            > mode
                .limit_bytes()
                .ok_or(QueueCapacityError::UnexpectedUsage)?
                .get()
        {
            return Err(QueueCapacityError::LimitExceeded);
        }
        Ok(value)
    }

    pub(crate) fn refund(
        self,
        mode: QueueCapacityMode,
        original: MessageCharge,
    ) -> Result<Self, QueueCapacityError> {
        self.validate_mode(mode)?;
        original.validate_generation(self.generation)?;
        let bytes = self
            .reserved_bytes
            .checked_sub(original.charged_bytes)
            .ok_or(QueueCapacityError::ArithmeticUnderflow)?;
        let count = self
            .message_count
            .checked_sub(1)
            .ok_or(QueueCapacityError::ArithmeticUnderflow)?;
        let value = Self::new(self.generation, bytes, count)?;
        value.validate_mode(mode)?;
        Ok(value)
    }

    /// A retained-message change replaces its original credit, never its count.
    pub(crate) fn replace(
        self,
        mode: QueueCapacityMode,
        original: MessageCharge,
        proposed: MessageCharge,
    ) -> Result<Self, QueueCapacityError> {
        self.validate_mode(mode)?;
        original.validate_generation(self.generation)?;
        proposed.validate_generation(self.generation)?;
        let bytes = self
            .reserved_bytes
            .checked_sub(original.charged_bytes)
            .ok_or(QueueCapacityError::ArithmeticUnderflow)?
            .checked_add(proposed.charged_bytes)
            .ok_or(QueueCapacityError::ArithmeticOverflow)?;
        let value = Self::new(self.generation, bytes, self.message_count)?;
        if bytes
            > mode
                .limit_bytes()
                .ok_or(QueueCapacityError::UnexpectedUsage)?
                .get()
        {
            return Err(QueueCapacityError::LimitExceeded);
        }
        Ok(value)
    }

    pub(crate) fn encode(self) -> Result<Vec<u8>, QueueCapacityError> {
        self.validate_generation(self.generation)?;
        encode_record(&self)
    }

    pub(crate) fn decode(envelope: &[u8], expected: u64) -> Result<Self, QueueCapacityError> {
        let value: Self = decode_record(envelope)?;
        value.validate_generation(expected)?;
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ChargeComponents {
    producer_bytes: u64,
    original_session_bytes: u64,
    dead_letter_projection_bytes: u64,
    charged_bytes: u64,
}

impl ChargeComponents {
    fn new(
        producer_bytes: u64,
        original_session_bytes: u64,
        dead_letter_projection_bytes: u64,
    ) -> Result<Self, QueueCapacityError> {
        if producer_bytes < VARIABLE_BYTES
            || (original_session_bytes != 0
                && !(VARIABLE_BYTES + 1..=MAX_ORIGINAL_SESSION_BYTES)
                    .contains(&original_session_bytes))
            || (dead_letter_projection_bytes != 0
                && dead_letter_projection_bytes < DEAD_LETTER_PROJECTION_BYTES)
        {
            return Err(QueueCapacityError::InvalidCharge);
        }
        let charged_bytes = producer_bytes
            .checked_add(original_session_bytes)
            .and_then(|value| value.checked_add(BASE_RESERVE_BYTES))
            .and_then(|value| {
                value.checked_add(DEAD_LETTER_RESERVE_BYTES.max(dead_letter_projection_bytes))
            })
            .ok_or(QueueCapacityError::ArithmeticOverflow)?;
        Ok(Self {
            producer_bytes,
            original_session_bytes,
            dead_letter_projection_bytes,
            charged_bytes,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ObservedSession {
    Live { bytes: u64 },
    DeadLetterWithoutOuterSession,
}

/// A numeric capture of original content, without cloning its body or envelope.
/// A shadow observation never substitutes zero for its ledger-owned original S.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecordChargeObservation {
    sequence: SequenceNumber,
    producer_bytes: u64,
    dead_letter_projection_bytes: u64,
    session: ObservedSession,
}

impl RecordChargeObservation {
    pub(crate) const fn sequence(self) -> SequenceNumber {
        self.sequence
    }

    /// This describes retained metadata, not physical ownership of a shadow.
    pub(crate) fn is_dead_letter(self) -> bool {
        matches!(self.session, ObservedSession::DeadLetterWithoutOuterSession)
    }

    fn components(
        self,
        original_session_bytes: u64,
    ) -> Result<ChargeComponents, QueueCapacityError> {
        if matches!(self.session, ObservedSession::Live { bytes } if bytes != original_session_bytes)
        {
            return Err(QueueCapacityError::InvalidSessionMetadata);
        }
        ChargeComponents::new(
            self.producer_bytes,
            original_session_bytes,
            self.dead_letter_projection_bytes,
        )
    }
}

/// Capture after ordinary held/session/config checks and before property edits.
/// Retain this Result until substantive validation completes; do not propagate
/// its error early or read the ledger merely to build the observation.
pub(crate) fn observe_record(
    record: &MessageRecord,
) -> Result<RecordChargeObservation, QueueCapacityError> {
    let session = if record.dead_letter.is_some() {
        if record.session_id.is_some() {
            return Err(QueueCapacityError::InvalidSessionMetadata);
        }
        ObservedSession::DeadLetterWithoutOuterSession
    } else {
        ObservedSession::Live {
            bytes: session_bytes(record.session_id.as_ref())?,
        }
    };
    let producer_bytes =
        producer_bytes(&record.message_id, &record.body, record.envelope.as_deref())?;
    let dead_letter_projection_bytes = record.dead_letter.as_ref().map_or(Ok(0), |info| {
        dead_letter_bytes(info.reason.as_str().len(), info.description.len())
    })?;
    Ok(RecordChargeObservation {
        sequence: record.sequence,
        producer_bytes,
        dead_letter_projection_bytes,
        session,
    })
}

/// One physical message's reservation. D stores the full canonical DLQ
/// projection, not just the excess above the existing 256-byte reserve.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct MessageCharge {
    schema: u8,
    generation: u64,
    model: u8,
    producer_bytes: u64,
    original_session_bytes: u64,
    dead_letter_projection_bytes: u64,
    charged_bytes: u64,
}

impl MessageCharge {
    fn new(generation: u64, components: ChargeComponents) -> Result<Self, QueueCapacityError> {
        let value = Self {
            schema: QUEUE_CAPACITY_SCHEMA,
            generation,
            model: QUEUE_CAPACITY_MODEL,
            producer_bytes: components.producer_bytes,
            original_session_bytes: components.original_session_bytes,
            dead_letter_projection_bytes: components.dead_letter_projection_bytes,
            charged_bytes: components.charged_bytes,
        };
        value.validate_generation(generation)?;
        Ok(value)
    }

    /// New admission starts outside the DLQ. A shadow charge must instead be
    /// derived from the existing ledger so the original session reserve survives.
    #[cfg(test)]
    pub(crate) fn for_new_record(
        generation: u64,
        record: &MessageRecord,
    ) -> Result<Self, QueueCapacityError> {
        if record.dead_letter.is_some() {
            return Err(QueueCapacityError::OriginalSessionRequired);
        }
        Self::for_new_observation(generation, observe_record(record)?)
    }

    pub(crate) fn for_new_observation(
        generation: u64,
        observation: RecordChargeObservation,
    ) -> Result<Self, QueueCapacityError> {
        let ObservedSession::Live { bytes } = observation.session else {
            return Err(QueueCapacityError::OriginalSessionRequired);
        };
        Self::new(generation, observation.components(bytes)?)
    }

    #[cfg(test)]
    pub(crate) const fn generation(self) -> u64 {
        self.generation
    }
    #[cfg(test)]
    pub(crate) const fn producer_bytes(self) -> u64 {
        self.producer_bytes
    }
    #[cfg(test)]
    pub(crate) const fn original_session_bytes(self) -> u64 {
        self.original_session_bytes
    }
    #[cfg(test)]
    pub(crate) const fn dead_letter_projection_bytes(self) -> u64 {
        self.dead_letter_projection_bytes
    }
    pub(crate) const fn charged_bytes(self) -> u64 {
        self.charged_bytes
    }

    pub(crate) fn validate_generation(self, expected: u64) -> Result<(), QueueCapacityError> {
        validate_identity(self.schema, self.generation, expected)?;
        validate_model(self.model)?;
        let components = ChargeComponents::new(
            self.producer_bytes,
            self.original_session_bytes,
            self.dead_letter_projection_bytes,
        )?;
        if components.charged_bytes != self.charged_bytes {
            return Err(QueueCapacityError::InvalidCharge);
        }
        Ok(())
    }

    /// Compare the unmodified original record after ordinary command validation.
    /// The caller captures that record before applying settlement property edits.
    #[cfg(test)]
    pub(crate) fn validate_record(self, record: &MessageRecord) -> Result<(), QueueCapacityError> {
        self.validate_observation(observe_record(record)?)
    }

    pub(crate) fn validate_observation(
        self,
        original: RecordChargeObservation,
    ) -> Result<(), QueueCapacityError> {
        self.validate_generation(self.generation)?;
        let components = original.components(self.original_session_bytes)?;
        if components.producer_bytes != self.producer_bytes
            || components.dead_letter_projection_bytes != self.dead_letter_projection_bytes
            || components.charged_bytes != self.charged_bytes
        {
            return Err(QueueCapacityError::RecordMismatch);
        }
        Ok(())
    }

    /// Preserve saved S while recomputing P and D for a proposed retained record.
    /// This does not replace validation of the original record against its ledger.
    #[cfg(test)]
    pub(crate) fn recharge_retained_record(
        self,
        proposed: &MessageRecord,
    ) -> Result<Self, QueueCapacityError> {
        self.recharge_observation(observe_record(proposed)?)
    }

    pub(crate) fn recharge_observation(
        self,
        proposed: RecordChargeObservation,
    ) -> Result<Self, QueueCapacityError> {
        self.validate_generation(self.generation)?;
        Self::new(
            self.generation,
            proposed.components(self.original_session_bytes)?,
        )
    }

    pub(crate) fn encode(self) -> Result<Vec<u8>, QueueCapacityError> {
        self.validate_generation(self.generation)?;
        encode_record(&self)
    }

    pub(crate) fn decode(envelope: &[u8], expected: u64) -> Result<Self, QueueCapacityError> {
        let value: Self = decode_record(envelope)?;
        value.validate_generation(expected)?;
        Ok(value)
    }
}

fn checked_length(value: usize) -> Result<u64, QueueCapacityError> {
    u64::try_from(value).map_err(|_| QueueCapacityError::ArithmeticOverflow)
}

fn checked_content_tally(value: usize) -> Result<u64, QueueCapacityError> {
    if value == usize::MAX {
        return Err(QueueCapacityError::SaturatedContentTally);
    }
    checked_length(value)
}

fn producer_bytes(
    message_id: &str,
    body: &[u8],
    envelope: Option<&MessageEnvelope>,
) -> Result<u64, QueueCapacityError> {
    let body_bytes = checked_length(body.len())?;
    let identifier_bytes = || {
        VARIABLE_BYTES
            .checked_add(checked_length(message_id.len())?)
            .ok_or(QueueCapacityError::ArithmeticOverflow)
    };
    match envelope {
        None => body_bytes
            .checked_add(identifier_bytes()?)
            .ok_or(QueueCapacityError::ArithmeticOverflow),
        Some(envelope) => {
            let mut rich_bytes = checked_content_tally(envelope.content_size())?;
            if envelope.properties.message_id.is_none() {
                rich_bytes = rich_bytes
                    .checked_add(identifier_bytes()?)
                    .ok_or(QueueCapacityError::ArithmeticOverflow)?;
            }
            Ok(body_bytes.max(rich_bytes))
        }
    }
}

fn session_bytes(session: Option<&SessionId>) -> Result<u64, QueueCapacityError> {
    let Some(session) = session else {
        return Ok(0);
    };
    let value = session.as_str();
    if value.is_empty() || value.len() > MAX_SESSION_ID_BYTES || value.chars().any(char::is_control)
    {
        return Err(QueueCapacityError::InvalidSessionMetadata);
    }
    VARIABLE_BYTES
        .checked_add(checked_length(value.len())?)
        .ok_or(QueueCapacityError::ArithmeticOverflow)
}

fn dead_letter_bytes(
    reason_bytes: usize,
    description_bytes: usize,
) -> Result<u64, QueueCapacityError> {
    let reason_bytes = checked_length(reason_bytes)?;
    let description_bytes = checked_length(description_bytes)?;
    DEAD_LETTER_PROJECTION_BYTES
        .checked_add(reason_bytes)
        .and_then(|value| value.checked_add(description_bytes))
        .ok_or(QueueCapacityError::ArithmeticOverflow)
}

fn validate_identity(schema: u8, generation: u64, expected: u64) -> Result<(), QueueCapacityError> {
    if schema != QUEUE_CAPACITY_SCHEMA {
        return Err(QueueCapacityError::InvalidSchema);
    }
    if generation == 0 || expected == 0 {
        return Err(QueueCapacityError::InvalidGeneration);
    }
    if generation != expected {
        return Err(QueueCapacityError::GenerationMismatch);
    }
    Ok(())
}

fn validate_model(model: u8) -> Result<(), QueueCapacityError> {
    if model != QUEUE_CAPACITY_MODEL {
        return Err(QueueCapacityError::InvalidModel);
    }
    Ok(())
}

fn encode_record(value: &impl Serialize) -> Result<Vec<u8>, QueueCapacityError> {
    let envelope = codec::encode(value)?;
    if envelope.first() != Some(&codec::VALUE_FORMAT_V11) {
        return Err(QueueCapacityError::UnsupportedEnvelope);
    }
    if envelope.len() > MAX_CAPACITY_RECORD_BYTES {
        return Err(QueueCapacityError::RecordTooLarge);
    }
    Ok(envelope)
}

fn decode_record<T: Serialize + DeserializeOwned>(
    envelope: &[u8],
) -> Result<T, QueueCapacityError> {
    if envelope.len() > MAX_CAPACITY_RECORD_BYTES {
        return Err(QueueCapacityError::RecordTooLarge);
    }
    let (version, payload) = codec::split(envelope)?;
    if version != codec::VALUE_FORMAT_V11 {
        return Err(QueueCapacityError::UnsupportedEnvelope);
    }
    let value = codec::decode_payload(payload)?;
    if encode_record(&value)? != envelope {
        return Err(QueueCapacityError::NonCanonicalEnvelope);
    }
    Ok(value)
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(crate) enum QueueCapacityError {
    #[error("queue capacity schema is unsupported")]
    InvalidSchema,
    #[error("queue capacity accounting model is unsupported")]
    InvalidModel,
    #[error("queue capacity generation must be nonzero")]
    InvalidGeneration,
    #[error("queue capacity generation does not match its owner")]
    GenerationMismatch,
    #[error("finite queue capacity must be nonzero")]
    InvalidLimit,
    #[error("queue capacity usage is inconsistent")]
    InvalidUsage,
    #[error("queue capacity message charge is inconsistent")]
    InvalidCharge,
    #[error("queue capacity session metadata is inconsistent")]
    InvalidSessionMetadata,
    #[error("dead-letter accounting requires its original message charge")]
    OriginalSessionRequired,
    #[error("queue capacity content tally is saturated")]
    SaturatedContentTally,
    #[error("queue capacity arithmetic overflow")]
    ArithmeticOverflow,
    #[error("queue capacity arithmetic underflow")]
    ArithmeticUnderflow,
    #[error("finite capacity does not yet support required sessions or duplicate detection")]
    UnsupportedFiniteQueue,
    #[error("non-finite queues must not carry finite usage records")]
    UnexpectedUsage,
    #[error("finite queue capacity is exhausted")]
    LimitExceeded,
    #[error("queue capacity message charge does not match its original record")]
    RecordMismatch,
    #[error("queue capacity record exceeds its stored-value bound")]
    RecordTooLarge,
    #[error("queue capacity record requires value format 11")]
    UnsupportedEnvelope,
    #[error("queue capacity record encoding is not canonical")]
    NonCanonicalEnvelope,
    #[error(transparent)]
    Codec(#[from] CodecError),
}

#[cfg(test)]
mod tests;

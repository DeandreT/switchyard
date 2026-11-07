//! Bounded AMQP content preflight for retained legacy domain records.
//!
//! Every message record's retained AMQP content (ready, scheduled, deferred,
//! locked, topic placeholder and dead-letter rows alike) is checked under
//! [`LEGACY_CONTENT_LIMITS`]: the V1 envelope or V2 original as a whole message,
//! each V2 overlay value and each filter-projection application-property value
//! as a standalone value. Each retained correlation-rule property value is
//! checked the same way. These are per-content parser
//! safety bounds, not queue admission limits, storage quotas or a whole-image
//! allocation guarantee; current entity limits are never reapplied.
//!
//! Each content-bearing row's key and outer value are checked as canonical
//! before any recursive decoding. Message rows use the frozen message-record
//! codec and rules the V1 value codec; both are non-recursive, with allocations
//! bounded by the stored value length. Raw message bodies (V1 bodies and V2 raw
//! content) and session state are opaque bytes, not AMQP, and are not parsed. Other
//! domain tags carry no AMQP content, external F0/F1 rows belong to their own
//! owners, and any other tag refuses so no location is silently skipped.
//! Nothing is normalized, rewritten, replayed or certified as healthy.

use message_content::{decode_content, decode_value};
use serde::{Serialize, de::DeserializeOwned};
use storage::{Key, Value};
use thiserror::Error;

pub use message_content::{Error as ContentError, Limits as ContentLimits};

use crate::{
    RuleDefinition, RuleFilter,
    codec::{
        self,
        message::{self as message_codec, CanonicalError},
    },
    keys::{self, StateKey},
};

/// The fixed V3 profile, applied independently to each content item. It is the
/// same profile new sends parse with.
pub const LEGACY_CONTENT_LIMITS: ContentLimits = crate::content::CONTENT_LIMITS;

const TAG_MESSAGE: u8 = 0x03;
const TAG_SUBSCRIPTION_RULE: u8 = 0x10;
const LAST_DOMAIN_TAG: u8 = 0x11;

/// The checked location within one row. Ordinals follow the retained map's
/// canonical order; names and payload bytes are never reported.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LegacyContentField {
    /// The V1 envelope or V2 original AMQP message.
    Envelope,
    /// One V2 copy-local overlay value, in stored order.
    OverlayProperty {
        ordinal: usize,
    },
    FilterProperty {
        ordinal: usize,
    },
    RuleProperty {
        ordinal: usize,
    },
}

/// Bounded refusal. Row ordinals refer to the original input order.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum LegacyContentError {
    #[error("legacy row {row} has an empty key")]
    EmptyKey { row: usize },
    #[error("legacy row {row} does not advance in strict key order")]
    InputOrder { row: usize },
    #[error("legacy row {row} has unrecognized tag {tag:#04x}")]
    UnrecognizedTag { row: usize, tag: u8 },
    #[error("legacy row {row} has an invalid content-bearing key: {detail}")]
    InvalidKey { row: usize, detail: &'static str },
    #[error("legacy row {row} has an invalid content-bearing value: {detail}")]
    InvalidRecord { row: usize, detail: &'static str },
    #[error("legacy row {row} {field:?} is outside the bounded content contract: {error}")]
    Content {
        row: usize,
        field: LegacyContentField,
        error: ContentError,
    },
}

/// Observed counts only, never a health or eligibility certificate.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LegacyContentValidation {
    message_rows: usize,
    rule_rows: usize,
    envelopes: usize,
    values: usize,
}

impl LegacyContentValidation {
    pub fn message_rows(&self) -> usize {
        self.message_rows
    }
    pub fn rule_rows(&self) -> usize {
        self.rule_rows
    }
    /// Retained envelopes and V2 originals decoded as complete AMQP messages.
    pub fn envelopes(&self) -> usize {
        self.envelopes
    }
    /// Standalone overlay, filter-projection and rule property values decoded.
    pub fn values(&self) -> usize {
        self.values
    }
}

/// Checks every content location in one strictly ordered logical image.
pub fn validate_legacy_content(
    records: &[(Key, Value)],
) -> Result<LegacyContentValidation, LegacyContentError> {
    let mut report = LegacyContentValidation::default();
    for (row, (key, value)) in records.iter().enumerate() {
        let Some(&tag) = key.first() else {
            return Err(LegacyContentError::EmptyKey { row });
        };
        if row > 0 && records[row - 1].0.as_slice() >= key.as_slice() {
            return Err(LegacyContentError::InputOrder { row });
        }
        match tag {
            TAG_MESSAGE => {
                if !matches!(keys::state_parts(key), Some(StateKey::Message(..))) {
                    return Err(invalid_key(row, "message key is not canonical"));
                }
                let record =
                    message_codec::decode_canonical(value).map_err(|error| match error {
                        CanonicalError::Codec(_) => invalid_record(row, "value cannot be decoded"),
                        CanonicalError::NonCanonical => {
                            invalid_record(row, "value is not canonical")
                        }
                    })?;
                report.message_rows += 1;
                // V2 raw content has no envelope; a V1 overlay is unrepresentable.
                if let Some(envelope) = &record.envelope {
                    decode_content(envelope.as_bytes(), LEGACY_CONTENT_LIMITS)
                        .map_err(|error| content(row, LegacyContentField::Envelope, error))?;
                    report.envelopes += 1;
                    for (ordinal, property) in envelope.overlay().iter().enumerate() {
                        decode_value(&property.value, LEGACY_CONTENT_LIMITS).map_err(|error| {
                            content(row, LegacyContentField::OverlayProperty { ordinal }, error)
                        })?;
                        report.values += 1;
                    }
                    let properties = &envelope.filter_properties().application_properties;
                    for (ordinal, property) in properties.values().enumerate() {
                        decode_value(property.as_bytes(), LEGACY_CONTENT_LIMITS).map_err(
                            |error| {
                                content(row, LegacyContentField::FilterProperty { ordinal }, error)
                            },
                        )?;
                        report.values += 1;
                    }
                }
            }
            TAG_SUBSCRIPTION_RULE => {
                if keys::catalog_rule_parts(key).is_none() {
                    return Err(invalid_key(row, "rule key is not canonical"));
                }
                let definition = canonical::<RuleDefinition>(value, row)?;
                report.rule_rows += 1;
                if let RuleFilter::Correlation(filter) = &definition.filter {
                    for (ordinal, property) in filter.application_properties.values().enumerate() {
                        decode_value(property.as_bytes(), LEGACY_CONTENT_LIMITS).map_err(
                            |error| {
                                content(row, LegacyContentField::RuleProperty { ordinal }, error)
                            },
                        )?;
                        report.values += 1;
                    }
                }
            }
            // No AMQP content: catalog, counters, indexes, sessions, duplicates.
            0x00..=LAST_DOMAIN_TAG => {}
            // Journal and indexed owners validate their own reserved rows.
            0xF0 | 0xF1 => {}
            tag => return Err(LegacyContentError::UnrecognizedTag { row, tag }),
        }
    }
    Ok(report)
}

fn canonical<T: DeserializeOwned + Serialize>(
    raw: &[u8],
    row: usize,
) -> Result<T, LegacyContentError> {
    let value = codec::decode(raw).map_err(|_| invalid_record(row, "value cannot be decoded"))?;
    if codec::encode(&value).map_err(|_| invalid_record(row, "value cannot be encoded"))? != raw {
        return Err(invalid_record(row, "value is not canonical"));
    }
    Ok(value)
}

fn invalid_key(row: usize, detail: &'static str) -> LegacyContentError {
    LegacyContentError::InvalidKey { row, detail }
}

fn invalid_record(row: usize, detail: &'static str) -> LegacyContentError {
    LegacyContentError::InvalidRecord { row, detail }
}

fn content(row: usize, field: LegacyContentField, error: ContentError) -> LegacyContentError {
    LegacyContentError::Content { row, field, error }
}

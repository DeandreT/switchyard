//! Canonical subscription-rule records. The value fence is local to rules: a
//! filter that predates SQL keeps its exact V1 bytes, and only a SQL rule uses
//! V2, which every reader without this codec refuses as an unsupported version.

use serde::{Deserialize, Serialize};

use crate::{RuleDefinition, RuleFilter, RuleName, SqlRuleFilter, Timestamp};

use super::{CodecError, VALUE_FORMAT_V1, message::CanonicalError};

pub const VALUE_FORMAT_V2: u8 = 2;

// Stored DTO frozen independently of the public rule serde. The name is the
// display spelling, as in V1; the expression is the exact retained text.
#[derive(Serialize, Deserialize)]
struct SqlRuleV2 {
    name: String,
    expression: String,
    created_at: u64,
}

/// Rule policy, including SQL compilation, belongs to callers, so corrupt or
/// no-longer-compilable fixtures are not silently normalized here.
pub fn encode(definition: &RuleDefinition) -> Result<Vec<u8>, CodecError> {
    match &definition.filter {
        RuleFilter::Sql(filter) => {
            let mut bytes = vec![VALUE_FORMAT_V2];
            bytes.extend(
                postcard::to_stdvec(&SqlRuleV2 {
                    name: definition.name.display_name().to_owned(),
                    expression: filter.expression().to_owned(),
                    created_at: definition.created_at.as_millis(),
                })
                .map_err(|_| CodecError::Encode)?,
            );
            Ok(bytes)
        }
        RuleFilter::True | RuleFilter::False | RuleFilter::Correlation(_) => {
            super::encode(definition)
        }
    }
}

/// Exact framing and re-encoding reject trailing, nonminimal and aliasing
/// records, including a SQL filter inside a V1 envelope.
pub fn decode(bytes: &[u8]) -> Result<RuleDefinition, CodecError> {
    decode_canonical(bytes).map_err(|error| match error {
        CanonicalError::Codec(error) => error,
        CanonicalError::NonCanonical => CodecError::Decode,
    })
}

/// [`decode`] that tells undecodable rows apart from decodable but
/// noncanonical ones, for validators that report the difference.
pub(crate) fn decode_canonical(bytes: &[u8]) -> Result<RuleDefinition, CanonicalError> {
    let (definition, unused) = read(bytes).map_err(CanonicalError::Codec)?;
    if !unused.is_empty() || encode(&definition).map_err(CanonicalError::Codec)? != bytes {
        return Err(CanonicalError::NonCanonical);
    }
    Ok(definition)
}

fn read(bytes: &[u8]) -> Result<(RuleDefinition, &[u8]), CodecError> {
    let (&version, payload) = bytes.split_first().ok_or(CodecError::EmptyEnvelope)?;
    match version {
        VALUE_FORMAT_V1 => {
            let (definition, unused) = postcard::take_from_bytes::<RuleDefinition>(payload)
                .map_err(|_| CodecError::Decode)?;
            if !definition.filter.is_legacy_representable() {
                return Err(CodecError::Decode);
            }
            Ok((definition, unused))
        }
        VALUE_FORMAT_V2 => {
            let (stored, unused) =
                postcard::take_from_bytes::<SqlRuleV2>(payload).map_err(|_| CodecError::Decode)?;
            Ok((
                RuleDefinition {
                    name: RuleName::new(stored.name).map_err(|_| CodecError::Decode)?,
                    filter: RuleFilter::Sql(SqlRuleFilter::new(stored.expression)),
                    created_at: Timestamp::from_millis(stored.created_at),
                },
                unused,
            ))
        }
        version => Err(CodecError::UnsupportedVersion { version }),
    }
}

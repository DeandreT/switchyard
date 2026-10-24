//! Canonical topic records. The value fence is local to topic profiles; the
//! store format and all generic value primitives remain unchanged.

use serde::{Deserialize, Serialize};

use crate::{DEFAULT_DUPLICATE_DETECTION_HISTORY_MILLIS, TopicConfig};

use super::{CodecError, VALUE_FORMAT_V1};

pub const VALUE_FORMAT_V2: u8 = 2;

#[derive(Serialize, Deserialize)]
struct LegacyTopic {
    default_time_to_live_millis: Option<u64>,
    max_message_bytes: usize,
}

// Stored DTOs are frozen independently of the public configuration's serde.
#[derive(Serialize, Deserialize)]
struct TopicV2 {
    default_time_to_live_millis: Option<u64>,
    max_message_bytes: usize,
    requires_duplicate_detection: bool,
    duplicate_detection_history_millis: u64,
}

/// Preserve exact legacy bytes iff both new settings have their legacy values.
/// Policy validation belongs to callers, so corrupt-profile fixtures and invalid
/// proposal intent are not silently normalized here.
pub fn encode(config: &TopicConfig) -> Result<Vec<u8>, CodecError> {
    if config.is_legacy_representable() {
        super::encode(&LegacyTopic {
            default_time_to_live_millis: config.default_time_to_live_millis,
            max_message_bytes: config.max_message_bytes,
        })
    } else {
        let mut bytes = vec![VALUE_FORMAT_V2];
        bytes.extend(
            postcard::to_stdvec(&TopicV2 {
                default_time_to_live_millis: config.default_time_to_live_millis,
                max_message_bytes: config.max_message_bytes,
                requires_duplicate_detection: config.requires_duplicate_detection,
                duplicate_detection_history_millis: config.duplicate_detection_history_millis,
            })
            .map_err(|_| CodecError::Encode)?,
        );
        Ok(bytes)
    }
}

/// Exact framing and re-encoding reject trailing, nonminimal and redundant V2
/// records, independently of topic policy validation.
pub fn decode(bytes: &[u8]) -> Result<TopicConfig, CodecError> {
    let (&version, payload) = bytes.split_first().ok_or(CodecError::EmptyEnvelope)?;
    let (config, unused) = match version {
        VALUE_FORMAT_V1 => {
            let (old, unused) = postcard::take_from_bytes::<LegacyTopic>(payload)
                .map_err(|_| CodecError::Decode)?;
            (
                TopicConfig {
                    default_time_to_live_millis: old.default_time_to_live_millis,
                    max_message_bytes: old.max_message_bytes,
                    requires_duplicate_detection: false,
                    duplicate_detection_history_millis: DEFAULT_DUPLICATE_DETECTION_HISTORY_MILLIS,
                },
                unused,
            )
        }
        VALUE_FORMAT_V2 => {
            let (stored, unused) =
                postcard::take_from_bytes::<TopicV2>(payload).map_err(|_| CodecError::Decode)?;
            (
                TopicConfig {
                    default_time_to_live_millis: stored.default_time_to_live_millis,
                    max_message_bytes: stored.max_message_bytes,
                    requires_duplicate_detection: stored.requires_duplicate_detection,
                    duplicate_detection_history_millis: stored.duplicate_detection_history_millis,
                },
                unused,
            )
        }
        version => return Err(CodecError::UnsupportedVersion { version }),
    };
    if !unused.is_empty() || encode(&config)? != bytes {
        return Err(CodecError::Decode);
    }
    Ok(config)
}

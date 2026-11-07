//! Canonical queue-profile records, shared by ordinary queues, subscription
//! backing queues and every dead-letter shadow. The value fence is local to
//! queue profiles; the store format and generic value primitives are unchanged.

use serde::{Deserialize, Serialize};

use crate::QueueConfig;

use super::{CodecError, VALUE_FORMAT_V1};

pub const VALUE_FORMAT_V2: u8 = 2;

// Stored DTOs are frozen independently of the public configuration's serde.
#[derive(Serialize, Deserialize)]
struct LegacyQueue {
    lock_duration_millis: u64,
    max_delivery_count: u32,
    default_time_to_live_millis: Option<u64>,
    max_message_bytes: usize,
    requires_session: bool,
    requires_duplicate_detection: bool,
    duplicate_detection_history_millis: u64,
}

#[derive(Serialize, Deserialize)]
struct QueueV2 {
    lock_duration_millis: u64,
    max_delivery_count: u32,
    default_time_to_live_millis: Option<u64>,
    max_message_bytes: usize,
    requires_session: bool,
    requires_duplicate_detection: bool,
    duplicate_detection_history_millis: u64,
    dead_lettering_on_message_expiration: bool,
}

/// Preserve exact legacy bytes iff expired messages are dead-lettered, the
/// only behavior a legacy reader implements. Policy validation belongs to
/// callers, so corrupt-profile fixtures are not silently normalized here.
pub fn encode(config: &QueueConfig) -> Result<Vec<u8>, CodecError> {
    if config.is_legacy_representable() {
        super::encode(&LegacyQueue {
            lock_duration_millis: config.lock_duration_millis,
            max_delivery_count: config.max_delivery_count,
            default_time_to_live_millis: config.default_time_to_live_millis,
            max_message_bytes: config.max_message_bytes,
            requires_session: config.requires_session,
            requires_duplicate_detection: config.requires_duplicate_detection,
            duplicate_detection_history_millis: config.duplicate_detection_history_millis,
        })
    } else {
        let mut bytes = vec![VALUE_FORMAT_V2];
        bytes.extend(
            postcard::to_stdvec(&QueueV2 {
                lock_duration_millis: config.lock_duration_millis,
                max_delivery_count: config.max_delivery_count,
                default_time_to_live_millis: config.default_time_to_live_millis,
                max_message_bytes: config.max_message_bytes,
                requires_session: config.requires_session,
                requires_duplicate_detection: config.requires_duplicate_detection,
                duplicate_detection_history_millis: config.duplicate_detection_history_millis,
                dead_lettering_on_message_expiration: config.dead_lettering_on_message_expiration,
            })
            .map_err(|_| CodecError::Encode)?,
        );
        Ok(bytes)
    }
}

/// Exact framing and re-encoding reject trailing, nonminimal and redundant V2
/// records, independently of queue policy validation.
pub fn decode(bytes: &[u8]) -> Result<QueueConfig, CodecError> {
    let (config, unused) = parse(bytes)?;
    if !unused.is_empty() || encode(&config)? != bytes {
        return Err(CodecError::Decode);
    }
    Ok(config)
}

/// Whether a record that [`decode`] refuses still parses as a known version,
/// so that validators can keep distinguishing undecodable from noncanonical.
pub(crate) fn is_noncanonical(bytes: &[u8]) -> bool {
    parse(bytes).is_ok() && decode(bytes).is_err()
}

fn parse(bytes: &[u8]) -> Result<(QueueConfig, &[u8]), CodecError> {
    let (&version, payload) = bytes.split_first().ok_or(CodecError::EmptyEnvelope)?;
    let (config, unused) = match version {
        VALUE_FORMAT_V1 => {
            let (old, unused) = postcard::take_from_bytes::<LegacyQueue>(payload)
                .map_err(|_| CodecError::Decode)?;
            (
                QueueConfig {
                    lock_duration_millis: old.lock_duration_millis,
                    max_delivery_count: old.max_delivery_count,
                    default_time_to_live_millis: old.default_time_to_live_millis,
                    max_message_bytes: old.max_message_bytes,
                    requires_session: old.requires_session,
                    requires_duplicate_detection: old.requires_duplicate_detection,
                    duplicate_detection_history_millis: old.duplicate_detection_history_millis,
                    dead_lettering_on_message_expiration: true,
                },
                unused,
            )
        }
        VALUE_FORMAT_V2 => {
            let (stored, unused) =
                postcard::take_from_bytes::<QueueV2>(payload).map_err(|_| CodecError::Decode)?;
            (
                QueueConfig {
                    lock_duration_millis: stored.lock_duration_millis,
                    max_delivery_count: stored.max_delivery_count,
                    default_time_to_live_millis: stored.default_time_to_live_millis,
                    max_message_bytes: stored.max_message_bytes,
                    requires_session: stored.requires_session,
                    requires_duplicate_detection: stored.requires_duplicate_detection,
                    duplicate_detection_history_millis: stored.duplicate_detection_history_millis,
                    dead_lettering_on_message_expiration: stored
                        .dead_lettering_on_message_expiration,
                },
                unused,
            )
        }
        version => return Err(CodecError::UnsupportedVersion { version }),
    };
    Ok((config, unused))
}

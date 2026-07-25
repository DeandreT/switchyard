use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;

/// Leading byte of every stored value.
///
/// A rolling upgrade writes the version its whole voter set advertises, so a
/// replica must be able to decode the version it is given rather than assume
/// the current one. Reads accept any known version; writes always use the
/// active one.
pub const VALUE_FORMAT_V1: u8 = 1;

/// Adds a session identifier to a stored message. Every other record has the
/// same shape it had in version 1.
pub const VALUE_FORMAT_V2: u8 = 2;

pub const ACTIVE_VALUE_FORMAT: u8 = VALUE_FORMAT_V2;

/// Encodes a value into a versioned envelope.
///
/// Postcard is used because its output is deterministic for a given value and
/// schema. Two replicas applying the same command must produce byte-identical
/// records, otherwise their snapshots and hash chains diverge.
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, CodecError> {
    let payload = postcard::to_stdvec(value).map_err(|_| CodecError::Encode)?;
    let mut envelope = Vec::with_capacity(payload.len() + 1);
    envelope.push(ACTIVE_VALUE_FORMAT);
    envelope.extend_from_slice(&payload);
    Ok(envelope)
}

/// Decodes a record whose shape is the same in every known format version.
///
/// A record that gained a field needs to know which version it is reading, and
/// decodes through [`split`] and [`decode_payload`] instead.
pub fn decode<T: DeserializeOwned>(envelope: &[u8]) -> Result<T, CodecError> {
    let (_, payload) = split(envelope)?;
    decode_payload(payload)
}

/// Splits a stored envelope into its format version and its payload, rejecting
/// a version this build does not know.
pub fn split(envelope: &[u8]) -> Result<(u8, &[u8]), CodecError> {
    let (version, payload) = envelope.split_first().ok_or(CodecError::EmptyEnvelope)?;
    if *version != VALUE_FORMAT_V1 && *version != VALUE_FORMAT_V2 {
        return Err(CodecError::UnsupportedVersion { version: *version });
    }
    Ok((*version, payload))
}

pub fn decode_payload<T: DeserializeOwned>(payload: &[u8]) -> Result<T, CodecError> {
    postcard::from_bytes(payload).map_err(|_| CodecError::Decode)
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CodecError {
    #[error("stored value envelope is empty")]
    EmptyEnvelope,
    #[error("stored value uses unsupported format version {version}")]
    UnsupportedVersion { version: u8 },
    #[error("value could not be encoded")]
    Encode,
    #[error("stored value payload could not be decoded")]
    Decode,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_a_versioned_envelope() -> Result<(), CodecError> {
        let envelope = encode(&(7_u64, String::from("orders")))?;
        assert_eq!(envelope.first(), Some(&ACTIVE_VALUE_FORMAT));
        assert_eq!(
            decode::<(u64, String)>(&envelope)?,
            (7, String::from("orders"))
        );
        Ok(())
    }

    #[test]
    fn encoding_is_byte_identical_for_equal_values() -> Result<(), CodecError> {
        assert_eq!(encode(&(1_u64, "a"))?, encode(&(1_u64, "a"))?);
        Ok(())
    }

    #[test]
    fn a_record_that_never_changed_shape_reads_under_either_version() -> Result<(), CodecError> {
        let payload = postcard::to_stdvec(&(7_u64, String::from("orders"))).expect("encodes");
        for version in [VALUE_FORMAT_V1, VALUE_FORMAT_V2] {
            let mut envelope = vec![version];
            envelope.extend_from_slice(&payload);
            assert_eq!(
                decode::<(u64, String)>(&envelope)?,
                (7, String::from("orders"))
            );
            assert_eq!(split(&envelope)?.0, version);
        }
        Ok(())
    }

    #[test]
    fn rejects_an_unknown_format_version() {
        assert_eq!(
            decode::<u64>(&[99, 0]),
            Err(CodecError::UnsupportedVersion { version: 99 })
        );
        assert_eq!(
            split(&[99, 0]),
            Err(CodecError::UnsupportedVersion { version: 99 })
        );
        assert_eq!(decode::<u64>(&[]), Err(CodecError::EmptyEnvelope));
    }
}

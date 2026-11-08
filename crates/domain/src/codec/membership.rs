//! Canonical topic-membership records. A membership names its exact child
//! subscription path. The default record is the V1 envelope of that path, so
//! every subscription that dead-letters filter-error copies keeps its exact
//! legacy bytes. Only a subscription that discards them stores V2, which a
//! legacy reader refuses as a membership that does not encode its child.

use serde::Serialize;

use crate::EntityPath;

use super::{CodecError, VALUE_FORMAT_V1};

pub const VALUE_FORMAT_V2: u8 = 2;

// Frozen stored DTO, independent of the public configuration's serde.
#[derive(Serialize)]
struct MembershipV2<'a> {
    child: &'a EntityPath,
    dead_lettering_on_filter_evaluation_exceptions: bool,
}

pub fn encode(
    child: &EntityPath,
    dead_lettering_on_filter_evaluation_exceptions: bool,
) -> Result<Vec<u8>, CodecError> {
    if dead_lettering_on_filter_evaluation_exceptions {
        return super::encode(child);
    }
    let mut bytes = vec![VALUE_FORMAT_V2];
    bytes.extend(
        postcard::to_stdvec(&MembershipV2 {
            child,
            dead_lettering_on_filter_evaluation_exceptions,
        })
        .map_err(|_| CodecError::Encode)?,
    );
    Ok(bytes)
}

/// Returns the filter-error setting of a membership that canonically names
/// `child`. Deserialization folds ASCII case, so only exact re-encoded bytes
/// prove the record names this canonical child.
pub fn decode(child: &EntityPath, bytes: &[u8]) -> Result<bool, CodecError> {
    match bytes.first() {
        None => Err(CodecError::EmptyEnvelope),
        Some(&VALUE_FORMAT_V1) if bytes == encode(child, true)? => Ok(true),
        Some(&VALUE_FORMAT_V2) if bytes == encode(child, false)? => Ok(false),
        Some(&VALUE_FORMAT_V1 | &VALUE_FORMAT_V2) => Err(CodecError::Decode),
        Some(&version) => Err(CodecError::UnsupportedVersion { version }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn child() -> EntityPath {
        EntityPath::new("orders")
            .unwrap()
            .subscription(&crate::SubscriptionName::new("audit").unwrap())
            .unwrap()
    }

    #[test]
    fn the_default_keeps_the_legacy_envelope() {
        let child = child();
        assert_eq!(
            encode(&child, true).unwrap(),
            super::super::encode(&child).unwrap()
        );
        assert_eq!(decode(&child, &encode(&child, true).unwrap()), Ok(true));
        assert_eq!(decode(&child, &encode(&child, false).unwrap()), Ok(false));
    }

    #[test]
    fn noncanonical_and_foreign_records_refuse() {
        let child = child();
        let other = EntityPath::new("orders")
            .unwrap()
            .subscription(&crate::SubscriptionName::new("other").unwrap())
            .unwrap();
        let mut redundant = vec![VALUE_FORMAT_V2];
        redundant.extend(
            postcard::to_stdvec(&MembershipV2 {
                child: &child,
                dead_lettering_on_filter_evaluation_exceptions: true,
            })
            .unwrap(),
        );
        let mut trailing = encode(&child, false).unwrap();
        trailing.push(0);
        for bytes in [
            redundant,
            trailing,
            encode(&other, false).unwrap(),
            encode(&other, true).unwrap(),
        ] {
            assert_eq!(decode(&child, &bytes), Err(CodecError::Decode));
        }
        assert_eq!(
            decode(&child, &[3]),
            Err(CodecError::UnsupportedVersion { version: 3 })
        );
        assert_eq!(decode(&child, &[]), Err(CodecError::EmptyEnvelope));
    }
}

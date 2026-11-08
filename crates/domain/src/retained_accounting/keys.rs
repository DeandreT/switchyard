//! Reserved accounting key only. Existing snapshot validators intentionally
//! refuse tag 0x12 until the writer/recovery fences implement its ownership.

use super::CapacityOwner;

/// Newly allocated domain tag, after existing entity metadata tag 0x11.
pub const USAGE_TAG: u8 = 0x12;

pub fn usage(owner: &CapacityOwner) -> Vec<u8> {
    let mut key = vec![USAGE_TAG];
    key.extend_from_slice(owner.namespace().as_str().as_bytes());
    key.push(0);
    key.extend_from_slice(owner.parent().as_str().as_bytes());
    key.push(0);
    key
}

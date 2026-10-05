use std::collections::{BTreeMap, BTreeSet};

use openraft::{BasicNode, Membership, StoredMembership};

use super::*;
use crate::experimental_log::{bounded_membership_len, encode_membership};

#[test]
fn borrowed_counter_matches_frozen_membership_encoding_without_collecting() -> TestResult {
    for membership in [
        captured::members(),
        Membership::new(
            vec![BTreeSet::from([0, u64::MAX]), BTreeSet::from([u64::MAX])],
            BTreeMap::from([
                (0, BasicNode::new("a")),
                (u64::MAX, BasicNode::new("\u{0800}".repeat(170))),
            ]),
        ),
    ] {
        assert_eq!(
            bounded_membership_len(&membership)?,
            encode_membership(&membership)?.len()
        );
    }
    assert_eq!(
        bounded_membership_len(&Membership::<u64, BasicNode>::default())?,
        2
    );
    Ok(())
}

#[test]
fn arbitrary_meta_shapes_are_bounded_before_owned_request_allocations() -> TestResult {
    let members = [
        Membership::new(
            vec![BTreeSet::from([7]); 3],
            BTreeMap::from([(7, BasicNode::new("n"))]),
        ),
        Membership::new(
            vec![(0..33).collect::<BTreeSet<_>>()],
            (0..33)
                .map(|i| (i, BasicNode::new("n")))
                .collect::<BTreeMap<_, _>>(),
        ),
        Membership::new(
            vec![BTreeSet::from([7])],
            (0..33)
                .map(|i| (i, BasicNode::new("n")))
                .collect::<BTreeMap<_, _>>(),
        ),
        Membership::new(
            vec![BTreeSet::from([7])],
            BTreeMap::from([(7, BasicNode::new("a".repeat(513)))]),
        ),
    ];
    for membership in members {
        let mut source = fixture::source(false)?;
        source.carrier.meta.last_membership = StoredMembership::new(None, membership);
        assert_eq!(
            OwnedTrustedNativeReplacement::new(
                captured::stream()?,
                source.checkpoint.clone(),
                source.checkpoint,
                source.digest,
                source.carrier
            )
            .err(),
            Some(NativeSnapshotMetadataError::LimitExceeded)
        );
    }
    Ok(())
}

#[test]
fn total_canonical_membership_payload_is_bounded_not_only_each_address() -> TestResult {
    let membership = Membership::new(
        vec![(0..8).collect::<BTreeSet<_>>()],
        (0..8)
            .map(|i| (i, BasicNode::new("a".repeat(512))))
            .collect::<BTreeMap<_, _>>(),
    );
    assert!(bounded_membership_len(&membership).is_err());
    let mut source = fixture::source(false)?;
    source.carrier.meta.last_membership = StoredMembership::new(None, membership);
    assert_eq!(
        fixture::request(source.checkpoint.clone(), source)
            .err()
            .and_then(|error| error.downcast::<NativeSnapshotMetadataError>().ok())
            .map(|error| *error),
        Some(NativeSnapshotMetadataError::LimitExceeded)
    );
    Ok(())
}

#[test]
fn snapshot_id_exact_bound_and_request_diagnostics_are_static() -> TestResult {
    let mut source = fixture::source(false)?;
    source.carrier.meta.snapshot_id = format!("{}xx", "PRIVATE".repeat(11));
    let request = fixture::request(source.checkpoint.clone(), source)?;
    assert!(!format!("{request:?}").contains("PRIVATE"));
    let mut source = fixture::source(false)?;
    source.carrier.meta.snapshot_id = "x".repeat(80);
    assert_eq!(
        OwnedTrustedNativeReplacement::new(
            captured::stream()?,
            source.checkpoint.clone(),
            source.checkpoint,
            source.digest,
            source.carrier
        )
        .err(),
        Some(NativeSnapshotMetadataError::LimitExceeded)
    );
    Ok(())
}

#[test]
fn exact_canonical_payload_boundary_matches_existing_encoder() -> TestResult {
    let make = |length| {
        Membership::new(
            vec![BTreeSet::from([u64::MAX])],
            (0..9)
                .map(|i| {
                    (
                        i,
                        BasicNode::new("a".repeat(if i == 0 { length } else { 500 })),
                    )
                })
                .chain(std::iter::once((u64::MAX, BasicNode::new(""))))
                .collect::<BTreeMap<_, _>>(),
        )
    };
    let mut accepted = 0;
    for length in 0..=512 {
        let membership = make(length);
        match encode_membership(&membership) {
            Ok(encoded) => {
                assert_eq!(bounded_membership_len(&membership)?, encoded.len());
                accepted = length;
            }
            Err(_) => assert!(bounded_membership_len(&membership).is_err()),
        }
    }
    assert!(accepted < 512);
    assert!(bounded_membership_len(&make(accepted + 1)).is_err());
    Ok(())
}

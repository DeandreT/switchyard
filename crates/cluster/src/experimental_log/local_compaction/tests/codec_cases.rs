use super::*;
use crate::experimental_local_compaction::LocalCompactionError as Error;
use sha2::{Digest, Sha256};

#[test]
fn empty_baseline_has_frozen_canonical_frame() -> TestResult {
    let profile = profile()?;
    let baseline = codec::Baseline::empty(&profile)?;
    assert_eq!(&baseline.bytes[..12], b"SWLF\0\x01\0\x01\0\0\0\x14");
    let mut payload = vec![7];
    payload.extend_from_slice(&[93; 16]);
    payload.extend_from_slice(&[0, 0, 0]);
    assert_eq!(&baseline.bytes[12..32], payload);
    assert_eq!(
        &baseline.bytes[32..],
        Sha256::digest(&baseline.bytes[..32]).as_slice()
    );
    assert_eq!(
        &baseline.bytes[32..],
        &[
            0x74, 0x07, 0xb1, 0x86, 0xbe, 0xa9, 0x5b, 0x4a, 0x3b, 0x7a, 0xdc, 0x1e, 0x88, 0xf2,
            0x38, 0x48, 0xbb, 0x02, 0xb1, 0x22, 0xba, 0x7b, 0xc1, 0xe8, 0xdc, 0x7f, 0xac, 0xf9,
            0x57, 0x7c, 0xd0, 0x42
        ]
    );
    assert_eq!(baseline.ordinal, 0);
    assert!(baseline.through().is_none());
    assert_eq!(
        codec::Baseline::decode(&profile, baseline.bytes.clone())?.bytes,
        baseline.bytes
    );
    assert_eq!(
        codec::Baseline::make(&profile, 0, b"not-empty").err(),
        Some(Error::InvalidHistory)
    );
    Ok(())
}

#[test]
fn baseline_rejects_schema_role_checksum_lengths_and_noncanonical_wire() -> TestResult {
    let profile = profile()?;
    let baseline = codec::Baseline::empty(&profile)?.bytes;
    for offset in [0, 5, 7, 11, baseline.len() - 1] {
        let mut bad = baseline.clone();
        bad[offset] ^= 1;
        assert_eq!(
            codec::Baseline::decode(&profile, bad).err(),
            Some(Error::InvalidHistory)
        );
    }
    let mut extra = baseline.clone();
    extra.push(0);
    assert_eq!(
        codec::Baseline::decode(&profile, extra).err(),
        Some(Error::InvalidHistory)
    );
    assert_eq!(
        codec::Baseline::decode(&profile, vec![0; codec::MAX_BASELINE_BYTES + 1]).err(),
        Some(Error::InvalidHistory)
    );
    let mut noncanonical = baseline[..32].to_vec();
    // The canonical node 7 is one byte; encode it with an overlong varint.
    noncanonical.splice(12..13, [0x87, 0]);
    noncanonical[8..12].copy_from_slice(&21u32.to_be_bytes());
    let checksum = Sha256::digest(&noncanonical);
    noncanonical.extend_from_slice(&checksum);
    assert_eq!(
        codec::Baseline::decode(&profile, noncanonical).err(),
        Some(Error::InvalidHistory)
    );
    Ok(())
}

#[test]
fn create_is_pristine_one_commit_and_open_requires_all_three_records() -> TestResult {
    let profile = profile()?;
    let (writer, control) = Writer::new();
    let state = state::StoreState::create(writer, profile.clone())?;
    assert_eq!(control.commits.load(Ordering::SeqCst), 1);
    assert_eq!(control.records.snapshot()?.entries().len(), 3);
    drop(state);
    let reopened = state::StoreState::open(Writer(control.clone()), profile.clone())?;
    drop(reopened);
    assert_eq!(
        state::StoreState::open(
            Writer(control.clone()),
            LogProfile::new(8, profile.stream())?
        )
        .err(),
        Some(Error::InvalidPair)
    );
    assert_eq!(
        state::StoreState::open(
            Writer(control.clone()),
            LogProfile::new(7, CommittedStreamId::new([94; 16])?)?
        )
        .err(),
        Some(Error::InvalidPair)
    );
    assert_eq!(control.commits.load(Ordering::SeqCst), 1);
    assert_eq!(
        state::StoreState::create(Writer(control.clone()), profile.clone()).err(),
        Some(Error::InvalidPair)
    );
    let mut batch = WriteBatch::default();
    batch.push_delete(codec::BASELINE_KEY);
    control.records.apply(batch)?;
    assert_eq!(
        state::StoreState::open(Writer(control.clone()), profile.clone()).err(),
        Some(Error::InvalidHistory)
    );
    put(
        &control,
        codec::BASELINE_KEY,
        &codec::Baseline::empty(&profile)?.bytes,
    )?;
    put(&control, &[4], b"unknown")?;
    assert_eq!(
        state::StoreState::open(Writer(control), profile).err(),
        Some(Error::InvalidHistory)
    );
    Ok(())
}

#[test]
fn ordinary_and_local_roles_refuse_each_other() -> TestResult {
    let profile = profile()?;
    let (writer, ordinary) = Writer::new();
    let ordinary_state = super::super::super::state::StoreState::create(writer, profile.clone())?;
    drop(ordinary_state);
    assert_eq!(
        state::StoreState::open(Writer(ordinary), profile.clone()).err(),
        Some(Error::InvalidPair)
    );
    let (writer, local) = Writer::new();
    let local_state = state::StoreState::create(writer, profile.clone())?;
    drop(local_state);
    assert!(super::super::super::state::StoreState::open(Writer(local), profile).is_err());
    Ok(())
}

#[test]
fn initialized_partial_records_and_uninitialized_rows_are_never_adopted() -> TestResult {
    let profile = profile()?;
    for keys in [
        vec![vec![1]],
        vec![vec![2]],
        vec![vec![3]],
        vec![vec![1], vec![2]],
    ] {
        let (writer, control) = Writer::new();
        for key in keys {
            put(&control, &key, b"partial")?;
        }
        control.initialized.store(true, Ordering::SeqCst);
        assert!(state::StoreState::open(writer, profile.clone()).is_err());
        assert_eq!(control.commits.load(Ordering::SeqCst), 0);
    }
    let (writer, control) = Writer::new();
    put(&control, &[0x10], b"row")?;
    assert_eq!(
        state::StoreState::create(writer, profile.clone()).err(),
        Some(Error::InvalidHistory)
    );
    assert_eq!(control.commits.load(Ordering::SeqCst), 0);
    let (_, metadata, _) = source(&entries()?)?;
    let mut summary =
        crate::experimental_state_machine::NativeCheckpointSummary::decode(metadata.as_bytes())?;
    summary.last.as_mut().unwrap().id.index = 0;
    summary.previous = None;
    assert_eq!(
        codec::Baseline::make(&profile, 1, &summary.encode_for_test()?).err(),
        Some(Error::InvalidHistory)
    );
    summary.last.as_mut().unwrap().id.term = 0;
    summary.last.as_mut().unwrap().id.node_id = 0;
    assert_eq!(
        codec::Baseline::make(&profile, 1, &summary.encode_for_test()?).err(),
        Some(Error::InvalidHistory)
    );
    Ok(())
}

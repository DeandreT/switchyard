use std::{
    collections::{BTreeMap, BTreeSet},
    io::SeekFrom,
};

use openraft::{
    BasicNode, CommittedLeaderId, Membership, StoredMembership,
    storage::{RaftSnapshotBuilder, RaftStateMachine},
};
use storage::{Mutation, SnapshotCatalogReader, StateStore};
use tokio::io::AsyncSeekExt;

use super::*;
use observed::Fault;

pub(super) async fn exact_selected_body_moves_without_cursor_or_postcommit_io<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control, target) = fixture::target(writer).await?;
    let result = async {
        let old = control.reader().snapshot()?;
        let mut source = fixture::source(true)?;
        source.carrier.snapshot.seek(SeekFrom::End(-1)).await?;
        let (request, rows, checkpoint, pointer) = fixture::prepared(target, source)?;
        let receipt = machine
            .replace_create_send_image_with_catalog(request)
            .await?;
        assert_eq!(std::mem::size_of_val(&receipt), 0);
        fixture::assert_counts(&control, 1, 1);
        assert_eq!(control.committed_pointers()[0].1, pointer);
        let batches = control.catalog_batches();
        assert_eq!(batches.len(), 1);
        let puts = batches[0]
            .mutations()
            .iter()
            .filter_map(|mutation| match mutation {
                Mutation::Put { key, value } => Some((key.clone(), value.clone())),
                _ => None,
            })
            .collect::<Vec<_>>();
        let deletes = batches[0]
            .mutations()
            .iter()
            .filter_map(|mutation| match mutation {
                Mutation::Delete { key } => Some(key),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(puts.as_slice(), rows.entries());
        let stale = old
            .entries()
            .iter()
            .filter(|(key, _)| {
                rows.entries()
                    .binary_search_by(|(other, _)| other.cmp(key))
                    .is_err()
            })
            .map(|(key, _)| key)
            .collect::<Vec<_>>();
        assert_eq!(deletes, stale);
        fixture::exact_target(&control, &rows, &checkpoint)?;
        // These explicit later caller queries are not part of replacement.
        assert_eq!(machine.checkpoint().await?, checkpoint);
        assert_eq!(
            machine.applied_state().await?.0.map(|id| id.index),
            checkpoint.last().map(|last| last.id.index)
        );
        Ok(())
    }
    .await;
    fixture::finish(machine, result).await
}

pub(super) async fn initial_image_removes_populated_rows_and_preserves_default_membership<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control, target) = fixture::target(writer).await?;
    let result = async {
        let source = fixture::from_selected(captured::initial()?)?;
        let (request, rows, checkpoint, _) = fixture::prepared(target, source)?;
        machine
            .replace_create_send_image_with_catalog(request)
            .await?;
        fixture::assert_counts(&control, 1, 1);
        fixture::exact_target(&control, &rows, &checkpoint)?;
        assert!(checkpoint.last().is_none());
        assert!(checkpoint.membership().is_none());
        assert_eq!(
            machine.applied_state().await?,
            (None, StoredMembership::default())
        );
        Ok(())
    }
    .await;
    fixture::finish(machine, result).await
}

pub(super) async fn independent_selection_fields_are_not_inferred_from_actual_pair<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control, target) = fixture::target(writer).await?;
    let result = async {
        let original = control.reader().snapshot()?;
        for case in 0..3 {
            let source = fixture::source(false)?;
            let mut selected = source.checkpoint;
            let mut stream = captured::stream()?;
            let mut digest = source.digest;
            match case {
                0 => stream = domain::CommittedStreamId::new([8; 16])?,
                1 => {
                    selected = captured::altered_checkpoint(&captured::selected(false)?, |wire| {
                        wire.highest_timestamp += 1
                    })?
                    .checkpoint
                }
                _ => digest[0] ^= 1,
            }
            let request = OwnedTrustedNativeReplacement::new(
                stream,
                target.clone(),
                selected,
                digest,
                source.carrier,
            )?;
            let error = machine
                .replace_create_send_image_with_catalog(request)
                .await
                .err()
                .ok_or("selection unexpectedly accepted")?;
            assert!(matches!(
                error,
                StateMachineImageReplacementError::Domain(
                    CommittedImageReplacementError::InvalidSelection
                        | CommittedImageReplacementError::SelectionMismatch
                )
            ));
            fixture::assert_counts(&control, 0, 0);
            assert_eq!(control.reader().snapshot()?, original);
        }
        assert_eq!(machine.checkpoint().await?, target);
        Ok(())
    }
    .await;
    fixture::finish(machine, result).await
}

pub(super) async fn same_full_checkpoint_different_valid_body_still_requires_exact_trusted_digest<
    W,
>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control, target) = fixture::target(writer).await?;
    let result = async {
        let original_target = control.reader().snapshot()?;
        let old = catalog_fixture::source(&control)?;
        let old_metadata = EncodedNativeSnapshotMetadata::encode(old.image.as_bytes())?;
        control.retain(old_metadata.as_bytes(), old.image.as_bytes())?;
        control.reset();
        let original = captured::selected(false)?;
        let trusted_digest = captured::digest(original.image.as_bytes());
        let key = domain::keys::message(
            &captured::namespace()?,
            &captured::entity()?,
            domain::SequenceNumber::new(1),
        );
        let value = original
            .snapshot
            .entries()
            .iter()
            .find(|(candidate, _)| candidate == &key)
            .ok_or("seq1 message absent")?
            .1
            .as_slice();
        let mut record = domain::MessageRecord::decode(value)?;
        *record.body.first_mut().ok_or("seq1 body empty")? ^= 1;
        let altered = captured::with_record(&original, &key, domain::codec::encode(&record)?)?;
        captured::supported(&altered)?;
        assert_eq!(altered.checkpoint, original.checkpoint);
        assert_ne!(captured::digest(altered.image.as_bytes()), trusted_digest);
        let source = fixture::from_selected(altered)?;
        // Actual native metadata is fully correct for the changed body; the
        // independently trusted original digest must nevertheless refuse it.
        let request = OwnedTrustedNativeReplacement::new(
            captured::stream()?,
            target.clone(),
            original.checkpoint,
            trusted_digest,
            source.carrier,
        )?;
        assert_eq!(
            machine
                .replace_create_send_image_with_catalog(request)
                .await
                .err(),
            Some(StateMachineImageReplacementError::Domain(
                CommittedImageReplacementError::SelectionMismatch
            ))
        );
        fixture::assert_counts(&control, 0, 0);
        assert_eq!(control.reader().snapshot()?, original_target);
        let retained = control
            .catalog_reader()
            .read_catalog()?
            .ok_or("old catalog absent")?;
        assert_eq!(retained.artifact(), old.image.as_bytes());
        assert_eq!(retained.metadata(), old_metadata.as_bytes());
        assert_eq!(machine.checkpoint().await?, target);
        Ok(())
    }
    .await;
    fixture::finish(machine, result).await
}

pub(super) async fn standalone_builder_carrier_is_consumed_without_body_copy<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut source_owner, control) = catalog_fixture::seeded(writer, true).await?;
    let setup = async {
        let source = catalog_fixture::source(&control)?;
        let snapshot = source_owner
            .create_send_snapshot_builder()
            .build_snapshot()
            .await?;
        let pointer = snapshot.snapshot.as_bytes().as_ptr() as usize;
        assert!(snapshot.snapshot.as_bytes().len() > domain::MAX_COMMITTED_BODY_BYTES);
        assert_eq!(control.committed_pointers()[0].1, pointer);
        Ok::<_, Box<dyn Error>>((source, snapshot, pointer))
    }
    .await;
    let joined = source_owner.shutdown().await;
    let (source, snapshot, pointer) = setup?;
    joined?;
    let mut machine = ExperimentalStateMachine::open_with_snapshot_replacement(
        control.writer(),
        captured::stream()?,
    )?;
    control.reset();
    let result = async {
        let request = OwnedTrustedNativeReplacement::new(
            captured::stream()?,
            source.checkpoint.clone(),
            source.checkpoint.clone(),
            captured::digest(source.image.as_bytes()),
            snapshot,
        )?;
        machine
            .replace_create_send_image_with_catalog(request)
            .await?;
        fixture::assert_counts(&control, 1, 1);
        assert_eq!(control.committed_pointers()[0].1, pointer);
        fixture::exact_target(&control, &source.snapshot, &source.checkpoint)?;
        Ok(())
    }
    .await;
    fixture::finish(machine, result).await
}

pub(super) async fn every_actual_native_meta_field_must_match_before_target_capture<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control, target) = fixture::target(writer).await?;
    let result = async {
        let original = control.reader().snapshot()?;
        for case in 0..8 {
            let mut source = fixture::source(false)?;
            match case {
                0 => {
                    source
                        .carrier
                        .meta
                        .last_log_id
                        .as_mut()
                        .ok_or("last absent")?
                        .leader_id
                        .term += 1
                }
                1 => {
                    source
                        .carrier
                        .meta
                        .last_log_id
                        .as_mut()
                        .ok_or("last absent")?
                        .leader_id
                        .node_id += 1
                }
                2 => {
                    source
                        .carrier
                        .meta
                        .last_log_id
                        .as_mut()
                        .ok_or("last absent")?
                        .index += 1
                }
                3 => {
                    source.carrier.meta.last_membership = StoredMembership::new(
                        Some(crate::LogId::new(CommittedLeaderId::new(1, 7), 1)),
                        captured::members(),
                    )
                }
                4 => {
                    source.carrier.meta.last_membership = StoredMembership::new(
                        *source.carrier.meta.last_membership.log_id(),
                        Membership::new(
                            vec![BTreeSet::from([7, 8, 9])],
                            BTreeMap::from([
                                (7, BasicNode::new("different")),
                                (8, BasicNode::new("node-8")),
                                (9, BasicNode::new("node-9")),
                            ]),
                        ),
                    )
                }
                5 => {
                    source.carrier.meta.last_membership = StoredMembership::new(
                        *source.carrier.meta.last_membership.log_id(),
                        Membership::new(
                            vec![BTreeSet::from([7, 8])],
                            BTreeMap::from([
                                (7, BasicNode::new("node-7")),
                                (8, BasicNode::new("node-8")),
                                (9, BasicNode::new("node-9")),
                            ]),
                        ),
                    )
                }
                6 => source.carrier.meta.snapshot_id = "PRIVATE-wrong-id".into(),
                _ => source.carrier.meta.last_membership = StoredMembership::default(),
            }
            let request = fixture::request(target.clone(), source)?;
            assert_eq!(
                machine
                    .replace_create_send_image_with_catalog(request)
                    .await
                    .err(),
                Some(StateMachineImageReplacementError::Metadata(
                    NativeSnapshotMetadataError::ImageMismatch
                ))
            );
            fixture::assert_counts(&control, 0, 0);
            assert_eq!(control.reader().snapshot()?, original);
        }
        assert_eq!(machine.checkpoint().await?, target);
        Ok(())
    }
    .await;
    fixture::finish(machine, result).await
}

pub(super) async fn invalid_and_native_incompatible_sources_refuse_before_target_io<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control, target) = fixture::target(writer).await?;
    let result = async {
        let original = control.reader().snapshot()?;
        for case in 0..4 {
            let mut source = fixture::source(false)?;
            match case {
                0 => {
                    source.carrier.snapshot = Box::new(crate::BoundedSnapshotData::from_bytes(
                        b"PRIVATE-malformed",
                    )?)
                }
                1 => {
                    let selected = captured::selected(false)?;
                    let altered = captured::altered_checkpoint(&selected, |wire| {
                        wire.membership.as_mut().expect("membership").schema_version += 1
                    })?;
                    source.carrier.snapshot =
                        Box::new(crate::BoundedSnapshotData::from_image(altered.image)?);
                }
                2 => {
                    let selected = captured::selected(false)?;
                    let altered =
                        captured::with_record(&selected, &[0xff], b"PRIVATE-invalid-row".to_vec())?;
                    source.carrier.snapshot =
                        Box::new(crate::BoundedSnapshotData::from_image(altered.image)?);
                }
                _ => {
                    let selected = captured::selected(false)?;
                    let altered = captured::altered_checkpoint(&selected, |wire| {
                        wire.previous.as_mut().expect("previous").id.node_id = 8
                    })?;
                    source.carrier.snapshot =
                        Box::new(crate::BoundedSnapshotData::from_image(altered.image)?);
                }
            }
            let request = fixture::request(target.clone(), source)?;
            assert!(matches!(
                machine
                    .replace_create_send_image_with_catalog(request)
                    .await
                    .err(),
                Some(StateMachineImageReplacementError::Metadata(_))
            ));
            fixture::assert_counts(&control, 0, 0);
            assert_eq!(control.reader().snapshot()?, original);
        }
        assert_eq!(machine.checkpoint().await?, target);
        Ok(())
    }
    .await;
    fixture::finish(machine, result).await
}

pub(super) async fn old_checkpoint_mismatch_and_target_limits_are_nonfatal<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control, target) = fixture::target(writer).await?;
    let result = async {
        let original = control.reader().snapshot()?;
        let old = catalog_fixture::source(&control)?;
        let wrong_old =
            captured::altered_checkpoint(&old, |wire| wire.highest_timestamp += 1)?.checkpoint;
        assert_eq!(wrong_old.last(), target.last());
        let request = fixture::request(wrong_old, fixture::source(false)?)?;
        assert_eq!(
            machine
                .replace_create_send_image_with_catalog(request)
                .await
                .err(),
            Some(StateMachineImageReplacementError::Domain(
                CommittedImageReplacementError::TargetMismatch
            ))
        );
        fixture::assert_counts(&control, 1, 0);
        control.reset();
        control.fault(Fault::CaptureLimit);
        let request = fixture::request(target.clone(), fixture::source(false)?)?;
        assert_eq!(
            machine
                .replace_create_send_image_with_catalog(request)
                .await
                .err(),
            Some(StateMachineImageReplacementError::Domain(
                CommittedImageReplacementError::LimitExceeded
            ))
        );
        fixture::assert_counts(&control, 1, 0);
        assert_eq!(control.reader().snapshot()?, original);
        assert_eq!(machine.checkpoint().await?, target);
        Ok(())
    }
    .await;
    fixture::finish(machine, result).await
}

pub(super) async fn physical_capture_and_all_commit_errors_poison_without_later_io<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control, _) = fixture::target(writer).await?;
    for fault in [
        Fault::CapturePhysical,
        Fault::CommitBefore,
        Fault::CommitLimit,
        Fault::CommitAfter,
    ] {
        let result = async {
            let target = catalog_fixture::source(&control)?.checkpoint;
            control.reset();
            control.fault(fault);
            let request = fixture::request(target, fixture::from_selected(captured::initial()?)?)?;
            let expected = if matches!(fault, Fault::CapturePhysical) {
                CommittedImageReplacementError::TargetReadFailed
            } else {
                CommittedImageReplacementError::CommitUnknown
            };
            assert_eq!(
                machine
                    .replace_create_send_image_with_catalog(request)
                    .await
                    .err(),
                Some(StateMachineImageReplacementError::Domain(expected))
            );
            fixture::assert_counts(
                &control,
                1,
                usize::from(!matches!(fault, Fault::CapturePhysical)),
            );
            let before = control.counts();
            let mut overlong = fixture::source(false)?;
            overlong.carrier.meta.snapshot_id = "x".repeat(80);
            assert_eq!(
                OwnedTrustedNativeReplacement::new(
                    captured::stream()?,
                    overlong.checkpoint.clone(),
                    overlong.checkpoint,
                    overlong.digest,
                    overlong.carrier
                )
                .err(),
                Some(NativeSnapshotMetadataError::LimitExceeded)
            );
            let mut source = fixture::source(false)?;
            source.carrier.snapshot =
                Box::new(crate::BoundedSnapshotData::from_bytes(b"PRIVATE-invalid")?);
            let request = fixture::request(source.checkpoint.clone(), source)?;
            assert_eq!(
                machine
                    .replace_create_send_image_with_catalog(request)
                    .await
                    .err(),
                Some(StateMachineImageReplacementError::Owner(
                    StateMachineError::Poisoned
                ))
            );
            assert_eq!(
                machine.checkpoint().await.err(),
                Some(StateMachineError::Poisoned)
            );
            assert_eq!(control.counts(), before);
            Ok(())
        }
        .await;
        fixture::finish(machine, result).await?;
        machine = ExperimentalStateMachine::open_with_snapshot_replacement(
            control.writer(),
            captured::stream()?,
        )?;
    }
    machine.shutdown().await?;
    Ok(())
}

pub(super) async fn all_existing_constructors_keep_replacement_disabled<W>(writer: W) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (machine, control, target) = fixture::target(writer).await?;
    machine.shutdown().await?;
    for case in 0..3 {
        let mut machine = match case {
            0 => ExperimentalStateMachine::open(control.writer(), captured::stream()?)?,
            1 => ExperimentalStateMachine::open_with_image_export(
                control.writer(),
                captured::stream()?,
            )?,
            _ => ExperimentalStateMachine::open_with_snapshot_catalog(
                control.writer(),
                captured::stream()?,
            )?,
        };
        control.reset();
        let result = async {
            let request = fixture::request(target.clone(), fixture::source(false)?)?;
            assert_eq!(
                machine
                    .replace_create_send_image_with_catalog(request)
                    .await
                    .err(),
                Some(StateMachineImageReplacementError::Disabled)
            );
            fixture::assert_counts(&control, 0, 0);
            Ok(())
        }
        .await;
        fixture::finish(machine, result).await?;
    }
    Ok(())
}

pub(super) async fn sealed_catalog_owner_refuses_replacement_before_disabled_without_io<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    use crate::experimental_local_compaction::frontier::{Frontier, PairIdentity};
    use crate::experimental_state_machine::owner::{Operation, Reply};

    fn require(condition: bool, message: &'static str) -> TestResult {
        if condition {
            Ok(())
        } else {
            Err(message.into())
        }
    }

    let (machine, control) = catalog_fixture::seeded(writer, false).await?;
    let result: TestResult = async {
        let target = machine.checkpoint().await?;
        let retained = machine.handle.clone();
        let request = fixture::request(target.clone(), fixture::source(false)?)?;
        require(
            matches!(
                retained
                    .request(Operation::ReplaceCatalog(Box::new(request)))
                    .await?,
                Reply::CatalogReplaced(Err(StateMachineImageReplacementError::Disabled))
            ),
            "unsealed catalog owner did not reach its disabled replacement capability",
        )?;
        let identity = PairIdentity::new();
        let (frontier, publisher) = Frontier::new(identity.clone());
        let sealed = retained
            .request(Operation::SealLocal {
                identity,
                publisher,
            })
            .await?;
        require(
            matches!(sealed, Reply::LocalSealed(ref selected) if selected.as_ref() == &target),
            "catalog owner did not seal at its exact checkpoint",
        )?;
        control.reset();
        let request = fixture::request(target.clone(), fixture::source(false)?)?;
        require(
            retained
                .request(Operation::ReplaceCatalog(Box::new(request)))
                .await
                .err()
                == Some(StateMachineError::Closed),
            "sealed catalog owner dispatched the new generic replacement variant",
        )?;
        require(
            control.counts() == observed::Counts::default(),
            "sealed replacement performed backend work",
        )?;
        let workload = retained.workload()?;
        require(
            workload.accepted_jobs == 0 && workload.encoded_bytes == 0,
            "sealed replacement retained admission capacity",
        )?;
        require(
            machine.checkpoint().await? == target,
            "sealed replacement changed source progress",
        )?;
        require(
            frontier.begin()? == 1,
            "sealed replacement closed the compaction frontier",
        )?;
        Ok(())
    }
    .await;
    fixture::finish(machine, result).await
}

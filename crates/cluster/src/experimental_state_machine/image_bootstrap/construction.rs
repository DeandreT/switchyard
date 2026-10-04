use domain::{
    CommittedImageBootstrapError as DomainError, CommittedStreamId, DecodedCommittedImage,
    SequenceNumber, Timestamp, TrustedCreateSendBootstrap,
};
use openraft::storage::RaftStateMachine;
use storage::{Mutation, StateStore, WriteBatch};

use super::{
    fixture::*,
    observed::{Counts, Fault, bootstrap_counts, observed},
    *,
};

pub(super) async fn finish(machine: ExperimentalStateMachine, result: TestResult) -> TestResult {
    let joined = machine.shutdown().await;
    result?;
    joined?;
    Ok(())
}

pub(super) async fn plain_bootstrap_performs_no_postcommit_read_and_keeps_export_disabled<W>(
    writer: W,
) -> TestResult
where
    W: CommittedStore,
{
    let source = selected(false)?;
    let (writer, control) = observed(writer);
    let mut machine =
        ExperimentalStateMachine::bootstrap_create_send_image(writer, source.request())?;
    let result = async {
        assert_eq!(control.counts(), bootstrap_counts());
        assert_eq!(control.drops(), (0, 0));
        assert_eq!(control.reader().snapshot()?, source.snapshot);
        let batches = control.batches();
        assert_eq!(batches.len(), 1);
        let puts = batches[0]
            .mutations()
            .iter()
            .map(|mutation| match mutation {
                Mutation::Put { key, value } => Ok((key.clone(), value.clone())),
                Mutation::Delete { .. } => Err("native bootstrap attempted a delete"),
            })
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(puts, source.snapshot.entries());
        control.reset();
        assert_eq!(
            machine.export_create_send_image().await.err(),
            Some(super::super::super::StateMachineImageExportError::Disabled)
        );
        assert_eq!(control.counts(), Counts::default());
        let applied = machine.applied_state().await?;
        assert_eq!(
            applied.0,
            Some(crate::LogId::new(openraft::CommittedLeaderId::new(1, 7), 4))
        );
        assert_eq!(applied.1.membership(), &members());
        assert_eq!(
            machine.apply([next_send()?]).await?,
            vec![crate::LogApplication::Sent { sequence: 3 }]
        );
        let state = domain::StateMachine::new(control.reader());
        let row = state
            .message(&namespace()?, &entity()?, SequenceNumber::new(3))?
            .ok_or("missing resumed sequence")?;
        assert_eq!(row.body, b"PRIVATE-next-body");
        assert!(
            state
                .message(&namespace()?, &entity()?, SequenceNumber::new(2))?
                .is_none()
        );
        assert_eq!(machine.workload()?.accepted_jobs, 0);
        Ok(())
    }
    .await;
    finish(machine, result).await?;
    assert_eq!(control.drops(), (1, 1));
    Ok(())
}

pub(super) async fn explicit_bootstrap_exports_exact_captured_bytes<W>(writer: W) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let source = selected(true)?;
    let (writer, control) = observed(writer);
    let mut machine = ExperimentalStateMachine::bootstrap_create_send_image_with_export(
        writer,
        source.request(),
    )?;
    let result = async {
        assert_eq!(control.counts(), bootstrap_counts());
        assert_eq!(control.reader().snapshot()?, source.snapshot);
        control.reset();
        let image = machine.export_create_send_image().await?;
        assert_eq!(image.as_bytes(), source.image.as_bytes());
        assert_eq!(
            DecodedCommittedImage::decode(image.as_bytes())?.checkpoint(),
            &source.checkpoint
        );
        assert_eq!(
            control.counts(),
            Counts {
                bounded: 1,
                ..Counts::default()
            }
        );
        assert_eq!(machine.workload()?.accepted_jobs, 0);
        assert_eq!(machine.workload()?.encoded_bytes, 0);
        Ok(())
    }
    .await;
    finish(machine, result).await?;
    assert_eq!(control.drops(), (1, 1));
    Ok(())
}

pub(super) async fn native_incompatible_full_identity_and_membership_never_touch_target<W>(
    writer: W,
) -> TestResult
where
    W: CommittedStore,
    W::Reader: BoundedStateStore,
{
    let source = selected(false)?;
    let variants = vec![
        altered_checkpoint(&source, |wire| {
            wire.previous.as_mut().expect("previous mark").id.node_id = 9;
        })?,
        altered_checkpoint(&source, |wire| {
            wire.last.as_mut().expect("last mark").id.node_id = 6;
        })?,
        altered_checkpoint(&source, |wire| {
            wire.membership.as_mut().expect("membership").source.node_id = 9;
        })?,
        altered_checkpoint(&source, |wire| {
            wire.membership.as_mut().expect("membership").schema_version = 2;
        })?,
        altered_checkpoint(&source, |wire| {
            wire.membership.as_mut().expect("membership").payload =
                b"PRIVATE-malformed-member".to_vec();
        })?,
        altered_checkpoint(&source, |wire| {
            wire.membership
                .as_mut()
                .expect("membership")
                .payload
                .push(0);
        })?,
    ];
    let (writer, control) = observed(writer);
    drop(writer);
    let before = control.reader().snapshot()?;
    for incompatible in variants {
        supported(&incompatible)?;
        assert!(super::super::recover(&incompatible.checkpoint).is_err());
        for enabled in [false, true] {
            control.reset();
            let result = if enabled {
                ExperimentalStateMachine::bootstrap_create_send_image_with_export(
                    control.writer(),
                    incompatible.request(),
                )
            } else {
                ExperimentalStateMachine::bootstrap_create_send_image(
                    control.writer(),
                    incompatible.request(),
                )
            };
            assert_eq!(
                result.err(),
                Some(StateMachineImageBootstrapError::IncompatibleMetadata)
            );
            assert_eq!(control.counts(), Counts::default());
            assert_eq!(control.reader().snapshot()?, before);
            assert!(!control.initialized()?);
        }
    }
    Ok(())
}

pub(super) async fn native_compatible_source_refusals_never_touch_target<W>(writer: W) -> TestResult
where
    W: CommittedStore,
{
    let source = selected(false)?;
    let changed = altered_checkpoint(&source, |wire| {
        wire.highest_timestamp += 1;
    })?;
    assert!(super::super::recover(&changed.checkpoint).is_ok());
    let unknown = with_record(&source, &[0x7f, 0x01], b"PRIVATE-unknown".to_vec())?;
    let mut bad_checksum = source.image.as_bytes().to_vec();
    *bad_checksum.last_mut().ok_or("empty image")? ^= 1;
    let invalid: CommittedStreamId = postcard::from_bytes(&postcard::to_stdvec(&[0u8; 16])?)?;
    let requests = [
        (
            TrustedCreateSendBootstrap::new(
                stream()?,
                &source.checkpoint,
                [0; 32],
                source.image.as_bytes(),
            ),
            DomainError::SelectionMismatch,
        ),
        (
            TrustedCreateSendBootstrap::new(
                stream()?,
                &changed.checkpoint,
                digest(source.image.as_bytes()),
                source.image.as_bytes(),
            ),
            DomainError::SelectionMismatch,
        ),
        (
            TrustedCreateSendBootstrap::new(
                invalid,
                &source.checkpoint,
                digest(source.image.as_bytes()),
                source.image.as_bytes(),
            ),
            DomainError::InvalidSelection,
        ),
        (unknown.request(), DomainError::UnsupportedProfile),
        (
            TrustedCreateSendBootstrap::new(
                stream()?,
                &source.checkpoint,
                digest(&bad_checksum),
                &bad_checksum,
            ),
            DomainError::InvalidImage,
        ),
    ];
    let (writer, control) = observed(writer);
    drop(writer);
    let before = control.reader().snapshot()?;
    for (request, error) in requests {
        control.reset();
        assert_eq!(
            ExperimentalStateMachine::bootstrap_create_send_image(control.writer(), request).err(),
            Some(StateMachineImageBootstrapError::Domain(error))
        );
        assert_eq!(control.counts(), Counts::default());
        assert_eq!(control.reader().snapshot()?, before);
        assert!(!control.initialized()?);
    }
    Ok(())
}

pub(super) async fn initialized_and_orphan_targets_are_never_replaced<W>(writer: W) -> TestResult
where
    W: CommittedStore,
{
    let source = selected(false)?;
    let (writer, control) = observed(writer);
    drop(writer);
    control.inject(WriteBatch::default())?;
    let empty = control.reader().snapshot()?;
    assert!(empty.entries().is_empty());
    for orphan in [false, true] {
        if orphan {
            control.inject(
                WriteBatch::default().put(b"PRIVATE-orphan".to_vec(), b"PRIVATE-target".to_vec()),
            )?;
        }
        let before = control.reader().snapshot()?;
        control.reset();
        assert_eq!(
            ExperimentalStateMachine::bootstrap_create_send_image(
                control.writer(),
                source.request()
            )
            .err(),
            Some(StateMachineImageBootstrapError::Domain(
                DomainError::TargetNotPristine
            ))
        );
        assert_eq!(
            control.counts(),
            Counts {
                reader_factories: 1,
                initialized: 1,
                ..Counts::default()
            }
        );
        assert_eq!(control.reader().snapshot()?, before);
    }
    control.force_uninitialized();
    let before = control.reader().snapshot()?;
    control.reset();
    assert_eq!(
        ExperimentalStateMachine::bootstrap_create_send_image(control.writer(), source.request())
            .err(),
        Some(StateMachineImageBootstrapError::Domain(
            DomainError::TargetNotPristine
        ))
    );
    assert_eq!(
        control.counts(),
        Counts {
            reader_factories: 1,
            initialized: 1,
            scans: vec![(Vec::new(), Vec::new(), 1)],
            ..Counts::default()
        }
    );
    assert_eq!(control.reader().snapshot()?, before);
    Ok(())
}

pub(super) async fn target_read_failures_are_static_and_precommit<W>(writer: W) -> TestResult
where
    W: CommittedStore,
{
    let source = selected(false)?;
    let (writer, control) = observed(writer);
    drop(writer);
    let before = control.reader().snapshot()?;
    for fault in [Fault::Initialized, Fault::Scan] {
        control.reset();
        control.fault(fault);
        let error = ExperimentalStateMachine::bootstrap_create_send_image(
            control.writer(),
            source.request(),
        )
        .err()
        .ok_or("target fault was accepted")?;
        assert_eq!(
            error,
            StateMachineImageBootstrapError::Domain(DomainError::TargetReadFailed)
        );
        let scans = if matches!(fault, Fault::Scan) {
            vec![(Vec::new(), Vec::new(), 1)]
        } else {
            Vec::new()
        };
        assert_eq!(
            control.counts(),
            Counts {
                reader_factories: 1,
                initialized: 1,
                scans,
                ..Counts::default()
            }
        );
        assert!(!format!("{error:?}: {error}").contains("PRIVATE"));
        assert_eq!(control.reader().snapshot()?, before);
        assert!(!control.initialized()?);
    }
    Ok(())
}

pub(super) async fn physical_commit_errors_remain_unknown<W>(writer: W) -> TestResult
where
    W: CommittedStore,
{
    let source = selected(false)?;
    let (writer, control) = observed(writer);
    drop(writer);
    let before = control.reader().snapshot()?;
    for fault in [Fault::CommitBefore, Fault::CommitAfter] {
        control.reset();
        control.fault(fault);
        let starts = std::sync::atomic::AtomicUsize::new(0);
        let error =
            super::super::bootstrap_with_starter(control.writer(), source.request(), |state| {
                starts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                drop(state);
                Err(StateMachineError::ThreadStart)
            })
            .err()
            .ok_or("physical failure returned a facade")?;
        assert_eq!(
            error,
            StateMachineImageBootstrapError::Domain(DomainError::CommitUnknown)
        );
        assert_eq!(starts.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(control.counts(), bootstrap_counts());
        assert!(!format!("{error:?}: {error}").contains("PRIVATE"));
        if matches!(fault, Fault::CommitBefore) {
            assert!(!control.initialized()?);
            assert_eq!(control.reader().snapshot()?, before);
        } else {
            assert!(control.initialized()?);
            assert_eq!(control.reader().snapshot()?, source.snapshot);
        }
    }
    // No retry follows the unknown after-commit result. A separate explicit
    // open uses the observed complete checkpoint and performs no bootstrap.
    let mut machine = ExperimentalStateMachine::open(control.writer(), stream()?)?;
    let result = async {
        assert_eq!(machine.applied_state().await?.0.map(|id| id.index), Some(4));
        Ok(())
    }
    .await;
    finish(machine, result).await
}

pub(super) async fn simulated_startup_refusal_preserves_the_known_commit<W>(writer: W) -> TestResult
where
    W: CommittedStore,
{
    let source = selected(false)?;
    let (writer, control) = observed(writer);
    let starts = std::sync::atomic::AtomicUsize::new(0);
    let result = super::super::bootstrap_with_starter(writer, source.request(), |state| {
        starts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(control.counts(), bootstrap_counts());
        assert!(state.image_export.is_none());
        drop(state);
        Err(StateMachineError::ThreadStart)
    });
    assert_eq!(
        result.err(),
        Some(StateMachineImageBootstrapError::OwnerStartAfterCommit)
    );
    assert_eq!(starts.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(control.counts(), bootstrap_counts());
    assert_eq!(control.drops(), (1, 1));
    assert!(control.initialized()?);
    assert_eq!(control.reader().snapshot()?, source.snapshot);
    let mut machine = ExperimentalStateMachine::open(control.writer(), stream()?)?;
    let result = async {
        assert_eq!(machine.applied_state().await?.0.map(|id| id.index), Some(4));
        Ok(())
    }
    .await;
    finish(machine, result).await
}

pub(super) async fn initial_checkpoint_bootstrap_is_native_compatible<W>(writer: W) -> TestResult
where
    W: CommittedStore,
{
    let source = initial()?;
    let (writer, control) = observed(writer);
    let mut machine =
        ExperimentalStateMachine::bootstrap_create_send_image(writer, source.request())?;
    let result = async {
        assert_eq!(control.counts(), bootstrap_counts());
        assert_eq!(
            machine.applied_state().await?,
            (None, openraft::StoredMembership::default())
        );
        assert_eq!(control.reader().snapshot()?, source.snapshot);
        assert_eq!(source.checkpoint.highest_timestamp(), Timestamp::UNIX_EPOCH);
        Ok(())
    }
    .await;
    finish(machine, result).await
}

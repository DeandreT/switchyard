use storage::{FjallCatalogReplicaStore, SnapshotCatalogReader};

use super::*;

const DIRECTORY: &str = "SWITCHYARD_REPLACEMENT_TEST_DIRECTORY";
const STAGE: &str = "SWITCHYARD_REPLACEMENT_EXIT_STAGE";
const CHILD_TEST: &str = "catalog::replacement::crash::child_process_exit_boundary";

#[test]
fn child_process_exit_boundary() -> TestResult {
    let Some(directory) = std::env::var_os(DIRECTORY) else {
        return Ok(());
    };
    let fault = match std::env::var(STAGE)?.as_str() {
        "before" => Fault::ExitBefore,
        "after" => Fault::ExitAfter,
        _ => return Err("unknown replacement child stage".into()),
    };
    let selected = source(Kind::Populated)?;
    let (writer, control) = observed(FjallCatalogReplicaStore::open(directory)?);
    let mut machine = CommittedStateMachine::open(writer, stream()?)?;
    let old = machine.checkpoint()?;
    control.fault(fault);
    machine.replace_create_send_image_with_catalog(
        request(&selected, &old),
        b"private-child-selected-pair",
    )?;
    Err("the replacement process exit did not occur".into())
}

#[test]
fn abrupt_exit_before_commit_recovers_the_complete_old_business_and_catalog() -> TestResult {
    recover("before", 71, false)
}

#[test]
fn abrupt_exit_after_completed_sync_recovers_only_the_complete_selected_pair() -> TestResult {
    recover("after", 72, true)
}

fn recover(stage: &str, code: i32, committed: bool) -> TestResult {
    let selected = source(Kind::Populated)?;
    let directory = testkit::DurableProvider::temporary()?;
    let (old_rows, old_checkpoint, old_image, old_pair) = {
        let writer = FjallCatalogReplicaStore::open(directory.path())?;
        let (mut machine, control) = target(writer)?;
        let rows = control.reader().snapshot()?;
        let checkpoint = machine.checkpoint()?;
        let image = machine.export_create_send_image()?;
        let pair = control.catalog()?.ok_or("missing before-child catalog")?;
        drop(machine);
        drop(control);
        (rows, checkpoint, image, pair)
    };
    let (status, output) = crate::bootstrap::crash::run_child_for_test(
        directory.path(),
        stage,
        CHILD_TEST,
        DIRECTORY,
        STAGE,
    )?;
    assert_eq!(status.code(), Some(code), "replacement child: {output}");
    let (expected_rows, expected_checkpoint, expected_artifact, expected_metadata) = if committed {
        (
            &selected.rows,
            &selected.checkpoint,
            selected.image.as_bytes(),
            b"private-child-selected-pair".as_slice(),
        )
    } else {
        (
            &old_rows,
            &old_checkpoint,
            old_pair.artifact(),
            old_pair.metadata(),
        )
    };
    let retained = {
        let writer = FjallCatalogReplicaStore::open(directory.path())?;
        assert!(writer.is_initialized()?);
        assert_eq!(writer.reader().snapshot()?, *expected_rows);
        let pair = writer
            .catalog_reader()
            .read_catalog()?
            .ok_or("missing recovered low-level pair")?;
        assert_eq!(pair.artifact(), expected_artifact);
        assert_eq!(pair.metadata(), expected_metadata);
        let mut machine = CommittedStateMachine::open(writer, stream()?)?;
        assert_eq!(machine.checkpoint()?, *expected_checkpoint);
        let retained = machine
            .read_create_send_catalog()?
            .ok_or("missing recovered validated pair")?;
        assert_eq!(retained.image_bytes(), expected_artifact);
        assert_eq!(retained.metadata(), expected_metadata);
        assert_eq!(retained.checkpoint(), expected_checkpoint);
        retained
    };
    // The runner has reaped the child and joined both bounded output readers.
    // All acquisition handles above dropped; only immutable owned evidence lives.
    let writer = FjallCatalogReplicaStore::open(directory.path())?;
    let mut reopened = CommittedStateMachine::open(writer, stream()?)?;
    assert_eq!(reopened.reader().snapshot()?, *expected_rows);
    assert_eq!(reopened.checkpoint()?, *expected_checkpoint);
    let pair = reopened
        .read_create_send_catalog()?
        .ok_or("missing pair on second recovery open")?;
    assert_eq!(pair.image_bytes(), retained.image_bytes());
    assert_eq!(pair.metadata(), retained.metadata());
    assert_eq!(old_image.as_bytes(), old_pair.artifact());
    assert_eq!(
        selected.image.as_bytes(),
        source(Kind::Populated)?.image.as_bytes()
    );
    Ok(())
}

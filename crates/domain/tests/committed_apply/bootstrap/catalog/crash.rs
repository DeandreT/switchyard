use storage::{FjallCatalogReplicaStore, SnapshotCatalogReader};

use super::*;

const DIRECTORY: &str = "SWITCHYARD_CATALOG_BOOTSTRAP_TEST_DIRECTORY";
const STAGE: &str = "SWITCHYARD_CATALOG_BOOTSTRAP_EXIT_STAGE";
const CHILD_TEST: &str = "bootstrap::catalog::crash::child_process_exit_boundary";
const METADATA: &[u8] = b"opaque combined catalog crash metadata\x00\xff";

#[test]
fn child_process_exit_boundary() -> TestResult {
    let Some(directory) = std::env::var_os(DIRECTORY) else {
        return Ok(());
    };
    let fault = match std::env::var(STAGE)?.as_str() {
        "before" => Fault::ExitBefore,
        "after" => Fault::ExitAfter,
        _ => return Err("unknown catalog bootstrap child stage".into()),
    };
    let selected = source(false)?;
    let (writer, control) = observed(FjallCatalogReplicaStore::open(directory)?);
    control.fault(fault);
    bootstrap_without_business_bound(writer, &selected, METADATA)?;
    Err("combined bootstrap child exit did not occur".into())
}

fn recover(stage: &str, code: i32, installed: bool) -> TestResult {
    let selected = source(false)?;
    let directory = testkit::DurableProvider::temporary()?;
    {
        let writer = FjallCatalogReplicaStore::open(directory.path())?;
        assert!(!writer.is_initialized()?);
        assert!(writer.reader().snapshot()?.entries().is_empty());
        assert!(writer.catalog_reader().read_catalog()?.is_none());
    }
    // The shared existing runner bounds time/output, reaps the child and joins
    // both output readers before returning. The legacy child identity is unchanged.
    let (status, output) = super::super::crash::run_child_for_test(
        directory.path(),
        stage,
        CHILD_TEST,
        DIRECTORY,
        STAGE,
    )?;
    assert_eq!(
        status.code(),
        Some(code),
        "combined bootstrap child: {output}"
    );
    {
        let writer = FjallCatalogReplicaStore::open(directory.path())?;
        assert_eq!(writer.is_initialized()?, installed);
        if installed {
            assert_eq!(writer.reader().snapshot()?, selected.snapshot);
            let catalog = writer
                .catalog_reader()
                .read_catalog()?
                .ok_or("crash recovery lost complete catalog")?;
            assert_eq!(catalog.metadata(), METADATA);
            assert_eq!(catalog.artifact(), selected.image.as_bytes());
            assert_eq!(
                bootstrap_without_business_bound(writer, &selected, b"must not replace").err(),
                Some(CommittedImageBootstrapError::TargetNotPristine)
            );
        } else {
            assert!(writer.reader().snapshot()?.entries().is_empty());
            assert!(writer.catalog_reader().read_catalog()?.is_none());
            assert_eq!(
                CommittedStateMachine::open(writer, stream()?).err(),
                Some(domain::CommittedApplyError::NotInitialized)
            );
        }
    }
    // Every process/backend handle above has gone away before this directory
    // opens again. Only inspected pristine state permits a new selected attempt.
    let writer = FjallCatalogReplicaStore::open(directory.path())?;
    let mut machine = if installed {
        CommittedStateMachine::open(writer, stream()?)?
    } else {
        bootstrap_without_business_bound(writer, &selected, METADATA)?
    };
    assert_eq!(machine.checkpoint()?, selected.checkpoint);
    assert_eq!(machine.reader().snapshot()?, selected.snapshot);
    let retained = machine
        .read_create_send_catalog()?
        .ok_or("missing domain pair after child recovery")?;
    assert_eq!(retained.metadata(), METADATA);
    assert_eq!(retained.image_bytes(), selected.image.as_bytes());
    assert_eq!(retained.checkpoint(), &selected.checkpoint);
    drop(machine);
    let writer = FjallCatalogReplicaStore::open(directory.path())?;
    let mut machine = CommittedStateMachine::open(writer, stream()?)?;
    assert_eq!(machine.checkpoint()?, selected.checkpoint);
    assert_eq!(
        machine
            .read_create_send_catalog()?
            .ok_or("pair lost on second crash recovery reopen")?
            .image_bytes(),
        retained.image_bytes()
    );
    Ok(())
}

#[test]
fn exit_before_combined_commit_reopens_records_init_and_catalog_exactly_pristine() -> TestResult {
    recover("before", 71, false)
}

#[test]
fn exit_after_combined_sync_commit_reopens_every_row_init_and_both_catalog_pieces() -> TestResult {
    recover("after", 72, true)
}

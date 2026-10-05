use std::{
    future::{Future, poll_fn},
    task::Poll,
    time::Duration,
};

use storage::{
    BoundedStateStore, CatalogCommittedStore, CommittedStore, FjallCatalogReplicaStore,
    SnapshotCatalogReader, StateStore,
};

use super::{
    TestResult, captured,
    fixture::{finish, seeded, source},
    observed::{Counts, GateKind},
    *,
};

fn require(condition: bool, message: &'static str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

async fn probe<F: Future>(mut future: std::pin::Pin<&mut F>) -> Poll<F::Output> {
    poll_fn(|context| Poll::Ready(future.as_mut().poll(context))).await
}

pub(super) async fn lost_build_waiter_keeps_full_custody_until_real_retirement<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control) = seeded(writer, false).await?;
    let before = match source(&control) {
        Ok(before) => before,
        Err(error) => {
            let _ = machine.shutdown().await;
            return Err(error);
        }
    };
    let gate = control.gate(GateKind::Capture);
    let mut builder = machine.create_send_snapshot_builder();
    let result: TestResult = async {
        let mut build = Box::pin(builder.build_snapshot());
        let build_probe = probe(build.as_mut()).await;
        require(
            build_probe.is_pending(),
            "build completed before capture gate",
        )?;
        tokio::time::timeout(Duration::from_secs(5), gate.entered()).await?;
        drop(build);
        require(
            machine.workload()?.accepted_jobs == 1,
            "lost waiter refunded accepted build",
        )?;
        require(
            machine.workload()?.encoded_bytes == domain::MAX_COMMITTED_IMAGE_BYTES,
            "lost waiter refunded full image reservation",
        )?;
        require(
            builder.build_snapshot().await.is_err(),
            "sibling build did not encounter Busy",
        )?;
        require(
            machine.read_create_send_catalog().await.err()
                == Some(StateMachineCatalogError::Owner(StateMachineError::Busy)),
            "sibling read was not Busy",
        )?;
        require(
            control.counts()
                == Counts {
                    bounded: 1,
                    ..Counts::default()
                },
            "Busy performed additional source I/O",
        )?;
        Ok(())
    }
    .await;
    let handle = machine.handle.clone();
    let mut shutdown = Box::pin(machine.shutdown());
    let shutdown_probe = probe(shutdown.as_mut()).await;
    let shutdown_pending = shutdown_probe.is_pending();
    gate.release();
    let joined = match shutdown_probe {
        Poll::Ready(joined) => joined,
        Poll::Pending => shutdown.await,
    };
    result?;
    require(
        shutdown_pending,
        "shutdown finished while actual capture remained gated",
    )?;
    joined?;
    require(
        handle.workload()?.accepted_jobs == 0,
        "joined build kept its job",
    )?;
    require(
        handle.workload()?.encoded_bytes == 0,
        "joined build kept artifact custody",
    )?;
    require(
        control.counts()
            == Counts {
                bounded: 1,
                catalog_commits: 1,
                ..Counts::default()
            },
        "lost waiter prevented sole retention",
    )?;
    let retained = control
        .catalog_reader()
        .read_catalog()?
        .ok_or("lost waiter prevented durable catalog")?;
    require(
        retained.artifact() == before.image.as_bytes(),
        "lost waiter changed captured bytes",
    )?;
    DecodedNativeSnapshotPair::decode(retained.metadata(), retained.artifact())?;
    require(
        control.reader().snapshot()? == before.snapshot,
        "standalone build changed business state",
    )?;
    assert!(builder.build_snapshot().await.is_err());
    Ok(())
}

#[tokio::test]
async fn inert_builder_and_unpolled_build_survive_joined_all_handle_fjall_reopen() -> TestResult {
    let directory = testkit::DurableProvider::temporary()?;
    let (mut machine, control) =
        seeded(FjallCatalogReplicaStore::open(directory.path())?, true).await?;
    let setup = async {
        let before = source(&control)?;
        let mut builder = machine.create_send_snapshot_builder();
        let snapshot = builder.build_snapshot().await?;
        Ok::<_, Box<dyn std::error::Error>>((before, snapshot))
    }
    .await;
    let (before, snapshot) = match setup {
        Ok(values) => values,
        Err(error) => {
            let _ = machine.shutdown().await;
            return Err(error);
        }
    };
    let mut inert = machine.create_send_snapshot_builder();
    let mut unpolled = Box::pin(inert.build_snapshot());
    control.reset();
    let result = (|| {
        assert_eq!(control.counts(), Counts::default());
        assert_eq!(machine.workload()?.accepted_jobs, 0);
        assert_eq!(machine.workload()?.encoded_bytes, 0);
        Ok::<_, Box<dyn std::error::Error>>(())
    })();
    let joined = machine.shutdown().await;
    result?;
    joined?;
    drop(control); // Releases the last store-bearing test wrapper after real join.
    // Even the borrowed trait future carries no facade/backend lifetime.
    let writer = FjallCatalogReplicaStore::open(directory.path())?;
    let reader = writer.reader();
    assert_eq!(reader.snapshot()?, before.snapshot);
    let raw = writer
        .catalog_reader()
        .read_catalog()?
        .ok_or("missing durable pair")?;
    assert_eq!(raw.artifact(), snapshot.snapshot.as_bytes());
    assert_eq!(raw.artifact(), before.image.as_bytes());
    assert_eq!(
        DecodedNativeSnapshotPair::decode(raw.metadata(), raw.artifact())?.snapshot_meta()?,
        snapshot.meta
    );
    let mut machine =
        ExperimentalStateMachine::open_with_snapshot_catalog(writer, captured::stream()?)?;
    let result = async {
        let pair = machine
            .read_create_send_catalog()
            .await?
            .ok_or("missing reopened pair")?;
        assert_eq!(pair.image_bytes(), snapshot.snapshot.as_bytes());
        assert_eq!(pair.snapshot_meta(), &snapshot.meta);
        assert!(unpolled.as_mut().await.is_err());
        Ok(())
    }
    .await;
    finish(machine, result).await?;
    drop(reader);
    let writer = FjallCatalogReplicaStore::open(directory.path())?;
    assert_eq!(writer.reader().snapshot()?, before.snapshot);
    assert_eq!(
        writer
            .catalog_reader()
            .read_catalog()?
            .ok_or("missing final pair")?
            .artifact(),
        snapshot.snapshot.as_bytes()
    );
    Ok(())
}

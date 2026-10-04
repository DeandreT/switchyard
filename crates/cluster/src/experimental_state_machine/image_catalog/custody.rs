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

async fn pending<F: Future>(future: std::pin::Pin<&mut F>) -> bool {
    let mut future = future;
    poll_fn(|context| Poll::Ready(future.as_mut().poll(context)))
        .await
        .is_pending()
}

fn owned<F: Future + Send + 'static>(_: &F) {}

pub(super) async fn lost_capture_waiter_keeps_full_custody_and_delays_real_join<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control) = seeded(writer, false).await?;
    let before = match source(&control) {
        Ok(source) => source,
        Err(error) => {
            let _ = machine.shutdown().await;
            return Err(error);
        }
    };
    let gate = control.gate(GateKind::Capture);
    let mut build = Box::pin(machine.build_create_send_catalog());
    owned(&build);
    let result: TestResult = async {
        require(
            pending(build.as_mut()).await,
            "capture unexpectedly completed before gate",
        )?;
        tokio::time::timeout(Duration::from_secs(5), gate.entered()).await?;
        drop(build);
        require(
            machine.workload()?.accepted_jobs == 1,
            "lost caller refunded accepted capture",
        )?;
        require(
            machine.workload()?.encoded_bytes == domain::MAX_COMMITTED_IMAGE_BYTES,
            "lost caller refunded full artifact reservation",
        )?;
        require(
            machine.read_create_send_catalog().await.err()
                == Some(StateMachineCatalogError::Owner(StateMachineError::Busy)),
            "sibling read was not refused",
        )?;
        require(
            control.counts()
                == Counts {
                    bounded: 1,
                    ..Counts::default()
                },
            "busy request performed source I/O",
        )?;
        Ok(())
    }
    .await;
    let handle = machine.handle.clone();
    let mut shutdown = Box::pin(machine.shutdown());
    let shutdown_probe = poll_fn(|context| Poll::Ready(shutdown.as_mut().poll(context))).await;
    let shutdown_pending = shutdown_probe.is_pending();
    gate.release();
    let joined = match shutdown_probe {
        Poll::Ready(joined) => joined,
        Poll::Pending => shutdown.await,
    };
    result?;
    require(
        shutdown_pending,
        "shutdown finished while actual capture was gated",
    )?;
    joined?;
    require(
        handle.workload()?.accepted_jobs == 0,
        "joined capture did not refund jobs",
    )?;
    require(
        handle.workload()?.encoded_bytes == 0,
        "joined capture did not refund bytes",
    )?;
    require(
        control.counts()
            == Counts {
                bounded: 1,
                catalog_commits: 1,
                ..Counts::default()
            },
        "capture did not perform exactly one retention",
    )?;
    let slot = control
        .catalog_reader()
        .read_catalog()?
        .ok_or("lost caller prevented actual retention")?;
    require(
        slot.artifact() == before.image.as_bytes(),
        "retention changed original capture",
    )?;
    DecodedNativeSnapshotPair::decode(slot.metadata(), slot.artifact())?;
    require(
        control.reader().snapshot()? == before.snapshot,
        "capture changed source business records",
    )?;
    Ok(())
}

pub(super) async fn lost_catalog_read_waiter_keeps_owned_output_until_actual_join<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control) = seeded(writer, false).await?;
    let setup: TestResult = async {
        machine.build_create_send_catalog().await?;
        Ok(())
    }
    .await;
    if let Err(error) = setup {
        let _ = machine.shutdown().await;
        return Err(error);
    }
    let before = match source(&control) {
        Ok(source) => source,
        Err(error) => {
            let _ = machine.shutdown().await;
            return Err(error);
        }
    };
    control.reset();
    let gate = control.gate(GateKind::Catalog);
    let mut read = Box::pin(machine.read_create_send_catalog());
    let result: TestResult = async {
        require(
            pending(read.as_mut()).await,
            "read unexpectedly completed before gate",
        )?;
        tokio::time::timeout(Duration::from_secs(5), gate.entered()).await?;
        drop(read);
        require(
            machine.workload()?.accepted_jobs == 1,
            "lost caller refunded retained read",
        )?;
        require(
            machine.workload()?.encoded_bytes == domain::MAX_COMMITTED_IMAGE_BYTES,
            "lost caller refunded retained output custody",
        )?;
        require(
            machine.build_create_send_catalog().await.err()
                == Some(StateMachineCatalogError::Owner(StateMachineError::Busy)),
            "sibling build was not refused",
        )?;
        require(
            control.counts()
                == Counts {
                    catalog_factories: 1,
                    catalog_reads: 1,
                    ..Counts::default()
                },
            "busy build performed another capture",
        )?;
        Ok(())
    }
    .await;
    let handle = machine.handle.clone();
    let mut shutdown = Box::pin(machine.shutdown());
    let shutdown_probe = poll_fn(|context| Poll::Ready(shutdown.as_mut().poll(context))).await;
    let shutdown_pending = shutdown_probe.is_pending();
    gate.release();
    let joined = match shutdown_probe {
        Poll::Ready(joined) => joined,
        Poll::Pending => shutdown.await,
    };
    result?;
    require(
        shutdown_pending,
        "shutdown finished while actual retained read was gated",
    )?;
    joined?;
    require(
        handle.workload()?.accepted_jobs == 0,
        "joined retained read did not refund jobs",
    )?;
    require(
        handle.workload()?.encoded_bytes == 0,
        "joined retained read did not refund bytes",
    )?;
    require(
        control.counts()
            == Counts {
                catalog_factories: 1,
                catalog_reads: 1,
                ..Counts::default()
            },
        "retained read used extra source work",
    )?;
    require(
        control.reader().snapshot()? == before.snapshot,
        "retained read mutated business records",
    )?;
    Ok(())
}

pub(super) async fn unpolled_owned_factories_do_not_keep_the_backend_owner_alive<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control) = seeded(writer, false).await?;
    let build = machine.build_create_send_catalog();
    let read = machine.read_create_send_catalog();
    owned(&build);
    owned(&read);
    let result: TestResult = async {
        assert_eq!(control.counts(), Counts::default());
        assert_eq!(machine.workload()?.accepted_jobs, 0);
        assert_eq!(machine.workload()?.encoded_bytes, 0);
        Ok(())
    }
    .await;
    let joined = machine.shutdown().await;
    result?;
    joined?;
    assert_eq!(
        build.await.err(),
        Some(StateMachineCatalogError::Owner(StateMachineError::Closed))
    );
    assert_eq!(
        read.await.err(),
        Some(StateMachineCatalogError::Owner(StateMachineError::Closed))
    );
    assert_eq!(control.counts(), Counts::default());
    Ok(())
}

#[tokio::test]
async fn owned_catalog_results_and_inert_future_survive_actual_fjall_handle_release() -> TestResult
{
    let directory = testkit::DurableProvider::temporary()?;
    let (before, built, retained, inert) = {
        let (mut machine, control) =
            seeded(FjallCatalogReplicaStore::open(directory.path())?, true).await?;
        let result = async {
            let before = source(&control)?;
            let built = machine.build_create_send_catalog().await?;
            let retained = machine
                .read_create_send_catalog()
                .await?
                .ok_or("missing catalog pair")?;
            let inert = machine.build_create_send_catalog();
            Ok::<_, Box<dyn std::error::Error>>((before, built, retained, inert))
        }
        .await;
        let joined = machine.shutdown().await;
        let values = result?;
        joined?;
        drop(control); // The last test-owned physical writer and every matching reader.
        values
    };
    // No backend handle is carried by the DTOs or the still-unpolled factory.
    let writer = FjallCatalogReplicaStore::open(directory.path())?;
    let reader = writer.reader();
    assert_eq!(reader.snapshot()?, before.snapshot);
    let raw = writer
        .catalog_reader()
        .read_catalog()?
        .ok_or("missing durable catalog")?;
    assert_eq!(raw.artifact(), built.image_bytes());
    assert_eq!(raw.metadata(), built.metadata_bytes());
    assert_eq!(retained.image_bytes(), built.image_bytes());
    assert_eq!(retained.snapshot_meta(), built.snapshot_meta());
    let mut machine =
        ExperimentalStateMachine::open_with_snapshot_catalog(writer, captured::stream()?)?;
    let result = async {
        let reopened = machine
            .read_create_send_catalog()
            .await?
            .ok_or("missing reopened projection")?;
        assert_eq!(reopened.image_bytes(), built.image_bytes());
        assert_eq!(reopened.metadata_bytes(), built.metadata_bytes());
        assert_eq!(reopened.snapshot_meta(), built.snapshot_meta());
        assert_eq!(
            inert.await.err(),
            Some(StateMachineCatalogError::Owner(StateMachineError::Closed))
        );
        Ok(())
    }
    .await;
    finish(machine, result).await?;
    drop(reader);
    // A second independent physical open pins release of the reopened owner.
    let writer = FjallCatalogReplicaStore::open(directory.path())?;
    assert_eq!(writer.reader().snapshot()?, before.snapshot);
    assert_eq!(
        writer
            .catalog_reader()
            .read_catalog()?
            .ok_or("missing final slot")?
            .artifact(),
        built.image_bytes()
    );
    Ok(())
}

use std::{
    future::{Future, poll_fn},
    pin::Pin,
    task::Poll,
    time::Duration,
};

use super::*;
use observed::{Fault, GateKind};

fn require(condition: bool, message: &'static str) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}

async fn pending<F: Future>(mut future: Pin<&mut F>) -> bool {
    poll_fn(|context| Poll::Ready(future.as_mut().poll(context)))
        .await
        .is_pending()
}

fn owned<F: Future + Send + 'static>(_: &F) {}

pub(super) async fn accepted_lost_waiter_keeps_custody_refunds_and_blocks_real_join<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control, target) = fixture::target(writer).await?;
    let setup = (|| {
        fixture::prepared(
            target.clone(),
            fixture::from_selected(captured::initial()?)?,
        )
    })();
    let (request, rows, checkpoint, pointer) = match setup {
        Ok(values) => values,
        Err(error) => {
            let _ = machine.shutdown().await;
            return Err(error);
        }
    };
    let gate = control.gate(GateKind::Capture);
    let mut work = Box::pin(machine.replace_create_send_image_with_catalog(request));
    owned(&work);
    let result: TestResult = async {
        require(
            pending(work.as_mut()).await,
            "replacement completed before actual gate",
        )?;
        tokio::time::timeout(Duration::from_secs(5), gate.entered()).await?;
        drop(work);
        require(
            machine.workload()?.accepted_jobs == 1,
            "lost caller refunded accepted replacement",
        )?;
        require(
            machine.workload()?.encoded_bytes == domain::MAX_COMMITTED_IMAGE_BYTES,
            "lost caller refunded source custody",
        )?;
        let request = fixture::request(target, fixture::source(false)?)?;
        require(
            machine
                .replace_create_send_image_with_catalog(request)
                .await
                .err()
                == Some(StateMachineImageReplacementError::Owner(
                    StateMachineError::Busy,
                )),
            "second replacement bypassed full admission charge",
        )?;
        require(
            control.counts()
                == observed::Counts {
                    bounded: 1,
                    ..observed::Counts::default()
                },
            "busy replacement performed target work",
        )?;
        Ok(())
    }
    .await;
    let handle = machine.handle.clone();
    let mut shutdown = Box::pin(machine.shutdown());
    let probe = poll_fn(|context| Poll::Ready(shutdown.as_mut().poll(context))).await;
    let was_pending = probe.is_pending();
    gate.release();
    let joined = match probe {
        Poll::Ready(joined) => joined,
        Poll::Pending => shutdown.await,
    };
    result?;
    require(
        was_pending,
        "real owner join completed while target capture was gated",
    )?;
    joined?;
    require(
        handle.workload()?.accepted_jobs == 0 && handle.workload()?.encoded_bytes == 0,
        "joined replacement failed to refund",
    )?;
    fixture::assert_counts(&control, 1, 1);
    assert_eq!(control.committed_pointers()[0].1, pointer);
    fixture::exact_target(&control, &rows, &checkpoint)?;
    Ok(())
}

pub(super) async fn unpolled_and_closed_future_inputs_never_keep_database_handles_alive<W>(
    writer: W,
) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control, target) = fixture::target(writer).await?;
    let setup = (|| fixture::request(target, fixture::source(false)?))();
    let request = match setup {
        Ok(request) => request,
        Err(error) => {
            let _ = machine.shutdown().await;
            return Err(error);
        }
    };
    let inert = machine.replace_create_send_image_with_catalog(request);
    owned(&inert);
    let result: TestResult = async {
        require(
            control.counts() == observed::Counts::default(),
            "unpolled input performed target I/O",
        )?;
        require(
            machine.workload()?.accepted_jobs == 0 && machine.workload()?.encoded_bytes == 0,
            "unpolled input claimed admission",
        )?;
        Ok(())
    }
    .await;
    let joined = machine.shutdown().await;
    result?;
    joined?;
    assert_eq!(
        inert.await.err(),
        Some(StateMachineImageReplacementError::Owner(
            StateMachineError::Closed
        ))
    );
    fixture::assert_counts(&control, 0, 0);
    Ok(())
}

pub(super) async fn capture_panic_refunds_and_really_joins_owner<W>(writer: W) -> TestResult
where
    W: CatalogCommittedStore,
    W::Reader: BoundedStateStore,
{
    let (mut machine, control, target) = fixture::target(writer).await?;
    control.fault(Fault::CapturePanic);
    let setup = (|| fixture::request(target, fixture::source(false)?))();
    let request = match setup {
        Ok(request) => request,
        Err(error) => {
            let _ = machine.shutdown().await;
            return Err(error);
        }
    };
    let handle = machine.handle.clone();
    let result: TestResult = async {
        assert_eq!(
            machine
                .replace_create_send_image_with_catalog(request)
                .await
                .err(),
            Some(StateMachineImageReplacementError::Owner(
                StateMachineError::Panicked
            ))
        );
        assert_eq!(handle.workload()?.accepted_jobs, 0);
        assert_eq!(handle.workload()?.encoded_bytes, 0);
        fixture::assert_counts(&control, 1, 0);
        Ok(())
    }
    .await;
    let joined = machine.shutdown().await;
    result?;
    assert_eq!(joined, Err(StateMachineError::Panicked));
    Ok(())
}

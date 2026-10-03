use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

mod receive_claim;

fn job(done: Arc<AtomicUsize>, result: Result<(), ReceiveExit>) -> work::WorkFuture {
    Box::pin(async move {
        done.fetch_add(1, Ordering::AcqRel);
        result
    })
}

#[tokio::test]
async fn detach_drains_every_existing_job_after_individual_native_detach_errors() {
    let done = Arc::new(AtomicUsize::new(0));
    let mut work = FuturesUnordered::new();
    work.push(job(Arc::clone(&done), Err(ReceiveExit::Detached)));
    work.push(job(Arc::clone(&done), Ok(())));
    work.push(job(Arc::clone(&done), Err(ReceiveExit::Detached)));
    work.push(job(Arc::clone(&done), Ok(())));
    assert!(matches!(
        drain_after_detach(&mut work, None).await,
        Err(ReceiveExit::Detached)
    ));
    assert_eq!(done.load(Ordering::Acquire), 4);
    assert!(work.is_empty());
}

#[tokio::test]
async fn detach_retains_a_serious_failure_only_after_other_ready_jobs_finish() {
    let done = Arc::new(AtomicUsize::new(0));
    let mut work = FuturesUnordered::new();
    work.push(job(Arc::clone(&done), Err(ReceiveExit::Detached)));
    work.push(job(
        Arc::clone(&done),
        Err(ReceiveExit::Refused(error_for(
            AmqpError::ResourceLimitExceeded,
            "a bounded refusal".to_owned(),
        ))),
    ));
    work.push(job(Arc::clone(&done), Ok(())));
    let Err(ReceiveExit::Refused(error)) = drain_after_detach(&mut work, None).await else {
        panic!("the serious refusal is preserved");
    };
    assert_eq!(
        error.condition,
        ErrorCondition::Amqp(AmqpError::ResourceLimitExceeded)
    );
    assert_eq!(error.description.as_deref(), Some("a bounded refusal"));
    assert_eq!(done.load(Ordering::Acquire), 3);
    assert!(work.is_empty());
}

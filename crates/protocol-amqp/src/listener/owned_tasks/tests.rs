use std::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll, Waker},
    time::Duration,
};

use super::control::BlockingGate;
use super::*;

mod admission;
mod custody;
mod fixture;
mod interruptions;
mod payloads;
mod runtimes;

use fixture::*;

type TestResult = Result<(), Box<dyn std::error::Error>>;
const OBSERVATION_LIMIT: Duration = Duration::from_secs(5);

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

async fn observe(
    observer: &mut Observer,
    condition: fn(Progress) -> bool,
) -> Result<Progress, Box<dyn std::error::Error>> {
    tokio::time::timeout(OBSERVATION_LIMIT, observer.wait_until(condition))
        .await?
        .ok_or_else(|| std::io::Error::other("status sender closed before observation").into())
}

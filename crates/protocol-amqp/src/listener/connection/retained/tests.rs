use super::*;
use crate::listener::retained_connection::{
    RetainedConnectionJoinReport, RetainedConnectionOwner, RetainedConnectionResult,
    controls::{Gate, Site},
};
use std::{
    error::Error,
    io,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    task::{Context, Poll},
    time::Duration,
};

mod custody;
mod fixture;
mod io_gate;
mod policy;
mod public;
mod ready;
mod runtime;
mod websocket;

use fixture::{Harness, NoBroker, TestDriver, TestResult, bounded, hello, send_begin};
use io_gate::{IoPlan, PayloadError, PayloadWitness};

fn primary_error<A>(
    report: &RetainedConnectionJoinReport<A>,
) -> Option<&(dyn Error + Send + Sync + 'static)> {
    match &report.outcomes().primary {
        Some(Outcome::Finished(Err(error))) => Some(error.as_ref()),
        _ => None,
    }
}

fn successful_socket_joins<A>(report: &RetainedConnectionJoinReport<A>) {
    assert!(matches!(report.wrapper(), Some(Ok(()))));
    assert!(matches!(report.actor(), Some(Ok(()))));
    assert!(normal_reader_join(report.reader()));
}

fn normal_reader_join(result: Option<&Result<(), tokio::task::JoinError>>) -> bool {
    // Existing shutdown aborts pending input; normal completion can win the race.
    match result {
        Some(Ok(())) => true,
        Some(Err(error)) => error.is_cancelled(),
        None => false,
    }
}

use crate::broker::{Broker, BrokerHandle, COMMAND_QUEUE_DEPTH, Request};
use crate::{Clock, MaintenanceClockAssessment as Assessment, TimerWorker};
use domain::{
    Command, CommandKind, EntityPath, NamespaceName, QueueConfig, StateMachine, Timestamp,
    TopicConfig,
};
use std::{
    error::Error,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};
use storage::{MemoryStore, StateStore, StorageError, StoreSnapshot, WriteBatch};

mod clock;
mod custody;
mod fixture;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const WAIT: Duration = Duration::from_secs(5);

fn names() -> (NamespaceName, EntityPath) {
    (
        NamespaceName::new("tenant").expect("constant namespace"),
        EntityPath::new("orders").expect("constant path"),
    )
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

fn send_kind() -> CommandKind {
    CommandKind::Send {
        message_id: "probe-test".into(),
        body: vec![1],
        time_to_live_millis: None,
        session_id: None,
    }
}

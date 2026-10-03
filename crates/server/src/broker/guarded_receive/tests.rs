use std::{
    error::Error,
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use domain::{
    Command, DeleteEntityTarget, EntityIncarnationKind, MessageRecord, MessageState, QueueCounters,
    ReceiveMode, SequenceNumber, StateMachine, codec, keys,
};
use protocol_amqp::{ReceiveClaimError, ReceiveClaimState};
use storage::{Key, StorageError, StoreSnapshot, Value as StoredValue, WriteBatch};
use testkit::{DurableProvider, MemoryProvider, StoreProvider};
use tokio::time::timeout;

use super::*;

mod admission;
mod effects;
mod fixture;

use fixture::Node;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const DEADLINE: Duration = Duration::from_secs(5);

fn poll_pending<T: Future>(future: std::pin::Pin<&mut T>) {
    assert!(matches!(
        future.poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
}

fn assert_owned<F: Future<Output = ReceiveResult> + Send + 'static>(future: F) -> F {
    future
}

macro_rules! cases {
    ($memory:ident, $durable:ident, $case:path) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $memory() -> TestResult {
            $case(MemoryProvider::new()).await
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn $durable() -> TestResult {
            $case(DurableProvider::temporary()?).await
        }
    };
}

cases!(
    memory_unpolled_and_capacity_cancel,
    durable_unpolled_and_capacity_cancel,
    admission::unpolled_and_capacity_cancel
);
cases!(
    memory_queued_cancel,
    durable_queued_cancel,
    admission::queued_cancel
);
cases!(
    memory_queued_expiry,
    durable_queued_expiry,
    admission::queued_expiry
);
cases!(
    memory_stale_and_wrong_routes,
    durable_stale_and_wrong_routes,
    admission::stale_and_wrong_routes
);
cases!(
    memory_started_response_loss,
    durable_started_response_loss,
    effects::started_response_loss
);
cases!(
    memory_storage_unknown,
    durable_storage_unknown,
    effects::storage_unknown
);
cases!(
    memory_empty_and_deadletter_effects,
    durable_empty_and_deadletter_effects,
    effects::empty_and_deadletter_effects
);

#[test]
fn unavailable_causes_do_not_expose_private_owner_details() {
    let errors = [
        ProposeError::UnexpectedOutcome {
            outcome: "private message body and backend path".to_owned(),
        },
        ProposeError::ClockWentBackward {
            last_applied: Timestamp::from_millis(8_765_432),
            now: Timestamp::from_millis(1),
            allowed_millis: 77,
        },
        ProposeError::Broker(BrokerError::Storage(StorageError::Backend {
            operation: "private operation",
            detail: "private message body and backend path".to_owned(),
        })),
    ];
    for (error, cause) in errors.into_iter().zip([
        ReceiveOwnerUnavailableCause::UnexpectedOutcome,
        ReceiveOwnerUnavailableCause::Clock,
        ReceiveOwnerUnavailableCause::Storage,
    ]) {
        let error = receive_error(error);
        assert_eq!(error, ReceiveSubmitError::OwnerUnavailable(cause));
        let text = format!("{error:?} {error}");
        for private in ["private", "8765432", "77"] {
            assert!(!text.contains(private), "private diagnostic leaked: {text}");
        }
    }
}

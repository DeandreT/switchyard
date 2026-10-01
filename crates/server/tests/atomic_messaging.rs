//! Trusted atomic queue groups share one owner turn and publish committed effects.

use std::{
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use domain::{
    AtomicMessagingApplication, BrokerError, CommandKind, CommandOutcome, Delivery, EntityBinding,
    EntityPath, LockToken, NamespaceName, QueueConfig, ReceiveMode, SequenceNumber, StateMachine,
};
use protocol_amqp::{Attachment, Broker as _};
use server::{Broker, LocalProposer, ManualClock, ProposeError, SubmitError};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::StoreProvider;
use tokio::time::timeout;

#[path = "atomic_messaging/fixture.rs"]
mod fixture;
#[path = "atomic_messaging/lifecycle.rs"]
mod lifecycle;

use fixture::*;
use lifecycle::*;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const NO_WAKE: Duration = Duration::from_millis(30);
const DEADLINE: Duration = Duration::from_secs(2);

fn send(id: &str) -> CommandKind {
    CommandKind::Send {
        message_id: id.to_owned(),
        body: id.as_bytes().to_vec(),
        time_to_live_millis: None,
        session_id: None,
    }
}

macro_rules! suite {
    ($module:ident, $provider:expr) => {
        mod $module {
            use super::*;
            #[tokio::test(flavor = "current_thread")]
            async fn mixed_group_commits_and_wakes_only_its_source() -> TestResult {
                mixed_commit($provider).await
            }
            #[tokio::test(flavor = "current_thread")]
            async fn dead_letter_group_wakes_source_and_shadow() -> TestResult {
                dead_letter_commit($provider).await
            }
            #[tokio::test(flavor = "current_thread")]
            async fn failed_commit_and_late_refusal_publish_nothing() -> TestResult {
                failed_commit($provider).await
            }
            #[tokio::test(flavor = "current_thread")]
            async fn empty_groups_publish_nothing() -> TestResult {
                empty_group($provider).await
            }
            #[tokio::test(flavor = "current_thread")]
            async fn duplicate_only_groups_do_not_wake_receivers() -> TestResult {
                duplicate_only($provider).await
            }
            #[tokio::test(flavor = "current_thread")]
            async fn reported_commit_error_is_not_proof_of_absence() -> TestResult {
                ambiguous_commit($provider).await
            }
            #[tokio::test(flavor = "current_thread")]
            async fn concurrent_groups_do_not_interleave_members() -> TestResult {
                concurrent_groups($provider).await
            }
            #[tokio::test(flavor = "current_thread")]
            async fn canceling_caller_does_not_cancel_admitted_commit() -> TestResult {
                canceled_caller($provider).await
            }
        }
    };
}

suite!(memory, testkit::MemoryProvider::new());
suite!(durable, testkit::DurableProvider::temporary()?);

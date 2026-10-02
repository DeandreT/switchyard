//! Explicitly opted-in posting transactions use the real atomic broker owner.

use std::{
    collections::{HashMap, HashSet},
    error::Error,
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use amqp::{
    Attach, Begin, Body, Close, Coordinator, Declare, DeliveryState, Discharge, Disposition, End,
    Flow, Frame, Message, Open, Outcome, Performative, Properties, ProtocolHeader,
    ReceiverSettleMode, Role, SenderSettleMode, Source, Symbol, Target, TransactionCommand,
    TransactionId, TransactionalState, Transfer, Value, encode_message, read_frame,
    read_protocol_header, write_frame, write_protocol_header,
};
use domain::{
    Command, CommandKind, CommandOutcome, DeleteEntityTarget, EntityBinding, EntityPath,
    NamespaceName, QueueConfig, SequenceNumber, StateMachine, SubscriptionConfig, SubscriptionName,
    Timestamp, TopicConfig,
};
use protocol_amqp::{
    AtomicCommitPermit, AtomicCommitState, Attachment, BrokerRejection, EntityAdmission,
    EntityMetadata, NativeAtomicBroker, NativeAtomicBrokerCompletion,
    NativeAtomicResponseUnavailable, OwnedNativeAtomicMessagingSubmission,
};
use server::{Broker, BrokerHandle, Clock, LocalProposer};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value as StoredValue, WriteBatch};
use testkit::StoreProvider;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::timeout,
};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
const DEADLINE: Duration = Duration::from_secs(5);
const CONTROL: u16 = 19;
const POST: u16 = 23;
const SECOND: u16 = 29;
const HEALTHY: u16 = 37;
const CONTROL_HANDLE: u32 = 17;
const POST_HANDLE: u32 = 31;

#[path = "amqp_native_atomic/fixture.rs"]
mod fixture;
use fixture::*;
#[path = "amqp_native_atomic/atomicity.rs"]
mod atomicity;
#[path = "amqp_native_atomic/lifecycle.rs"]
mod lifecycle;
#[path = "amqp_native_atomic/refusals.rs"]
mod refusals;

macro_rules! backend_cases {
    ($module:ident, $provider:expr) => {
        mod $module {
            use super::*;
            #[tokio::test]
            async fn single_batch_and_cross_session_postings_commit_once() -> TestResult {
                lifecycle::single_batch_and_cross_session_postings_commit_once($provider).await
            }
            #[tokio::test]
            async fn abort_and_empty_discharge_never_touch_broker_storage() -> TestResult {
                lifecycle::abort_and_empty_discharge_never_touch_broker_storage($provider).await
            }
            #[tokio::test]
            async fn default_listener_still_refuses_coordinator_without_admission() -> TestResult {
                lifecycle::default_listener_still_refuses_coordinator_without_admission($provider)
                    .await
            }
            #[tokio::test]
            async fn unsupported_targets_and_bad_posts_are_link_scoped() -> TestResult {
                refusals::unsupported_targets_and_bad_posts_are_link_scoped($provider).await
            }
            #[tokio::test]
            async fn second_queue_cannot_join_a_bound_transaction() -> TestResult {
                refusals::second_queue_cannot_join_a_bound_transaction($provider).await
            }
            #[tokio::test]
            async fn scoped_authorization_precedes_missing_or_corrupt_target_admission()
            -> TestResult {
                refusals::scoped_authorization_precedes_missing_or_corrupt_target_admission(
                    $provider,
                )
                .await
            }
            #[tokio::test]
            async fn stale_queue_incarnation_cannot_commit_to_its_replacement() -> TestResult {
                atomicity::stale_queue_incarnation_cannot_commit_to_its_replacement($provider).await
            }
            #[tokio::test]
            async fn physical_commit_errors_are_indeterminate_without_retry_or_detail_leak()
            -> TestResult {
                atomicity::physical_commit_errors_are_indeterminate_without_retry_or_detail_leak(
                    $provider,
                )
                .await
            }
        }
    };
}

backend_cases!(memory, testkit::MemoryProvider::new());
backend_cases!(durable, testkit::DurableProvider::temporary()?);

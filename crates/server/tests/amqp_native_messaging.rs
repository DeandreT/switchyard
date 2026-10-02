//! Explicit native mixed messaging uses canonical broker locks, not wire aliases.

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
    Attach, Begin, Body, Close, Coordinator, Declare, DeliveryState, Discharge, Disposition, Flow,
    Frame, Message, Open, Outcome, Performative, Properties, ProtocolHeader, ReceiverSettleMode,
    Role, SenderSettleMode, Source, Symbol, Target, TransactionCommand, TransactionId,
    TransactionalState, Transfer, Value, decode_message, encode_message, read_frame,
    read_protocol_header, write_frame, write_protocol_header,
};
use domain::{
    Command, CommandKind, CommandOutcome, DeleteEntityTarget, EntityBinding, EntityPath,
    MessageState, NamespaceName, QueueConfig, ReceiveMode, SequenceNumber, StateMachine,
    SubscriptionConfig, SubscriptionName, Timestamp, TopicConfig,
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
    sync::Notify,
    task::JoinHandle,
    time::timeout,
};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
const DEADLINE: Duration = Duration::from_secs(5);
const CONTROL: u16 = 19;
const POST: u16 = 23;
const RECEIVE: u16 = 29;
const HEALTHY: u16 = 37;
const CONTROL_HANDLE: u32 = 17;
const POST_HANDLE: u32 = 31;
const RECEIVE_HANDLE: u32 = 47;

#[path = "amqp_native_messaging/fixture.rs"]
mod fixture;
use fixture::*;
#[path = "amqp_native_messaging/peer.rs"]
mod peer;
use peer::*;
#[path = "amqp_native_messaging/atomicity.rs"]
mod atomicity;
#[path = "amqp_native_messaging/lifecycle.rs"]
mod lifecycle;
#[path = "amqp_native_messaging/refusals.rs"]
mod refusals;

macro_rules! backend_cases {
    ($module:ident, $provider:expr) => {
        mod $module {
            use super::*;
            #[tokio::test]
            async fn mixed_batch_and_canonical_complete_commit_once() -> TestResult {
                lifecycle::mixed_batch_and_canonical_complete_commit_once($provider).await
            }
            #[tokio::test]
            async fn explicit_rollback_rearms_original_without_resend() -> TestResult {
                lifecycle::explicit_rollback_rearms_original_without_resend($provider).await
            }
            #[tokio::test]
            async fn ordinary_complete_settles_canonical_lock_without_atomic_handoff() -> TestResult
            {
                lifecycle::ordinary_outcome($provider, lifecycle::OrdinaryOutcome::Complete).await
            }
            #[tokio::test]
            async fn ordinary_release_requeues_canonical_lock_without_atomic_handoff() -> TestResult
            {
                lifecycle::ordinary_outcome($provider, lifecycle::OrdinaryOutcome::Abandon).await
            }
            #[tokio::test]
            async fn ordinary_modified_defers_canonical_lock_without_atomic_handoff() -> TestResult
            {
                lifecycle::ordinary_outcome($provider, lifecycle::OrdinaryOutcome::Defer).await
            }
            #[tokio::test]
            async fn ordinary_rejection_dead_letters_canonical_lock_without_atomic_handoff()
            -> TestResult {
                lifecycle::ordinary_outcome($provider, lifecycle::OrdinaryOutcome::DeadLetter).await
            }
            #[tokio::test]
            async fn expired_lock_rejects_entire_mixed_group() -> TestResult {
                atomicity::expired_lock_rejects_entire_mixed_group($provider).await
            }
            #[tokio::test]
            async fn replacement_incarnation_rejects_old_canonical_delivery() -> TestResult {
                atomicity::replacement_incarnation_rejects_old_canonical_delivery($provider).await
            }
            #[tokio::test]
            async fn physical_errors_close_without_retry_and_reopen_whole_commit() -> TestResult {
                atomicity::physical_errors_close_without_retry_and_reopen_whole_commit($provider)
                    .await
            }
            #[tokio::test]
            async fn source_or_controller_close_aborts_queued_handoff() -> TestResult {
                atomicity::source_or_controller_close_aborts_queued_handoff($provider).await
            }
            #[tokio::test]
            async fn restricted_profiles_and_posting_only_policy_refuse_before_bind() -> TestResult
            {
                refusals::restricted_profiles_and_posting_only_policy_refuse_before_bind($provider)
                    .await
            }
            #[tokio::test]
            async fn unsupported_sources_do_not_acquire_messages() -> TestResult {
                refusals::unsupported_sources_do_not_acquire_messages($provider).await
            }
            #[tokio::test]
            async fn listen_and_send_permissions_precede_target_binding() -> TestResult {
                refusals::listen_and_send_permissions_precede_target_binding($provider).await
            }
        }
    };
}

backend_cases!(memory, testkit::MemoryProvider::new());
backend_cases!(durable, testkit::DurableProvider::temporary()?);

//! Ordinary receiving links must obtain native credit before claiming broker messages.

use std::{
    collections::{HashMap, HashSet},
    error::Error,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use amqp::{
    Accepted, Attach, Begin, Body, Close, DeliveryState, Detach, Disposition, Flow, Frame, Message,
    Open, Performative, Properties, ProtocolHeader, ReceiverSettleMode, Role, SenderSettleMode,
    Source, Symbol, Target, Value, decode_message, encode_message, read_frame,
    read_protocol_header, write_frame, write_protocol_header,
};
use domain::{
    CommandKind, CommandOutcome, EntityBinding, EntityPath, MessageState, NamespaceName,
    QueueConfig, ReceiveMode, SequenceNumber, StateMachine, SubscriptionConfig, SubscriptionName,
    Timestamp, TopicConfig,
};
use protocol_amqp::{
    Attachment, BrokerRejection, EntityAdmission, EntityMetadata, OwnedReceiveSubmission,
    ReceiveClaimPermit, ReceiveClaimState, ReceiveSubmitError,
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
const DEADLINE: Duration = Duration::from_secs(6);
const CHANNEL: u16 = 19;
const HANDLE: u32 = 47;
const WINDOW: u32 = 2_048;

#[path = "amqp_receiving_credit/fixture.rs"]
mod fixture;
use fixture::*;
#[path = "amqp_receiving_credit/peer.rs"]
mod peer;
use peer::*;
#[path = "amqp_receiving_credit/auth.rs"]
mod auth;
#[path = "amqp_receiving_credit/lifecycle.rs"]
mod lifecycle;
#[path = "amqp_receiving_credit/pipeline.rs"]
mod pipeline;

macro_rules! backend_cases {
    ($module:ident, $provider:expr) => {
        mod $module {
            use super::*;

            #[tokio::test]
            async fn zero_credit_never_claims_or_deletes_queue_or_subscription_messages()
            -> TestResult {
                lifecycle::zero_credit_never_claims_or_deletes_queue_or_subscription_messages(
                    $provider,
                )
                .await
            }

            #[tokio::test]
            async fn one_credit_claims_one_original_and_replayed_flow_cannot_refresh_it()
            -> TestResult {
                lifecycle::one_credit_claims_one_original_and_replayed_flow_cannot_refresh_it(
                    $provider,
                )
                .await
            }

            #[tokio::test]
            async fn empty_receive_returns_credit_before_drain_and_later_enqueue() -> TestResult {
                lifecycle::empty_receive_returns_credit_before_drain_and_later_enqueue($provider)
                    .await
            }

            #[tokio::test]
            async fn detached_receiver_credit_cannot_be_spent_by_its_replacement() -> TestResult {
                lifecycle::detached_receiver_credit_cannot_be_spent_by_its_replacement($provider)
                    .await
            }

            #[tokio::test]
            async fn listen_expiry_before_credit_does_not_claim_a_message() -> TestResult {
                auth::listen_expiry_before_credit_does_not_claim_a_message($provider).await
            }

            #[tokio::test]
            async fn three_held_originals_settle_in_reverse_without_replayed_credit() -> TestResult {
                pipeline::three_held_originals_settle_in_reverse_without_replayed_credit($provider)
                    .await
            }

            #[tokio::test]
            async fn thirty_two_held_jobs_bound_a_thirty_three_credit_grant() -> TestResult {
                pipeline::thirty_two_held_jobs_bound_a_thirty_three_credit_grant($provider).await
            }

            #[tokio::test]
            async fn an_empty_lookup_drains_unused_credit_while_jobs_remain_held() -> TestResult {
                pipeline::an_empty_lookup_drains_unused_credit_while_jobs_remain_held($provider)
                    .await
            }

            #[tokio::test]
            async fn detach_preserves_held_locks_until_explicit_expiry_and_redelivery() -> TestResult {
                pipeline::detach_preserves_held_locks_until_explicit_expiry_and_redelivery($provider)
                    .await
            }

            #[tokio::test]
            async fn authorization_loss_preserves_several_locks_until_expiry() -> TestResult {
                auth::pipeline::authorization_loss_preserves_several_locks_until_expiry($provider)
                    .await
            }

            #[tokio::test]
            async fn a_started_receive_survives_another_jobs_final_ack() -> TestResult {
                pipeline::a_started_receive_survives_another_jobs_final_ack($provider).await
            }

            #[tokio::test]
            async fn queued_peek_lock_cancellation_precedes_all_receive_owner_work() -> TestResult {
                auth::guarded::queued_peek_lock_cancellation_precedes_all_receive_owner_work($provider).await
            }

            #[tokio::test]
            async fn queued_receive_expiry_is_an_exact_wire_refusal_without_owner_work() -> TestResult {
                auth::guarded::queued_receive_expiry_is_an_exact_wire_refusal_without_owner_work($provider).await
            }

            #[tokio::test]
            async fn a_stale_receive_binding_refuses_before_clock_and_cannot_touch_replacement() -> TestResult {
                auth::guarded::a_stale_receive_binding_refuses_before_clock_and_cannot_touch_replacement($provider).await
            }

            #[tokio::test]
            async fn started_receive_commits_once_despite_connection_and_reply_loss() -> TestResult {
                auth::guarded::started_receive_commits_once_despite_connection_and_reply_loss($provider).await
            }

            #[tokio::test]
            async fn queued_receive_and_delete_cancellation_preserves_the_original() -> TestResult {
                auth::guarded::queued_receive_and_delete_cancellation_preserves_the_original($provider).await
            }

            #[tokio::test]
            async fn valid_guarded_receive_preserves_canonical_delivery_and_settlement() -> TestResult {
                auth::guarded::valid_guarded_receive_preserves_canonical_delivery_and_settlement($provider).await
            }
        }
    };
}

backend_cases!(memory, testkit::MemoryProvider::new());
backend_cases!(durable, testkit::DurableProvider::temporary()?);

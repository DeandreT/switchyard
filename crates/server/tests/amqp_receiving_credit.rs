//! Ordinary receiving links must obtain native credit before claiming broker messages.

use std::{
    collections::{HashMap, HashSet},
    error::Error,
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
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
use protocol_amqp::{Attachment, BrokerRejection, EntityAdmission, EntityMetadata};
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
        }
    };
}

backend_cases!(memory, testkit::MemoryProvider::new());
backend_cases!(durable, testkit::DurableProvider::temporary()?);

//! A separate development-only binary socket enables native mixed messaging.

use std::{
    collections::{HashMap, HashSet},
    error::Error,
    sync::Arc,
    time::Duration,
};

use amqp::{
    Attach, Begin, Body, Close, Coordinator, Declare, DeliveryState, Discharge, Disposition, Flow,
    Frame, Message, Open, Outcome, Performative, Properties, ProtocolHeader, ReceiverSettleMode,
    Role, SenderSettleMode, Source, Symbol, Target, TransactionCommand, TransactionId,
    TransactionalState, Transfer, Value, decode_message, encode_message, read_frame,
    read_protocol_header, write_frame, write_protocol_header,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    time::timeout,
};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
const DEADLINE: Duration = Duration::from_secs(5);
const PROCESS_DEADLINE: Duration = Duration::from_secs(30);
const CONTROL: u16 = 19;
const POST: u16 = 23;
const RECEIVE: u16 = 29;
const CONTROL_HANDLE: u32 = 17;
const POST_HANDLE: u32 = 31;
const RECEIVE_HANDLE: u32 = 47;
const HOST: &str = "tenant.servicebus.windows.net";
const RULE: &str = "binary-manage";
const KEY: &str = "binary-private-shared-access-key";

#[allow(
    dead_code,
    reason = "The shared raw peer also supports failures covered by its owning suite."
)]
#[path = "amqp_native_messaging/peer.rs"]
mod peer;
use peer::*;
#[path = "experimental_atomic_messaging/fixture.rs"]
mod fixture;
#[path = "experimental_atomic_messaging/process.rs"]
mod process;
use fixture::*;
#[path = "experimental_atomic_messaging/security.rs"]
mod security;
#[path = "experimental_atomic_messaging/wire.rs"]
mod wire;

#[tokio::test]
async fn default_binary_listener_refuses_coordinator_without_experimental_socket() -> TestResult {
    wire::default_listener().await
}

#[tokio::test]
async fn memory_binary_mixed_commit_and_same_original_rollback_are_separate_from_defaults()
-> TestResult {
    wire::mixed_binary(false).await
}

#[tokio::test]
async fn fjall_binary_acknowledged_mixed_commit_survives_kill_and_reopen() -> TestResult {
    wire::mixed_binary(true).await
}

#[tokio::test]
async fn production_refuses_experimental_flag_before_credentials_or_data_directory() -> TestResult {
    fixture::production_refusal().await
}

#[tokio::test]
async fn occupied_experimental_socket_fails_before_any_listener_serves() -> TestResult {
    fixture::occupied_socket().await
}

#[tokio::test]
async fn tls_and_shared_access_policy_are_inherited_by_the_experimental_socket() -> TestResult {
    security::inherited_security().await
}

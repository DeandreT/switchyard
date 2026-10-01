//! The WebSocket listener carries the existing AMQP engine and entity authority.

use std::{error::Error, io, time::Duration};

use amqp::{
    ApplicationProperties, Body, ClientConnection, ClientReceiver, ClientSender, ClientSession,
    Frame, Message, Open, Outcome, Performative, Properties, ProtocolHeader, SaslInit, Symbol,
    Value, encode_frame,
};
use domain::{
    Command, CommandKind, CommandOutcome, DeleteEntityTarget, EntityPath, NamespaceName,
    QueueConfig, SequenceNumber, StateMachine, SubscriptionConfig, SubscriptionName, Timestamp,
    TopicConfig, keys,
};
use futures_util::{Sink, SinkExt, Stream, StreamExt};
use storage::{StateStore, StoreSnapshot};
use testkit::StoreProvider;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    time::timeout,
};
use tokio_tungstenite::{
    WebSocketStream, client_async,
    tungstenite::{
        Message as WsMessage,
        client::IntoClientRequest,
        protocol::frame::{
            Frame as WsFrame,
            coding::{CloseCode, Data, OpCode},
        },
    },
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const DEADLINE: Duration = Duration::from_secs(10);
const PATH: &str = "/$servicebus/websocket/";
const HOST: &str = "tenant.servicebus.windows.net";
const RULE: &str = "websocket-rule";
const KEY: &str = "websocket-secret";
const MESSAGE_LIMIT: usize = 4 * 1024 * 1024;

#[path = "amqp_websocket/fixture.rs"]
mod fixture;
use fixture::*;
#[path = "amqp_websocket/auth.rs"]
mod authorization;
#[path = "amqp_websocket/handshake.rs"]
mod handshake;
#[path = "amqp_websocket/lifecycle.rs"]
mod lifecycle;
#[path = "amqp_websocket/limits.rs"]
mod limits;

macro_rules! backend_cases {
    ($module:ident, $provider:expr) => {
        mod $module {
            use super::*;
            #[tokio::test]
            async fn rich_queue_roundtrip() -> TestResult {
                lifecycle::rich_queue_roundtrip($provider).await
            }
            #[tokio::test]
            async fn topic_subscriptions_settle_independently() -> TestResult {
                lifecycle::topic_subscriptions_settle_independently($provider).await
            }
            #[tokio::test]
            async fn session_fifo_and_release() -> TestResult {
                lifecycle::session_fifo_and_release($provider).await
            }
            #[tokio::test]
            async fn admitted_links_cannot_follow_recreated_entities() -> TestResult {
                lifecycle::admitted_links_cannot_follow_recreated_entities($provider).await
            }
            #[tokio::test]
            async fn split_coalesced_and_fragmented_frames() -> TestResult {
                lifecycle::split_coalesced_and_fragmented_frames($provider).await
            }
            #[tokio::test]
            async fn close_and_drop_release_admission() -> TestResult {
                lifecycle::close_and_drop_release_admission($provider).await
            }
            #[tokio::test]
            async fn wss_trust_and_plain_authentication() -> TestResult {
                lifecycle::wss_trust_and_plain_authentication($provider).await
            }
            #[tokio::test]
            async fn strict_http_path_and_subprotocol() -> TestResult {
                handshake::strict_http_path_and_subprotocol($provider).await
            }
            #[tokio::test]
            async fn incomplete_http_and_open_release_admission() -> TestResult {
                handshake::incomplete_http_and_open_release_admission($provider).await
            }
            #[tokio::test]
            async fn strict_first_and_post_sasl_headers() -> TestResult {
                handshake::strict_first_and_post_sasl_headers($provider).await
            }
            #[tokio::test]
            async fn text_and_oversized_messages_are_refused() -> TestResult {
                limits::text_and_oversized_messages_are_refused($provider).await
            }
            #[tokio::test]
            async fn malformed_websocket_frames_are_refused() -> TestResult {
                limits::malformed_websocket_frames_are_refused($provider).await
            }
            #[tokio::test]
            async fn oversized_http_and_frame_lengths_are_bounded() -> TestResult {
                limits::oversized_http_and_frame_lengths_are_bounded($provider).await
            }
            #[tokio::test]
            async fn wss_cbs_grants_do_not_follow_the_http_host() -> TestResult {
                authorization::wss_cbs_grants_do_not_follow_the_http_host($provider).await
            }
        }
    };
}

backend_cases!(memory, testkit::MemoryProvider::new());
backend_cases!(durable, testkit::DurableProvider::temporary()?);

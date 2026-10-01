//! Transaction types are understood but explicitly unsupported at the wire edge.

use std::{
    collections::{HashMap, HashSet},
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use amqp::{
    Accepted, Array, Attach, Begin, Body, Close, Coordinator, Declared, DeliveryState, Disposition,
    End, Fields, Flow, Frame, Message, Open, Outcome, Performative, Properties, ProtocolHeader,
    ReceiverSettleMode, Role, SenderSettleMode, Source, Symbol, Target, TargetTerminus,
    TransactionId, TransactionalState, Transfer, Value, encode_message, read_frame,
    read_protocol_header, write_frame, write_protocol_header,
};
use domain::{CommandKind, EntityPath, NamespaceName, QueueConfig, StateMachine, keys};
use server::{Broker, LocalProposer, ManualClock};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value as StoredValue, WriteBatch};
use testkit::StoreProvider;
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::timeout,
};

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const DEADLINE: Duration = Duration::from_secs(5);
const MAIN: u16 = 57;
const HEALTHY: u16 = 61;
const MAIN_HANDLE: u32 = 41;
const HEALTHY_HANDLE: u32 = 53;

#[path = "amqp_transaction_refusal/cases.rs"]
mod cases;
#[path = "amqp_transaction_refusal/fixture.rs"]
mod fixture;
use fixture::*;

macro_rules! backend_cases {
    ($module:ident, $provider:expr) => {
        mod $module {
            use super::*;

            #[tokio::test]
            async fn coordinator_attach_is_refused_before_broker_admission() -> TestResult {
                cases::coordinator_attach_is_refused_before_broker_admission($provider).await
            }
            #[tokio::test]
            async fn transactional_transfer_never_enqueues_a_message() -> TestResult {
                cases::transactional_transfer_never_enqueues_a_message($provider).await
            }
            #[tokio::test]
            async fn transactional_disposition_does_not_settle_a_held_message() -> TestResult {
                cases::transactional_disposition_does_not_settle_a_held_message($provider).await
            }
            #[tokio::test]
            async fn transactional_flow_is_refused_before_ordinary_flow_validation() -> TestResult {
                cases::transactional_flow_is_refused_before_ordinary_flow_validation($provider)
                    .await
            }
        }
    };
}

backend_cases!(memory, testkit::MemoryProvider::new());
backend_cases!(durable, testkit::DurableProvider::temporary()?);

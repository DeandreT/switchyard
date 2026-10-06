//! Explicit, externally driven retained ingress; current SDK and Memory only.

use std::{collections::BTreeSet, error::Error, fmt, path::Path, time::Duration};

use domain::{
    EntityPath, MessageBody, MessageIdentifier, MessageState, MessageValue, QueueCounters,
    SequenceNumber, StateMachine, keys,
};
use futures_util::FutureExt;
use protocol_amqp::{RetainedAtomicMessagingDrain, RetainedConnectionOutcome};
use storage::StateStore;

use super::{CURRENT_SDK, HOST, KEY, RULE, atomic_messaging::process, websocket};

#[path = "retained_ingress/disposal_tests.rs"]
mod disposal_tests;
#[path = "retained_ingress/fixture.rs"]
mod fixture;
#[path = "retained_ingress/offline_jwt.rs"]
mod offline_jwt;
#[path = "retained_ingress/tests.rs"]
mod tests;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;
const QUEUE: &str = "retained-sdk-send";
const SUCCESS: &str = "official .NET retained Memory warmed immediate-send rollback/commit passed";

fn success_marker_present(stdout: &str) -> bool {
    stdout
        .split_inclusive('\n')
        .filter(|line| {
            line.strip_suffix('\n')
                .map(|line| line.strip_suffix('\r').unwrap_or(line) == SUCCESS)
                .unwrap_or(false)
        })
        .count()
        == 1
}

fn expected_test_cancellation(
    error: &tokio::task::JoinError,
    abort_requested: bool,
    drain: RetainedAtomicMessagingDrain,
) -> bool {
    // Observation plus an exact-token request is not proof of cancellation cause.
    error.is_cancelled() && abort_requested && drain == RetainedAtomicMessagingDrain::External
}

fn unexpected_role_failure(
    join_error: Option<&tokio::task::JoinError>,
    original_error: Option<&(dyn Error + Send + Sync + 'static)>,
    abort_requested: bool,
    drain: RetainedAtomicMessagingDrain,
) -> bool {
    original_error.is_some()
        || join_error
            .is_some_and(|error| !expected_test_cancellation(error, abort_requested, drain))
}

fn completed_peer_close(socket: &protocol_amqp::RetainedConnectionJoinReport<()>) -> bool {
    socket
        .native_observations()
        .peer_close()
        .is_some_and(|peer| {
            peer.channel() == 0
                && peer.payload().is_empty()
                && peer.close().error.is_none()
                && !peer.locally_closing()
                && peer.reply_state() == amqp::ServerPeerCloseReplyState::Ready
                && matches!(peer.reply_result(), Some(Ok(())))
        })
}

fn actor_reader_shutdown_observed(
    socket: &protocol_amqp::RetainedConnectionJoinReport<()>,
) -> bool {
    socket.native_observations().reader().is_some_and(|reader| {
        reader.requested_by(amqp::ServerConnectionAbortSource::ActorReaderShutdown)
    })
}

fn cancelled_reader_matches_observation(
    socket: &protocol_amqp::RetainedConnectionJoinReport<()>,
) -> bool {
    match (socket.reader(), socket.native_observations().reader()) {
        (Some(Err(error)), Some(reader)) => {
            error.is_cancelled()
                && error.id() == reader.id()
                && reader.requested_by(amqp::ServerConnectionAbortSource::ActorReaderShutdown)
        }
        _ => false,
    }
}

fn unexpected_native_close(
    original: Option<&Result<(), amqp::EngineError>>,
    peer_completed: bool,
    actor_reader_shutdown: bool,
) -> bool {
    match original {
        None | Some(Ok(())) => false,
        Some(Err(amqp::EngineError::Stopped)) => !(peer_completed && actor_reader_shutdown),
        Some(Err(_)) => true,
    }
}

fn unexpected_report_failure(report: &fixture::Report) -> bool {
    let socket = report.socket();
    let peer_completed = completed_peer_close(socket);
    // This is a narrow conjunction of observations, not proof of either error's cause.
    if !matches!(socket.wrapper(), Some(Ok(())))
        || !matches!(socket.actor(), Some(Ok(())))
        || !peer_completed
        || !(matches!(socket.reader(), Some(Ok(())))
            || cancelled_reader_matches_observation(socket))
    {
        return true;
    }
    if !matches!(
        socket.outcomes().primary.as_ref(),
        Some(RetainedConnectionOutcome::Finished(Ok(())))
    ) || socket
        .outcomes()
        .websocket_close
        .as_ref()
        .is_some_and(Result::is_err)
        || unexpected_native_close(
            report.native_close(),
            peer_completed,
            actor_reader_shutdown_observed(socket),
        )
        || report
            .admissions()
            .iter()
            .any(|row| row.engine_error().is_some())
    {
        return true;
    }
    for row in report.sessions() {
        if unexpected_role_failure(
            row.join_error(),
            row.routing_error(),
            row.abort_requested(),
            row.drain(),
        ) {
            return true;
        }
    }
    report.workers().any(|row| {
        unexpected_role_failure(
            row.join_error(),
            row.worker_error(),
            row.abort_requested(),
            row.drain(),
        )
    })
}

struct GateFailure {
    fixture: Box<fixture::Fixture>,
}

fn join_status(result: Option<&Result<(), tokio::task::JoinError>>) -> &'static str {
    match result {
        None => "absent",
        Some(Ok(())) => "ok",
        Some(Err(error)) if error.is_cancelled() => "cancelled",
        Some(Err(error)) if error.is_panic() => "panic",
        Some(Err(_)) => "other-error",
    }
}

struct RedactedReport<'a>(&'a fixture::Report);

impl fmt::Debug for RedactedReport<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let report = self.0;
        let socket = report.socket();
        let native_close = match report.native_close() {
            None => "absent",
            Some(Ok(())) => "ok",
            Some(Err(amqp::EngineError::Stopped)) => "stopped",
            Some(Err(amqp::EngineError::RemoteClosed)) => "remote-closed",
            Some(Err(amqp::EngineError::Io(_))) => "io-error",
            Some(Err(amqp::EngineError::Timeout(_))) => "timeout",
            Some(Err(_)) => "other-error",
        };
        let observations = socket.native_observations();
        let peer = observations.peer_close();
        let reply_state = peer.map(|peer| match peer.reply_state() {
            amqp::ServerPeerCloseReplyState::NotRequired => "not-required",
            amqp::ServerPeerCloseReplyState::Pending => "pending",
            amqp::ServerPeerCloseReplyState::Ready => "ready",
            amqp::ServerPeerCloseReplyState::AbandonedBeforeReady => "abandoned-before-ready",
        });
        let reader = observations.reader();
        formatter
            .debug_struct("CoveredReport")
            .field("peer_close_present", &peer.is_some())
            .field("peer_channel_zero", &peer.map(|peer| peer.channel() == 0))
            .field(
                "peer_payload_empty",
                &peer.map(|peer| peer.payload().is_empty()),
            )
            .field(
                "peer_error_present",
                &peer.map(|peer| peer.close().error.is_some()),
            )
            .field(
                "peer_locally_closing",
                &peer.map(|peer| peer.locally_closing()),
            )
            .field("peer_reply_state", &reply_state)
            .field(
                "peer_reply_ok",
                &peer.and_then(|peer| peer.reply_result().map(Result::is_ok)),
            )
            .field("reader_observed", &reader.is_some())
            .field(
                "reader_abort_requested",
                &reader.map(|reader| reader.abort_requested()),
            )
            .field(
                "reader_actor_shutdown",
                &reader.map(|reader| {
                    reader.requested_by(amqp::ServerConnectionAbortSource::ActorReaderShutdown)
                }),
            )
            .field(
                "reader_owner_finish",
                &reader.map(|reader| {
                    reader.requested_by(amqp::ServerConnectionAbortSource::OwnerFinish)
                }),
            )
            .field(
                "reader_cancel_id_matches",
                &cancelled_reader_matches_observation(socket),
            )
            .field("wrapper", &join_status(socket.wrapper()))
            .field("actor", &join_status(socket.actor()))
            .field("reader", &join_status(socket.reader()))
            .field(
                "primary_ok",
                &matches!(
                    socket.outcomes().primary.as_ref(),
                    Some(RetainedConnectionOutcome::Finished(Ok(())))
                ),
            )
            .field(
                "websocket_close_error",
                &socket
                    .outcomes()
                    .websocket_close
                    .as_ref()
                    .is_some_and(Result::is_err),
            )
            .field("native_close", &native_close)
            .field(
                "admission_errors",
                &report
                    .admissions()
                    .iter()
                    .filter(|row| row.engine_error().is_some())
                    .count(),
            )
            .field(
                "session_errors",
                &report
                    .sessions()
                    .iter()
                    .filter(|row| {
                        unexpected_role_failure(
                            row.join_error(),
                            row.routing_error(),
                            row.abort_requested(),
                            row.drain(),
                        )
                    })
                    .count(),
            )
            .field(
                "worker_errors",
                &report
                    .workers()
                    .filter(|row| {
                        unexpected_role_failure(
                            row.join_error(),
                            row.worker_error(),
                            row.abort_requested(),
                            row.drain(),
                        )
                    })
                    .count(),
            )
            .finish()
    }
}

impl fmt::Debug for GateFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reports: [_; fixture::CONNECTION_HISTORY] = std::array::from_fn(|index| {
            self.fixture.controller.reports[index]
                .as_ref()
                .map(RedactedReport)
        });
        let client = match self.fixture.client.as_ref() {
            Some(Ok(output)) => Some((
                output.status.success(),
                success_marker_present(&output.stdout),
            )),
            Some(Err(_)) | None => None,
        };
        formatter
            .debug_struct("RetainedSdkGateFailure")
            .field("reports", &self.fixture.controller.report_count())
            .field("client_success_and_marker", &client)
            .field(
                "setup_failure",
                &self.fixture.controller.has_setup_failure(),
            )
            .field("covered_results", &reports)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for GateFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("retained SDK gate failed after covered original reports")
    }
}

impl Error for GateFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.fixture
            .client
            .as_ref()
            .and_then(|result| result.as_ref().err().map(|error| error.as_ref()))
    }
}

fn completed_gate_failed(fixture: &fixture::Fixture) -> bool {
    let client_failed = match fixture.client.as_ref() {
        Some(Ok(output)) => !output.status.success() || !success_marker_present(&output.stdout),
        Some(Err(_)) | None => true,
    };
    client_failed
        || fixture.controller.has_setup_failure()
        || fixture.controller.report_count() != fixture.controller.lifetime
        || fixture.controller.lifetime == 0
        || fixture
            .controller
            .reports
            .iter()
            .flatten()
            .any(unexpected_report_failure)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires dotnet and a NuGet restore"]
async fn current_stable_dotnet_client_retained_memory_send_commit_and_rollback() -> TestResult {
    let artifacts = process::build_retained_client().await?;
    let dll = artifacts
        .path()
        .join("bin/Switchyard.Conformance.DotNetCurrent.dll");
    run_gate(&dll).await
}

async fn run_gate(dll: &Path) -> TestResult {
    let mut fixture = fixture::Fixture::start(true).await?;
    let endpoint = fixture.endpoint.clone();
    let ca_file = fixture.ca_file.clone();
    let ca_directory = fixture.ca_directory.clone();
    let client = process::run_retained_client(dll, &endpoint, QUEUE, &ca_file, &ca_directory);
    tokio::pin!(client);
    let observation = std::panic::AssertUnwindSafe(fixture.drive_client(client.as_mut()))
        .catch_unwind()
        .await;
    fixture.finish_with_client(client.as_mut()).await;
    if let Err(payload) = observation {
        std::panic::resume_unwind(payload);
    }
    if let Some(payload) = fixture.client_panic.take() {
        std::panic::resume_unwind(payload);
    }
    if completed_gate_failed(&fixture) {
        return Err(Box::new(GateFailure {
            fixture: Box::new(fixture),
        }));
    }
    for (ordinal, report) in fixture.controller.reports.iter().enumerate() {
        let Some(report) = report else {
            continue;
        };
        assert_eq!(report.anchor().ordinal, ordinal);
        assert!(report.session_attempts() <= 32);
        assert!(report.worker_launches() <= 128);
        assert_eq!(report.session_joins(), report.sessions().len());
        assert_eq!(
            report.session_joins(),
            report
                .admissions()
                .iter()
                .filter(|row| row.launched().is_some())
                .count()
        );
        assert_eq!(report.worker_launches(), report.worker_joins());
        assert_eq!(report.worker_joins(), report.workers().count());
    }
    // Reports/anchors stay owned through every canonical state assertion.
    check_memory_state(&fixture)?;
    Ok(())
}

fn check_memory_state(fixture: &fixture::Fixture) -> TestResult {
    let expected = [
        (1, "retained-warm", "warm", "warm"),
        (2, "retained-commit-a", "commit-a", "commit"),
        (3, "retained-commit-b", "commit-b", "commit"),
    ];
    let store = &fixture.store;
    let namespace = &fixture.namespace;
    let queue = EntityPath::new(QUEUE)?;
    let machine = StateMachine::new(store.clone());
    assert_eq!(
        machine.queue_config(namespace, &queue)?,
        Some(domain::QueueConfig {
            lock_duration_millis: domain::MAX_LOCK_DURATION_MILLIS,
            default_time_to_live_millis: None,
            ..domain::QueueConfig::default()
        })
    );
    let counters = store
        .get(&keys::queue_counters(namespace, &queue))?
        .expect("retained SDK canonical queue counters");
    let counters: QueueCounters = domain::codec::decode(&counters)?;
    assert_eq!(counters.next_sequence, 4);
    assert_eq!(counters.next_lock_token, 1);
    let rows = store.scan_prefix(&keys::message_prefix(namespace, &queue), expected.len() + 1)?;
    let expected_keys = expected
        .iter()
        .map(|(sequence, _, _, _)| keys::message(namespace, &queue, SequenceNumber::new(*sequence)))
        .collect::<BTreeSet<_>>();
    assert_eq!(rows.len(), expected.len());
    assert_eq!(
        rows.iter()
            .map(|(key, _)| key.clone())
            .collect::<BTreeSet<_>>(),
        expected_keys
    );
    let mut ready = BTreeSet::new();
    let mut expiry = BTreeSet::new();
    for (number, id, body, phase) in expected {
        let sequence = SequenceNumber::new(number);
        let record = machine
            .message(namespace, &queue, sequence)?
            .expect("exact retained SDK row");
        assert_eq!(record.sequence, sequence);
        assert_eq!(record.state, MessageState::Ready);
        assert_eq!(record.delivery_count, 0);
        assert_eq!(record.message_id, id);
        assert_eq!(record.body, body.as_bytes());
        assert!(record.session_id.is_none());
        assert!(record.dead_letter.is_none());
        assert!(record.scheduled_enqueue_time.is_none());
        let envelope = record
            .envelope
            .as_ref()
            .expect("typed SDK producer content");
        assert_eq!(
            envelope.body,
            MessageBody::Data(vec![body.as_bytes().to_vec()])
        );
        assert_eq!(
            envelope.properties.message_id,
            Some(MessageIdentifier::String(id.into()))
        );
        assert_eq!(
            envelope.properties.correlation_id,
            Some(MessageIdentifier::String(format!("{id}-correlation")))
        );
        assert_eq!(envelope.properties.subject.as_deref(), Some("retained-sdk"));
        assert_eq!(
            envelope.properties.content_type.as_deref(),
            Some("text/plain")
        );
        assert_eq!(envelope.application_properties.len(), 3);
        assert_eq!(
            envelope.application_properties.get("phase"),
            Some(&MessageValue::String(phase.into()))
        );
        assert_eq!(
            envelope.application_properties.get("number"),
            Some(&MessageValue::Long(42))
        );
        assert_eq!(
            envelope.application_properties.get("enabled"),
            Some(&MessageValue::Bool(true))
        );
        ready.insert(keys::ready(namespace, &queue, sequence));
        if let Some(expires_at) = record.expires_at {
            assert!(expires_at >= record.enqueued_at);
            expiry.insert(keys::expiry(namespace, &queue, expires_at, sequence));
        }
    }
    for (prefix, expected) in [
        (keys::ready_prefix(namespace, &queue), ready),
        (keys::expiry_prefix(namespace, &queue), expiry),
    ] {
        let rows = store.scan_prefix(&prefix, expected.len() + 1)?;
        assert_eq!(rows.len(), expected.len());
        assert_eq!(
            rows.iter()
                .map(|(key, _)| key.clone())
                .collect::<BTreeSet<_>>(),
            expected
        );
        assert!(rows.iter().all(|(_, value)| value.is_empty()));
    }
    let shadow = queue.dead_letter_queue()?;
    for prefix in [
        keys::lock_prefix(namespace, &queue),
        keys::scheduled_prefix(namespace, &queue),
        keys::session_lock_prefix(namespace, &queue),
        keys::entity_session_prefix(namespace, &queue),
        keys::duplicate_history_prefix(namespace, &queue),
        keys::duplicate_history_expiry_prefix(namespace, &queue),
        keys::message_prefix(namespace, &shadow),
        keys::ready_prefix(namespace, &shadow),
        keys::lock_prefix(namespace, &shadow),
        keys::expiry_prefix(namespace, &shadow),
        keys::scheduled_prefix(namespace, &shadow),
        keys::session_lock_prefix(namespace, &shadow),
    ] {
        assert!(store.scan_prefix(&prefix, 1)?.is_empty());
    }
    Ok(())
}

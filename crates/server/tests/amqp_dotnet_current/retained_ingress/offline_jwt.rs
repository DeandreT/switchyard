//! Current-SDK conformance gate for offline JWT authorization over raw TLS.

use super::*;

const OFFLINE_SUCCESS: &str = "official .NET offline JWT Memory TLS send and LISTEN denial passed";
// Public test modulus matching OfflineJwtCases.cs, not an operational credential.
const MODULUS: &str = "yRE6rHuNR0QbHO3H3Kt2pOKGVhQqGZXInOduQNxXzuKlvQTLUTv4l4sggh5_CYYi_cvI-SXVT9kPWSKXxJXBXd_4LkvcPuUakBoAkfh-eiFVMh2VrUyWyj3MFl0HTVF9KwRXLAcwkREiS3npThHRyIxuy0ZMeZfxVL5arMhw1SRELB8HoGfG_AtH89BIE9jDBHZ9dLelK9a184zAf8LwoPLxvJb3Il5nncqPcSfKDDodMFBIMc4lQzDKL5gvmiXLXB1AGLm8KBjfE8s3L5xqi-yUod-j8MtvIj812dkS4QMiRVN_by2h3ZY8LYVGrqZXZTcgn2ujn8uKjXLZVD5TdQ";

fn offline_policy() -> TestResult<auth::JwtPolicy> {
    Ok(auth::JwtPolicy::from_json(&format!(
        r#"{{"version":1,"issuer":"https://issuer.example/","audience":"urn:switchyard:tenant","keys":[{{"kid":"key-1","kty":"RSA","alg":"RS256","use":"sig","n":"{MODULUS}","e":"AQAB"}}],"bindings":[{{"subject":"producer","scope":"amqps://{HOST}/{QUEUE}","permissions":["send"]}}]}}"#,
    ))?)
}

fn offline_marker(stdout: &str) -> bool {
    stdout
        .split_inclusive('\n')
        .filter(|line| {
            line.strip_suffix('\n')
                .map(|line| line.strip_suffix('\r').unwrap_or(line) == OFFLINE_SUCCESS)
                .unwrap_or(false)
        })
        .count()
        == 1
}

struct OfflineGateFailure {
    fixture: Box<fixture::Fixture>,
}

impl fmt::Debug for OfflineGateFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reports: [_; fixture::CONNECTION_HISTORY] = std::array::from_fn(|index| {
            self.fixture.controller.reports[index]
                .as_ref()
                .map(RedactedReport)
        });
        let client = self
            .fixture
            .client
            .as_ref()
            .and_then(|result| result.as_ref().ok())
            .map(|output| (output.status.success(), offline_marker(&output.stdout)));
        let runner = self.fixture.client.as_ref().and_then(|result| {
            result
                .as_ref()
                .err()
                .and_then(|error| process::offline_jwt_failure_summary(error.as_ref()))
        });
        formatter
            .debug_struct("OfflineJwtSdkGateFailure")
            .field("client_success_and_marker", &client)
            .field("client_failure", &runner)
            .field(
                "setup_failure",
                &self.fixture.controller.has_setup_failure(),
            )
            .field("covered_results", &reports)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for OfflineGateFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("offline JWT SDK gate failed after covered original reports")
    }
}

impl Error for OfflineGateFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.fixture
            .client
            .as_ref()
            .and_then(|result| result.as_ref().err().map(|error| error.as_ref()))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires dotnet and a NuGet restore"]
async fn current_stable_dotnet_client_uses_offline_jwt_for_tls_send_and_listen_denial() -> TestResult
{
    let artifacts = process::build_offline_jwt_client(CURRENT_SDK).await?;
    let dll = artifacts
        .path()
        .join("bin/Switchyard.Conformance.DotNetCurrent.dll");
    let mut fixture = fixture::Fixture::start_with_offline_jwt(offline_policy()?).await?;
    let endpoint = fixture.endpoint.clone();
    let ca_file = fixture.ca_file.clone();
    let ca_directory = fixture.ca_directory.clone();
    let client = process::run_offline_jwt_client(&dll, &endpoint, QUEUE, &ca_file, &ca_directory);
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
    let client_ok = fixture.client.as_ref().is_some_and(|result| {
        result
            .as_ref()
            .is_ok_and(|output| output.status.success() && offline_marker(&output.stdout))
    });
    if !client_ok
        || fixture.controller.has_setup_failure()
        || fixture.controller.lifetime == 0
        || fixture.controller.report_count() != fixture.controller.lifetime
        || fixture
            .controller
            .reports
            .iter()
            .flatten()
            .any(unexpected_report_failure)
    {
        return Err(Box::new(OfflineGateFailure {
            fixture: Box::new(fixture),
        }) as Box<dyn Error>);
    }
    for (ordinal, report) in fixture.controller.reports.iter().enumerate() {
        let Some(report) = report else { continue };
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
    check_offline_memory_state(&fixture)?;
    Ok(())
}

fn check_offline_memory_state(fixture: &fixture::Fixture) -> TestResult {
    // Original reports/anchors remain held through all canonical state assertions.
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
    let counters: QueueCounters = domain::codec::decode(
        &store
            .get(&keys::queue_counters(namespace, &queue))?
            .expect("offline JWT queue counters"),
    )?;
    assert_eq!(counters.next_sequence, 2);
    assert_eq!(counters.next_lock_token, 1);
    let sequence = SequenceNumber::new(1);
    let rows = store.scan_prefix(&keys::message_prefix(namespace, &queue), 2)?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, keys::message(namespace, &queue, sequence));
    let record = machine
        .message(namespace, &queue, sequence)?
        .expect("offline JWT original message");
    assert_eq!(record.sequence, sequence);
    assert_eq!(record.state, MessageState::Ready);
    assert_eq!(record.delivery_count, 0);
    assert_eq!(record.message_id, "offline-jwt-send");
    assert_eq!(record.body, b"offline-jwt-body");
    assert!(record.session_id.is_none());
    assert!(record.dead_letter.is_none());
    assert!(record.scheduled_enqueue_time.is_none());
    let envelope = record
        .envelope
        .as_ref()
        .expect("offline JWT typed producer content");
    assert_eq!(
        envelope.body,
        MessageBody::Data(vec![b"offline-jwt-body".to_vec()])
    );
    assert_eq!(
        envelope.properties.message_id,
        Some(MessageIdentifier::String("offline-jwt-send".into()))
    );
    assert_eq!(
        envelope.properties.correlation_id,
        Some(MessageIdentifier::String("offline-jwt-correlation".into()))
    );
    assert_eq!(envelope.properties.subject.as_deref(), Some("offline-jwt"));
    assert_eq!(
        envelope.properties.content_type.as_deref(),
        Some("text/plain")
    );
    assert!(envelope.application_properties.is_empty());
    let ready = store.scan_prefix(&keys::ready_prefix(namespace, &queue), 2)?;
    assert_eq!(
        ready,
        vec![(keys::ready(namespace, &queue, sequence), Vec::new())]
    );
    let expiry = store.scan_prefix(&keys::expiry_prefix(namespace, &queue), 2)?;
    let expected_expiry = record
        .expires_at
        .map(|deadline| {
            vec![(
                keys::expiry(namespace, &queue, deadline, sequence),
                Vec::new(),
            )]
        })
        .unwrap_or_default();
    assert_eq!(expiry, expected_expiry);
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

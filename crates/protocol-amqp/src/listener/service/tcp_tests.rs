use super::test_support::{Actor, OBSERVER, Observer, Point, QuietBroker, WAIT, drive, owner};
use super::*;
use amqp::{
    Attach, Begin, Body, Frame, Message, MessageId, Open, Performative, Properties, ProtocolHeader,
    ReceiverSettleMode, Role, SenderSettleMode, Target, Transfer, encode_message, read_frame,
    read_protocol_header, write_frame, write_protocol_header,
};
use std::{
    future::poll_fn,
    panic::{catch_unwind, panic_any},
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    task::Poll,
};
use storage::StateStore;
use tokio::{sync::oneshot, time::timeout};
use tracing::instrument::WithSubscriber;

#[derive(Debug)]
struct RawError(Arc<String>);
impl fmt::Display for RawError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str(&self.0)
    }
}
impl Error for RawError {}
fn marker(name: &str) -> Arc<String> {
    Arc::new(name.to_owned())
}
fn boxed(raw: &Arc<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(RawError(Arc::clone(raw)))
}
fn address(error: &(dyn Error + Send + Sync)) -> usize {
    std::ptr::from_ref(error) as *const () as usize
}
fn peer() -> SocketAddr {
    "127.0.0.1:1".parse().unwrap()
}

async fn pending_once<F: Future + ?Sized>(mut future: Pin<&mut F>) {
    assert!(
        tokio::task::unconstrained(poll_fn(|context| Poll::Ready(
            future.as_mut().poll(context)
        )))
        .await
        .is_pending()
    );
}

async fn joined_error(raw: Arc<String>) -> JoinError {
    tokio::spawn(async move {
        panic_any(raw);
    })
    .await
    .unwrap_err()
}

#[derive(Clone, Copy, Debug)]
enum Rank {
    Parent,
    Accept,
    Join,
    Returned,
    Missing,
    Report,
    Diagnostic,
    Success,
}
const RANKS: [Rank; 8] = [
    Rank::Parent,
    Rank::Accept,
    Rank::Join,
    Rank::Returned,
    Rank::Missing,
    Rank::Report,
    Rank::Diagnostic,
    Rank::Success,
];
struct Expected {
    raw: Arc<String>,
    box_address: Option<usize>,
    id: Option<Id>,
}

async fn install(
    primary: &mut Option<Primary>,
    failures: &mut FamilyFailures,
    rank: Rank,
) -> Expected {
    let raw = marker(&format!("qualified {rank:?}"));
    let mut expected = Expected {
        raw: Arc::clone(&raw),
        box_address: None,
        id: None,
    };
    match rank {
        Rank::Parent => *primary = Some(Err(Box::new(raw))),
        Rank::Accept => *primary = Some(Ok(Err(io::Error::other(RawError(raw))))),
        Rank::Join => {
            let error = joined_error(raw).await;
            expected.id = Some(error.id());
            failures.join = Some(error);
        }
        Rank::Returned => {
            let error = boxed(&raw);
            expected.box_address = Some(address(&*error));
            failures.returned = Some(error);
        }
        Rank::Missing => {
            let task = tokio::spawn(async {});
            expected.id = Some(task.id());
            task.await.unwrap();
            failures.missing = expected.id;
        }
        Rank::Report => failures.report = Some(Box::new(raw)),
        Rank::Diagnostic => failures.diagnostic = Some(Box::new(raw)),
        Rank::Success => {}
    }
    expected
}

fn selected(rank: Rank, result: std::thread::Result<AmqpListenerExit>, expected: Expected) {
    match (rank, result) {
        (Rank::Parent, Err(payload)) => assert!(Arc::ptr_eq(
            &payload.downcast::<Arc<String>>().unwrap(),
            &expected.raw
        )),
        (Rank::Accept, Ok(AmqpListenerExit::Complete(Err(AmqpListenerFailure::Accept(error))))) => {
            assert!(Arc::ptr_eq(
                &error
                    .get_ref()
                    .unwrap()
                    .downcast_ref::<RawError>()
                    .unwrap()
                    .0,
                &expected.raw
            ));
        }
        (Rank::Join, Ok(AmqpListenerExit::Complete(Err(AmqpListenerFailure::Join(error))))) => {
            assert_eq!(Some(error.id()), expected.id);
            assert!(Arc::ptr_eq(
                &error.into_panic().downcast::<Arc<String>>().unwrap(),
                &expected.raw
            ));
        }
        (
            Rank::Returned,
            Ok(AmqpListenerExit::Complete(Err(AmqpListenerFailure::Returned(error)))),
        ) => {
            assert_eq!(Some(address(&*error)), expected.box_address);
            assert!(Arc::ptr_eq(
                &error.downcast_ref::<RawError>().unwrap().0,
                &expected.raw
            ));
        }
        (
            Rank::Missing,
            Ok(AmqpListenerExit::Complete(Err(AmqpListenerFailure::MissingExit(id)))),
        ) => assert_eq!(Some(id), expected.id),
        (Rank::Report | Rank::Diagnostic, Ok(AmqpListenerExit::ReportOnly(payload))) => assert!(
            Arc::ptr_eq(&payload.downcast::<Arc<String>>().unwrap(), &expected.raw)
        ),
        (Rank::Success, Ok(AmqpListenerExit::Complete(Ok(())))) => {}
        _ => panic!("wrong selected qualified category {rank:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn qualified_listener_terminal_priority_preserves_each_raw_category() {
    // These constructed parts do not claim an actual socket accept failure.
    for (index, rank) in RANKS.iter().copied().enumerate() {
        let mut primary = None;
        let mut failures = FamilyFailures::new();
        let expected = install(&mut primary, &mut failures, rank).await;
        selected(
            rank,
            catch_unwind(AssertUnwindSafe(|| resolve_terminal(primary, failures))),
            expected,
        );
        for lower in RANKS.iter().copied().skip(index + 1) {
            if matches!((rank, lower), (Rank::Parent, Rank::Accept)) {
                continue;
            }
            let mut primary = None;
            let mut failures = FamilyFailures::new();
            let expected = install(&mut primary, &mut failures, rank).await;
            install(&mut primary, &mut failures, lower).await;
            selected(
                rank,
                catch_unwind(AssertUnwindSafe(|| resolve_terminal(primary, failures))),
                expected,
            );
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn qualified_bridge_keeps_returned_join_error_distinct_from_original_outer_join() {
    let inner = joined_error(marker("nested returned JoinError")).await;
    let inner_id = inner.id();
    let returned: Box<dyn Error + Send + Sync> = Box::new(inner);
    let original_address = address(&*returned);
    let receipt = PreparedTask::new().spawn(
        peer(),
        std::future::ready(ConnectionTaskExit::Complete(Err(returned))),
    );
    let original_id = receipt.id;
    let mut family = ConnectionFamily::new();
    family.adopt(receipt);
    family.finish().await;
    assert_eq!(family.finished()[0].id, original_id);
    assert!(
        family.finished()[0].joined.is_ok(),
        "typed return leaves outer unit join successful"
    );
    let AmqpListenerExit::Complete(Err(AmqpListenerFailure::Returned(error))) =
        resolve_terminal(None, family.take_failures())
    else {
        panic!("typed returned category");
    };
    assert_eq!(address(&*error), original_address);
    assert_eq!(error.downcast_ref::<JoinError>().unwrap().id(), inner_id);
}

#[tokio::test(flavor = "current_thread")]
async fn qualified_missing_packet_and_report_bridge_keep_original_ids_and_boxes() {
    let raw = marker("same known report box");
    let payload: PanicPayload = Box::new(Arc::clone(&raw));
    let original_address = std::ptr::from_ref(&*payload) as *const () as usize;
    let report = PreparedTask::new().spawn(
        peer(),
        std::future::ready(ConnectionTaskExit::ReportOnly(payload)),
    );
    let report_id = report.id;
    let mut family = ConnectionFamily::new();
    family.adopt(report);
    family.finish().await;
    assert_eq!(family.finished()[0].id, report_id);
    assert!(family.finished()[0].joined.is_ok());
    let AmqpListenerExit::ReportOnly(payload) = resolve_terminal(None, family.take_failures())
    else {
        panic!("known report remains diagnostic");
    };
    assert_eq!(
        std::ptr::from_ref(&*payload) as *const () as usize,
        original_address
    );
    assert!(Arc::ptr_eq(
        &payload.downcast::<Arc<String>>().unwrap(),
        &raw
    ));

    // Deliberately composed broken producer, not a reached live bridge fault.
    let (sender, exit) = oneshot::channel();
    drop(sender);
    let task = tokio::spawn(async {});
    let id = task.id();
    let mut family = ConnectionFamily::new();
    family.adopt(AdmittedConnectionTask {
        id,
        peer: peer(),
        task,
        exit,
        retirement: AmqpListenerRetirement::new(),
    });
    family.finish().await;
    assert!(
        matches!(resolve_terminal(None, family.take_failures()), AmqpListenerExit::Complete(Err(AmqpListenerFailure::MissingExit(found))) if found == id)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn qualified_partial_family_drain_retains_first_box_across_two_borrower_cancellations() {
    let raw = marker("first original returned error");
    let error = boxed(&raw);
    let error_address = address(&*error);
    let first = PreparedTask::new().spawn(
        peer(),
        std::future::ready(ConnectionTaskExit::Complete(Err(error))),
    );
    let first_id = first.id;
    let (release, held) = oneshot::channel();
    let second = PreparedTask::new().spawn(peer(), async {
        held.await.unwrap();
        ConnectionTaskExit::Complete(Ok(()))
    });
    let second_id = second.id;
    let second_request = second.retirement.clone();
    let mut family = ConnectionFamily::new();
    family.adopt(first);
    family.adopt(second);
    let observer = Arc::new(Observer::default());
    OBSERVER.scope(Arc::clone(&observer), async {
        let drain = family.finish();
        tokio::pin!(drain);
        tokio::select! { biased; () = observer.joined(first_id) => {}, () = &mut drain => panic!("held original cannot finish"), }
    }).await;
    assert!(
        second_request.is_requested(),
        "all originals requested before first await"
    );
    assert_eq!(family.pending_ids(), vec![second_id]);
    let packet_address = std::ptr::from_ref(&*family.finished()[0]) as usize;
    {
        let drain = family.finish();
        tokio::pin!(drain);
        pending_once(drain.as_mut()).await;
    }
    assert_eq!(
        std::ptr::from_ref(&*family.finished()[0]) as usize,
        packet_address
    );
    release.send(()).unwrap();
    family.finish().await;
    assert_eq!(family.finished_ids().len(), 2);
    let AmqpListenerExit::Complete(Err(AmqpListenerFailure::Returned(error))) =
        resolve_terminal(None, family.take_failures())
    else {
        panic!("same first failure");
    };
    assert_eq!(address(&*error), error_address);
}

#[tokio::test(flavor = "current_thread")]
async fn actual_tcp_cached_admission_survives_cancelled_borrowers_before_spawn_and_after_spawn() {
    for point in [
        Point::Accepted,
        Point::Prepared,
        Point::ReceiptCached,
        Point::Adopted,
    ] {
        let broker = QuietBroker::default();
        let (mut service, address) = owner(broker.clone()).await;
        let observer = Arc::new(Observer::default());
        observer.hold(point);
        let client = TcpStream::connect(address).await.unwrap();
        drive(&mut service, Arc::clone(&observer), observer.reached(point)).await;
        let facts = observer.facts(point);
        assert!(facts.finished.is_empty());
        match point {
            Point::Accepted | Point::Prepared => {
                assert!(facts.accepted);
                assert!(facts.receipt.is_none() && facts.pending.is_empty());
            }
            Point::ReceiptCached => {
                assert!(!facts.accepted);
                assert!(facts.receipt.is_some() && facts.pending.is_empty());
            }
            Point::Adopted => {
                assert!(!facts.accepted);
                assert!(facts.receipt.is_none());
                assert_eq!(facts.pending.len(), 1);
            }
            Point::Reaped => unreachable!(),
        }
        observer.release();
        service.finish().await;
        if let Some(id) = facts.receipt.or_else(|| facts.pending.first().copied()) {
            assert_eq!(service.family.finished_ids(), vec![id]);
            assert!(service.family.finished()[0].joined.is_ok());
        }
        assert!(broker.calls.lock().unwrap().is_empty());
        assert!(matches!(
            service.take_exit(),
            Some(AmqpListenerExit::Complete(Ok(())))
        ));
        assert!(service.take_exit().is_none());
        drop(client);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_parent_fault_after_receipt_cache_drains_before_resuming_same_payload() {
    let (mut service, address) = owner(QuietBroker::default()).await;
    let observer = Arc::new(Observer::default());
    let raw = marker("same parent fault after original receipt");
    observer.panic_at(Point::ReceiptCached, Arc::clone(&raw));
    let client = TcpStream::connect(address).await.unwrap();
    OBSERVER.scope(Arc::clone(&observer), service.serve()).await;
    let id = observer.facts(Point::ReceiptCached).receipt.unwrap();
    assert!(
        catch_unwind(AssertUnwindSafe(|| service.take_exit())).is_err(),
        "early extraction refuses without consuming primary"
    );
    service.finish().await;
    assert_eq!(service.family.finished_ids(), vec![id]);
    let payload = match catch_unwind(AssertUnwindSafe(|| service.take_exit())) {
        Err(payload) => payload,
        Ok(_) => panic!("original parent panic resumes after drain"),
    };
    assert!(Arc::ptr_eq(
        &payload.downcast::<Arc<String>>().unwrap(),
        &raw
    ));
    assert!(service.take_exit().is_none());
    drop(client);
}

#[tokio::test(flavor = "current_thread")]
async fn qualified_cached_accept_error_survives_two_cancelled_drains_and_early_extraction() {
    let (mut service, _) = owner(QuietBroker::default()).await;
    let raw = marker("cached original accept source");
    let source = boxed(&raw);
    let original_address = address(&*source);
    service.primary = Some(Ok(Err(io::Error::other(source))));
    // Injected raw accept result and typed children, not a socket accept failure.
    let first = PreparedTask::new().spawn(
        peer(),
        std::future::ready(ConnectionTaskExit::ReportOnly(Box::new(marker(
            "lower report",
        )))),
    );
    let first_id = first.id;
    let (release, held) = oneshot::channel();
    service.family.adopt(first);
    service
        .family
        .adopt(PreparedTask::new().spawn(peer(), async {
            held.await.unwrap();
            ConnectionTaskExit::Complete(Ok(()))
        }));
    let observer = Arc::new(Observer::default());
    OBSERVER.scope(Arc::clone(&observer), async {
        let drain = service.finish(); tokio::pin!(drain);
        tokio::select! { biased; () = observer.joined(first_id) => {}, () = &mut drain => panic!("qualified original remains held"), }
    }).await;
    let packet_address = std::ptr::from_ref(&*service.family.finished()[0]) as usize;
    assert!(catch_unwind(AssertUnwindSafe(|| service.take_exit())).is_err());
    {
        let drain = service.finish();
        tokio::pin!(drain);
        pending_once(drain.as_mut()).await;
    }
    assert_eq!(
        std::ptr::from_ref(&*service.family.finished()[0]) as usize,
        packet_address
    );
    release.send(()).unwrap();
    service.finish().await;
    let Some(AmqpListenerExit::Complete(Err(AmqpListenerFailure::Accept(error)))) =
        service.take_exit()
    else {
        panic!("same cached raw accept error");
    };
    assert_eq!(address(error.get_ref().unwrap()), original_address);
    assert!(Arc::ptr_eq(
        &error
            .get_ref()
            .unwrap()
            .downcast_ref::<RawError>()
            .unwrap()
            .0,
        &raw
    ));
    assert!(service.take_exit().is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn actual_empty_and_pre_retired_listener_preserve_one_drained_extraction() {
    let (mut service, _) = owner(QuietBroker::default()).await;
    assert!(service.family.is_empty());
    {
        let admission = service.serve();
        tokio::pin!(admission);
        pending_once(admission.as_mut()).await;
    }
    assert!(service.primary.is_none());
    service.retirement_handle().request();
    service.serve().await;
    assert!(
        service.primary.is_none(),
        "request-only retirement does not invent a primary result"
    );
    service.finish().await;
    service.finish().await;
    assert!(matches!(
        service.take_exit(),
        Some(AmqpListenerExit::Complete(Ok(())))
    ));
    assert!(service.take_exit().is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn actual_active_retirement_leaves_primary_absent_and_drains_same_admitted_original_once() {
    let broker = QuietBroker::default();
    let (mut service, address) = owner(broker.clone()).await;
    let observer = Arc::new(Observer::default());
    let retirement = service.retirement_handle();
    let client = TcpStream::connect(address).await.unwrap();
    timeout(
        WAIT,
        OBSERVER.scope(Arc::clone(&observer), async {
            tokio::join!(service.serve(), async {
                observer.reached(Point::Adopted).await;
                observer.stalled(Stage::Protocol).await;
                retirement.request();
            });
        }),
    )
    .await
    .expect("active original pump observes requested retirement");
    let id = observer.stalled_id(Stage::Protocol);
    assert!(
        service.primary.is_none(),
        "intentional active retirement is not an original primary success"
    );
    assert!(
        service.listener.is_some(),
        "serve returns before mandatory drain"
    );
    assert_eq!(service.family.pending_ids(), vec![id]);
    assert!(catch_unwind(AssertUnwindSafe(|| service.take_exit())).is_err());
    service.finish().await;
    assert_eq!(service.family.finished_ids(), vec![id]);
    assert!(service.family.finished()[0].joined.is_ok());
    assert!(broker.calls.lock().unwrap().is_empty());
    assert!(matches!(
        service.take_exit(),
        Some(AmqpListenerExit::Complete(Ok(())))
    ));
    assert!(service.take_exit().is_none());
    drop(client);
}

fn frame(channel: u16, performative: Performative) -> Frame {
    Frame::Amqp {
        channel,
        performative: Some(performative),
        payload: Vec::new(),
    }
}

async fn open(peer: &mut TcpStream) {
    write_protocol_header(peer, ProtocolHeader::AMQP)
        .await
        .unwrap();
    assert_eq!(
        read_protocol_header(peer).await.unwrap(),
        ProtocolHeader::AMQP
    );
    write_frame(
        peer,
        &frame(0, Performative::Open(Open::new("listener-owner-peer"))),
    )
    .await
    .unwrap();
    assert!(matches!(
        read_frame(peer).await.unwrap(),
        Frame::Amqp {
            channel: 0,
            performative: Some(Performative::Open(_)),
            ..
        }
    ));
}

async fn authenticate(peer: &mut TcpStream) {
    write_protocol_header(peer, ProtocolHeader::SASL)
        .await
        .unwrap();
    assert_eq!(
        read_protocol_header(peer).await.unwrap(),
        ProtocolHeader::SASL
    );
    assert!(matches!(
        read_frame(peer).await.unwrap(),
        Frame::Sasl(amqp::SaslPerformative::Mechanisms(_))
    ));
    write_frame(
        peer,
        &Frame::Sasl(amqp::SaslPerformative::Init(amqp::SaslInit {
            mechanism: amqp::Symbol::from("ANONYMOUS"),
            initial_response: None,
            hostname: None,
        })),
    )
    .await
    .unwrap();
    assert!(matches!(
        read_frame(peer).await.unwrap(),
        Frame::Sasl(amqp::SaslPerformative::Outcome(amqp::SaslOutcome {
            code: amqp::SaslCode::Ok,
            ..
        }))
    ));
}

fn stalled_tls() -> rustls::ServerConfig {
    rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
        .unwrap()
        .with_no_client_auth()
        .with_cert_resolver(Arc::new(rustls::server::ResolvesServerCertUsingSni::new()))
}

#[tokio::test(flavor = "current_thread")]
async fn actual_tls_header_sasl_init_and_open_stalls_retire_same_original_without_broker_work() {
    for scenario in ["tls", "header", "sasl-init", "open"] {
        let broker = QuietBroker::default();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut config = AmqpListener::new(
            broker.clone(),
            domain::NamespaceName::new("tenant").unwrap(),
        );
        if scenario == "tls" {
            config = config.with_tls(stalled_tls());
        }
        if scenario == "sasl-init" {
            config = config.with_shared_access_authentication(
                crate::SharedAccessAuthentication::new(
                    auth::SharedAccessPolicy::new([]).unwrap(),
                    "tenant.example",
                )
                .unwrap(),
            );
        }
        let mut service = config.into_tcp_service(listener);
        let observer = Arc::new(Observer::default());
        let mut client = TcpStream::connect(address).await.unwrap();
        let stage = if scenario == "tls" {
            Stage::Tls
        } else {
            Stage::Protocol
        };
        drive(&mut service, Arc::clone(&observer), async {
            observer.stalled(stage).await;
            if scenario == "sasl-init" {
                write_protocol_header(&mut client, ProtocolHeader::SASL)
                    .await
                    .unwrap();
                assert_eq!(
                    read_protocol_header(&mut client).await.unwrap(),
                    ProtocolHeader::SASL
                );
                assert!(matches!(
                    read_frame(&mut client).await.unwrap(),
                    Frame::Sasl(amqp::SaslPerformative::Mechanisms(_))
                ));
            } else if scenario == "open" {
                write_protocol_header(&mut client, ProtocolHeader::AMQP)
                    .await
                    .unwrap();
                assert_eq!(
                    read_protocol_header(&mut client).await.unwrap(),
                    ProtocolHeader::AMQP
                );
            }
        })
        .await;
        let id = observer.stalled_id(stage);
        assert!(
            service.family.pending_ids().contains(&id)
                || service
                    .receipt
                    .as_ref()
                    .is_some_and(|receipt| receipt.id == id)
        );
        service.retirement_handle().request();
        service.finish().await;
        assert_eq!(service.family.finished_ids(), vec![id]);
        assert!(service.family.finished()[0].joined.is_ok());
        assert!(broker.calls.lock().unwrap().is_empty());
        assert!(matches!(
            service.take_exit(),
            Some(AmqpListenerExit::Complete(Ok(())))
        ));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_native_ready_custody_drains_original_after_pre_authorization_fault() {
    for authenticated in [false, true] {
        let broker = QuietBroker::default();
        let (mut service, address) = owner(broker.clone()).await;
        if authenticated {
            service.config.shared_access_authentication = Some(
                crate::SharedAccessAuthentication::new(
                    auth::SharedAccessPolicy::new([]).unwrap(),
                    "tenant.example",
                )
                .unwrap(),
            );
        }
        let observer = Arc::new(Observer::default());
        let raw = marker("after same native connection installed before authorization");
        observer.panic_after_native(Arc::clone(&raw));
        let mut client = TcpStream::connect(address).await.unwrap();
        drive(&mut service, Arc::clone(&observer), async {
            if authenticated {
                authenticate(&mut client).await;
            }
            open(&mut client).await;
            observer.native().await;
            observer.reached(Point::Reaped).await;
        })
        .await;
        let id = observer.native_ids()[0];
        assert_eq!(observer.facts(Point::Reaped).reaped, Some(id));
        assert!(
            service.family.is_empty(),
            "failed child reaped without stopping admission"
        );
        service.finish().await;
        let Some(AmqpListenerExit::Complete(Err(AmqpListenerFailure::Join(error)))) =
            service.take_exit()
        else {
            panic!("original outer task panic");
        };
        assert_eq!(error.id(), id);
        assert!(Arc::ptr_eq(
            &error.into_panic().downcast::<Arc<String>>().unwrap(),
            &raw
        ));
        assert!(broker.calls.lock().unwrap().is_empty());
    }
}

struct Reporter {
    reached: Arc<AtomicUsize>,
    payload: Arc<String>,
}
impl tracing::Subscriber for Reporter {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target().ends_with("::service::connection_family")
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, _: &tracing::Event<'_>) {
        self.reached.fetch_add(1, Ordering::SeqCst);
        panic_any(Arc::clone(&self.payload));
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

#[tokio::test(flavor = "current_thread")]
async fn actual_child_error_and_reached_reporter_panic_do_not_stop_healthy_admission() {
    let broker = QuietBroker::default();
    let (mut service, address) = owner(broker.clone()).await;
    let observer = Arc::new(Observer::default());
    let reached = Arc::new(AtomicUsize::new(0));
    let dispatch = tracing::Dispatch::new(Reporter {
        reached: Arc::clone(&reached),
        payload: marker("reached narrow listener reporter"),
    });
    let mut bad = TcpStream::connect(address).await.unwrap();
    drive(&mut service, Arc::clone(&observer), async {
        write_protocol_header(&mut bad, ProtocolHeader::SASL)
            .await
            .unwrap();
        observer.reached(Point::Reaped).await;
    })
    .with_subscriber(dispatch)
    .await;
    assert_eq!(reached.load(Ordering::SeqCst), 1);
    assert!(service.family.is_empty());
    let mut healthy = TcpStream::connect(address).await.unwrap();
    drive(&mut service, Arc::clone(&observer), async {
        open(&mut healthy).await;
        observer.native().await;
    })
    .await;
    assert_eq!(
        observer.native_ids().len(),
        1,
        "a later healthy connection still opens"
    );
    service.finish().await;
    assert!(
        matches!(
            service.take_exit(),
            Some(AmqpListenerExit::Complete(Err(
                AmqpListenerFailure::Returned(_)
            )))
        ),
        "returned original error outranks reached diagnostic"
    );
    assert!(broker.calls.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn actual_healthy_live_reaping_prunes_history_while_another_handshake_is_held() {
    let (mut service, address) = owner(QuietBroker::default()).await;
    let observer = Arc::new(Observer::default());
    let held = TcpStream::connect(address).await.unwrap();
    drive(
        &mut service,
        Arc::clone(&observer),
        observer.stalled(Stage::Protocol),
    )
    .await;
    let held_id = observer.stalled_id(Stage::Protocol);
    let mut healthy = TcpStream::connect(address).await.unwrap();
    drive(&mut service, Arc::clone(&observer), async {
        open(&mut healthy).await;
        write_frame(
            &mut healthy,
            &frame(0, Performative::Close(amqp::Close::default())),
        )
        .await
        .unwrap();
        assert!(matches!(
            read_frame(&mut healthy).await.unwrap(),
            Frame::Amqp {
                performative: Some(Performative::Close(_)),
                ..
            }
        ));
        observer.reached(Point::Reaped).await;
    })
    .await;
    let facts = observer.facts(Point::Reaped);
    assert_ne!(facts.reaped, Some(held_id));
    assert!(
        facts.finished.is_empty(),
        "healthy live joins do not accumulate"
    );
    assert_eq!(service.family.pending_ids(), vec![held_id]);
    service.finish().await;
    assert_eq!(service.family.finished_ids(), vec![held_id]);
    assert!(matches!(
        service.take_exit(),
        Some(AmqpListenerExit::Complete(Ok(())))
    ));
    drop(held);
}

async fn sender(peer: &mut TcpStream) {
    write_frame(peer, &frame(1, Performative::Begin(Begin::default())))
        .await
        .unwrap();
    assert!(matches!(
        read_frame(peer).await.unwrap(),
        Frame::Amqp {
            performative: Some(Performative::Begin(_)),
            ..
        }
    ));
    write_frame(
        peer,
        &frame(
            1,
            Performative::Attach(Box::new(Attach {
                name: "listener-send".to_owned(),
                handle: 1,
                role: Role::Sender,
                snd_settle_mode: SenderSettleMode::Unsettled,
                rcv_settle_mode: ReceiverSettleMode::First,
                source: None,
                target: Some(Target::new("orders")),
                unsettled: None,
                incomplete_unsettled: false,
                initial_delivery_count: Some(0),
                max_message_size: None,
                offered_capabilities: None,
                desired_capabilities: None,
                properties: None,
            })),
        ),
    )
    .await
    .unwrap();
    assert!(matches!(
        read_frame(peer).await.unwrap(),
        Frame::Amqp {
            performative: Some(Performative::Attach(_)),
            ..
        }
    ));
    assert!(matches!(
        read_frame(peer).await.unwrap(),
        Frame::Amqp {
            performative: Some(Performative::Flow(_)),
            ..
        }
    ));
}

async fn send(peer: &mut TcpStream) {
    let message = Message {
        properties: Some(Properties {
            message_id: Some(MessageId::String("held-send".to_owned())),
            ..Properties::default()
        }),
        body: Body::Data(vec![b"held-send".to_vec().into()]),
        ..Message::default()
    };
    write_frame(
        peer,
        &Frame::Amqp {
            channel: 1,
            performative: Some(Performative::Transfer(Transfer {
                handle: 1,
                delivery_id: Some(0),
                delivery_tag: Some(vec![0].into()),
                message_format: Some(0),
                settled: Some(false),
                more: false,
                rcv_settle_mode: None,
                state: None,
                resume: false,
                aborted: false,
                batchable: false,
            })),
            payload: encode_message(&message).unwrap(),
        },
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_memory_and_fjall_send_drain_survives_two_cancellations_before_and_after_commit() {
    for durable in [false, true] {
        for after in [false, true] {
            let mut actor = Actor::new(durable);
            let key = actor.key();
            actor.gate.arm(key.clone());
            let (mut service, address) = owner(actor.broker.as_ref().unwrap().clone()).await;
            let observer = Arc::new(Observer::default());
            let mut quiet = TcpStream::connect(address).await.unwrap();
            drive(&mut service, Arc::clone(&observer), async {
                open(&mut quiet).await;
                observer.native().await;
            })
            .await;
            let quiet_id = observer.native_ids()[0];
            let mut sending = TcpStream::connect(address).await.unwrap();
            drive(&mut service, Arc::clone(&observer), async {
                open(&mut sending).await;
                sender(&mut sending).await;
                send(&mut sending).await;
                actor.gate.reached(false).await;
                if after {
                    actor.gate.release(false);
                    actor.gate.reached(true).await;
                }
            })
            .await;
            let ids = observer.native_ids();
            assert_eq!(ids.len(), 2);
            let held_id = ids[1];
            assert_eq!(actor.store().get(&key).unwrap().is_some(), after);
            OBSERVER.scope(Arc::clone(&observer), async {
                let drain = service.finish(); tokio::pin!(drain);
                tokio::select! { biased; () = observer.joined(quiet_id) => {}, () = &mut drain => panic!("Send is positively held in store apply"), }
            }).await;
            assert_eq!(service.family.pending_ids(), vec![held_id]);
            assert_eq!(service.family.finished_ids(), vec![quiet_id]);
            let first_packet = std::ptr::from_ref(&*service.family.finished()[0]) as usize;
            assert!(
                service.listener.is_none(),
                "socket retires before drain waits"
            );
            assert!(catch_unwind(AssertUnwindSafe(|| service.take_exit())).is_err());
            {
                let drain = service.finish();
                tokio::pin!(drain);
                pending_once(drain.as_mut()).await;
            }
            assert_eq!(
                std::ptr::from_ref(&*service.family.finished()[0]) as usize,
                first_packet
            );
            actor.gate.release(false);
            actor.gate.reached(true).await;
            actor.gate.release(true);
            timeout(WAIT, service.finish())
                .await
                .expect("released original Send drains");
            assert_eq!(service.family.finished_ids().len(), 2);
            assert!(
                service
                    .family
                    .finished()
                    .iter()
                    .all(|packet| packet.joined.is_ok())
            );
            assert_eq!(actor.gate.commits(), 1);
            {
                let submissions = actor.log.lock().unwrap();
                let sends: Vec<_> = submissions
                    .iter()
                    .filter(|entry| matches!(entry.kind, domain::CommandKind::Send { .. }))
                    .collect();
                assert_eq!(sends.len(), 2, "one submission and one return");
                assert!(!sends[0].returned && sends[1].returned);
                assert_eq!(sends[0].worker, sends[1].worker);
            }
            assert!(matches!(
                service.take_exit(),
                Some(AmqpListenerExit::Complete(Ok(())))
            ));
            drop(service);
            drop(sending);
            drop(quiet);
            let committed = actor.store().get(&key).unwrap().unwrap();
            actor.reopen();
            assert_eq!(actor.store().get(&key).unwrap(), Some(committed));
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn qualified_legacy_adapter_keeps_accept_source_and_ignores_child_and_report_outcomes() {
    let raw = marker("same accept source through legacy adapter");
    let source: Box<dyn Error + Send + Sync> = boxed(&raw);
    let original_address = address(&*source);
    let error = io::Error::other(source);
    let error = legacy_result(AmqpListenerExit::Complete(Err(
        AmqpListenerFailure::Accept(error),
    )))
    .unwrap_err();
    assert_eq!(address(error.get_ref().unwrap()), original_address);
    assert!(Arc::ptr_eq(
        &error
            .get_ref()
            .unwrap()
            .downcast_ref::<RawError>()
            .unwrap()
            .0,
        &raw
    ));
    for rank in RANKS.into_iter().skip(2) {
        let mut primary = None;
        let mut failures = FamilyFailures::new();
        install(&mut primary, &mut failures, rank).await;
        assert!(legacy_result(resolve_terminal(primary, failures)).is_ok());
    }
}

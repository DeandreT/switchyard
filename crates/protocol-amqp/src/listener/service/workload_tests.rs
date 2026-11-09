//! Real, privately trusted TCP/WSS mTLS and original-task/storage witnesses.

use std::{future::Future, sync::Arc};

use amqp::{
    Body, ClientConnection as Connection, ClientReceiver as Receiver, ClientSender as Sender,
    ClientSession as Session, Message, OrderedMap, Outcome, Value,
};
use auth::{Permission, ResourceScope, SharedAccessPolicy};
use domain::{CommandKind, EntityPath, NamespaceName, SequenceNumber};
use sha2::{Digest, Sha256};
use storage::StateStore;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::timeout,
};

use super::{
    jwt_tests::{self, put_token},
    test_support::{self, WAIT},
    *,
};
use crate::{
    BrokerRejection, SharedAccessAuthentication, WorkloadListenerError,
    cbs::{JWT_TOKEN_TYPE, SAS_TOKEN_TYPE},
};

mod fixture;
use fixture::*;

fn message(text: &str) -> Message {
    Message::builder()
        .body(Body::Data(vec![text.as_bytes().to_vec().into()]))
        .build()
}

async fn send(session: &mut Session, name: &str, entity: &str) {
    let mut sender = Sender::attach(session, name, entity).await.unwrap();
    assert!(matches!(
        sender.send(message(name)).await.unwrap(),
        Outcome::Accepted(_)
    ));
    sender.close().await.unwrap();
}

async fn refused(session: &mut Session, name: &str, entity: &str) {
    let mut sender = Sender::attach(session, name, entity).await.unwrap();
    assert!(sender.send(message("denied")).await.is_err());
}

async fn close(mut connection: Connection) {
    let _ = connection.close().await;
    timeout(WAIT, connection.on_close()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn certificate_only_tcp_and_wss_enforce_locally_mapped_rights_and_socket_isolation() {
    for websocket in [false, true] {
        let tls = TlsFixture::new(30);
        let owner = owner();
        owner
            .handle()
            .submit_blocking(
                NamespaceName::new("tenant").unwrap(),
                EntityPath::new("events").unwrap(),
                CommandKind::CreateTopic {
                    config: domain::TopicConfig::default(),
                },
            )
            .unwrap();
        owner
            .handle()
            .submit_blocking(
                NamespaceName::new("tenant").unwrap(),
                EntityPath::new("events").unwrap(),
                CommandKind::CreateSubscription {
                    name: domain::SubscriptionName::new("consumer").unwrap(),
                    config: domain::SubscriptionConfig::default(),
                },
            )
            .unwrap();
        let recording = Recording::new(HandleBroker(owner.handle()));
        let calls = recording.calls.clone();
        let mut node = Node::new(recording, tls.endpoint(false, 60), None, websocket).await;
        let address = node.address;
        node.exercise(async {
            let producer = tls
                .connect(address, websocket, Some(&tls.sender))
                .await
                .unwrap();
            let consumer = tls
                .connect(address, websocket, Some(&tls.listener))
                .await
                .unwrap();
            let manager = tls
                .connect(address, websocket, Some(&tls.manager))
                .await
                .unwrap();
            let mut producer = producer;
            let mut consumer = consumer;
            let mut manager = manager;
            let mut ps = Session::begin(&mut producer).await.unwrap();
            let mut cs = Session::begin(&mut consumer).await.unwrap();
            let mut ms = Session::begin(&mut manager).await.unwrap();
            refused(&mut ps, "sibling-denied", "orders-archive").await;
            refused(&mut ps, "foreign-denied", "amqps://other.example/orders").await;
            refused(&mut cs, "listener-send-denied", "orders").await;
            assert!(
                calls.lock().unwrap().is_empty(),
                "refusals precede any command"
            );
            send(
                &mut ps,
                "producer",
                "amqps://tenant.servicebus.windows.net/ORDERS",
            )
            .await;
            let mut receiver = Receiver::attach(&mut cs, "consumer", "orders")
                .await
                .unwrap();
            let delivery = receiver.recv().await.unwrap();
            assert_eq!(delivery.message().body, message("producer").body);
            receiver.accept(&delivery).await.unwrap();
            receiver.close().await.unwrap();
            let mut denied_receiver = Receiver::attach(&mut ps, "producer-no-listen", "orders")
                .await
                .unwrap();
            assert!(denied_receiver.recv().await.is_err());
            let mut responses = Receiver::builder()
                .name("send-management-reply")
                .source("orders/$management")
                .target("management-reply")
                .attach(&mut ps)
                .await
                .unwrap();
            let mut requests = Sender::attach(&mut ps, "send-management", "orders/$management")
                .await
                .unwrap();
            assert_eq!(
                jwt_tests::management(
                    &mut requests,
                    &mut responses,
                    crate::SCHEDULE_MESSAGE_OPERATION,
                    jwt_tests::schedule_body()
                )
                .await,
                200
            );
            let mut cancel = OrderedMap::new();
            cancel.insert(
                Value::String("sequence-numbers".into()),
                Value::Array(amqp::Array::from(vec![Value::Long(2)])),
            );
            assert_eq!(
                jwt_tests::management(
                    &mut requests,
                    &mut responses,
                    crate::CANCEL_SCHEDULED_MESSAGE_OPERATION,
                    cancel
                )
                .await,
                200
            );
            let mut peek = OrderedMap::new();
            peek.insert(Value::String("from-sequence-number".into()), Value::Long(0));
            peek.insert(Value::String("message-count".into()), Value::Int(1));
            assert_eq!(
                jwt_tests::management(
                    &mut requests,
                    &mut responses,
                    crate::PEEK_MESSAGE_OPERATION,
                    peek
                )
                .await,
                401
            );
            let mut rules = OrderedMap::new();
            rules.insert(Value::String("skip".into()), Value::Int(0));
            rules.insert(Value::String("top".into()), Value::Int(10));
            assert_eq!(
                jwt_tests::management(
                    &mut requests,
                    &mut responses,
                    crate::ENUMERATE_RULES_OPERATION,
                    rules.clone()
                )
                .await,
                401
            );
            let mut replies = Receiver::builder()
                .name("manager-reply")
                .source("events/Subscriptions/consumer/$management")
                .target("management-reply")
                .attach(&mut ms)
                .await
                .unwrap();
            let mut manages = Sender::attach(
                &mut ms,
                "manager-request",
                "events/Subscriptions/consumer/$management",
            )
            .await
            .unwrap();
            assert_eq!(
                jwt_tests::management(
                    &mut manages,
                    &mut replies,
                    crate::ENUMERATE_RULES_OPERATION,
                    rules
                )
                .await,
                200
            );
            // Namespace manager authority did not migrate onto the concurrently
            // open producer socket when another principal authenticated.
            refused(&mut ps, "still-no-events", "events").await;
            close(producer).await;
            close(consumer).await;
            close(manager).await;
        })
        .await;
        node.finish().await;
        drop(owner);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn optional_absence_requires_tokens_and_has_no_fabricated_certificate_lease() {
    for websocket in [false, true] {
        let tls = TlsFixture::new(30);
        let owner = owner();
        let recording = Recording::new(HandleBroker(owner.handle()));
        let calls = recording.calls.clone();
        let mut node = Node::new(
            recording,
            tls.endpoint(true, 2),
            Some(alternative(false)),
            websocket,
        )
        .await;
        let address = node.address;
        node.exercise(async {
            let mut anonymous = tls.connect(address, websocket, None).await.unwrap();
            let mut session = Session::begin(&mut anonymous).await.unwrap();
            refused(&mut session, "no-token-no-proof", "orders").await;
            assert!(calls.lock().unwrap().is_empty());
            assert_eq!(
                put_token(
                    &mut session,
                    SAS_TOKEN_TYPE,
                    RESOURCE,
                    jwt_tests::sas_token(now() + 600)
                )
                .await,
                202
            );
            send(&mut session, "token-before", "orders").await;
            let mut certified = tls
                .connect(address, websocket, Some(&tls.sender))
                .await
                .unwrap();
            timeout(WAIT, certified.on_close())
                .await
                .expect("presented certificate cap closes idle connection");
            // This original anonymous socket outlives the certificate cap and
            // still requires its independently verified SAS grant.
            send(&mut session, "token-after", "orders").await;
            close(anonymous).await;
        })
        .await;
        node.finish().await;
        drop(owner);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_presented_clients_never_reach_cbs_or_fall_back_to_a_valid_token() {
    for websocket in [false, true] {
        let tls = TlsFixture::new(30);
        let owner = owner();
        let recording = Recording::new(HandleBroker(owner.handle()));
        let calls = recording.calls.clone();
        let binds = recording.binds.clone();
        let mut config = tls.config(true, 60);
        config["revoked_leaf_sha256"] =
            serde_json::json!([format!("{:x}", Sha256::digest(tls.sender.cert.der()))]);
        let mut node = Node::new(
            recording,
            tls.endpoint_config(config, HOST),
            Some(alternative(true)),
            websocket,
        )
        .await;
        let address = node.address;
        node.exercise(async {
            for bad in [tls.untrusted(), tls.unbound(), tls.bad_usage()] {
                assert!(tls.connect(address, websocket, Some(&bad)).await.is_err());
            }
            assert!(
                tls.connect(address, websocket, Some(&tls.sender))
                    .await
                    .is_err(),
                "revoked leaf cannot use configured JWT fallback"
            );
            assert!(
                tls.connect_with_connector(address, websocket, tls.malformed_client())
                    .await
                    .is_err(),
                "malformed DER cannot reach valid CBS credentials"
            );
            assert!(calls.lock().unwrap().is_empty());
            assert!(binds.lock().unwrap().is_empty());
            let mut good = tls.connect(address, websocket, None).await.unwrap();
            let mut session = Session::begin(&mut good).await.unwrap();
            assert_eq!(
                put_token(
                    &mut session,
                    JWT_TOKEN_TYPE,
                    RESOURCE,
                    jwt_tests::token(now(), now() + 600)
                )
                .await,
                202
            );
            send(&mut session, "independent-jwt", "orders").await;
            close(good).await;
        })
        .await;
        node.finish().await;
        drop(owner);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_tokens_union_local_rights_but_cannot_refresh_the_outer_certificate_lease() {
    for websocket in [false, true] {
        let tls = TlsFixture::new(30);
        let owner = owner();
        let recording = Recording::new(HandleBroker(owner.handle()));
        let mut node = Node::new(
            recording,
            tls.endpoint(false, 3),
            Some(alternative(true)),
            websocket,
        )
        .await;
        let address = node.address;
        node.exercise(async {
            let mut connection = tls
                .connect(address, websocket, Some(&tls.listener))
                .await
                .unwrap();
            let mut session = Session::begin(&mut connection).await.unwrap();
            refused(&mut session, "listen-only", "orders").await;
            assert_eq!(
                put_token(
                    &mut session,
                    JWT_TOKEN_TYPE,
                    RESOURCE,
                    jwt_tests::token(now(), now() + 300)
                )
                .await,
                202
            );
            send(&mut session, "jwt-adds-send", "orders").await;
            assert_eq!(
                put_token(&mut session, JWT_TOKEN_TYPE, RESOURCE, "invalid".into()).await,
                401
            );
            send(&mut session, "failed-refresh-preserves", "orders").await;
            assert_eq!(
                put_token(
                    &mut session,
                    SAS_TOKEN_TYPE,
                    RESOURCE,
                    jwt_tests::sas_token(now() + 3600)
                )
                .await,
                202
            );
            assert_eq!(
                put_token(
                    &mut session,
                    JWT_TOKEN_TYPE,
                    RESOURCE,
                    jwt_tests::token(now(), now() + 600)
                )
                .await,
                202
            );
            let mut sender = Sender::attach(&mut session, "retained-sender", "orders")
                .await
                .unwrap();
            let mut receiver = Receiver::attach(&mut session, "retained-listener", "orders")
                .await
                .unwrap();
            timeout(WAIT, connection.on_close())
                .await
                .expect("longer independent grants did not renew TLS");
            assert!(sender.send(message("late")).await.is_err());
            // A previously begun receive may already be buffered on the client;
            // that is not a fresh post-cutoff broker admission.
            if let Ok(delivery) = receiver.recv().await {
                assert!(receiver.accept(&delivery).await.is_err());
            }
            let mut fresh = tls
                .connect(address, websocket, Some(&tls.sender))
                .await
                .unwrap();
            let mut session = Session::begin(&mut fresh).await.unwrap();
            assert_eq!(
                put_token(&mut session, JWT_TOKEN_TYPE, RESOURCE, "bad-cbs".into()).await,
                401
            );
            send(&mut session, "certificate-survives-bad-cbs", "orders").await;
            close(fresh).await;
        })
        .await;
        node.finish().await;
        drop(owner);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn required_certificate_absence_cannot_fall_back_to_independently_configured_tokens() {
    for websocket in [false, true] {
        let tls = TlsFixture::new(30);
        let owner = owner();
        let mut node = Node::new(
            HandleBroker(owner.handle()),
            tls.endpoint(false, 60),
            Some(alternative(true)),
            websocket,
        )
        .await;
        let address = node.address;
        node.exercise(async {
            assert!(tls.connect(address, websocket, None).await.is_err());
            let mut valid = tls
                .connect(address, websocket, Some(&tls.sender))
                .await
                .unwrap();
            let mut session = Session::begin(&mut valid).await.unwrap();
            send(&mut session, "still-healthy", "orders").await;
            close(valid).await;
        })
        .await;
        node.finish().await;
        drop(owner);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fallible_conversion_rejects_tls_namespace_and_optional_credential_conflicts() {
    let tls = TlsFixture::new(30);
    let owner = owner();
    let namespace = NamespaceName::new("tenant").unwrap();
    let listener = || AmqpListener::new(HandleBroker(owner.handle()), namespace.clone());
    assert!(matches!(
        listener()
            .with_tls(tls.legacy_tls())
            .with_workload_tls(tls.endpoint(false, 60)),
        Err(WorkloadListenerError::ConflictingTls)
    ));
    assert!(matches!(
        listener().with_workload_tls(tls.endpoint(true, 60)),
        Err(WorkloadListenerError::AlternativeCredentialRequired)
    ));
    let foreign =
        SharedAccessAuthentication::new(SharedAccessPolicy::default(), "tenant.other.example")
            .unwrap();
    assert!(matches!(
        listener()
            .with_shared_access_authentication(foreign)
            .with_workload_tls(tls.endpoint(false, 60)),
        Err(WorkloadListenerError::NamespaceMismatch)
    ));
    assert!(matches!(
        AmqpListener::new(
            HandleBroker(owner.handle()),
            NamespaceName::new("other").unwrap()
        )
        .with_workload_tls(tls.endpoint(false, 60)),
        Err(WorkloadListenerError::NamespaceMismatch)
    ));
    assert!(
        listener()
            .with_workload_tls(tls.endpoint(false, 60))
            .is_ok()
    );
    assert!(
        listener()
            .with_shared_access_authentication(alternative(false))
            .with_workload_tls(tls.endpoint(true, 60))
            .is_ok()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lease_expires_during_partial_wss_upgrade_sasl_and_open_with_original_join_evidence() {
    for stage in [0, 1, 2] {
        let tls = TlsFixture::new(30);
        let owner = owner();
        let recording = Recording::new(HandleBroker(owner.handle()));
        let calls = recording.calls.clone();
        let binds = recording.binds.clone();
        let mut node = Node::new(recording, tls.endpoint(false, 2), None, stage == 0).await;
        let address = node.address;
        let observer = Arc::new(test_support::Observer::default());
        test_support::drive(&mut node.service, observer.clone(), async {
            let mut stream = tls.tls(address, Some(&tls.sender)).await.unwrap();
            let pending = if stage == 0 {
                stream
                    .write_all(b"GET /$servicebus/websocket HTTP/1.1\r\nHost: local")
                    .await
                    .unwrap();
                Stage::Upgrade
            } else if stage == 1 {
                stream.write_all(b"AMQP\x03\x01\x00\x00").await.unwrap();
                Stage::Protocol
            } else {
                stream.write_all(b"AMQP\x00\x01\x00\x00").await.unwrap();
                Stage::Protocol
            };
            observer.stalled(pending).await;
            let id = observer.stalled_id(pending);
            observer.reached(test_support::Point::Reaped).await;
            assert_eq!(observer.facts(test_support::Point::Reaped).reaped, Some(id));
            let mut bytes = Vec::new();
            let _ = stream.read_to_end(&mut bytes).await;
            assert!(calls.lock().unwrap().is_empty());
            assert!(binds.lock().unwrap().is_empty());
        })
        .await;
        node.finish().await;
        drop(owner);
    }
}

fn raw_send() -> CommandKind {
    CommandKind::Send {
        message_id: "guarded".into(),
        body: b"guarded".to_vec(),
        time_to_live_millis: None,
        session_id: None,
        scheduled_enqueue_at: None,
        envelope: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_bound_and_name_futures_recheck_the_actual_broker_frontier_after_expiry() {
    for durable in [false, true] {
        let tls = TlsFixture::new(30);
        let endpoint = tls.endpoint(false, 1);
        let mut actor = test_support::Actor::new(durable);
        let recording = Recording::new(actor.broker.as_ref().unwrap().clone());
        let calls = recording.calls.clone();
        let binds = recording.binds.clone();
        let binding = recording
            .inner
            .bind_entity(actor.namespace.clone(), actor.entity.clone())
            .await
            .unwrap();
        let (proof, server, client) = fixture::peer(&tls, endpoint, &tls.manager).await;
        let guarded = crate::workload::LeaseBroker::new(recording, Some(proof.clone()));
        // Named frontier: future preparation is not invocation of the real
        // bound broker. Hold these unpolled originals across the certificate cap.
        let bound = guarded.submit_bound(binding, raw_send());
        let named = guarded.submit(actor.namespace.clone(), actor.entity.clone(), raw_send());
        let bind = guarded.bind_entity(actor.namespace.clone(), actor.entity.clone());
        assert!(calls.lock().unwrap().is_empty());
        assert!(binds.lock().unwrap().is_empty());
        assert!(proof.allows(&ResourceScope::parse(RESOURCE).unwrap(), Permission::Send));
        {
            let release = tokio::sync::Notify::new();
            let held = async {
                release.notified().await;
                bound.await
            };
            tokio::pin!(held);
            std::future::poll_fn(|cx| {
                assert!(held.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            proof.expired().await;
            release.notify_one();
            assert_eq!(held.await, Err(BrokerRejection::AuthorizationExpired));
        }
        assert_eq!(named.await, Err(BrokerRejection::AuthorizationExpired));
        assert!(matches!(
            bind.await,
            Err(BrokerRejection::AuthorizationExpired)
        ));
        assert!(calls.lock().unwrap().is_empty());
        assert!(binds.lock().unwrap().is_empty());
        assert!(actor.store().get(&actor.key()).unwrap().is_none());
        drop((guarded, proof, server, client));
        actor.reopen();
        assert!(
            actor.store().get(&actor.key()).unwrap().is_none(),
            "no late mutation before or after reopen"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn begun_actual_tcp_wss_mutation_survives_expiry_once_and_reopens_after_original_joins() {
    for durable in [false, true] {
        for websocket in [false, true] {
            let tls = TlsFixture::new(30);
            let mut actor = test_support::Actor::new(durable);
            actor.gate.arm(actor.key());
            let gate = actor.gate.clone();
            let mut node = Node::new(
                actor.broker.as_ref().unwrap().clone(),
                tls.endpoint(false, 2),
                None,
                websocket,
            )
            .await;
            let address = node.address;
            node.exercise(async {
            let mut connection=tls.connect(address,websocket,Some(&tls.sender)).await.unwrap();
            let mut session=Session::begin(&mut connection).await.unwrap();
            let mut sender=Sender::attach(&mut session,"begun-send","orders").await.unwrap();
            let sending=sender.send(message("once")); tokio::pin!(sending);
            tokio::select! { _=&mut sending=>panic!("gated commit returned early"), ()=gate.reached(false)=>{} }
            timeout(WAIT,connection.on_close()).await.expect("expiry interrupts actual native IO while commit is held");
            assert_eq!(gate.commits(),0);
            gate.release(false); gate.reached(true).await; assert_eq!(gate.commits(),1);
            gate.release(true);
            let _=timeout(WAIT,&mut sending).await.expect("original client handoff ended after disconnect");
        }).await;
            node.finish().await;
            assert_eq!(actor.gate.commits(), 1);
            let log = actor.log.lock().unwrap();
            assert_eq!(
                log.iter()
                    .filter(
                        |row| matches!(row.kind, CommandKind::SendContent { .. }) && !row.returned
                    )
                    .count(),
                1
            );
            assert_eq!(
                log.iter()
                    .filter(
                        |row| matches!(row.kind, CommandKind::SendContent { .. }) && row.returned
                    )
                    .count(),
                1
            );
            drop(log);
            actor.reopen();
            assert!(actor.store().get(&actor.key()).unwrap().is_some());
            assert!(
                actor
                    .store()
                    .get(&domain::keys::message(
                        &actor.namespace,
                        &actor.entity,
                        SequenceNumber::new(2)
                    ))
                    .unwrap()
                    .is_none()
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_grants_lock_frontier_cannot_publish_or_authorize_after_the_certificate_expires() {
    let tls = TlsFixture::new(30);
    let endpoint = tls.endpoint(false, 1);
    let (proof, _server, _client) = fixture::peer(&tls, endpoint, &tls.sender).await;
    let auth = ConnectionAuthorization::new_for_workload_transport(
        alternative(false),
        None,
        None,
        Some(proof.clone()),
    );
    let lock = auth.grant_write_lock().await;
    let check = auth.authorize_entity("orders", Permission::Send);
    tokio::pin!(check);
    let token = jwt_tests::sas_token(now() + 600);
    let publish = auth.validate_token_and_add(SAS_TOKEN_TYPE, &token, RESOURCE);
    tokio::pin!(publish);
    std::future::poll_fn(|cx| {
        assert!(check.as_mut().poll(cx).is_pending());
        assert!(publish.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    proof.expired().await;
    drop(lock);
    assert!(check.await.is_err());
    assert!(publish.await.is_err());
    assert!(auth.grant_snapshot().await.is_empty());
    assert!(!auth.has_valid_grant().await);
    auth.wait_until_unauthorized(&ResourceScope::parse(RESOURCE).unwrap(), Permission::Send)
        .await;
}

async fn partial_after_open<Io>(mut stream: Io, observer: &test_support::Observer)
where
    Io: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    amqp::write_protocol_header(&mut stream, amqp::ProtocolHeader::SASL)
        .await
        .unwrap();
    assert_eq!(
        amqp::read_protocol_header(&mut stream).await.unwrap(),
        amqp::ProtocolHeader::SASL
    );
    assert!(matches!(
        amqp::read_frame(&mut stream).await.unwrap(),
        amqp::Frame::Sasl(amqp::SaslPerformative::Mechanisms(_))
    ));
    amqp::write_frame(
        &mut stream,
        &amqp::Frame::Sasl(amqp::SaslPerformative::Init(amqp::SaslInit {
            mechanism: "ANONYMOUS".into(),
            initial_response: None,
            hostname: Some("foreign.example".into()),
        })),
    )
    .await
    .unwrap();
    assert!(matches!(
        amqp::read_frame(&mut stream).await.unwrap(),
        amqp::Frame::Sasl(amqp::SaslPerformative::Outcome(_))
    ));
    amqp::write_protocol_header(&mut stream, amqp::ProtocolHeader::AMQP)
        .await
        .unwrap();
    assert_eq!(
        amqp::read_protocol_header(&mut stream).await.unwrap(),
        amqp::ProtocolHeader::AMQP
    );
    amqp::write_frame(
        &mut stream,
        &amqp::Frame::Amqp {
            channel: 0,
            performative: Some(amqp::Performative::Open(amqp::Open::new("held-reader"))),
            payload: Vec::new(),
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        amqp::read_frame(&mut stream).await.unwrap(),
        amqp::Frame::Amqp {
            performative: Some(amqp::Performative::Open(_)),
            ..
        }
    ));
    observer.native().await;
    let id = observer.native_ids()[0];
    let begin = amqp::encode_frame(&amqp::Frame::Amqp {
        channel: 0,
        performative: Some(amqp::Performative::Begin(amqp::Begin::default())),
        payload: Vec::new(),
    })
    .unwrap();
    stream.write_all(&begin[..4]).await.unwrap();
    stream.flush().await.unwrap();
    observer.reached(test_support::Point::Reaped).await;
    assert_eq!(observer.facts(test_support::Point::Reaped).reaped, Some(id));
    let mut bytes = Vec::new();
    let _ = stream.read_to_end(&mut bytes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expiry_interrupts_a_retained_native_reader_with_partial_tcp_or_wss_frames() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    for websocket in [false, true] {
        let tls = TlsFixture::new(30);
        let owner = owner();
        let recording = Recording::new(HandleBroker(owner.handle()));
        let calls = recording.calls.clone();
        let binds = recording.binds.clone();
        let mut node = Node::new(recording, tls.endpoint(false, 2), None, websocket).await;
        let address = node.address;
        let observer = Arc::new(test_support::Observer::default());
        test_support::drive(&mut node.service, observer.clone(), async {
            let stream = tls.tls(address, Some(&tls.sender)).await.unwrap();
            if websocket {
                let mut request = format!("wss://{address}/$servicebus/websocket")
                    .into_client_request()
                    .unwrap();
                request
                    .headers_mut()
                    .insert("sec-websocket-protocol", "AMQPWSB10".parse().unwrap());
                let (stream, _) = tokio_tungstenite::client_async(request, stream)
                    .await
                    .unwrap();
                partial_after_open(crate::WebSocketIo::new(stream), &observer).await;
            } else {
                partial_after_open(stream, &observer).await;
            }
            assert!(calls.lock().unwrap().is_empty());
            assert!(binds.lock().unwrap().is_empty());
        })
        .await;
        node.finish().await;
        drop(owner);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expiring_one_principal_does_not_retire_an_existing_healthy_connection_or_listener() {
    for websocket in [false, true] {
        let tls = TlsFixture::new(5);
        let owner = owner();
        let mut node = Node::new(
            HandleBroker(owner.handle()),
            tls.endpoint(false, 60),
            None,
            websocket,
        )
        .await;
        let address = node.address;
        node.exercise(async {
            let mut short = tls
                .connect(address, websocket, Some(&tls.short))
                .await
                .unwrap();
            let mut healthy = tls
                .connect(address, websocket, Some(&tls.manager))
                .await
                .unwrap();
            let mut session = Session::begin(&mut healthy).await.unwrap();
            send(&mut session, "healthy-before", "orders-archive").await;
            timeout(WAIT, short.on_close()).await.unwrap();
            send(&mut session, "healthy-after", "orders-archive").await;
            close(healthy).await;
        })
        .await;
        node.finish().await;
        drop(owner);
    }
}

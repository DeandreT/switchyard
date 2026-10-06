use super::session_paging::{next_session_until, release_accepted};
use super::*;
use amqp::{
    Begin, Frame, Open, Performative, ProtocolHeader, ScopedConnectionAcceptance,
    ServerConnectionJoinReport, ServerConnectionOwner, Source, read_frame, read_protocol_header,
    write_frame, write_protocol_header,
};
use auth::{PermissionSet, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use domain::{
    EntityBinding, LockToken, SessionCursor, SessionId, SessionLock, SessionPageOutcome, Timestamp,
};
use futures_util::FutureExt;
use std::{
    collections::VecDeque,
    future::Future,
    panic::AssertUnwindSafe,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::{
    io::DuplexStream,
    sync::{Notify, Semaphore},
    time::{Instant, timeout},
};

const DEADLINE: Duration = Duration::from_secs(3);
async fn bounded<T>(future: impl Future<Output = T>) -> T {
    timeout(DEADLINE, future)
        .await
        .expect("bounded original operation")
}
async fn caught<T>(future: impl Future<Output = T>) -> Result<T, Box<dyn std::any::Any + Send>> {
    AssertUnwindSafe(future).catch_unwind().await
}
fn rethrow(result: Result<(), Box<dyn std::any::Any + Send>>) {
    if let Err(payload) = result {
        std::panic::resume_unwind(payload);
    }
}
fn namespace() -> NamespaceName {
    NamespaceName::new("tenant").expect("namespace")
}
fn entity() -> EntityPath {
    EntityPath::new("orders").expect("entity")
}
fn cursor(index: usize) -> SessionCursor {
    SessionCursor {
        namespace: namespace(),
        entity: entity(),
        session_id: SessionId::new(format!("s{index:03}")).expect("session"),
    }
}
fn accepted() -> AcceptedSession {
    AcceptedSession {
        session_id: SessionId::new("accepted").expect("session"),
        lock: SessionLock {
            token: LockToken::new(7),
            locked_until: Timestamp::from_millis(30000),
        },
        state: vec![1, 2, 3],
    }
}
fn authorization() -> Arc<ConnectionAuthorization> {
    let policy = SharedAccessPolicy::new([SharedAccessRule::new(
        "listen",
        ResourceScope::namespace("tenant.servicebus.windows.net").expect("scope"),
        SharedAccessKey::new("secret").expect("key"),
        None,
        PermissionSet::LISTEN,
    )
    .expect("rule")])
    .expect("policy");
    let grant = policy
        .authenticate_plain("listen", "secret")
        .expect("grant");
    ConnectionAuthorization::new(
        SharedAccessAuthentication::new(policy, "tenant.servicebus.windows.net")
            .expect("authentication"),
        Some(grant),
    )
}
#[derive(Clone)]
struct Script {
    replies: Arc<Mutex<VecDeque<SessionPageOutcome>>>,
    commands: Arc<Mutex<Vec<(EntityBinding, CommandKind)>>>,
    pause_page: Arc<AtomicBool>,
    page_entered: Arc<Notify>,
    page_go: Arc<Semaphore>,
    pause_release: Arc<AtomicBool>,
    release_entered: Arc<Notify>,
    release_go: Arc<Semaphore>,
    release_error: bool,
}
impl Script {
    fn new(replies: impl IntoIterator<Item = SessionPageOutcome>) -> Self {
        Self {
            replies: Arc::new(Mutex::new(replies.into_iter().collect())),
            commands: Arc::default(),
            pause_page: Arc::default(),
            page_entered: Arc::default(),
            page_go: Arc::new(Semaphore::new(0)),
            pause_release: Arc::default(),
            release_entered: Arc::default(),
            release_go: Arc::new(Semaphore::new(0)),
            release_error: false,
        }
    }
    fn bound(&self) -> BoundBroker<Self> {
        BoundBroker::new(
            self.clone(),
            crate::broker::test_admission(
                namespace(),
                Attachment::Queue(entity()),
                crate::EntityMetadata::Queue(domain::QueueConfig {
                    requires_session: true,
                    ..domain::QueueConfig::default()
                }),
            )
            .binding,
        )
    }
    fn release_gates(&self) {
        self.page_go.add_permits(1);
        self.release_go.add_permits(1);
    }
}
impl Broker for Script {
    async fn bind(
        &self,
        _: NamespaceName,
        _: Attachment,
    ) -> Result<Option<crate::EntityAdmission>, BrokerRejection> {
        panic!("already-bound planning must not rebind")
    }
    async fn submit_fenced(
        &self,
        binding: EntityBinding,
        target: EntityPath,
        kind: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        assert_eq!(binding.namespace(), &namespace());
        assert_eq!(binding.target(), &target);
        {
            let mut commands = self.commands.lock().expect("commands");
            assert!(commands.len() < 8);
            commands.push((binding, kind.clone()));
        }
        match kind {
            CommandKind::AcceptNextSessionPage { .. } => {
                let reply = self
                    .replies
                    .lock()
                    .expect("replies")
                    .pop_front()
                    .expect("scripted page");
                if self.pause_page.swap(false, Ordering::SeqCst) {
                    self.page_entered.notify_one();
                    self.page_go.acquire().await.expect("page release").forget();
                }
                Ok(CommandOutcome::SessionPage(reply))
            }
            CommandKind::ReleaseSession { session } => {
                assert_eq!(session, accepted().hold());
                if self.pause_release.swap(false, Ordering::SeqCst) {
                    self.release_entered.notify_one();
                    self.release_go
                        .acquire()
                        .await
                        .expect("release completion")
                        .forget();
                }
                if self.release_error {
                    Err(BrokerRejection::Unavailable(
                        "original release refusal".into(),
                    ))
                } else {
                    Ok(CommandOutcome::SessionReleased)
                }
            }
            _ => panic!("only page and exact release are admitted"),
        }
    }
    async fn submit(
        &self,
        _: NamespaceName,
        _: EntityPath,
        _: CommandKind,
    ) -> Result<CommandOutcome, BrokerRejection> {
        panic!("unfenced command")
    }
    async fn entity_metadata(
        &self,
        _: NamespaceName,
        _: Attachment,
    ) -> Result<Option<crate::EntityMetadata>, BrokerRejection> {
        panic!("no metadata re-read")
    }
    async fn rules(
        &self,
        _: NamespaceName,
        _: EntityPath,
        _: domain::SubscriptionName,
    ) -> Result<Vec<domain::RuleDefinition>, BrokerRejection> {
        panic!("no rules")
    }
    async fn rules_fenced(
        &self,
        _: EntityBinding,
        _: EntityPath,
        _: domain::SubscriptionName,
    ) -> Result<Vec<domain::RuleDefinition>, BrokerRejection> {
        panic!("no rules")
    }
    async fn deliverable(&self, _: &NamespaceName, _: &EntityPath) {
        panic!("no receive")
    }
}
struct Original {
    owner: ServerConnectionOwner<Arc<()>>,
    connection: ServerConnection,
    session: ServerSession,
    receipt: amqp::IncomingAttach,
    peer: DuplexStream,
    report: Option<ServerConnectionJoinReport<Arc<()>>>,
}
impl Original {
    async fn new() -> Self {
        let (io, mut peer) = tokio::io::duplex(65536);
        let (mut owner, acceptor) =
            ServerConnectionOwner::new(tokio::runtime::Handle::current(), Arc::new(()));
        let mut connection = None;
        let mut session = None;
        let mut receipt = None;
        let observed = caught(async {
            let (accepted, handshake) = tokio::join!(
                bounded(acceptor.accept_with_options(
                    io,
                    "session-pages",
                    None,
                    amqp::ConnectionOptions::default()
                )),
                bounded(async {
                    write_protocol_header(&mut peer, ProtocolHeader::AMQP).await?;
                    read_protocol_header(&mut peer).await?;
                    write_frame(
                        &mut peer,
                        &Frame::Amqp {
                            channel: 0,
                            performative: Some(Performative::Open(Open::new("peer"))),
                            payload: Vec::new(),
                        },
                    )
                    .await?;
                    read_frame(&mut peer).await?;
                    Ok::<(), EngineError>(())
                })
            );
            handshake.expect("handshake");
            let ScopedConnectionAcceptance::Accepted(accepted) = accepted.expect("acceptance")
            else {
                panic!("native open")
            };
            connection = Some(accepted);
            bounded(write_frame(
                &mut peer,
                &Frame::Amqp {
                    channel: 9,
                    performative: Some(Performative::Begin(Begin::default())),
                    payload: Vec::new(),
                },
            ))
            .await
            .expect("Begin");
            let original = bounded(
                connection
                    .as_mut()
                    .expect("connection")
                    .next_incoming_session(),
            )
            .await
            .expect("incoming session");
            session = Some(
                bounded(
                    connection
                        .as_ref()
                        .expect("connection")
                        .accept_session(original),
                )
                .await
                .expect("original session"),
            );
            bounded(read_frame(&mut peer)).await.expect("Begin reply");
            let mut source = Source::new("orders");
            let mut filter = amqp::FilterSet::default();
            filter.insert(Symbol::from(crate::SESSION_FILTER), Value::Null);
            source.filter = Some(filter);
            bounded(write_frame(
                &mut peer,
                &Frame::Amqp {
                    channel: 9,
                    performative: Some(Performative::Attach(Box::new(Attach {
                        name: "pages".into(),
                        handle: 0,
                        role: Role::Receiver,
                        snd_settle_mode: SenderSettleMode::Unsettled,
                        rcv_settle_mode: amqp::ReceiverSettleMode::First,
                        source: Some(source),
                        target: None,
                        unsettled: None,
                        incomplete_unsettled: false,
                        initial_delivery_count: None,
                        max_message_size: None,
                        offered_capabilities: None,
                        desired_capabilities: None,
                        properties: None,
                    }))),
                    payload: Vec::new(),
                },
            ))
            .await
            .expect("Attach");
            receipt = Some(
                bounded(session.as_mut().expect("session").next_incoming_attach())
                    .await
                    .expect("original attach"),
            );
        })
        .await;
        if let Err(payload) = observed {
            owner.stop();
            let _report = bounded(owner.finish()).await;
            std::panic::resume_unwind(payload);
        }
        Self {
            owner,
            connection: connection.expect("connection"),
            session: session.expect("session"),
            receipt: receipt.expect("receipt"),
            peer,
            report: None,
        }
    }
    async fn finish(&mut self) {
        self.owner.stop();
        self.report = Some(bounded(self.owner.finish()).await.expect("original report"));
        assert!(self.report.as_ref().expect("report").actor().is_some());
        assert!(self.report.as_ref().expect("report").reader().is_some());
        let _original_peer = &self.peer;
    }
}

#[tokio::test]
async fn next_available_requires_original_origin_before_page_admission() {
    let mut original = Original::new().await;
    let script = Script::new([SessionPageOutcome::End]);
    let broker = script.bound();
    let observed = caught(async {
        for changed in [false, true] {
            let mut receipt = original.receipt.clone();
            receipt.name.push_str("-changed");
            let origin = changed.then_some((&original.session, &receipt));
            let failure = bounded(next_session_until(&broker, &namespace(), &entity(), origin, None,
                Instant::now() + DEADLINE)).await.expect_err("origin refusal");
            assert_eq!(failure.primary.condition.as_symbol().as_str(), crate::INVALID_FIELD);
            assert!(failure.release.is_none());
        }
        assert!(script.commands.lock().expect("commands").is_empty());
        for error in [domain::BrokerError::InvalidSessionCursor, domain::BrokerError::SessionCursorScopeMismatch {
            namespace: namespace(), entity: entity(), cursor_namespace: namespace(),
            cursor_entity: EntityPath::new("other").expect("entity") }] {
            assert_eq!(crate::condition_for(&error), crate::INVALID_FIELD);
            assert!(!crate::is_retryable(&error));
        }
        let end = Script::new([SessionPageOutcome::Continue(cursor(31)),
            SessionPageOutcome::Continue(cursor(63)), SessionPageOutcome::End]);
        let bound = end.bound();
        let failure = bounded(next_session_until(&bound, &namespace(), &entity(),
            Some((&original.session, &original.receipt)), None, Instant::now() + DEADLINE)).await
            .expect_err("actual End, not the first full held page");
        assert_eq!(failure.primary.condition.as_symbol().as_str(), crate::TIMEOUT);
        let commands = end.commands.lock().expect("commands");
        assert_eq!(commands.len(), 3);
        assert!(matches!(&commands[0].1, CommandKind::AcceptNextSessionPage { after: None, .. }));
        assert!(matches!(&commands[1].1, CommandKind::AcceptNextSessionPage { after: Some(after), .. } if after == &cursor(31)));
        assert!(matches!(&commands[2].1, CommandKind::AcceptNextSessionPage { after: Some(after), .. } if after == &cursor(63)));
    }).await;
    original.finish().await;
    rethrow(observed);
}

#[tokio::test]
async fn page_budget_stops_new_admission_but_retains_an_admitted_grant() {
    for grant in [false, true] {
        let mut original = Original::new().await;
        let script = Script::new([if grant {
            SessionPageOutcome::Accepted(accepted())
        } else {
            SessionPageOutcome::Continue(cursor(31))
        }]);
        script.pause_page.store(true, Ordering::SeqCst);
        let broker = script.bound();
        let cutoff = Instant::now() + Duration::from_secs(10);
        let ns = namespace();
        let target = entity();
        let mut planning = Box::pin(next_session_until(
            &broker,
            &ns,
            &target,
            Some((&original.session, &original.receipt)),
            None,
            cutoff,
        ));
        let mut outcome = None;
        let observed = caught(async {
            let expired = bounded(next_session_until(&broker, &ns, &target,
                Some((&original.session, &original.receipt)), None, Instant::now())).await
                .expect_err("expired admission budget");
            assert_eq!(expired.primary.condition.as_symbol().as_str(), crate::TIMEOUT);
            assert!(script.commands.lock().expect("commands").is_empty());
            tokio::select! {
                () = bounded(script.page_entered.notified()) => {},
                result = &mut planning => { outcome = Some(result); panic!("admitted reply must remain pending") }
            }
            tokio::time::sleep_until(cutoff).await;
            script.page_go.add_permits(1);
            outcome = Some(bounded(&mut planning).await);
            if grant { assert_eq!(outcome.as_ref().expect("result").as_ref().expect("grant"), &accepted()); }
            else { assert_eq!(outcome.as_ref().expect("result").as_ref().expect_err("cutoff").primary.condition.as_symbol().as_str(), crate::TIMEOUT); }
            assert_eq!(script.commands.lock().expect("commands").len(), 1);
        }).await;
        script.release_gates();
        let finishing = caught(async {
            if outcome.is_none() {
                outcome = Some(planning.as_mut().await);
            }
        })
        .await;
        drop(planning);
        let releasing = caught(async {
            if let Some(Ok(grant)) = &outcome {
                assert!(matches!(
                    release_accepted(&broker, &ns, &target, grant).await,
                    Ok(CommandOutcome::SessionReleased)
                ));
            }
        })
        .await;
        original.finish().await;
        rethrow(observed);
        rethrow(finishing);
        rethrow(releasing);
    }
}

#[tokio::test]
async fn controlled_grant_invalidation_and_origin_retirement_stop_later_pages() {
    for retire in [false, true] {
        let mut original = Original::new().await;
        let script = Script::new([
            SessionPageOutcome::Continue(cursor(31)),
            SessionPageOutcome::End,
        ]);
        script.pause_page.store(true, Ordering::SeqCst);
        let connection = authorization();
        let auth = LinkAuthorization {
            connection: connection.clone(),
            resource: ResourceScope::entity("tenant.servicebus.windows.net", "orders")
                .expect("scope"),
            permission: Permission::Listen,
        };
        let broker = script.bound();
        let ns = namespace();
        let target = entity();
        let mut planning = Box::pin(next_session_until(
            &broker,
            &ns,
            &target,
            Some((&original.session, &original.receipt)),
            Some(&auth),
            Instant::now() + DEADLINE,
        ));
        let mut outcome = None;
        let observed = caught(async {
            tokio::select! { () = bounded(script.page_entered.notified()) => {},
            result = &mut planning => { outcome = Some(result); panic!("pause") } }
            if retire {
                bounded(original.connection.shutdown()).await;
            } else {
                connection.invalidate_grants_for_test().await;
            }
            script.page_go.add_permits(1);
            outcome = Some(bounded(&mut planning).await);
            let failure = outcome
                .as_ref()
                .expect("result")
                .as_ref()
                .expect_err("recheck");
            assert_eq!(
                failure.primary.condition.as_symbol().as_str(),
                if retire {
                    crate::INVALID_FIELD
                } else {
                    "amqp:unauthorized-access"
                }
            );
            assert!(failure.release.is_none());
            assert_eq!(script.commands.lock().expect("commands").len(), 1);
        })
        .await;
        script.release_gates();
        let finishing = caught(async {
            if outcome.is_none() {
                outcome = Some(planning.as_mut().await);
            }
        })
        .await;
        drop(planning);
        original.finish().await;
        let _retained_original_outcome = &outcome;
        rethrow(observed);
        rethrow(finishing);
    }
}

#[tokio::test]
async fn admitted_original_hold_is_released_before_post_result_refusal_returns() {
    for invalidate in [false, true] {
        for release_error in [false, true] {
            let mut original = Original::new().await;
            let mut script = Script::new([SessionPageOutcome::Accepted(accepted())]);
            script.release_error = release_error;
            script.pause_page.store(true, Ordering::SeqCst);
            script.pause_release.store(true, Ordering::SeqCst);
            let connection = authorization();
            let auth = LinkAuthorization {
                connection: connection.clone(),
                resource: ResourceScope::entity("tenant.servicebus.windows.net", "orders")
                    .expect("scope"),
                permission: Permission::Listen,
            };
            let broker = script.bound();
            let ns = namespace();
            let target = entity();
            let mut planning = Box::pin(next_session_until(
                &broker,
                &ns,
                &target,
                Some((&original.session, &original.receipt)),
                Some(&auth),
                Instant::now() + DEADLINE,
            ));
            let mut outcome = None;
            let observed = caught(async {
                tokio::select! { () = bounded(script.page_entered.notified()) => {},
                    result = &mut planning => { outcome = Some(result); panic!("original owner result paused") } }
                if invalidate { connection.invalidate_grants_for_test().await; }
                else { bounded(original.connection.shutdown()).await; }
                script.page_go.add_permits(1);
                tokio::select! { () = bounded(script.release_entered.notified()) => {},
                    result = &mut planning => { outcome = Some(result); panic!("must await actual release result") } }
                let rows = script.commands.lock().expect("commands").clone();
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0].0, rows[1].0);
                assert!(matches!(&rows[1].1, CommandKind::ReleaseSession { session } if session == &accepted().hold()));
                script.release_go.add_permits(1);
                outcome = Some(bounded(&mut planning).await);
                let failure = outcome.as_ref().expect("result").as_ref().expect_err("post-result refusal");
                assert_eq!(failure.cleanup_failed(), release_error);
                if invalidate { assert!(failure.origin.is_none()); }
                else { assert!(matches!(failure.origin, Some(EngineError::RemoteDetached))); }
                if release_error { assert_eq!(failure.release, Some(Err(BrokerRejection::Unavailable("original release refusal".into())))); }
                else { assert_eq!(failure.release, Some(Ok(CommandOutcome::SessionReleased))); }
                let source = std::error::Error::source(failure).expect("original primary source");
                if let Some(origin) = failure.origin.as_ref() {
                    assert!(std::ptr::eq(source.downcast_ref::<EngineError>().expect("original native source"), origin));
                } else {
                    assert!(std::ptr::eq(source.downcast_ref::<session_paging::ProtocolPrimary>().expect("original protocol source"), &failure.primary));
                }
                let diagnostic = format!("{failure:?}");
                assert!(diagnostic.contains(if release_error { "broker-unavailable" } else { "session-released" }));
                assert!(!diagnostic.contains("original release refusal"));
            }).await;
            script.release_gates();
            let finishing = caught(async {
                if outcome.is_none() {
                    outcome = Some(planning.as_mut().await);
                }
            })
            .await;
            drop(planning);
            original.finish().await;
            let _retained_original_outcome = &outcome;
            rethrow(observed);
            rethrow(finishing);
        }
    }
}

use std::{
    fs,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use domain::{
    CommandKind, CommandOutcome, EntityBinding, EntityPath, NamespaceName, QueueCapacityView,
    RuleDefinition, StateMachine, SubscriptionName, Timestamp,
};
use protocol_amqp::{
    Attachment, Broker as ProtocolBroker, BrokerRejection, EntityAdmission, EntityMetadata,
    OwnedReceiveSubmission, ReceiveSubmitError, RetainedConnectionOutcome,
    RetainedConnectionOutcomes, RetainedConnectionOwner, RetainedConnectionStartCause,
    RetainedConnectionTaskJoins, SharedAccessAuthentication,
};
use server::{Broker, BrokerHandle, Clock, LocalProposer, SystemClock};
use storage::{Key, StateStore, StorageError, StoreSnapshot, Value, WriteBatch};
use testkit::StoreProvider;
use tokio::{
    net::TcpListener,
    sync::oneshot,
    task::JoinHandle,
    time::{Instant, timeout},
};

use super::{HOST, KEY, RULE, TestResult};

const DEADLINE: Duration = Duration::from_secs(5);
const MAX_STAGE_SOCKETS: usize = 16;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct Effects {
    pub(super) attempts: usize,
    pub(super) commits: usize,
    pub(super) clock: usize,
}

struct Observed<S> {
    inner: S,
    attempts: AtomicUsize,
    batches: Mutex<Vec<WriteBatch>>,
}

struct ProbeStore<S>(Arc<Observed<S>>);
impl<S> Clone for ProbeStore<S> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<S: StateStore> StateStore for ProbeStore<S> {
    fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        self.0.inner.get(key)
    }
    fn scan_from(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        self.0.inner.scan_from(prefix, start, limit)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        self.0.inner.snapshot()
    }
    fn apply(&self, batch: WriteBatch) -> Result<(), StorageError> {
        self.0.attempts.fetch_add(1, Ordering::SeqCst);
        self.0.inner.apply(batch.clone())?;
        // Journal only original batches whose delegate actually returned success.
        self.0
            .batches
            .lock()
            .expect("capacity SDK committed journal")
            .push(batch);
        Ok(())
    }
}

#[derive(Clone)]
struct ProbeClock(Arc<AtomicUsize>);
impl Clock for ProbeClock {
    fn now(&self) -> Timestamp {
        self.0.fetch_add(1, Ordering::SeqCst);
        SystemClock.now()
    }
}

#[derive(Clone)]
struct TrackingBroker {
    inner: BrokerHandle,
    lease: Arc<()>,
}

struct LeasedFuture<F> {
    // Declaration order keeps the lease alive through original cancellation-guard destruction.
    original: std::pin::Pin<Box<F>>,
    lease: Arc<()>,
}
impl<F> LeasedFuture<F> {
    fn new(original: F, lease: Arc<()>) -> Self {
        Self {
            original: Box::pin(original),
            lease,
        }
    }
}
impl<F: std::future::Future> std::future::Future for LeasedFuture<F> {
    type Output = F::Output;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let packet = self.get_mut();
        let _ = &packet.lease;
        packet.original.as_mut().poll(context)
    }
}

impl ProtocolBroker for TrackingBroker {
    fn receive_fenced_owned(
        &self,
        submission: OwnedReceiveSubmission,
    ) -> impl std::future::Future<Output = Result<Option<domain::Delivery>, ReceiveSubmitError>>
    + Send
    + 'static {
        // Arm the original owned receive synchronously, before constructing a wrapper.
        let original = ProtocolBroker::receive_fenced_owned(&self.inner, submission);
        LeasedFuture::new(original, self.lease.clone())
    }
    fn bind(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> impl std::future::Future<Output = Result<Option<EntityAdmission>, BrokerRejection>> + Send
    {
        LeasedFuture::new(
            ProtocolBroker::bind(&self.inner, namespace, target),
            self.lease.clone(),
        )
    }
    fn submit_fenced(
        &self,
        binding: EntityBinding,
        entity: EntityPath,
        kind: CommandKind,
    ) -> impl std::future::Future<Output = Result<CommandOutcome, BrokerRejection>> + Send {
        LeasedFuture::new(
            ProtocolBroker::submit_fenced(&self.inner, binding, entity, kind),
            self.lease.clone(),
        )
    }
    fn rules_fenced(
        &self,
        binding: EntityBinding,
        topic: EntityPath,
        subscription: SubscriptionName,
    ) -> impl std::future::Future<Output = Result<Vec<RuleDefinition>, BrokerRejection>> + Send
    {
        LeasedFuture::new(
            ProtocolBroker::rules_fenced(&self.inner, binding, topic, subscription),
            self.lease.clone(),
        )
    }
    fn rules(
        &self,
        namespace: NamespaceName,
        topic: EntityPath,
        subscription: SubscriptionName,
    ) -> impl std::future::Future<Output = Result<Vec<RuleDefinition>, BrokerRejection>> + Send
    {
        LeasedFuture::new(
            ProtocolBroker::rules(&self.inner, namespace, topic, subscription),
            self.lease.clone(),
        )
    }
    fn entity_metadata(
        &self,
        namespace: NamespaceName,
        target: Attachment,
    ) -> impl std::future::Future<Output = Result<Option<EntityMetadata>, BrokerRejection>> + Send
    {
        LeasedFuture::new(
            ProtocolBroker::entity_metadata(&self.inner, namespace, target),
            self.lease.clone(),
        )
    }
    fn submit(
        &self,
        namespace: NamespaceName,
        entity: EntityPath,
        kind: CommandKind,
    ) -> impl std::future::Future<Output = Result<CommandOutcome, BrokerRejection>> + Send {
        LeasedFuture::new(
            ProtocolBroker::submit(&self.inner, namespace, entity, kind),
            self.lease.clone(),
        )
    }
    fn deliverable(
        &self,
        namespace: &NamespaceName,
        entity: &EntityPath,
    ) -> impl std::future::Future<Output = ()> + Send {
        LeasedFuture::new(
            ProtocolBroker::deliverable(&self.inner, namespace, entity),
            self.lease.clone(),
        )
    }
}

struct SocketRoot {
    owner: RetainedConnectionOwner<Arc<()>>,
    lease: Weak<()>,
    report_consumed: bool,
}
struct Accepted {
    roots: Vec<SocketRoot>,
    failure: Option<std::io::Error>,
    start_causes: Vec<RetainedConnectionStartCause>,
}
struct Observation {
    joins: RetainedConnectionTaskJoins,
    outcomes: RetainedConnectionOutcomes,
    websocket_io_kind: Option<std::io::ErrorKind>,
}

#[derive(Clone, Copy, Debug)]
struct CloseFacts {
    valid_frame: bool,
    clean: bool,
    peer_initiated: bool,
    reply_ready: bool,
    reply_ok: bool,
}

fn supported_reader(
    join: &Option<Result<(), tokio::task::JoinError>>,
    observed: Option<(tokio::task::Id, bool)>,
    close: Option<CloseFacts>,
) -> bool {
    let clean_peer_reply = matches!(
        close,
        Some(CloseFacts {
            valid_frame: true,
            clean: true,
            peer_initiated: true,
            reply_ready: true,
            reply_ok: true,
        })
    );
    clean_peer_reply
        && match join {
            Some(Ok(())) => true,
            Some(Err(error)) => {
                error.is_cancelled()
                    && observed
                        .is_some_and(|(id, actor_shutdown)| id == error.id() && actor_shutdown)
            }
            None => false,
        }
}

fn terminal_close_io_kind(kind: Option<std::io::ErrorKind>) -> bool {
    matches!(
        kind,
        Some(
            std::io::ErrorKind::UnexpectedEof
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::BrokenPipe
        )
    )
}

fn supported_websocket(
    original: &Option<protocol_amqp::RetainedConnectionResult>,
    observed_kind: Option<std::io::ErrorKind>,
    actor_reader_shutdown: bool,
    close: Option<CloseFacts>,
) -> bool {
    match original {
        Some(Ok(())) => true,
        Some(Err(_)) => {
            terminal_close_io_kind(observed_kind)
                && actor_reader_shutdown
                && matches!(
                    close,
                    Some(CloseFacts {
                        valid_frame: true,
                        clean: true,
                        peer_initiated: true,
                        reply_ready: true,
                        reply_ok: true,
                    })
                )
        }
        None => false,
    }
}

fn websocket_cause(mut original: &(dyn std::error::Error + 'static)) -> &'static str {
    use tokio_tungstenite::tungstenite::{Error, error::ProtocolError};

    for _ in 0..8 {
        if let Some(error) = original.downcast_ref::<Error>() {
            return match error {
                Error::Protocol(ProtocolError::ResetWithoutClosingHandshake) => {
                    "missing-close-handshake"
                }
                Error::Io(error) => match error.kind() {
                    std::io::ErrorKind::BrokenPipe => "broken-pipe",
                    std::io::ErrorKind::ConnectionReset => "connection-reset",
                    std::io::ErrorKind::UnexpectedEof => "unexpected-eof",
                    _ => "other-io",
                },
                Error::Protocol(_) => "other-protocol",
                _ => "other-websocket",
            };
        }
        let source = original
            .downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::get_ref)
            .map(|source| source as &(dyn std::error::Error + 'static))
            .or_else(|| original.source());
        let Some(source) = source else {
            return "unavailable";
        };
        original = source;
    }
    "source-depth-exceeded"
}

impl Observation {
    fn accepted_disposition(&self) -> bool {
        let native = &self.joins.native_observations;
        let reader = native.reader().map(|task| {
            (
                task.id(),
                task.requested_by(amqp::ServerConnectionAbortSource::ActorReaderShutdown),
            )
        });
        let close = native.peer_close().map(|close| CloseFacts {
            valid_frame: close.channel() == 0 && close.payload().is_empty(),
            clean: close.close().error.is_none(),
            peer_initiated: !close.locally_closing(),
            reply_ready: close.reply_state() == amqp::ServerPeerCloseReplyState::Ready,
            reply_ok: matches!(close.reply_result(), Some(Ok(()))),
        });
        let accepted = matches!(&self.joins.wrapper, Some(Ok(())))
            && matches!(&self.joins.actor, Some(Ok(())))
            && supported_reader(&self.joins.reader, reader, close)
            && matches!(
                &self.outcomes.primary,
                Some(RetainedConnectionOutcome::Finished(Ok(())))
            )
            && supported_websocket(
                &self.outcomes.websocket_close,
                self.websocket_io_kind,
                reader.is_some_and(|(_, requested)| requested),
                close,
            );
        if !accepted {
            let reader_error = self
                .joins
                .reader
                .as_ref()
                .and_then(|result| result.as_ref().err());
            let websocket_io_kind = self
                .outcomes
                .websocket_close
                .as_ref()
                .and_then(|result| result.as_ref().err())
                .and_then(|error| error.downcast_ref::<std::io::Error>())
                .map(std::io::Error::kind);
            let websocket_original = self
                .outcomes
                .websocket_close
                .as_ref()
                .and_then(|result| result.as_ref().err())
                .map(|error| websocket_cause(error.as_ref()));
            eprintln!(
                "capacity-sdk disposition wrapper_ok={} actor_ok={} reader_ok={} reader_cancelled={} reader_panic={} original_reader_id={} actor_reader_shutdown={} primary_ok={} websocket_close_ok={} websocket_io_kind={websocket_io_kind:?} websocket_original={websocket_original:?} close={close:?}",
                matches!(&self.joins.wrapper, Some(Ok(()))),
                matches!(&self.joins.actor, Some(Ok(()))),
                matches!(&self.joins.reader, Some(Ok(()))),
                reader_error.is_some_and(tokio::task::JoinError::is_cancelled),
                reader_error.is_some_and(tokio::task::JoinError::is_panic),
                reader_error
                    .zip(reader)
                    .is_some_and(|(error, (id, _))| error.id() == id),
                reader.is_some_and(|(_, requested)| requested),
                matches!(
                    &self.outcomes.primary,
                    Some(RetainedConnectionOutcome::Finished(Ok(())))
                ),
                matches!(&self.outcomes.websocket_close, Some(Ok(()))),
            );
        }
        accepted
    }
}

pub(super) struct Fixture<P: StoreProvider> {
    pub(super) namespace: NamespaceName,
    pub(super) ca_file: std::path::PathBuf,
    pub(super) ca_directory: std::path::PathBuf,
    broker: Option<Broker>,
    store: Option<ProbeStore<P::Store>>,
    stopped_store: Option<P::Store>,
    clock: ProbeClock,
    provider: Option<P>,
    tls: rustls::ServerConfig,
    authentication: SharedAccessAuthentication,
    acceptance: Option<JoinHandle<Accepted>>,
    shutdown: Option<oneshot::Sender<()>>,
    roots: Vec<SocketRoot>,
    discharged: Vec<Weak<()>>,
    observations: Vec<Observation>,
    accept_failures: Vec<std::io::Error>,
    start_causes: Vec<RetainedConnectionStartCause>,
    _certificates: tempfile::TempDir,
}

impl<P: StoreProvider> Fixture<P> {
    pub(super) fn start(provider: P, namespace: NamespaceName) -> TestResult<Self> {
        let (tls, ca_pem) = super::super::websocket::signed_localhost_config()?;
        let certificates = tempfile::TempDir::new()?;
        let ca_file = certificates.path().join("private-ca.pem");
        let ca_directory = certificates.path().join("empty-trust-directory");
        fs::write(&ca_file, ca_pem)?;
        fs::create_dir(&ca_directory)?;
        let rule = SharedAccessRule::new(
            RULE,
            ResourceScope::namespace(HOST)?,
            SharedAccessKey::new(KEY)?,
            None,
            PermissionSet::MANAGE,
        )?;
        let authentication =
            SharedAccessAuthentication::new(SharedAccessPolicy::new([rule])?, HOST)?
                .with_authorization_timeout(Duration::from_secs(10));
        let store = ProbeStore(Arc::new(Observed {
            inner: provider.open()?,
            attempts: AtomicUsize::new(0),
            batches: Mutex::new(Vec::new()),
        }));
        let clock = ProbeClock(Arc::default());
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            clock.clone(),
        ));
        Ok(Self {
            namespace,
            ca_file,
            ca_directory,
            broker: Some(broker),
            store: Some(store),
            stopped_store: None,
            clock,
            provider: Some(provider),
            tls,
            authentication,
            acceptance: None,
            shutdown: None,
            roots: Vec::new(),
            discharged: Vec::new(),
            observations: Vec::new(),
            accept_failures: Vec::new(),
            start_causes: Vec::new(),
            _certificates: certificates,
        })
    }

    pub(super) fn handle(&self) -> BrokerHandle {
        self.broker.as_ref().expect("capacity SDK owner").handle()
    }
    pub(super) fn snapshot(&self) -> TestResult<StoreSnapshot> {
        match &self.store {
            Some(store) => Ok(store.0.inner.snapshot()?),
            None => Ok(self
                .stopped_store
                .as_ref()
                .expect("discharged capacity SDK store")
                .snapshot()?),
        }
    }
    pub(super) fn raw(&self, key: &[u8]) -> TestResult<Option<Value>> {
        Ok(self
            .store
            .as_ref()
            .expect("capacity SDK store")
            .0
            .inner
            .get(key)?)
    }
    pub(super) fn effects(&self) -> Effects {
        let store = self.store.as_ref().expect("capacity SDK store");
        Effects {
            attempts: store.0.attempts.load(Ordering::SeqCst),
            commits: store
                .0
                .batches
                .lock()
                .expect("capacity SDK committed journal")
                .len(),
            clock: self.clock.0.load(Ordering::SeqCst),
        }
    }
    pub(super) fn batches_since(&self, effects: Effects) -> Vec<WriteBatch> {
        self.store
            .as_ref()
            .expect("capacity SDK store")
            .0
            .batches
            .lock()
            .expect("capacity SDK committed journal")[effects.commits..]
            .to_vec()
    }
    pub(super) async fn view(&self, entity: &EntityPath) -> TestResult<QueueCapacityView> {
        self.handle()
            .describe_queue_capacity(self.namespace.clone(), entity.clone())
            .await?
            .ok_or_else(|| "capacity SDK queue disappeared".into())
    }

    pub(super) async fn begin_stage(&mut self) -> TestResult<String> {
        assert!(self.acceptance.is_none() && self.roots.is_empty() && self.discharged.is_empty());
        let socket = TcpListener::bind("127.0.0.1:0").await?;
        let port = socket.local_addr()?.port();
        let handle = self.handle();
        let namespace = self.namespace.clone();
        let tls = self.tls.clone();
        let authentication = self.authentication.clone();
        let (shutdown, mut stopped) = oneshot::channel();
        self.shutdown = Some(shutdown);
        self.acceptance = Some(tokio::spawn(async move {
            let mut accepted = Accepted {
                roots: Vec::new(),
                failure: None,
                start_causes: Vec::new(),
            };
            loop {
                let incoming = tokio::select! {
                    biased;
                    _ = &mut stopped => break,
                    incoming = socket.accept() => incoming,
                };
                let stream = match incoming {
                    Ok((stream, _)) => stream,
                    Err(error) => {
                        accepted.failure = Some(error);
                        break;
                    }
                };
                if accepted.roots.len() >= MAX_STAGE_SOCKETS {
                    drop(stream);
                    accepted.failure =
                        Some(std::io::Error::other("capacity SDK socket bound exceeded"));
                    break;
                }
                let lease = Arc::new(());
                let weak = Arc::downgrade(&lease);
                let (owner, starter) =
                    RetainedConnectionOwner::new(tokio::runtime::Handle::current(), lease.clone());
                let broker = TrackingBroker {
                    inner: handle.clone(),
                    lease,
                };
                let listener = protocol_amqp::AmqpListener::new(broker, namespace.clone())
                    .with_tls(tls.clone())
                    .with_websocket()
                    .with_shared_access_authentication(authentication.clone());
                // Keep the root before synchronous installation of the original wrapper.
                accepted.roots.push(SocketRoot {
                    owner,
                    lease: weak,
                    report_consumed: false,
                });
                if let Err(error) = listener.start_retained_connection(stream, starter) {
                    let (cause, request) = error.into_parts();
                    accepted.start_causes.push(cause);
                    drop(request);
                    break;
                }
            }
            accepted
        }));
        Ok(format!("wss://localhost:{port}/ignored-custom-path"))
    }

    pub(super) async fn end_stage(&mut self) -> TestResult {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(mut acceptance) = self.acceptance.take() {
            match timeout(DEADLINE, &mut acceptance).await {
                Ok(Ok(accepted)) => {
                    if accepted.roots.is_empty() {
                        self.accept_failures.push(std::io::Error::other(
                            "capacity SDK child created no original socket root",
                        ));
                    }
                    self.roots.extend(accepted.roots);
                    self.start_causes.extend(accepted.start_causes);
                    if let Some(error) = accepted.failure {
                        self.accept_failures.push(error);
                    }
                }
                Ok(Err(error)) => return Err(error.into()),
                Err(_) => {
                    self.acceptance = Some(acceptance);
                    return Err("original capacity SDK acceptance did not join".into());
                }
            }
        }
        for root in &mut self.roots {
            if root.report_consumed {
                continue;
            }
            // SDK disposal starts close without joining the original transport handler.
            if timeout(DEADLINE, root.owner.join_wrapper()).await.is_err() {
                self.accept_failures.push(std::io::Error::other(
                    "original capacity SDK wrapper did not join before stop",
                ));
                root.owner.stop();
            }
            // A timeout restores the same borrowed tokens; retry that original root once.
            let report = match timeout(DEADLINE, root.owner.finish()).await {
                Ok(report) => report,
                Err(_) => timeout(DEADLINE, root.owner.finish())
                    .await
                    .map_err(|_| "original capacity SDK socket root did not finish")?,
            };
            if let Some(report) = report {
                let websocket_io_kind = report.websocket_close_io_error_kind();
                if let Some(kind) = websocket_io_kind {
                    eprintln!("capacity-sdk original-close io_kind={kind:?}");
                }
                let (joins, outcomes, anchor) = report.into_parts();
                self.observations.push(Observation {
                    joins,
                    outcomes,
                    websocket_io_kind,
                });
                drop(anchor);
            } else {
                self.accept_failures.push(std::io::Error::other(
                    "capacity SDK original socket report was missing",
                ));
            }
            root.report_consumed = true;
            self.discharged.push(root.lease.clone());
        }
        self.roots.clear();
        timeout(DEADLINE, async {
            while self
                .discharged
                .iter()
                .any(|lease| lease.upgrade().is_some())
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| "capacity SDK facade descendants did not release their leases")?;
        self.discharged.clear();
        if !self.accept_failures.is_empty()
            || !self.start_causes.is_empty()
            || self
                .observations
                .iter()
                .any(|observation| !observation.accepted_disposition())
        {
            return Err("original capacity SDK socket disposition was unsupported".into());
        }
        Ok(())
    }

    pub(super) async fn stop(&mut self) -> TestResult {
        self.end_stage().await?;
        // Drop joins the original serialized owner, independently of socket-role joins.
        drop(self.broker.take());
        let Some(store) = self.store.take() else {
            return Ok(());
        };
        let mut original = store.0;
        let deadline = Instant::now() + DEADLINE;
        loop {
            match Arc::try_unwrap(original) {
                Ok(observed) => {
                    self.stopped_store = Some(observed.inner);
                    return Ok(());
                }
                Err(retained) => original = retained,
            }
            if Instant::now() >= deadline {
                self.store = Some(ProbeStore(original));
                return Err("original capacity SDK store clones remained after owner join".into());
            }
            tokio::task::yield_now().await;
        }
    }

    pub(super) fn into_stopped_parts(mut self) -> (P, P::Store, NamespaceName) {
        assert!(
            self.broker.is_none()
                && self.acceptance.is_none()
                && self.roots.is_empty()
                && self.store.is_none()
        );
        (
            self.provider.take().expect("capacity SDK provider"),
            self.stopped_store
                .take()
                .expect("unwrapped original capacity SDK backend"),
            self.namespace.clone(),
        )
    }
}

impl<P: StoreProvider> Drop for Fixture<P> {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        for root in &self.roots {
            root.owner.stop();
        }
        // Fallback only: Drop cannot establish socket joins or safe backend reopen.
        drop(self.broker.take());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::MemoryStore;

    #[derive(Clone)]
    struct Refuses(MemoryStore);
    impl StateStore for Refuses {
        fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
            self.0.get(key)
        }
        fn scan_from(
            &self,
            prefix: &[u8],
            start: &[u8],
            limit: usize,
        ) -> Result<Vec<(Key, Value)>, StorageError> {
            self.0.scan_from(prefix, start, limit)
        }
        fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
            self.0.snapshot()
        }
        fn apply(&self, _: WriteBatch) -> Result<(), StorageError> {
            Err(StorageError::LockPoisoned)
        }
    }

    #[test]
    fn journal_records_only_delegate_successful_original_batches() -> TestResult {
        let store = ProbeStore(Arc::new(Observed {
            inner: MemoryStore::default(),
            attempts: AtomicUsize::new(0),
            batches: Mutex::new(Vec::new()),
        }));
        let original = WriteBatch::default().put(b"owned".to_vec(), b"original".to_vec());
        store.apply(original.clone())?;
        assert_eq!(*store.0.batches.lock().unwrap(), [original]);
        assert_eq!(store.0.attempts.load(Ordering::SeqCst), 1);
        let refused = ProbeStore(Arc::new(Observed {
            inner: Refuses(MemoryStore::default()),
            attempts: AtomicUsize::new(0),
            batches: Mutex::new(Vec::new()),
        }));
        assert_eq!(
            refused.apply(WriteBatch::default()),
            Err(StorageError::LockPoisoned)
        );
        assert!(refused.0.batches.lock().unwrap().is_empty());
        assert_eq!(refused.0.attempts.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[test]
    fn backend_discharge_requires_the_original_arc_to_be_unique() {
        let original = ProbeStore(Arc::new(Observed {
            inner: MemoryStore::default(),
            attempts: AtomicUsize::new(0),
            batches: Mutex::new(Vec::new()),
        }));
        let retained = original.clone();
        let original = Arc::try_unwrap(original.0)
            .err()
            .expect("retained clone prevents discharge");
        drop(retained);
        assert!(Arc::try_unwrap(original).is_ok());
    }

    #[test]
    fn original_operation_drops_before_its_lease_even_without_completion() {
        use std::{future::Future, sync::atomic::AtomicBool};
        struct Operation {
            lease: Weak<()>,
            dropped: Arc<AtomicBool>,
        }
        impl Future for Operation {
            type Output = ();
            fn poll(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<()> {
                std::task::Poll::Pending
            }
        }
        impl Drop for Operation {
            fn drop(&mut self) {
                assert!(
                    self.lease.upgrade().is_some(),
                    "lease vanished before original cancellation guard"
                );
                self.dropped.store(true, Ordering::SeqCst);
            }
        }
        for poll in [false, true] {
            let lease = Arc::new(());
            let weak = Arc::downgrade(&lease);
            let dropped = Arc::new(AtomicBool::new(false));
            let mut packet = LeasedFuture::new(
                Operation {
                    lease: weak.clone(),
                    dropped: dropped.clone(),
                },
                lease,
            );
            if poll {
                let waker = futures_util::task::noop_waker();
                let mut context = std::task::Context::from_waker(&waker);
                assert!(
                    std::pin::Pin::new(&mut packet)
                        .poll(&mut context)
                        .is_pending()
                );
            }
            drop(packet);
            assert!(dropped.load(Ordering::SeqCst));
            assert!(weak.upgrade().is_none());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_reader_requires_original_shutdown_and_peer_reply_facts() {
        let task = tokio::spawn(std::future::pending::<()>());
        let id = task.id();
        task.abort();
        let cancelled = Some(Err(task.await.unwrap_err()));
        let other = tokio::spawn(std::future::pending::<()>());
        let wrong_id = other.id();
        other.abort();
        let _ = other.await;
        assert_ne!(id, wrong_id);
        let close = CloseFacts {
            valid_frame: true,
            clean: true,
            peer_initiated: true,
            reply_ready: true,
            reply_ok: true,
        };
        assert!(supported_reader(&cancelled, Some((id, true)), Some(close)));
        assert!(!supported_reader(&cancelled, None, Some(close)));
        assert!(!supported_reader(
            &cancelled,
            Some((wrong_id, true)),
            Some(close)
        ));
        // OwnerFinish-only and absent abort observations cannot supply this fact.
        assert!(!supported_reader(
            &cancelled,
            Some((id, false)),
            Some(close)
        ));
        assert!(!supported_reader(&cancelled, Some((id, true)), None));
        for invalid in [
            CloseFacts {
                valid_frame: false,
                ..close
            },
            CloseFacts {
                clean: false,
                ..close
            },
            CloseFacts {
                peer_initiated: false,
                ..close
            },
            CloseFacts {
                reply_ready: false,
                ..close
            },
            CloseFacts {
                reply_ok: false,
                ..close
            },
        ] {
            assert!(!supported_reader(
                &cancelled,
                Some((id, true)),
                Some(invalid)
            ));
        }
        let task: JoinHandle<()> = tokio::spawn(async { panic!("owned reader sentinel") });
        let panic_id = task.id();
        let panic = Some(Err(task.await.unwrap_err()));
        assert!(!supported_reader(
            &panic,
            Some((panic_id, true)),
            Some(close)
        ));
        assert!(!supported_reader(&None, Some((id, true)), Some(close)));
        assert!(supported_reader(
            &Some(Ok(())),
            Some((id, true)),
            Some(close)
        ));
    }

    #[test]
    fn websocket_terminal_disposition_requires_exact_observation_and_peer_reply_facts() {
        let original = Some(Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "controlled raw close failure",
        )
        .into()));
        let close = CloseFacts {
            valid_frame: true,
            clean: true,
            peer_initiated: true,
            reply_ready: true,
            reply_ok: true,
        };
        for kind in [
            std::io::ErrorKind::UnexpectedEof,
            std::io::ErrorKind::ConnectionReset,
            std::io::ErrorKind::BrokenPipe,
        ] {
            assert!(terminal_close_io_kind(Some(kind)));
            assert!(supported_websocket(
                &original,
                Some(kind),
                true,
                Some(close)
            ));
        }
        for kind in [
            None,
            Some(std::io::ErrorKind::Other),
            Some(std::io::ErrorKind::TimedOut),
            Some(std::io::ErrorKind::WouldBlock),
            Some(std::io::ErrorKind::Interrupted),
            Some(std::io::ErrorKind::ConnectionAborted),
        ] {
            assert!(!terminal_close_io_kind(kind));
            assert!(!supported_websocket(&original, kind, true, Some(close)));
        }
        assert!(!supported_websocket(
            &original,
            Some(std::io::ErrorKind::UnexpectedEof),
            false,
            Some(close)
        ));
        assert!(!supported_websocket(
            &original,
            Some(std::io::ErrorKind::UnexpectedEof),
            true,
            None
        ));
        for invalid in [
            CloseFacts {
                valid_frame: false,
                ..close
            },
            CloseFacts {
                clean: false,
                ..close
            },
            CloseFacts {
                peer_initiated: false,
                ..close
            },
            CloseFacts {
                reply_ready: false,
                ..close
            },
            CloseFacts {
                reply_ok: false,
                ..close
            },
        ] {
            assert!(!supported_websocket(
                &original,
                Some(std::io::ErrorKind::UnexpectedEof),
                true,
                Some(invalid)
            ));
        }
        assert!(!supported_websocket(
            &None,
            Some(std::io::ErrorKind::UnexpectedEof),
            true,
            Some(close)
        ));
        assert!(
            original.as_ref().unwrap().is_err(),
            "raw failure remains a failure"
        );
        assert!(supported_websocket(&Some(Ok(())), None, false, None));
    }
}

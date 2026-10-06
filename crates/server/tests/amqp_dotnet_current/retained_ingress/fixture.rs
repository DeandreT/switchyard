use std::{
    future::{Future, pending, poll_fn},
    io,
    pin::Pin,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::Poll,
};

use auth::{PermissionSet, ResourceScope, SharedAccessKey, SharedAccessPolicy, SharedAccessRule};
use domain::{CommandKind, EntityPath, NamespaceName, QueueConfig, StateMachine};
use futures_util::{StreamExt, stream::FuturesUnordered};
use protocol_amqp::{
    AmqpListener, RetainedAtomicMessagingBuildError, RetainedAtomicMessagingLimits,
    RetainedAtomicMessagingOwner, RetainedAtomicMessagingReport, RetainedConnectionStartError,
    SharedAccessAuthentication,
};
use server::{Broker, BrokerHandle, LocalProposer, SystemClock};
use storage::MemoryStore;
use tokio::{
    net::{TcpListener, TcpStream},
    runtime::Handle,
    sync::{OwnedSemaphorePermit, Semaphore},
};

use super::*;

pub(super) const CONNECTION_HISTORY: usize = 4;
type Root = RetainedAtomicMessagingOwner<SlotAnchor, BrokerHandle>;
pub(super) type Report = RetainedAtomicMessagingReport<SlotAnchor>;
type Accepted = io::Result<(TcpStream, std::net::SocketAddr)>;
type ClientResult = TestResult<process::Output>;
type PanicPayload = Box<dyn std::any::Any + Send>;

pub(super) struct SlotAnchor {
    pub(super) ordinal: usize,
    _permit: OwnedSemaphorePermit,
    drops: Arc<AtomicUsize>,
    _not_send: Rc<()>,
}

impl Drop for SlotAnchor {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

pub(super) struct Controller {
    listener: Option<TcpListener>,
    pub(super) accepted: Option<Accepted>,
    pub(super) accept_ready: Arc<AtomicBool>,
    roots: [Option<Root>; CONNECTION_HISTORY],
    pub(super) reports: [Option<Report>; CONNECTION_HISTORY],
    starts: [Option<Result<(), RetainedConnectionStartError<BrokerHandle>>>; CONNECTION_HISTORY],
    build_error: Option<RetainedAtomicMessagingBuildError<SlotAnchor>>,
    accept_error: Option<io::Error>,
    unlaunched: Option<TcpStream>,
    pub(super) overflow: Option<TcpStream>,
    pub(super) lifetime: usize,
    next_root: usize,
    pub(super) permits: Arc<Semaphore>,
    pub(super) anchor_drops: Arc<AtomicUsize>,
    broker: BrokerHandle,
    namespace: NamespaceName,
    tls: Option<rustls::ServerConfig>,
    authentication: Option<SharedAccessAuthentication>,
    runtime: Handle,
}

impl Controller {
    fn new(
        listener: TcpListener,
        broker: BrokerHandle,
        namespace: NamespaceName,
        tls: Option<rustls::ServerConfig>,
        authentication: Option<SharedAccessAuthentication>,
    ) -> Self {
        Self {
            listener: Some(listener),
            accepted: None,
            accept_ready: Arc::new(AtomicBool::new(false)),
            roots: std::array::from_fn(|_| None),
            reports: std::array::from_fn(|_| None),
            starts: std::array::from_fn(|_| None),
            build_error: None,
            accept_error: None,
            unlaunched: None,
            overflow: None,
            lifetime: 0,
            next_root: 0,
            permits: Arc::new(Semaphore::new(CONNECTION_HISTORY)),
            anchor_drops: Arc::new(AtomicUsize::new(0)),
            broker,
            namespace,
            tls,
            authentication,
            runtime: Handle::current(),
        }
    }

    pub(super) fn accepting(&self) -> bool {
        self.listener.is_some()
    }

    pub(super) fn report_count(&self) -> usize {
        self.reports
            .iter()
            .filter(|report| report.is_some())
            .count()
    }

    pub(super) fn close_and_stop(&mut self) {
        self.listener.take();
        for root in self.roots.iter_mut().flatten() {
            root.stop();
        }
    }

    pub(super) async fn observe_accept(&mut self, hold_ready: bool) {
        observe_accept(
            self.listener.as_ref(),
            &mut self.accepted,
            &self.accept_ready,
            hold_ready,
        )
        .await;
    }

    pub(super) fn install_accepted(&mut self) {
        let Some(receipt) = self.accepted.take() else {
            return;
        };
        let stream = match receipt {
            Ok((stream, _)) => stream,
            Err(error) => {
                self.accept_error = Some(error);
                self.close_and_stop();
                return;
            }
        };
        if self.lifetime == CONNECTION_HISTORY {
            self.overflow = Some(stream);
            self.close_and_stop();
            return;
        }
        let ordinal = self.lifetime;
        self.lifetime += 1;
        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .expect("one permit for each never-reused lifetime slot");
        let anchor = SlotAnchor {
            ordinal,
            _permit: permit,
            drops: self.anchor_drops.clone(),
            _not_send: Rc::new(()),
        };
        let limits =
            RetainedAtomicMessagingLimits::new(32, 128).expect("fixed bounded fixture limits");
        let (root, starter) = match Root::new(self.runtime.clone(), limits, anchor) {
            Ok(parts) => parts,
            Err(error) => {
                self.build_error = Some(error);
                self.unlaunched = Some(stream);
                self.close_and_stop();
                return;
            }
        };
        self.roots[ordinal] = Some(root);
        let mut listener = AmqpListener::new(self.broker.clone(), self.namespace.clone());
        if let Some(tls) = &self.tls {
            listener = listener.with_tls(tls.clone());
        }
        if let Some(authentication) = &self.authentication {
            listener = listener.with_shared_access_authentication(authentication.clone());
        }
        self.starts[ordinal] =
            Some(listener.start_retained_collected_atomic_messaging(stream, starter));
        if self.starts[ordinal].as_ref().is_some_and(Result::is_err) {
            self.close_and_stop();
        }
    }

    pub(super) async fn step(&mut self) {
        let accepted = {
            let mut steps = FuturesUnordered::new();
            let (earlier, later) = self.roots.split_at_mut(self.next_root);
            for root in later.iter_mut().chain(earlier.iter_mut()).flatten() {
                steps.push(root.drive_step());
            }
            tokio::select! {
                () = observe_accept(
                    self.listener.as_ref(), &mut self.accepted, &self.accept_ready, false
                ), if self.listener.is_some() && self.accepted.is_none() => true,
                _ = steps.next(), if !steps.is_empty() => false,
                else => { pending::<()>().await; false }
            }
        };
        self.next_root = (self.next_root + 1) % CONNECTION_HISTORY;
        if accepted {
            self.install_accepted();
        } else {
            tokio::task::yield_now().await;
        }
    }

    pub(super) async fn finish_all(&mut self) {
        self.close_and_stop();
        let mut joins = FuturesUnordered::new();
        for (root, report) in self.roots.iter_mut().zip(self.reports.iter_mut()) {
            if report.is_some() {
                continue;
            }
            if let Some(root) = root {
                joins.push(finish_root(root, report));
            }
        }
        while joins.next().await.is_some() {}
    }

    pub(super) fn has_setup_failure(&self) -> bool {
        self.accept_error.is_some()
            || self.build_error.is_some()
            || self.unlaunched.is_some()
            || self.overflow.is_some()
            || self.starts.iter().flatten().any(Result::is_err)
    }
}

async fn finish_root(root: &mut Root, destination: &mut Option<Report>) {
    let future = root.finish();
    tokio::pin!(future);
    poll_fn(|context| match future.as_mut().poll(context) {
        Poll::Ready(report) => {
            *destination = report;
            Poll::Ready(())
        }
        Poll::Pending => Poll::Pending,
    })
    .await;
}

impl Drop for Controller {
    fn drop(&mut self) {
        // Unreported roots then invoke their permanent-holder Drop path.
        self.close_and_stop();
    }
}

async fn observe_accept(
    listener: Option<&TcpListener>,
    destination: &mut Option<Accepted>,
    ready: &AtomicBool,
    hold_ready: bool,
) {
    let Some(listener) = listener else {
        return pending().await;
    };
    let future = listener.accept();
    tokio::pin!(future);
    poll_fn(|context| match future.as_mut().poll(context) {
        Poll::Ready(receipt) => {
            *destination = Some(receipt);
            ready.store(true, Ordering::SeqCst);
            Poll::Ready(())
        }
        Poll::Pending => Poll::Pending,
    })
    .await;
    if hold_ready {
        pending::<()>().await;
    }
}

pub(super) struct Fixture {
    pub(super) endpoint: String,
    pub(super) ca_file: std::path::PathBuf,
    pub(super) ca_directory: std::path::PathBuf,
    pub(super) store: MemoryStore,
    pub(super) namespace: NamespaceName,
    pub(super) controller: Controller,
    pub(super) client: Option<ClientResult>,
    pub(super) client_panic: Option<PanicPayload>,
    broker: Option<Broker>,
    _certificates: tempfile::TempDir,
}

impl Fixture {
    pub(super) async fn start(secure: bool) -> TestResult<Self> {
        let store = MemoryStore::default();
        let namespace = NamespaceName::new("tenant")?;
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            SystemClock,
        ));
        broker.handle().submit_blocking(
            namespace.clone(),
            EntityPath::new(QUEUE)?,
            CommandKind::CreateQueue {
                config: QueueConfig {
                    lock_duration_millis: domain::MAX_LOCK_DURATION_MILLIS,
                    default_time_to_live_millis: None,
                    ..QueueConfig::default()
                },
            },
        )?;
        let certificates = tempfile::TempDir::new()?;
        let ca_file = certificates.path().join("trusted-ca.pem");
        let ca_directory = certificates.path().join("empty-ca-directory");
        let (tls, authentication) = if secure {
            let (tls, ca_pem) = websocket::signed_localhost_config()?;
            std::fs::write(&ca_file, ca_pem)?;
            std::fs::create_dir(&ca_directory)?;
            let authentication = SharedAccessAuthentication::new(
                SharedAccessPolicy::new([SharedAccessRule::new(
                    RULE,
                    ResourceScope::namespace(HOST)?,
                    SharedAccessKey::new(KEY)?,
                    None,
                    PermissionSet::MANAGE,
                )?])?,
                HOST,
            )?
            .with_authorization_timeout(Duration::from_secs(15));
            (Some(tls), Some(authentication))
        } else {
            (None, None)
        };
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("sb://localhost:{}", listener.local_addr()?.port());
        let controller = Controller::new(
            listener,
            broker.handle(),
            namespace.clone(),
            tls,
            authentication,
        );
        Ok(Self {
            endpoint,
            ca_file,
            ca_directory,
            store,
            namespace,
            controller,
            client: None,
            client_panic: None,
            broker: Some(broker),
            _certificates: certificates,
        })
    }

    pub(super) async fn start_with_offline_jwt(policy: auth::JwtPolicy) -> TestResult<Self> {
        let (tls, ca_pem) = websocket::signed_localhost_config()?;
        let authentication = SharedAccessAuthentication::new(SharedAccessPolicy::new([])?, HOST)?
            .with_authorization_timeout(Duration::from_secs(15))
            .with_offline_jwt_policy(policy);
        let certificates = tempfile::TempDir::new()?;
        let ca_file = certificates.path().join("trusted-ca.pem");
        let ca_directory = certificates.path().join("empty-ca-directory");
        std::fs::write(&ca_file, ca_pem)?;
        std::fs::create_dir(&ca_directory)?;
        let store = MemoryStore::default();
        let namespace = NamespaceName::new("tenant")?;
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            SystemClock,
        ));
        broker.handle().submit_blocking(
            namespace.clone(),
            EntityPath::new(QUEUE)?,
            CommandKind::CreateQueue {
                config: QueueConfig {
                    lock_duration_millis: domain::MAX_LOCK_DURATION_MILLIS,
                    default_time_to_live_millis: None,
                    ..QueueConfig::default()
                },
            },
        )?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("sb://localhost:{}", listener.local_addr()?.port());
        let controller = Controller::new(
            listener,
            broker.handle(),
            namespace.clone(),
            Some(tls),
            Some(authentication),
        );
        Ok(Self {
            endpoint,
            ca_file,
            ca_directory,
            store,
            namespace,
            controller,
            client: None,
            client_panic: None,
            broker: Some(broker),
            _certificates: certificates,
        })
    }

    pub(super) async fn connect_peer(&self) -> TestResult<TcpStream> {
        let address = self.endpoint.trim_start_matches("sb://localhost:");
        Ok(TcpStream::connect(format!("127.0.0.1:{address}")).await?)
    }

    pub(super) async fn drive_client<F>(&mut self, mut client: Pin<&mut F>)
    where
        F: Future<Output = ClientResult>,
    {
        while self.client.is_none() && self.client_panic.is_none() {
            tokio::select! {
                () = observe_client(client.as_mut(), &mut self.client, &mut self.client_panic) => {},
                () = self.controller.step() => {},
            }
        }
    }

    pub(super) async fn finish_with_client<F>(&mut self, mut client: Pin<&mut F>)
    where
        F: Future<Output = ClientResult>,
    {
        self.controller.close_and_stop();
        let joins = self.controller.finish_all();
        tokio::pin!(joins);
        loop {
            tokio::select! {
                () = &mut joins => break,
                () = observe_client(client.as_mut(), &mut self.client, &mut self.client_panic),
                    if self.client.is_none() && self.client_panic.is_none() => {},
            }
        }
        if self.client.is_none() && self.client_panic.is_none() {
            observe_client(client, &mut self.client, &mut self.client_panic).await;
        }
        // Existing Broker Drop discards its owner join result; not a health proof.
        drop(self.broker.take());
    }

    pub(super) async fn finish(&mut self) {
        self.controller.finish_all().await;
        drop(self.broker.take());
    }
}

async fn observe_client<F>(
    mut future: Pin<&mut F>,
    destination: &mut Option<ClientResult>,
    panic_destination: &mut Option<PanicPayload>,
) where
    F: Future<Output = ClientResult>,
{
    poll_fn(|context| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            future.as_mut().poll(context)
        })) {
            Ok(Poll::Ready(result)) => {
                *destination = Some(result);
                Poll::Ready(())
            }
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => {
                *panic_destination = Some(payload);
                Poll::Ready(())
            }
        }
    })
    .await;
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.controller.close_and_stop();
        drop(self.broker.take());
    }
}

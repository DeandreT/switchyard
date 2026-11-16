//! Supported Atom Get retains its actual backend original after its TLS IO ends.
#![cfg(feature = "workload-tls")]
mod workload;
use admin_api::{
    EntityBackend, EntityService, EntityTarget, ServiceError,
    atom::{
        https::{Configuration, HttpsListener, Limits},
        subscription_get::SubscriptionGet,
    },
    rpc::InternalBackendErrors,
};
use domain::{
    AdminCatalogCursor, AdminCatalogEntry, AdminCatalogKind, AdminCatalogPage, CommandKind,
    CommandOutcome, EntityPath, NamespaceName, StateMachine, Timestamp,
};
use server::{
    Broker, BrokerHandle, Clock, LocalProposer, ShutdownBudget, SubmitError, SystemClock,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
    mpsc,
};
use storage::{FjallStore, MemoryStore, StateStore};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{Notify, oneshot},
};
use workload::*;
struct Gate {
    entered: oneshot::Sender<()>,
    released: mpsc::Receiver<()>,
}
#[derive(Clone, Default)]
struct GatedClock(Arc<Mutex<Option<Gate>>>);
impl Clock for GatedClock {
    fn now(&self) -> Timestamp {
        if let Some(gate) = self.0.lock().unwrap().take() {
            let _ = gate.entered.send(());
            gate.released.recv_timeout(WAIT).unwrap();
        }
        SystemClock.now()
    }
}
struct Release(Option<mpsc::Sender<()>>);
impl Drop for Release {
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.send(());
        }
    }
}
#[derive(Clone)]
struct Observed {
    inner: BrokerHandle,
    started: Arc<Notify>,
    calls: Arc<AtomicUsize>,
    returned: Arc<AtomicUsize>,
}
impl EntityBackend for Observed {
    type Error = SubmitError;
    async fn create(
        &self,
        ns: NamespaceName,
        entity: EntityPath,
        command: CommandKind,
    ) -> std::result::Result<CommandOutcome, ServiceError<SubmitError>> {
        self.inner.create(ns, entity, command).await
    }
    async fn get(
        &self,
        ns: NamespaceName,
        target: EntityTarget,
    ) -> std::result::Result<Option<AdminCatalogEntry>, ServiceError<SubmitError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        let result = EntityBackend::get(&self.inner, ns, target).await;
        self.returned.fetch_add(1, Ordering::SeqCst);
        result
    }
    async fn list(
        &self,
        ns: NamespaceName,
        kind: AdminCatalogKind,
        after: Option<AdminCatalogCursor>,
        limit: usize,
    ) -> std::result::Result<AdminCatalogPage, ServiceError<SubmitError>> {
        self.inner.list(ns, kind, after, limit).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supported_atom_original_read_survives_expiry_and_disconnect_and_joins_before_memory_fjall_reopen()
-> Result {
    for durable in [false, true] {
        for cancel in [false, true] {
            let dir = tempfile::tempdir()?;
            let memory = MemoryStore::default();
            let clock = GatedClock::default();
            let broker = if durable {
                Broker::spawn(LocalProposer::new(
                    StateMachine::new(FjallStore::open(dir.path())?),
                    clock.clone(),
                ))
            } else {
                Broker::spawn(LocalProposer::new(
                    StateMachine::new(memory.clone()),
                    clock.clone(),
                ))
            };
            let ns = NamespaceName::new("tenant")?;
            let topic = EntityPath::new("events")?;
            let name = domain::SubscriptionName::new("consumer")?;
            broker
                .handle()
                .submit(
                    ns.clone(),
                    topic.clone(),
                    CommandKind::CreateTopic {
                        config: domain::TopicConfig::default(),
                    },
                )
                .await?;
            broker
                .handle()
                .submit(
                    ns.clone(),
                    topic.clone(),
                    CommandKind::CreateSubscription {
                        name: name.clone(),
                        config: domain::SubscriptionConfig::default(),
                    },
                )
                .await?;
            let observed = Observed {
                inner: broker.handle(),
                started: Arc::default(),
                calls: Arc::default(),
                returned: Arc::default(),
            };
            let f = Fixture::new();
            let limits = Limits {
                header_timeout: std::time::Duration::from_secs(30),
                body_timeout: std::time::Duration::from_secs(30),
                request_timeout: std::time::Duration::from_secs(60),
                connection_timeout: std::time::Duration::from_secs(120),
                ..Limits::default()
            };
            for ordinary in [
                limits.handshake_timeout,
                limits.header_timeout,
                limits.body_timeout,
                limits.request_timeout,
                limits.connection_timeout,
            ] {
                assert!(LEASE_IO_BOUND < ordinary);
            }
            let config = Configuration::workload(
                f.endpoint(f.policy(&[&f.old], false, IO_LEASE_SECONDS), b"http/1.1"),
                None,
                ns.clone(),
                auth::ResourceScope::parse(ROOT)?,
            )?
            .with_limits(limits)?;
            let socket = TcpListener::bind("127.0.0.1:0").await?;
            let address = socket.local_addr()?;
            let mut listener = HttpsListener::with_handler(
                socket,
                config,
                Arc::new(SubscriptionGet::new(
                    EntityService::new(ns.clone(), observed.clone()),
                    InternalBackendErrors,
                )),
            );
            let retirement = listener.retirement_handle();
            let serving = tokio::spawn(async move {
                listener.serve().await;
                listener
            });
            let (entered, reached) = oneshot::channel();
            let (release, released) = mpsc::channel();
            let release = Release(Some(release));
            *clock.0.lock().unwrap() = Some(Gate { entered, released });
            let handle = broker.handle();
            let namespace = ns.clone();
            let mutation = tokio::spawn(async move {
                handle
                    .submit(
                        namespace,
                        EntityPath::new("once").unwrap(),
                        CommandKind::CreateQueue {
                            config: domain::QueueConfig::default(),
                        },
                    )
                    .await
            });
            tokio::time::timeout(WAIT, reached).await??;
            // Start the deadline BEFORE dialing: no delayed handshake/request
            // observation can move it into an ordinary timeout's window.
            let io_deadline = tokio::time::Instant::now() + LEASE_IO_BOUND;
            let mut stream = tokio::time::timeout_at(
                io_deadline,
                f.tls12(address, Some(&f.entity), b"http/1.1"),
            )
            .await??;
            stream
                .write_all(
                    format!("GET {SUB} HTTP/1.1\r\nHost: untrusted.example\r\n\r\n").as_bytes(),
                )
                .await?;
            tokio::time::timeout_at(io_deadline, observed.started.notified()).await?;
            if cancel {
                // This branch proves custody under client cancellation, not IO
                // expiry. Only the non-cancel branch is a causal expiry witness.
                drop(stream);
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            } else {
                let mut bytes = Vec::new();
                let _ =
                    tokio::time::timeout_at(io_deadline, stream.read_to_end(&mut bytes)).await?;
                assert!(
                    tokio::time::Instant::now() < io_deadline,
                    "an overdue ready read must not bypass the certificate-specific bound"
                );
                assert!(
                    bytes.is_empty(),
                    "neither backend reply nor ordinary HTTP timeout may impersonate lease retirement"
                );
            }
            assert_eq!(observed.calls.load(Ordering::SeqCst), 1);
            assert_eq!(observed.returned.load(Ordering::SeqCst), 0);
            retirement.request();
            let mut listener = tokio::time::timeout(WAIT, serving).await??;
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(20), listener.finish())
                    .await
                    .is_err()
            );
            drop(release);
            assert!(mutation.await?.is_ok());
            tokio::time::timeout(WAIT, listener.finish()).await?;
            assert!(listener.take_failure().is_none());
            assert_eq!(observed.calls.load(Ordering::SeqCst), 1);
            assert_eq!(observed.returned.load(Ordering::SeqCst), 1);
            let mut owner = Some(broker);
            let mut timer = None;
            server::stop_timer_and_broker(
                &ShutdownBudget::new(tokio::time::Instant::now(), WAIT),
                &mut timer,
                &mut owner,
            )
            .await?;
            assert!(owner.is_none());
            // No failed store is reopened. The successful path requires every actual
            // request/connection join, the mutation receipt and real owner stop first.
            let check = |store: &dyn ReadStore| {
                assert!(store.contains(&domain::keys::queue_config(
                    &ns,
                    &EntityPath::new("once").unwrap()
                )));
                assert!(store.contains(&domain::keys::topic_subscription(&ns, &topic, &name)));
            };
            if durable {
                check(&FjallStore::open(dir.path())?);
            } else {
                check(&memory);
            }
        }
    }
    Ok(())
}
trait ReadStore {
    fn contains(&self, key: &[u8]) -> bool;
}
impl<S: StateStore> ReadStore for S {
    fn contains(&self, key: &[u8]) -> bool {
        self.get(key).unwrap().is_some()
    }
}

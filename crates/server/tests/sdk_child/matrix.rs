//! Test-only backend selection and ownership of the original listener/timer/store.

use std::{future::Future, io, path::Path, sync::Arc, thread, time::Duration};

use domain::{CommandKind, CommandOutcome, EntityPath, NamespaceName, ReceiveMode};
use protocol_amqp::{AmqpListenerExit, AmqpListenerService};
use server::{Broker, BrokerHandle, Shutdown, TimerWorker};
use storage::{FjallStore, StateStore};

#[derive(Clone, Copy, Debug)]
pub enum Backend {
    Memory,
    Fjall,
}

impl Backend {
    pub fn name(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Fjall => "fjall",
        }
    }
}

pub struct Timer {
    stop: Arc<Shutdown>,
    original: thread::JoinHandle<()>,
}

impl Timer {
    pub fn start(broker: BrokerHandle) -> io::Result<Self> {
        let controlled = super::diagnostic_controls::timer_lag()?.is_some();
        let stop = Arc::new(Shutdown::default());
        let worker_stop = Arc::clone(&stop);
        let original = thread::Builder::new()
            .name("sdk-matrix-timer".into())
            .spawn(move || {
                let worker = TimerWorker::new(&broker);
                let interval = Duration::from_millis(25);
                if controlled {
                    // Retain/join this exact worker thread; only this explicit
                    // diagnostic probe withholds sweeps, never the broker clock.
                    while !worker_stop.is_signalled() {
                        thread::sleep(interval);
                        if worker_stop.is_signalled() {
                            break;
                        }
                        if !super::diagnostic_controls::timer_held() {
                            let report = worker.sweep_once().expect("controlled timer sweep");
                            if !report.is_idle() {
                                super::diagnostics::emit(
                                    "CONTROL timer-sweep NOT-QUALIFICATION",
                                    format!("{report:?}"),
                                );
                            }
                        }
                    }
                } else {
                    worker.run(interval, &worker_stop);
                }
            })?;
        Ok(Self { stop, original })
    }

    async fn join(self) -> io::Result<()> {
        self.stop.signal();
        tokio::task::spawn_blocking(move || self.original.join())
            .await
            .map_err(io::Error::other)?
            .map_err(|_| io::Error::other("the original SDK matrix timer panicked"))
    }
}

/// Borrow the retained service while the controlled SDK child runs. No detached
/// accept task or replacement resources are used as shutdown evidence.
pub async fn serve<T, B: protocol_amqp::Broker>(
    service: &mut AmqpListenerService<B>,
    child: impl Future<Output = io::Result<T>>,
) -> io::Result<T> {
    tokio::select! {
        result = child => result,
        () = service.serve() => Err(io::Error::other("SDK listener stopped before the child finished")),
    }
}

/// Stop admission, finish/join the original protocol task family, then signal
/// and join the original timer. Errors are returned only after both finish.
/// These cooperative joins are not a universal finite-progress guarantee.
pub async fn quiesce<B: protocol_amqp::Broker>(
    mut service: AmqpListenerService<B>,
    timer: Timer,
) -> io::Result<()> {
    service.retirement_handle().request();
    service.serve().await;
    service.finish().await;
    let exit = service.take_exit();
    drop(service);
    let timer_result = timer.join().await;
    timer_result?;
    match exit {
        Some(AmqpListenerExit::Complete(Ok(()))) => Ok(()),
        Some(AmqpListenerExit::Complete(Err(error))) => Err(io::Error::other(error.to_string())),
        Some(AmqpListenerExit::ReportOnly(_)) => Err(io::Error::other(
            "the original SDK listener task family panicked",
        )),
        None => Err(io::Error::other(
            "SDK listener did not yield its terminal outcome",
        )),
    }
}

pub fn empty(broker: &Broker, entity: EntityPath) -> Result<(), Box<dyn std::error::Error>> {
    let outcome = broker.handle().submit_blocking(
        NamespaceName::new("tenant")?,
        entity.clone(),
        CommandKind::Receive {
            mode: ReceiveMode::ReceiveAndDelete,
            lock_duration_millis: None,
            session: None,
        },
    )?;
    if outcome != CommandOutcome::Received(None) {
        return Err(
            format!("native SDK postcondition: {entity:?} was not empty: {outcome:?}").into(),
        );
    }
    Ok(())
}

/// Called only after the original protocol/timer joins and native postconditions.
/// Dropping Broker explicitly joins its owner; the retained store clone is used
/// for a quiescent logical snapshot, then dropped before opening the same path.
pub async fn finish<S: StateStore>(
    broker: Broker,
    store: S,
    directory: Option<&Path>,
) -> Result<(), Box<dyn std::error::Error>> {
    tokio::task::spawn_blocking(move || drop(broker)).await?;
    let snapshot = store.snapshot();
    drop(store);
    let snapshot = snapshot?;
    if let Some(directory) = directory {
        let reopened = FjallStore::open(directory)?;
        assert_eq!(
            snapshot,
            reopened.snapshot()?,
            "logical records changed across reopen"
        );
        drop(reopened);
        println!(
            "SDK_DURABLE_REOPEN_VERIFIED original listener/timer/broker joined; stores dropped; logical records equal"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use domain::{QueueConfig, StateMachine};
    use server::{LocalProposer, SystemClock};
    use storage::MemoryStore;
    use tokio::net::TcpListener;

    async fn failed_child_still_joins<S: StateStore>(store: S, directory: Option<&Path>) {
        let broker = Broker::spawn(LocalProposer::new(
            StateMachine::new(store.clone()),
            SystemClock,
        ));
        broker
            .handle()
            .submit_blocking(
                NamespaceName::new("tenant").unwrap(),
                EntityPath::new("retained").unwrap(),
                CommandKind::CreateQueue {
                    config: QueueConfig::default(),
                },
            )
            .unwrap();
        assert!(!store.snapshot().unwrap().entries().is_empty());
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = socket.local_addr().unwrap();
        let mut service = protocol_amqp::AmqpListener::new(
            broker.handle(),
            NamespaceName::new("tenant").unwrap(),
        )
        .into_tcp_service(socket);
        let timer = Timer::start(broker.handle()).unwrap();
        let child = serve(&mut service, async {
            Err::<(), _>(io::Error::other("controlled child failure"))
        })
        .await;
        assert!(child.is_err());
        quiesce(service, timer).await.unwrap();
        assert!(
            std::net::TcpStream::connect(address).is_err(),
            "original listener socket closed"
        );
        finish(broker, store, directory).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_failed_child_cleanup() {
        failed_child_still_joins(MemoryStore::default(), None).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fjall_failed_child_cleanup_and_nonempty_logical_reopen() {
        let directory = tempfile::tempdir().unwrap();
        failed_child_still_joins(
            FjallStore::open(directory.path()).unwrap(),
            Some(directory.path()),
        )
        .await;
    }
}

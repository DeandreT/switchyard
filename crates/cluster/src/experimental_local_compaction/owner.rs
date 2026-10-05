use std::{
    future::Future,
    sync::{Arc, Mutex},
};

use super::{
    Details, ExperimentalLocalCompactionPair, LocalCompactionError as Error,
    LocalCompactionProgress,
    frontier::{Frontier, PairIdentity},
};
use crate::{
    ExperimentalStateMachine, StateMachineError,
    experimental_log::local_compaction::ExperimentalCompactionLogStore,
    experimental_owner::{OwnerJoinError, RetiredOwner},
};
use tokio::{
    runtime::Handle as Runtime,
    sync::{Notify, oneshot, watch},
    task::JoinHandle,
};

struct Admission {
    state: Mutex<AdmissionState>,
    closed: Notify,
    frontier: Frontier,
}
struct AdmissionState {
    closed: bool,
    busy: bool,
}
struct Lease(Arc<Admission>);
impl Drop for Lease {
    fn drop(&mut self) {
        self.0.state.lock().unwrap_or_else(|e| e.into_inner()).busy = false;
    }
}
type Published = (
    Result<LocalCompactionProgress, Error>,
    oneshot::Receiver<()>,
);
#[cfg(test)]
type ExitReceiver = watch::Receiver<Option<Result<(), Error>>>;
struct Packet {
    reply: Option<oneshot::Sender<Published>>,
    lease: Option<Lease>,
}
impl Packet {
    fn finish(mut self, result: Result<LocalCompactionProgress, Error>) {
        let (completed, receiver) = oneshot::channel();
        if let Some(reply) = self.reply.take() {
            let _ = reply.send((result, receiver));
        }
        drop(self.lease.take());
        let _ = completed.send(());
    }
}

#[derive(Clone)]
pub(super) struct Handle {
    sender: flume::Sender<Packet>,
    admission: Arc<Admission>,
}
impl Handle {
    #[cfg(test)]
    pub(super) fn observer(&self) -> Frontier {
        self.admission.frontier.clone()
    }
    pub(super) fn close(&self) {
        self.admission
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .closed = true;
        self.admission.frontier.close(Error::Closed);
        self.admission.closed.notify_waiters();
    }
    pub(super) async fn compact(&self) -> Result<LocalCompactionProgress, Error> {
        let (reply, receiver) = oneshot::channel();
        let admitted = {
            let mut state = self.admission.state.lock().map_err(|_| Error::TaskFailed)?;
            if state.closed {
                return Err(Error::Closed);
            }
            if state.busy {
                return Err(Error::Busy);
            }
            state.busy = true;
            self.sender
                .try_send(Packet {
                    reply: Some(reply),
                    lease: Some(Lease(self.admission.clone())),
                })
                .map_err(|error| error.into_inner())
        };
        if let Err(packet) = admitted {
            packet.finish(Err(Error::Closed));
            return Err(Error::Closed);
        }
        let (result, completed) = receiver.await.map_err(|_| Error::TaskFailed)?;
        completed.await.map_err(|_| Error::TaskFailed)?;
        result
    }
}

struct Resources {
    log: Option<ExperimentalCompactionLogStore>,
    state: Option<ExperimentalStateMachine>,
    log_token: Option<RetiredOwner<Error>>,
    state_token: Option<RetiredOwner<StateMachineError>>,
    log_join: Option<JoinHandle<Result<(), OwnerJoinError<Error>>>>,
    state_join: Option<JoinHandle<Result<(), OwnerJoinError<StateMachineError>>>>,
    log_done: bool,
    state_done: bool,
    failed: bool,
    #[cfg(test)]
    cleanup_started: Option<oneshot::Sender<()>>,
}
impl Resources {
    fn new(log: ExperimentalCompactionLogStore, state: ExperimentalStateMachine) -> Self {
        Self {
            log: Some(log),
            state: Some(state),
            log_token: None,
            state_token: None,
            log_join: None,
            state_join: None,
            log_done: false,
            state_done: false,
            failed: false,
            #[cfg(test)]
            cleanup_started: None,
        }
    }
    fn arm(&mut self) -> Result<(), Error> {
        let mut missing = false;
        if let Some(log) = self.log.take() {
            let (log, token) = log.into_parts();
            missing |= token.is_none();
            self.log = Some(log);
            self.log_token = token;
        } else {
            missing = true;
        }
        if let Some(state) = self.state.take() {
            let (state, token) = state.into_local_compaction_parts();
            missing |= token.is_none();
            self.state = Some(state);
            self.state_token = token;
        } else {
            missing = true;
        }
        if missing {
            Err(Error::OwnerFailure)
        } else {
            Ok(())
        }
    }
    async fn finish(&mut self, fallback: &Runtime) -> Result<(), Error> {
        drop(self.log.take());
        drop(self.state.take());
        if let Some(token) = self.log_token.take() {
            self.log_join = Some(fallback.spawn(token.join()));
        }
        if let Some(token) = self.state_token.take() {
            self.state_join = Some(fallback.spawn(token.join()));
        }
        #[cfg(test)]
        if let Some(started) = self.cleanup_started.take() {
            let _ = started.send(());
        }
        if !self.log_done {
            if let Some(join) = &mut self.log_join {
                self.failed |= !matches!(join.await, Ok(Ok(())));
            }
            self.log_done = true;
            self.log_join = None;
        }
        if !self.state_done {
            if let Some(join) = &mut self.state_join {
                self.failed |= !matches!(join.await, Ok(Ok(())));
            }
            self.state_done = true;
            self.state_join = None;
        }
        if self.failed {
            Err(Error::OwnerFailure)
        } else {
            Ok(())
        }
    }
}

/// Accepted first poll arms fallback rescue, including before the worker polls.
/// A rescue only rearms on its first poll, avoiding recursive dead-fallback spawn.
struct Cleanup {
    resources: Option<Resources>,
    ready: Option<oneshot::Sender<Result<ExperimentalLocalCompactionPair, Error>>>,
    active: Option<Packet>,
    active_result: Option<Result<LocalCompactionProgress, Error>>,
    exit: Option<watch::Sender<Option<Result<(), Error>>>>,
    frontier: Frontier,
    runtime: Runtime,
    polled: bool,
}
impl Cleanup {
    async fn finish(&mut self, failure: Option<Error>) -> Result<(), Error> {
        let result = match &mut self.resources {
            Some(resources) => resources.finish(&self.runtime).await,
            None => Err(Error::TaskFailed),
        };
        let final_result = result.and_then(|()| failure.map_or(Ok(()), Err));
        if let Some(packet) = self.active.take() {
            let response = match result {
                Err(error) => Err(error),
                Ok(()) => self
                    .active_result
                    .take()
                    .unwrap_or_else(|| Err(final_result.err().unwrap_or(Error::Closed))),
            };
            packet.finish(response);
        }
        if let Some(ready) = self.ready.take() {
            let _ = ready.send(Err(final_result.err().unwrap_or(Error::TaskFailed)));
        }
        if let Some(exit) = self.exit.take() {
            let _ = exit.send(Some(final_result));
        }
        self.resources = None;
        final_result
    }
    async fn rescue(mut self) {
        self.polled = true;
        self.frontier.close(Error::TaskFailed);
        let _ = self.finish(Some(Error::TaskFailed)).await;
    }
}
impl Drop for Cleanup {
    fn drop(&mut self) {
        if self.resources.is_none() {
            return;
        }
        self.frontier.close(Error::TaskFailed);
        if !self.polled {
            return;
        }
        let rescued = Self {
            resources: self.resources.take(),
            ready: self.ready.take(),
            active: self.active.take(),
            active_result: self.active_result.take(),
            exit: self.exit.take(),
            frontier: self.frontier.clone(),
            runtime: self.runtime.clone(),
            polled: false,
        };
        drop(self.runtime.spawn(rescued.rescue()));
    }
}

pub(super) fn prepare(
    node_id: u64,
    log: ExperimentalCompactionLogStore,
    state: ExperimentalStateMachine,
    fallback: Runtime,
) -> impl Future<Output = Result<ExperimentalLocalCompactionPair, Error>> + Send + 'static {
    let packet = prepare_packet(node_id, log, state, fallback);
    async move { packet.start().await }
}

#[cfg(test)]
pub(super) fn prepare_observed(
    node_id: u64,
    log: ExperimentalCompactionLogStore,
    state: ExperimentalStateMachine,
    fallback: Runtime,
) -> (
    impl Future<Output = Result<ExperimentalLocalCompactionPair, Error>> + Send + 'static,
    ExitReceiver,
) {
    let packet = prepare_packet(node_id, log, state, fallback);
    let exit = packet.exit.as_ref().unwrap().clone();
    (async move { packet.start().await }, exit)
}

#[cfg(test)]
pub(super) struct CleanupObservation {
    pub(super) exit: watch::Receiver<Option<Result<(), Error>>>,
    pub(super) started: oneshot::Receiver<()>,
}

#[cfg(test)]
pub(super) fn prepare_observed_with_cleanup(
    node_id: u64,
    log: ExperimentalCompactionLogStore,
    state: ExperimentalStateMachine,
    fallback: Runtime,
) -> (
    impl Future<Output = Result<ExperimentalLocalCompactionPair, Error>> + Send + 'static,
    CleanupObservation,
) {
    let mut packet = prepare_packet(node_id, log, state, fallback);
    let exit = packet.exit.as_ref().unwrap().clone();
    let (started, receiver) = oneshot::channel();
    packet
        .cleanup
        .as_mut()
        .unwrap()
        .resources
        .as_mut()
        .unwrap()
        .cleanup_started = Some(started);
    (
        async move { packet.start().await },
        CleanupObservation {
            exit,
            started: receiver,
        },
    )
}

fn prepare_packet(
    node_id: u64,
    log: ExperimentalCompactionLogStore,
    state: ExperimentalStateMachine,
    fallback: Runtime,
) -> Unstarted {
    let identity = PairIdentity::new();
    let (frontier, publisher) = Frontier::new(identity.clone());
    let (ready, receiver) = oneshot::channel();
    let (exit, exit_receiver) = watch::channel(None);
    let mut resources = Resources::new(log, state);
    // Infallible ownership handoffs are all attempted before reporting any
    // missing token. Arming does not query or write either source.
    let arm_error = resources.arm().err();
    let guard = Cleanup {
        resources: Some(resources),
        ready: Some(ready),
        active: None,
        active_result: None,
        exit: Some(exit),
        frontier: frontier.clone(),
        runtime: fallback.clone(),
        polled: false,
    };
    Unstarted {
        node_id,
        identity,
        frontier,
        publisher: Some(publisher),
        cleanup: Some(guard),
        receiver: Some(receiver),
        exit: Some(exit_receiver),
        fallback,
        arm_error,
    }
}

struct Unstarted {
    node_id: u64,
    identity: PairIdentity,
    frontier: Frontier,
    publisher: Option<super::frontier::Publisher>,
    cleanup: Option<Cleanup>,
    receiver: Option<oneshot::Receiver<Result<ExperimentalLocalCompactionPair, Error>>>,
    exit: Option<watch::Receiver<Option<Result<(), Error>>>>,
    fallback: Runtime,
    arm_error: Option<Error>,
}
impl Unstarted {
    async fn start(mut self) -> Result<ExperimentalLocalCompactionPair, Error> {
        let current = Runtime::try_current();
        let runtime = current
            .as_ref()
            .cloned()
            .unwrap_or_else(|_| self.fallback.clone());
        let mut cleanup = self.cleanup.take().ok_or(Error::TaskFailed)?;
        cleanup.polled = true;
        let publisher = self.publisher.take().ok_or(Error::TaskFailed)?;
        let exit = self.exit.take().ok_or(Error::TaskFailed)?;
        let unavailable = if current.is_err() {
            Some(Error::RuntimeUnavailable)
        } else {
            self.arm_error
        };
        drop(runtime.spawn(run(
            self.node_id,
            self.identity.clone(),
            self.frontier.clone(),
            publisher,
            cleanup,
            exit,
            unavailable,
        )));
        self.receiver
            .as_mut()
            .ok_or(Error::TaskFailed)?
            .await
            .map_err(|_| Error::TaskFailed)?
    }
}
impl Drop for Unstarted {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            self.frontier.close(Error::Closed);
            drop(self.fallback.spawn(cleanup.rescue()));
        }
    }
}

async fn run(
    node_id: u64,
    identity: PairIdentity,
    frontier: Frontier,
    publisher: super::frontier::Publisher,
    mut cleanup: Cleanup,
    exit: watch::Receiver<Option<Result<(), Error>>>,
    unavailable: Option<Error>,
) {
    cleanup.polled = true;
    if let Some(error) = unavailable {
        let _ = cleanup.finish(Some(error)).await;
        return;
    }
    let prepared = validate(
        node_id,
        identity.clone(),
        publisher,
        cleanup.resources.as_mut(),
    )
    .await;
    let (checkpoint, cached) = match prepared {
        Ok(prepared) => prepared,
        Err(error) => {
            let _ = cleanup.finish(Some(error)).await;
            return;
        }
    };
    let (sender, receiver) = flume::bounded(1);
    let admission = Arc::new(Admission {
        state: Mutex::new(AdmissionState {
            closed: false,
            busy: false,
        }),
        closed: Notify::new(),
        frontier: frontier.clone(),
    });
    let handle = Handle { sender, admission };
    let worker_handle = handle.clone();
    // The worker retains admission only, never its own ingress sender.
    let worker_admission = worker_handle.admission.clone();
    drop(worker_handle);
    let pair = ExperimentalLocalCompactionPair { handle, exit };
    match cleanup.ready.take() {
        Some(ready) => {
            if let Err(pair) = ready.send(Ok(pair)) {
                drop(pair);
            }
        }
        None => drop(pair),
    }
    let mut cached = cached;
    loop {
        let closed = worker_admission.closed.notified();
        tokio::pin!(closed);
        closed.as_mut().enable();
        if worker_admission
            .state
            .lock()
            .map_or(true, |state| state.closed)
        {
            if let Ok(packet) = receiver.try_recv() {
                cleanup.active = Some(packet);
            }
            break;
        }
        let packet = tokio::select! {
            biased;
            _ = &mut closed => continue,
            packet = receiver.recv_async() => match packet { Ok(packet) => packet, Err(_) => break },
        };
        cleanup.active = Some(packet);
        let result = match &cached {
            Some(progress) => Ok(progress.clone()),
            None => {
                compact(
                    node_id,
                    &identity,
                    &frontier,
                    &checkpoint,
                    cleanup.resources.as_ref(),
                )
                .await
            }
        };
        if let Ok(progress) = &result {
            cached = Some(progress.clone());
        }
        if worker_admission
            .state
            .lock()
            .map_or(true, |state| state.closed)
        {
            let failure = result
                .as_ref()
                .err()
                .copied()
                .filter(|error| *error != Error::Closed && error.terminal());
            cleanup.active_result = Some(result);
            let _ = cleanup.finish(failure).await;
            return;
        }
        if result.as_ref().is_err_and(|error| error.terminal()) {
            frontier.close(result.as_ref().err().copied().unwrap_or(Error::TaskFailed));
            let failure = result.as_ref().err().copied();
            cleanup.active_result = Some(result);
            let _ = cleanup.finish(failure).await;
            return;
        }
        if let Some(packet) = cleanup.active.take() {
            packet.finish(result);
        }
    }
    frontier.close(Error::Closed);
    let _ = cleanup.finish(None).await;
}

async fn validate(
    node_id: u64,
    identity: PairIdentity,
    publisher: super::frontier::Publisher,
    resources: Option<&mut Resources>,
) -> Result<(domain::CommittedCheckpoint, Option<LocalCompactionProgress>), Error> {
    let resources = resources.ok_or(Error::TaskFailed)?;
    let state = resources.state.as_mut().ok_or(Error::Closed)?;
    let checkpoint = state
        .seal_local_compaction(identity.clone(), publisher)
        .await?;
    let catalog = state
        .read_create_send_catalog()
        .await
        .map_err(Error::from_catalog)?;
    let encoded = match &catalog {
        Some(catalog) => {
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(catalog.metadata_bytes().len())
                .map_err(|_| Error::Allocation)?;
            bytes.extend_from_slice(catalog.metadata_bytes());
            Some(bytes)
        }
        None => None,
    };
    let report = resources
        .log
        .as_ref()
        .ok_or(Error::Closed)?
        .seal(identity, Box::new(checkpoint.clone()), encoded)
        .await?;
    if report.profile.node_id() != node_id {
        return Err(Error::InvalidPair);
    }
    let cached = if report.already_compacted {
        let catalog = catalog.as_ref().ok_or(Error::InvalidHistory)?;
        Some(LocalCompactionProgress {
            node_id,
            ordinal: report.ordinal,
            retained_entries: report.retained_entries,
            retained_bytes: report.retained_bytes,
            details: Arc::new(Details::Cached {
                checkpoint: Box::new(checkpoint.clone()),
                projection: catalog.snapshot_meta().clone(),
            }),
        })
    } else {
        None
    };
    drop(catalog);
    Ok((checkpoint, cached))
}

async fn compact(
    node_id: u64,
    identity: &PairIdentity,
    frontier: &Frontier,
    checkpoint: &domain::CommittedCheckpoint,
    resources: Option<&Resources>,
) -> Result<LocalCompactionProgress, Error> {
    let resources = resources.ok_or(Error::TaskFailed)?;
    let attempt = frontier.begin()?;
    let state = resources.state.as_ref().ok_or(Error::Closed)?;
    let _built = state
        .build_local_compaction(
            identity.clone(),
            node_id,
            attempt,
            Box::new(checkpoint.clone()),
        )
        .await?;
    let receipt = frontier.receipt(attempt).await?;
    let details = Arc::new(Details::Captured(receipt.clone()));
    let log = resources.log.as_ref().ok_or(Error::Closed)?;
    let permit = log.permit(frontier.clone(), receipt).await?;
    let report = log.compact(permit).await?;
    Ok(LocalCompactionProgress {
        node_id,
        ordinal: report.ordinal,
        retained_entries: report.retained_entries,
        retained_bytes: report.retained_bytes,
        details,
    })
}

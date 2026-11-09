use std::{
    any::Any,
    future::{Future, pending, poll_fn},
    pin::pin,
    sync::{Arc, atomic::Ordering},
    task::Poll,
};

use storage::ConsensusBackend;
use tokio::{
    sync::mpsc,
    time::{Interval, MissedTickBehavior},
};

use super::{
    DriverError, DriverHandle, DriverOptions, DriverStatus,
    engine::Engine,
    input::{Event, Shared},
};
use crate::{
    consensus::{ConsensusIdentity, ConsensusStore},
    transport::{PeerSender, PeerTransport, SendReceipt, TransportReport},
};

struct Sending {
    peer: u64,
    bytes: usize,
    receipt: SendReceipt,
}
struct AdmissionFence(Arc<Shared>);
#[cfg(not(feature = "consensus-driver-testkit"))]
type DriverPeer = DriverHandle;
#[cfg(feature = "consensus-driver-testkit")]
type DriverPeer = super::testing::Handler;
impl Drop for AdmissionFence {
    fn drop(&mut self) {
        self.0.retire();
    }
}

/// Sole core/store/transport owner. `run` and `finish` borrow it: cancellation
/// preserves queued/begun/completed originals. Retain until finish returns, then
/// destroy/recover the store. Dropping this owner or its ancestor task is NOT
/// graceful shutdown. Synchronous backend work is intentionally not detached.
///
/// ```compile_fail
/// use cluster::consensus::driver::ConsensusDriver;
/// fn duplicate<B: storage::ConsensusBackend>(owner: ConsensusDriver<B>) {
///     let alias = owner.clone();
/// }
/// ```
#[must_use = "retain and finish the original consensus driver"]
pub struct ConsensusDriver<B: ConsensusBackend> {
    // This drop only fences new admissions. It cannot join or certify drain.
    _admission_fence: AdmissionFence,
    engine: Engine<B>,
    transport: PeerTransport<DriverPeer>,
    sender: PeerSender,
    handle: DriverHandle,
    input: mpsc::Receiver<Event>,
    sending: Vec<Sending>,
    sending_bytes: usize,
    options: DriverOptions,
    timer: Option<Interval>,
    report: Option<TransportReport>,
    #[cfg(feature = "consensus-driver-testkit")]
    controls: super::testing::NetworkControl,
    #[cfg(feature = "consensus-driver-testkit")]
    dispatch_gate: super::testing::DispatchGate,
}
impl<B: ConsensusBackend> ConsensusDriver<B> {
    /// Validate all options and bind the fixed endpoint BEFORE invoking factory.
    /// `fresh` means explicit empty-store bootstrap, never history adoption.
    /// Factory is synchronous; cancellation cannot detach its begun native open.
    pub async fn bind(
        identity: ConsensusIdentity,
        local: u64,
        options: DriverOptions,
        fresh: bool,
        factory: impl FnOnce() -> Result<B, storage::StorageError>,
    ) -> Result<(Self, DriverHandle), DriverError> {
        let codec = options.validate(&identity, local)?;
        let shared = Shared::new(identity.clone(), local, &options);
        let (queue, input) = mpsc::channel(options.max_events);
        let handle = DriverHandle { shared, queue };
        #[cfg(feature = "consensus-driver-testkit")]
        let dispatch_gate = super::testing::DispatchGate::default();
        #[cfg(feature = "consensus-driver-testkit")]
        let handler = super::testing::Handler {
            handle: handle.clone(),
            gate: dispatch_gate.clone(),
        };
        #[cfg(not(feature = "consensus-driver-testkit"))]
        let handler = handle.clone();
        let (transport, sender) =
            PeerTransport::bind(identity.clone(), local, options.transport.clone(), handler)
                .await?;
        let backend = factory()?;
        let store = if fresh {
            ConsensusStore::bootstrap(backend, identity, local, options.retained)?
        } else {
            ConsensusStore::open(backend, identity, local, options.retained)?
        };
        let engine = Engine::new(store, options.clone(), codec)?;
        let timer = options.tick_interval.map(|d| {
            let mut timer = tokio::time::interval_at(tokio::time::Instant::now() + d, d);
            timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
            timer
        });
        let driver = Self {
            _admission_fence: AdmissionFence(handle.shared.clone()),
            engine,
            transport,
            sender,
            handle: handle.clone(),
            input,
            sending: Vec::new(),
            sending_bytes: 0,
            options,
            timer,
            report: None,
            #[cfg(feature = "consensus-driver-testkit")]
            controls: super::testing::NetworkControl::default(),
            #[cfg(feature = "consensus-driver-testkit")]
            dispatch_gate,
        };
        driver.publish();
        Ok((driver, handle))
    }
    pub fn status(&self) -> DriverStatus {
        self.handle.status()
    }
    pub fn transport_status(&self) -> crate::transport::TransportStatus {
        self.transport.status()
    }
    pub fn error(&self) -> Option<&DriverError> {
        self.engine.error.as_ref()
    }
    pub fn panic_payload(&self) -> Option<&(dyn Any + Send)> {
        self.engine.panic.as_deref()
    }
    #[cfg(feature = "consensus-driver-testkit")]
    pub fn network_control(&self) -> super::testing::NetworkControl {
        self.controls.clone()
    }
    #[cfg(feature = "consensus-driver-testkit")]
    pub fn dispatch_gate(&self) -> super::testing::DispatchGate {
        self.dispatch_gate.clone()
    }
    pub fn retire(&self) {
        self.handle.retire();
        self.transport.retire();
    }
    fn publish(&self) {
        let mut status = self.engine.status.clone();
        status.pending_outbound = self.sending.len() + self.engine.outbox.len();
        status.pending_outbound_bytes = self.sending_bytes + self.engine.outbox_bytes;
        *self.handle.shared.status.lock().expect("driver status") = status;
    }
    fn fail_transport(&mut self) {
        self.engine.status.retired = true;
        self.engine.status.accepting_proposals = false;
        self.handle.retire();
    }
    fn flush(&mut self) {
        if !self.handle.shared.begin() {
            return;
        }
        // A bounded loss set, not an async retry queue. Never recursively step
        // the core during a Ready or wait for transport capacity in a callback.
        let mut lost = [false; 3];
        while let Some(message) = self.engine.pop() {
            if self.engine.status.retired {
                break;
            }
            let slot = self
                .handle
                .shared
                .identity
                .voters()
                .iter()
                .position(|v| v.id == message.to)
                .expect("core voter");
            #[cfg(feature = "consensus-driver-testkit")]
            let message = match self.controls.intercept(message) {
                super::testing::Disposition::Deliver(m) => m,
                super::testing::Disposition::Held => continue,
                super::testing::Disposition::Lost => {
                    lost[slot] = true;
                    continue;
                }
            };
            let bytes = protobuf::Message::compute_size(&message) as usize + 128;
            if self.sending.len() >= self.options.max_outbound
                || self.sending_bytes + bytes > self.options.max_outbound_bytes
            {
                lost[slot] = true;
                continue;
            }
            match self.sender.try_send(message) {
                Ok(receipt) => {
                    self.sending_bytes += bytes;
                    self.sending.push(Sending {
                        peer: self.handle.shared.identity.voters()[slot].id,
                        bytes,
                        receipt,
                    });
                }
                Err(_) => {
                    lost[slot] = true;
                }
            }
        }
        for (slot, lost) in lost.into_iter().enumerate() {
            if lost {
                self.engine.status.send_failures =
                    self.engine.status.send_failures.saturating_add(1);
                self.engine
                    .unreachable(self.handle.shared.identity.voters()[slot].id);
            }
        }
        if self.engine.status.retired {
            self.handle.retire();
        }
        self.publish();
    }
    pub async fn run(&mut self) -> TransportReport {
        if let Some(report) = &self.report {
            return *report;
        }
        // Restarting a canceled borrower resumes originals, never reconstructs
        // the RawNode from cached status or retries a failed Ready.
        if self.handle.shared.begin() {
            let _ = self.engine.guarded(|e| e.persist_ready());
        }
        loop {
            if self.engine.status.retired || self.handle.shared.retired.load(Ordering::Acquire) {
                return self.finish().await;
            }
            self.flush();
            if self.engine.status.retired {
                continue;
            }
            enum Wake {
                Event(Option<Event>),
                Tick,
                Stop,
                Transport,
                Receipt(usize, Result<(), crate::transport::TransportError>),
                Control,
            }
            let wake = tokio::select! {
                event=self.input.recv()=>Wake::Event(event),
                _=tick(&mut self.timer)=>Wake::Tick,
                _=self.handle.shared.stop.notified()=>Wake::Stop,
                _=self.transport.serve()=>Wake::Transport,
                (index,result)=poll_sending(&mut self.sending)=>Wake::Receipt(index,result),
                _=async {
                    #[cfg(feature="consensus-driver-testkit")]
                    self.controls.changed().await;
                    #[cfg(not(feature="consensus-driver-testkit"))]
                    pending::<()>().await;
                }=>Wake::Control,
            };
            match wake {
                Wake::Event(Some(event)) => {
                    if self.handle.shared.begin() {
                        self.engine.status.processing = true;
                        self.publish();
                        #[cfg(feature = "consensus-driver-testkit")]
                        let append = super::testing::append_metadata(&event);
                        self.engine.process(event);
                        #[cfg(feature = "consensus-driver-testkit")]
                        if !self.engine.status.retired {
                            self.controls.observe_processed(append, &self.engine.core);
                        }
                        self.engine.status.processing = false;
                    } else {
                        event.reject(super::AdmissionError::Retired);
                    }
                }
                Wake::Event(None) | Wake::Transport => self.fail_transport(),
                Wake::Tick => {
                    if self.handle.shared.begin() {
                        let _ = self.engine.tick();
                    }
                }
                Wake::Stop => {}
                Wake::Receipt(index, result) => {
                    let sending = self.sending.swap_remove(index);
                    self.sending_bytes -= sending.bytes;
                    if result.is_err() && self.handle.shared.begin() {
                        self.engine.status.send_failures =
                            self.engine.status.send_failures.saturating_add(1);
                        self.engine.unreachable(sending.peer);
                    }
                }
                Wake::Control =>
                {
                    #[cfg(feature = "consensus-driver-testkit")]
                    for message in self.controls.take_released() {
                        let _ = self.engine.guarded(|e| e.stage_for_test(message));
                    }
                }
            }
            self.publish();
        }
    }
    pub async fn finish(&mut self) -> TransportReport {
        if let Some(report) = &self.report {
            return *report;
        }
        // Admission fence precedes EVERY drain. No deadline reset, abort or
        // timeout success: actual original transport joins determine completion.
        self.retire();
        self.input.close();
        self.engine.status.retired = true;
        self.engine.status.accepting_proposals = false;
        while let Ok(event) = self.input.try_recv() {
            event.reject(super::AdmissionError::Retired);
        }
        self.publish();
        let report = self.transport.finish().await;
        while !self.sending.is_empty() {
            let (index, _) = poll_sending(&mut self.sending).await;
            let sending = self.sending.swap_remove(index);
            self.sending_bytes -= sending.bytes;
        }
        self.engine.outbox.clear();
        self.engine.outbox_bytes = 0;
        #[cfg(feature = "consensus-driver-testkit")]
        self.controls.clear();
        self.engine.status.finished = true;
        self.report = Some(report);
        self.publish();
        report
    }
    /// Only after all originals finish. Retains the actual sole backend even
    /// after an ambiguous write; the caller MUST `reopen` before using it again.
    /// No raw backend or mutation authority escapes while the driver is live.
    pub fn into_finished_store(self) -> Result<ConsensusStore<B>, Box<Self>> {
        if self.report.is_none() {
            return Err(Box::new(self));
        }
        Ok(self.engine.core.raft.r.raft_log.store)
    }
}
async fn tick(timer: &mut Option<Interval>) {
    if let Some(timer) = timer {
        timer.tick().await;
    } else {
        pending::<()>().await;
    }
}
async fn poll_sending(
    sending: &mut [Sending],
) -> (usize, Result<(), crate::transport::TransportError>) {
    poll_fn(|cx| {
        for (index, sending) in sending.iter_mut().enumerate() {
            let mut future = pin!(sending.receipt.wait());
            if let Poll::Ready(result) = future.as_mut().poll(cx) {
                return Poll::Ready((index, result));
            }
        }
        Poll::Pending
    })
    .await
}

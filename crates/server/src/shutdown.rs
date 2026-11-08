//! Process shutdown: one ordered sequence against one budget.
//!
//! The stages run strictly in order, each only after the previous one ended:
//!
//! 1. Listener admission: both configured transports retire admission, including
//!    new RPCs on existing HTTP/2 peers, before either protocol drain starts.
//! 2. Protocol cleanup: both original connection/task families and accepted RPCs,
//!    sessions, links and settlement work they own, are joined.
//! 3. Timer: the sweep loop is told to stop and its thread is joined.
//! 4. Broker: the owner thread stops after the requests queued ahead of it and
//!    is joined, which drops the state machine and closes the store.
//!
//! The order is what keeps accepted work intact. The broker outlives protocol
//! cleanup and the timer, so a command they began still commits and answers
//! the caller that is waiting on it. No stage settles or acknowledges anything:
//! each one stops admission or waits for work that was already running, so a
//! client only ever hears about a commit that happened.
//!
//! One budget covers every stage. When it runs out the stage that was still
//! running is reported and the later ones are not started; the caller decides
//! what to do then. The process exits without waiting, which loses nothing a
//! client was told was committed, because nothing is acknowledged before its
//! batch is durable.

use std::{
    fmt, io,
    panic::resume_unwind,
    sync::{Arc, OnceLock},
    thread::{self, JoinHandle},
    time::Duration,
};

use protocol_amqp::{AmqpListenerExit, AmqpListenerService};
use thiserror::Error;
use tokio::time::{Instant, timeout_at};
use tracing::{info, warn};

use crate::{AdminListenerService, Broker, BrokerHandle, Shutdown, TimerWorker};

/// The budget a process gives its whole shutdown unless configured.
pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// The exit status of a process whose shutdown budget ran out.
///
/// Distinct from the status of a refused startup, so a supervisor can tell a
/// slow drain from a configuration error.
pub const SHUTDOWN_EXPIRED_EXIT_CODE: u8 = 3;

/// One step of the shutdown sequence, in the order they run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownStage {
    ListenerAdmission,
    ProtocolCleanup,
    Timer,
    Broker,
}

impl ShutdownStage {
    /// The name logged for this stage.
    pub const fn name(self) -> &'static str {
        match self {
            Self::ListenerAdmission => "listener-admission",
            Self::ProtocolCleanup => "protocol-cleanup",
            Self::Timer => "timer",
            Self::Broker => "broker",
        }
    }
}

impl fmt::Display for ShutdownStage {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str(self.name())
    }
}

/// The budget ran out while `stage` was still running.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("the shutdown budget expired during the {stage} stage")]
pub struct ShutdownExpired {
    pub stage: ShutdownStage,
}

/// The one-way switch every shutdown request goes through.
///
/// Only the first request starts shutdown and fixes when the budget began;
/// later ones, a second signal or a listener failure during the drain, join
/// the shutdown already running rather than restarting or skipping a stage.
#[derive(Clone, Debug, Default)]
pub struct ShutdownTrigger {
    started: Arc<OnceLock<Instant>>,
}

impl ShutdownTrigger {
    /// Requests shutdown, returning true only for the request that started it.
    pub fn request(&self) -> bool {
        self.started.set(Instant::now()).is_ok()
    }

    /// When shutdown was first requested, if it has been.
    pub fn started(&self) -> Option<Instant> {
        self.started.get().copied()
    }
}

/// The deadline every stage of one shutdown shares.
#[derive(Clone, Copy, Debug)]
pub struct ShutdownBudget {
    deadline: Instant,
}

impl ShutdownBudget {
    /// A budget of `budget`, counted from `started` rather than from whenever
    /// the first stage happens to begin.
    pub fn new(started: Instant, budget: Duration) -> Self {
        Self {
            deadline: started
                .checked_add(budget)
                .unwrap_or_else(|| started + Duration::from_secs(60 * 60 * 24 * 365)),
        }
    }

    /// Runs one stage against what remains of the budget.
    ///
    /// On expiry `work` is dropped where it stands; anything it handed to a
    /// thread of its own keeps running.
    pub async fn stage<T>(
        &self,
        stage: ShutdownStage,
        work: impl Future<Output = T>,
    ) -> Result<T, ShutdownExpired> {
        let began = Instant::now();
        let output = timeout_at(self.deadline, work)
            .await
            .map_err(|_| ShutdownExpired { stage })?;
        info!(
            stage = %stage,
            elapsed_millis = u64::try_from(began.elapsed().as_millis()).unwrap_or(u64::MAX),
            "shutdown stage finished"
        );
        Ok(output)
    }
}

/// A thread that runs until its latch is signalled, normally the timer worker.
pub struct TimerThread {
    latch: Arc<Shutdown>,
    thread: JoinHandle<()>,
}

impl TimerThread {
    /// Starts the timer worker, sweeping every `interval`.
    pub fn spawn(broker: BrokerHandle, interval: Duration) -> io::Result<Self> {
        let latch = Arc::new(Shutdown::default());
        let thread = thread::Builder::new()
            .name(String::from("switchyard-timer"))
            .spawn({
                let latch = Arc::clone(&latch);
                move || TimerWorker::new(&broker).run(interval, &latch)
            })?;
        Ok(Self::new(latch, thread))
    }

    /// Takes custody of a thread that returns once `latch` is signalled.
    pub fn new(latch: Arc<Shutdown>, thread: JoinHandle<()>) -> Self {
        Self { latch, thread }
    }

    /// Signals the thread and waits for it on a blocking thread, so a sweep
    /// still waiting on the broker does not hold an executor thread.
    pub async fn stop(self) {
        self.latch.signal();
        let thread = self.thread;
        let joined = tokio::task::spawn_blocking(move || thread.join()).await;
        match joined {
            Ok(Ok(())) => {}
            // The worker already stopped; its panic was reported where it
            // happened and nothing it left half-done is acknowledged.
            Ok(Err(_)) => warn!("the timer thread panicked before shutdown"),
            Err(error) => resume_unwind(error.into_panic()),
        }
    }
}

/// Stops `broker` on a blocking thread: its drop joins the owner, which may
/// still be committing the last command queued ahead of the stop.
async fn stop_broker(broker: Broker) {
    if let Err(error) = tokio::task::spawn_blocking(move || drop(broker)).await {
        resume_unwind(error.into_panic());
    }
}

/// Runs the four stages in order against `budget`.
///
/// `timer` and `broker` are taken only when their stage starts, so after an
/// earlier stage expires they are still the caller's, untouched. Returns the
/// listener's terminal outcome once every stage has finished.
pub async fn shut_down<B: protocol_amqp::Broker>(
    budget: &ShutdownBudget,
    listener: &mut AmqpListenerService<B>,
    timer: &mut Option<TimerThread>,
    broker: &mut Option<Broker>,
) -> Result<Option<AmqpListenerExit>, ShutdownExpired> {
    shut_down_with_admin(budget, listener, None, timer, broker).await
}

/// Borrows the original accept pumps until either stops. Neither owner is
/// dropped when select cancels a borrower; failure and signals use the same
/// first-request timestamp and retire both RPC and socket admission.
pub async fn serve_transports<B: protocol_amqp::Broker>(
    listener: &mut AmqpListenerService<B>,
    mut admin: Option<&mut AdminListenerService>,
    trigger: &ShutdownTrigger,
) {
    if let Some(admin) = admin.as_mut() {
        tokio::select! {
            () = listener.serve() => {},
            () = admin.serve() => {},
        }
    } else {
        listener.serve().await;
    }
    trigger.request();
    listener.retirement_handle().request();
    if let Some(admin) = admin.as_ref() {
        admin.retirement_handle().request();
    }
}

/// Retires BOTH admissions before awaiting either protocol drain. The caller
/// retains both original owners on cancellation or budget expiry.
pub async fn shut_down_with_admin<B: protocol_amqp::Broker>(
    budget: &ShutdownBudget,
    listener: &mut AmqpListenerService<B>,
    mut admin: Option<&mut AdminListenerService>,
    timer: &mut Option<TimerThread>,
    broker: &mut Option<Broker>,
) -> Result<Option<AmqpListenerExit>, ShutdownExpired> {
    listener.retirement_handle().request();
    if let Some(admin) = admin.as_ref() {
        admin.retirement_handle().request();
    }
    budget
        .stage(ShutdownStage::ListenerAdmission, async {
            listener.retirement_handle().request();
            // Returns at once if the pump already observed retirement.
            listener.serve().await;
            if let Some(admin) = admin.as_mut() {
                admin.serve().await;
            }
        })
        .await?;
    let exit = budget
        .stage(ShutdownStage::ProtocolCleanup, async {
            if let Some(admin) = admin.as_mut() {
                tokio::join!(listener.finish(), admin.finish());
            } else {
                listener.finish().await;
            }
            listener.take_exit()
        })
        .await?;
    stop_timer_and_broker(budget, timer, broker).await?;
    Ok(exit)
}

/// Runs the timer and broker stages alone, for a process that never started
/// serving: startup failed after both were running, so there is no listener
/// to retire. Bounded by `budget` like a full shutdown.
pub async fn stop_timer_and_broker(
    budget: &ShutdownBudget,
    timer: &mut Option<TimerThread>,
    broker: &mut Option<Broker>,
) -> Result<(), ShutdownExpired> {
    if let Some(timer) = timer.take() {
        budget.stage(ShutdownStage::Timer, timer.stop()).await?;
    }
    if let Some(broker) = broker.take() {
        budget
            .stage(ShutdownStage::Broker, stop_broker(broker))
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_first_request_starts_shutdown() {
        let trigger = ShutdownTrigger::default();
        assert_eq!(trigger.started(), None);
        assert!(trigger.request());
        let started = trigger.started().expect("the first request starts it");
        let joined = trigger.clone();
        assert!(!joined.request(), "a repeated request joins");
        assert!(!trigger.request());
        assert_eq!(joined.started(), Some(started), "the budget start is kept");
    }

    #[tokio::test(start_paused = true)]
    async fn every_stage_shares_one_budget() {
        let budget = ShutdownBudget::new(Instant::now(), Duration::from_secs(10));
        budget
            .stage(
                ShutdownStage::ListenerAdmission,
                tokio::time::sleep(Duration::from_secs(6)),
            )
            .await
            .expect("the first stage fits");
        let began = Instant::now();
        assert_eq!(
            budget
                .stage(
                    ShutdownStage::ProtocolCleanup,
                    tokio::time::sleep(Duration::from_secs(6)),
                )
                .await,
            Err(ShutdownExpired {
                stage: ShutdownStage::ProtocolCleanup
            }),
            "the second stage gets only what the first left"
        );
        assert_eq!(began.elapsed(), Duration::from_secs(4));
    }

    #[tokio::test(start_paused = true)]
    async fn a_stalled_stage_expires_at_the_deadline() {
        let started = Instant::now();
        let budget = ShutdownBudget::new(started, Duration::from_secs(30));
        assert_eq!(
            budget
                .stage(ShutdownStage::Timer, std::future::pending::<()>())
                .await,
            Err(ShutdownExpired {
                stage: ShutdownStage::Timer
            })
        );
        assert_eq!(started.elapsed(), Duration::from_secs(30));
    }

    #[test]
    fn stages_are_named_for_the_log() {
        assert_eq!(
            [
                ShutdownStage::ListenerAdmission,
                ShutdownStage::ProtocolCleanup,
                ShutdownStage::Timer,
                ShutdownStage::Broker,
            ]
            .map(ShutdownStage::name),
            ["listener-admission", "protocol-cleanup", "timer", "broker"]
        );
        assert_eq!(
            ShutdownExpired {
                stage: ShutdownStage::ProtocolCleanup
            }
            .to_string(),
            "the shutdown budget expired during the protocol-cleanup stage"
        );
    }
}

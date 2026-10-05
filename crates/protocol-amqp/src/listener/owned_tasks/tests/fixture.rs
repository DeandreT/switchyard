use super::*;

pub(super) struct Anchor {
    pub(super) drops: Arc<AtomicUsize>,
}

impl Drop for Anchor {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::AcqRel);
    }
}

pub(super) fn anchor() -> (Anchor, Arc<AtomicUsize>) {
    let drops = Arc::new(AtomicUsize::new(0));
    (
        Anchor {
            drops: Arc::clone(&drops),
        },
        drops,
    )
}

pub(super) struct Releases(pub(super) Vec<Arc<BlockingGate>>);

impl Releases {
    pub(super) fn release(&self) {
        for gate in &self.0 {
            gate.release();
        }
    }
}

impl Drop for Releases {
    fn drop(&mut self) {
        self.release();
    }
}

pub(super) async fn drain_refused<T: Send + 'static, A>(refused: Refused<T, A>) {
    let Refused {
        mut tasks, anchor, ..
    } = refused;
    tasks.abort_all();
    let mut outcomes = Vec::new();
    while let Some(outcome) = tasks.join_next().await {
        outcomes.push(outcome);
    }
    drop(outcomes);
    drop(anchor);
}

pub(super) async fn admit<T: Send + 'static, A>(
    tasks: JoinSet<T>,
    anchor: A,
    plan: ControlPlan,
    releases: &Releases,
) -> Result<Accepted<T, A>, Box<dyn std::error::Error>> {
    match retire_with_controls(tasks, anchor, Some(Handle::current()), plan) {
        Ok(accepted) => Ok(accepted),
        Err(refused) => {
            releases.release();
            drain_refused(refused).await;
            Err(std::io::Error::other("unexpected isolated batch refusal").into())
        }
    }
}

pub(super) async fn ready_tasks(count: usize) -> JoinSet<usize> {
    let mut tasks = JoinSet::new();
    let mut signals = Vec::new();
    for value in 0..count {
        let (send, receive) = tokio::sync::oneshot::channel();
        tasks.spawn(async move {
            let _ = send.send(());
            value
        });
        signals.push(receive);
    }
    // Current-thread tests observe these after each non-suspending task poll.
    for signal in signals {
        let _ = signal.await;
    }
    tasks
}

pub(super) async fn gated_task<T: Send + 'static>(
    value: T,
    gate: &Arc<BlockingGate>,
) -> (JoinSet<T>, Result<(), tokio::time::error::Elapsed>) {
    let mut tasks = JoinSet::new();
    let worker_gate = Arc::clone(gate);
    tasks.spawn_blocking(move || {
        worker_gate.block();
        value
    });
    let entry = tokio::time::timeout(OBSERVATION_LIMIT, gate.entered()).await;
    (tasks, entry)
}

pub(super) struct Probe {
    pub(super) drops: Arc<AtomicUsize>,
    pub(super) panic_on_drop: bool,
}

impl Drop for Probe {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::AcqRel);
        assert!(!self.panic_on_drop, "controlled output destructor panic");
    }
}

pub(super) async fn probe_tasks(
    panic_payload: bool,
    gate: &Arc<BlockingGate>,
    dangerous: &Arc<AtomicUsize>,
    ordinary: &Arc<AtomicUsize>,
) -> (JoinSet<Probe>, Result<(), tokio::time::error::Elapsed>) {
    let (mut tasks, entry) = gated_task(
        Probe {
            drops: Arc::clone(ordinary),
            panic_on_drop: false,
        },
        gate,
    )
    .await;
    let (send, receive) = tokio::sync::oneshot::channel();
    let drops = Arc::clone(dangerous);
    tasks.spawn(async move {
        let probe = Probe {
            drops,
            panic_on_drop: true,
        };
        let _ = send.send(());
        if panic_payload {
            std::panic::panic_any(probe);
        }
        probe
    });
    let _ = receive.await;
    (tasks, entry)
}

pub(super) async fn finish<T: Send + 'static, A>(
    root: &mut JoinRoot<T, A>,
) -> Result<Report<T, A>, Box<dyn std::error::Error>> {
    root.finish()
        .await
        .ok_or_else(|| std::io::Error::other("missing first joined report").into())
}

pub(super) fn interrupted_plan(original: Checkpoint, rescue: Checkpoint) -> ControlPlan {
    ControlPlan {
        checkpoints: [original, rescue, Checkpoint::Never],
        ..ControlPlan::default()
    }
}

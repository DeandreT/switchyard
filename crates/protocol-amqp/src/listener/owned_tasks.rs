//! Isolated test model, not autonomous fixture custody.
//!
//! The external root must stay live and be driven on fallback runtime A.
//! Losing that root or A is excluded; disposable observers may all disappear.

use std::sync::{Arc, Mutex, MutexGuard};

use tokio::{
    runtime::Handle,
    sync::watch,
    task::{JoinError, JoinSet},
};

mod control;
mod handles;
mod tests;

use control::{Checkpoint, ControlPlan, Controls};
use handles::{OwnerResults, Owners, Role};

const TASK_LIMIT: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RefusalKind {
    NoFallback,
    TaskLimit,
    Reservation,
}

struct Refused<T, A> {
    kind: RefusalKind,
    tasks: JoinSet<T>,
    anchor: A,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Progress {
    original_submitted: bool,
    original_started: bool,
    original_exited: bool,
    original_joined: bool,
    rescue_submitted: bool,
    rescue_started: bool,
    rescue_exited: bool,
    rescue_joined: bool,
    inline_started: bool,
    children_joined: usize,
    drained: bool,
    report_ready: bool,
}

struct Observer {
    receiver: watch::Receiver<Progress>,
}

impl Observer {
    fn snapshot(&self) -> Progress {
        *self.receiver.borrow()
    }

    async fn wait_until(&mut self, condition: fn(Progress) -> bool) -> Option<Progress> {
        loop {
            let current = self.snapshot();
            if condition(current) {
                return Some(current);
            }
            if self.receiver.changed().await.is_err() {
                return None;
            }
        }
    }
}

struct Job<T> {
    tasks: JoinSet<T>,
    outcomes: Vec<Result<T, JoinError>>,
    staged: Option<Result<T, JoinError>>,
    aborted: bool,
    drained: bool,
}

enum JobState<T> {
    Ready(Job<T>),
    Leased,
    Reported,
}

struct JobCell<T> {
    state: Mutex<JobState<T>>,
    progress: watch::Sender<Progress>,
    controls: Arc<Controls>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

struct JobLease<T> {
    cell: Arc<JobCell<T>>,
    job: Option<Job<T>>,
}

impl<T: Send + 'static> JobLease<T> {
    fn take(cell: &Arc<JobCell<T>>) -> Option<Self> {
        let mut state = lock(&cell.state);
        if !matches!(&*state, JobState::Ready(_)) {
            return None;
        }
        let JobState::Ready(job) = std::mem::replace(&mut *state, JobState::Leased) else {
            unreachable!("checked whole-job lease");
        };
        Some(Self {
            cell: Arc::clone(cell),
            job: Some(job),
        })
    }

    async fn drain(&mut self, owner: Option<Role>) {
        self.cell.controls.check(owner, Checkpoint::AfterLease);
        let job = self.job.as_mut().expect("whole job remains in its lease");
        if !job.aborted {
            job.tasks.abort_all();
            job.aborted = true;
        }
        loop {
            job.staged = job.tasks.join_next().await;
            let Some(outcome) = job.staged.take() else {
                break;
            };
            // Reserved before acceptance: preserve the result before any hook.
            job.outcomes.push(outcome);
            self.cell.progress.send_modify(|progress| {
                progress.children_joined = job.outcomes.len();
            });
            self.cell.controls.check(owner, Checkpoint::AfterOneJoin);
        }
        job.drained = true;
    }
}

impl<T> Drop for JobLease<T> {
    fn drop(&mut self) {
        if let Some(job) = self.job.take() {
            let drained = job.drained;
            *lock(&self.cell.state) = JobState::Ready(job);
            self.cell.progress.send_modify(|progress| {
                progress.drained |= drained;
            });
        }
    }
}

struct WorkerExit<T> {
    cell: Arc<JobCell<T>>,
    role: Role,
}

impl<T> Drop for WorkerExit<T> {
    fn drop(&mut self) {
        self.cell.progress.send_modify(|progress| match self.role {
            Role::Original => progress.original_exited = true,
            Role::Rescue => progress.rescue_exited = true,
        });
        self.cell.controls.exit_gate(self.role);
    }
}

async fn supervise<T: Send + 'static>(cell: Arc<JobCell<T>>, role: Role) {
    let _exit = WorkerExit {
        cell: Arc::clone(&cell),
        role,
    };
    cell.progress.send_modify(|progress| match role {
        Role::Original => progress.original_started = true,
        Role::Rescue => progress.rescue_started = true,
    });
    cell.controls.check(Some(role), Checkpoint::BeforeLease);
    let mut lease = JobLease::take(&cell).expect("one worker leases the whole job");
    lease.drain(Some(role)).await;
    drop(lease);
    cell.controls.check(Some(role), Checkpoint::AfterDrain);
}

struct Shared<T> {
    job: Arc<JobCell<T>>,
    owners: Arc<Mutex<Owners>>,
    fallback: Handle,
}

struct Starter<T: Send + 'static> {
    shared: Option<Arc<Shared<T>>>,
    observer: Observer,
}

impl<T: Send + 'static> Starter<T> {
    fn submit(&mut self) {
        if let Some(shared) = self.shared.take() {
            shared.submit(Role::Original);
        }
    }

    async fn observe(mut self) -> Option<Progress> {
        self.submit();
        // This is only child-drain observation, never a supervisor-join receipt.
        self.observer.wait_until(|progress| progress.drained).await
    }
}

impl<T: Send + 'static> Drop for Starter<T> {
    fn drop(&mut self) {
        self.submit();
    }
}

struct JoinRoot<T: Send + 'static, A> {
    shared: Arc<Shared<T>>,
    admitted: usize,
    anchor: Option<A>,
    reported: bool,
}

struct Report<T, A> {
    outcomes: Vec<Result<T, JoinError>>,
    owners: OwnerResults,
    inline_used: bool,
    anchor: A,
}

#[derive(Debug, Eq, PartialEq)]
struct Summary {
    admitted: usize,
    returned: usize,
    cancelled: usize,
    panicked: usize,
    original_interrupted: bool,
    rescue_interrupted: bool,
    inline_used: bool,
}

impl<T, A> Report<T, A> {
    fn summary(&self) -> Summary {
        Summary {
            admitted: self.outcomes.len(),
            returned: self
                .outcomes
                .iter()
                .filter(|outcome| outcome.is_ok())
                .count(),
            cancelled: self
                .outcomes
                .iter()
                .filter(|outcome| outcome.as_ref().is_err_and(JoinError::is_cancelled))
                .count(),
            panicked: self
                .outcomes
                .iter()
                .filter(|outcome| outcome.as_ref().is_err_and(JoinError::is_panic))
                .count(),
            original_interrupted: self.owners.original.is_err(),
            rescue_interrupted: self.owners.rescue.as_ref().is_some_and(Result::is_err),
            inline_used: self.inline_used,
        }
    }
}

impl<T: Send + 'static, A> JoinRoot<T, A> {
    async fn finish(&mut self) -> Option<Report<T, A>> {
        if self.reported {
            return None;
        }
        self.shared.submit(Role::Original);
        self.shared.join_owner(Role::Original).await;
        if !self.is_drained() {
            self.shared.submit(Role::Rescue);
            self.shared.join_owner(Role::Rescue).await;
            if !self.is_drained() {
                // Inline polls run on the EXTERNAL driver's runtime, not a spawn.
                self.shared
                    .job
                    .progress
                    .send_modify(|progress| progress.inline_started = true);
                let mut lease = JobLease::take(&self.shared.job)
                    .expect("joined workers returned the whole job");
                lease.drain(None).await;
                drop(lease);
            }
        }
        assert!(self.is_drained(), "only an actually drained job can report");
        let owners = self
            .shared
            .take_results()
            .expect("all created owner handles actually joined");
        let job = {
            let mut state = lock(&self.shared.job.state);
            let JobState::Ready(job) = std::mem::replace(&mut *state, JobState::Reported) else {
                unreachable!("the final barrier owns the complete job");
            };
            job
        };
        assert_eq!(
            job.outcomes.len(),
            self.admitted,
            "every admitted child actually joined"
        );
        let report = Report {
            outcomes: job.outcomes,
            owners,
            inline_used: self.shared.job.progress.borrow().inline_started,
            anchor: self
                .anchor
                .take()
                .expect("root retains its synthetic anchor"),
        };
        self.reported = true;
        self.shared
            .job
            .progress
            .send_modify(|progress| progress.report_ready = true);
        Some(report)
    }

    fn is_drained(&self) -> bool {
        matches!(&*lock(&self.shared.job.state), JobState::Ready(job) if job.drained)
    }
}

struct Accepted<T: Send + 'static, A> {
    root: JoinRoot<T, A>,
    starter: Starter<T>,
    observer: Observer,
}

fn retire<T: Send + 'static, A>(
    tasks: JoinSet<T>,
    anchor: A,
    fallback: Option<Handle>,
) -> Result<Accepted<T, A>, Refused<T, A>> {
    retire_with_controls(tasks, anchor, fallback, ControlPlan::default())
}

fn retire_with_controls<T: Send + 'static, A>(
    tasks: JoinSet<T>,
    anchor: A,
    fallback: Option<Handle>,
    plan: ControlPlan,
) -> Result<Accepted<T, A>, Refused<T, A>> {
    let Some(fallback) = fallback else {
        return Err(Refused {
            kind: RefusalKind::NoFallback,
            tasks,
            anchor,
        });
    };
    // A supplied Handle is not proof of liveness: the external model retains A.
    let admitted = tasks.len();
    if admitted > TASK_LIMIT {
        return Err(Refused {
            kind: RefusalKind::TaskLimit,
            tasks,
            anchor,
        });
    }
    let mut outcomes = Vec::new();
    if outcomes.try_reserve_exact(admitted).is_err() {
        return Err(Refused {
            kind: RefusalKind::Reservation,
            tasks,
            anchor,
        });
    }
    let (progress, receiver) = watch::channel(Progress::default());
    let job = Arc::new(JobCell {
        state: Mutex::new(JobState::Ready(Job {
            tasks,
            outcomes,
            staged: None,
            aborted: false,
            drained: false,
        })),
        progress,
        controls: Controls::new(plan),
    });
    let shared = Arc::new(Shared {
        job,
        owners: Arc::new(Mutex::new(Owners::default())),
        fallback,
    });
    let starter = Starter {
        shared: Some(Arc::clone(&shared)),
        observer: Observer {
            receiver: receiver.clone(),
        },
    };
    let root = JoinRoot {
        shared,
        admitted,
        anchor: Some(anchor),
        reported: false,
    };
    Ok(Accepted {
        root,
        starter,
        observer: Observer { receiver },
    })
}

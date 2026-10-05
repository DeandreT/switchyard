use tokio::{
    sync::watch,
    task::{JoinError, JoinHandle},
};

use super::{Arc, Mutex, Progress, Shared, lock, supervise};

#[derive(Clone, Copy)]
pub(super) enum Role {
    Original,
    Rescue,
}

impl Role {
    pub(super) const fn index(self) -> usize {
        match self {
            Self::Original => 0,
            Self::Rescue => 1,
        }
    }
}

#[derive(Default)]
enum Slot {
    #[default]
    Vacant,
    Submitting,
    Pending(JoinHandle<()>),
    Leased,
    Complete(Result<(), JoinError>),
    Reported,
}

#[derive(Default)]
pub(super) struct Owners {
    slots: [Slot; 2],
}

pub(super) struct OwnerResults {
    pub(super) original: Result<(), JoinError>,
    pub(super) rescue: Option<Result<(), JoinError>>,
}

struct Installation {
    owners: Arc<Mutex<Owners>>,
    progress: watch::Sender<Progress>,
    role: Role,
    handle: Option<JoinHandle<()>>,
}

impl Drop for Installation {
    fn drop(&mut self) {
        let submitted = self.handle.is_some();
        {
            let mut owners = lock(&self.owners);
            owners.slots[self.role.index()] = match self.handle.take() {
                Some(handle) => Slot::Pending(handle),
                None => Slot::Vacant,
            };
        }
        self.progress.send_modify(|progress| match self.role {
            Role::Original => progress.original_submitted |= submitted,
            Role::Rescue => progress.rescue_submitted |= submitted,
        });
    }
}

struct HandleLease {
    owners: Arc<Mutex<Owners>>,
    progress: watch::Sender<Progress>,
    role: Role,
    handle: Option<JoinHandle<()>>,
    outcome: Option<Result<(), JoinError>>,
}

impl Drop for HandleLease {
    fn drop(&mut self) {
        let joined = self.outcome.is_some();
        {
            let mut owners = lock(&self.owners);
            owners.slots[self.role.index()] = match self.outcome.take() {
                Some(outcome) => Slot::Complete(outcome),
                None => Slot::Pending(
                    self.handle
                        .take()
                        .expect("pending lease retains its handle"),
                ),
            };
        }
        // A handle with a Ready observation is now physically joined.
        drop(self.handle.take());
        self.progress.send_modify(|progress| match self.role {
            Role::Original => progress.original_joined |= joined,
            Role::Rescue => progress.rescue_joined |= joined,
        });
    }
}

enum Observation {
    Vacant,
    Waiting,
    Complete,
    Leased(HandleLease),
}

impl<T: Send + 'static> Shared<T> {
    pub(super) fn submit(&self, role: Role) {
        {
            let mut owners = lock(&self.owners);
            if !matches!(owners.slots[role.index()], Slot::Vacant) {
                return;
            }
            owners.slots[role.index()] = Slot::Submitting;
        }
        let mut installation = Installation {
            owners: Arc::clone(&self.owners),
            progress: self.job.progress.clone(),
            role,
            handle: None,
        };
        // The worker captures ONLY JobCell, never these own-handle slots.
        installation.handle = Some(self.fallback.spawn(supervise(Arc::clone(&self.job), role)));
        drop(installation);
    }

    fn observe_handle(&self, role: Role) -> Observation {
        let mut owners = lock(&self.owners);
        match &owners.slots[role.index()] {
            Slot::Vacant => return Observation::Vacant,
            Slot::Submitting | Slot::Leased => return Observation::Waiting,
            Slot::Complete(_) | Slot::Reported => return Observation::Complete,
            Slot::Pending(_) => {}
        }
        let Slot::Pending(handle) =
            std::mem::replace(&mut owners.slots[role.index()], Slot::Leased)
        else {
            unreachable!("checked pending owner slot");
        };
        Observation::Leased(HandleLease {
            owners: Arc::clone(&self.owners),
            progress: self.job.progress.clone(),
            role,
            handle: Some(handle),
            outcome: None,
        })
    }

    pub(super) async fn join_owner(&self, role: Role) {
        let mut changes = self.job.progress.subscribe();
        loop {
            match self.observe_handle(role) {
                Observation::Complete => return,
                Observation::Vacant => self.submit(role),
                Observation::Waiting => {
                    if changes.changed().await.is_err() {
                        return;
                    }
                }
                Observation::Leased(mut lease) => {
                    lease.outcome = Some(
                        lease
                            .handle
                            .as_mut()
                            .expect("borrowed lease owns its handle")
                            .await,
                    );
                    drop(lease);
                    return;
                }
            }
        }
    }

    pub(super) fn take_results(&self) -> Option<OwnerResults> {
        let mut owners = lock(&self.owners);
        if !matches!(owners.slots[0], Slot::Complete(_))
            || !matches!(owners.slots[1], Slot::Vacant | Slot::Complete(_))
        {
            return None;
        }
        let Slot::Complete(original) = std::mem::replace(&mut owners.slots[0], Slot::Reported)
        else {
            unreachable!("checked original actual-join result");
        };
        let rescue = match std::mem::replace(&mut owners.slots[1], Slot::Reported) {
            Slot::Complete(outcome) => Some(outcome),
            Slot::Vacant => None,
            _ => unreachable!("checked optional rescue actual-join result"),
        };
        Some(OwnerResults { original, rescue })
    }
}

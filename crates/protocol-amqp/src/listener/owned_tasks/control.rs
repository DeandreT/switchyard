use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, Ordering},
};

use tokio::sync::Notify;

use super::{handles::Role, lock};

#[derive(Clone, Copy, Default, Eq, PartialEq)]
pub(super) enum Checkpoint {
    #[default]
    Never,
    BeforeLease,
    AfterLease,
    AfterOneJoin,
    AfterDrain,
}

#[derive(Default)]
pub(super) struct BlockingGate {
    released: Mutex<bool>,
    changed: Condvar,
    entered: AtomicBool,
    notify: Notify,
}

impl BlockingGate {
    pub(super) fn block(&self) {
        self.entered.store(true, Ordering::Release);
        self.notify.notify_one();
        let mut released = lock(&self.released);
        while !*released {
            released = self
                .changed
                .wait(released)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    pub(super) async fn entered(&self) {
        loop {
            if self.entered.load(Ordering::Acquire) {
                return;
            }
            self.notify.notified().await;
        }
    }

    pub(super) fn release(&self) {
        *lock(&self.released) = true;
        self.changed.notify_all();
    }
}

#[derive(Default)]
pub(super) struct ControlPlan {
    pub(super) checkpoints: [Checkpoint; 3],
    pub(super) exit_gates: [Option<Arc<BlockingGate>>; 2],
}

pub(super) struct Controls {
    plan: ControlPlan,
    fired: [AtomicBool; 3],
}

impl Controls {
    pub(super) fn new(plan: ControlPlan) -> Arc<Self> {
        Arc::new(Self {
            plan,
            fired: std::array::from_fn(|_| AtomicBool::new(false)),
        })
    }

    pub(super) fn check(&self, role: Option<Role>, point: Checkpoint) {
        let index = role.map_or(2, Role::index);
        if self.plan.checkpoints[index] == point && !self.fired[index].swap(true, Ordering::AcqRel)
        {
            panic!("controlled retirement-owner interruption");
        }
    }

    pub(super) fn exit_gate(&self, role: Role) {
        if let Some(gate) = &self.plan.exit_gates[role.index()] {
            gate.block();
        }
    }

    pub(super) fn disarm(&self) {
        for fired in &self.fired {
            fired.store(true, Ordering::Release);
        }
    }
}

use std::{
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use super::IncomingLedgerError;
use crate::server::{NativeConnectionIdentity, native_transactions::NativeRetirementHook};

#[derive(Clone, Debug)]
pub(in crate::server) struct LinkIdentity(Arc<LinkGeneration>);

struct LinkGeneration {
    retired: AtomicBool,
    // Only test-only unbound factories can omit connection provenance.
    connection: Option<NativeConnectionIdentity>,
    native_retirement: Mutex<NativeRetirementSlot>,
}

#[derive(Default)]
struct NativeRetirementSlot {
    retiring: bool,
    hook: Option<Arc<NativeRetirementHook>>,
}

impl fmt::Debug for LinkGeneration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LinkGeneration")
            .field("retired", &self.retired.load(Ordering::Acquire))
            .field("connection", &self.connection)
            .finish_non_exhaustive()
    }
}

impl LinkIdentity {
    #[cfg(test)]
    pub(in crate::server) fn new() -> Self {
        Self(Arc::new(LinkGeneration {
            retired: AtomicBool::new(false),
            connection: None,
            native_retirement: Mutex::new(NativeRetirementSlot::default()),
        }))
    }

    pub(in crate::server) fn for_connection(connection: &NativeConnectionIdentity) -> Self {
        Self(Arc::new(LinkGeneration {
            retired: AtomicBool::new(false),
            connection: Some(connection.clone()),
            native_retirement: Mutex::new(NativeRetirementSlot::default()),
        }))
    }

    pub(in crate::server) fn new_child(&self) -> Self {
        Self(Arc::new(LinkGeneration {
            retired: AtomicBool::new(false),
            connection: self.0.connection.clone(),
            native_retirement: Mutex::new(NativeRetirementSlot::default()),
        }))
    }

    pub(in crate::server) fn connection_identity(&self) -> Option<&NativeConnectionIdentity> {
        self.0.connection.as_ref()
    }

    pub(in crate::server) fn same_link(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub(in crate::server) fn retire(&self) {
        let hook = {
            let mut slot = self
                .0
                .native_retirement
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if slot.retiring {
                return;
            }
            slot.retiring = true;
            slot.hook.take()
        };
        if let Some(hook) = hook {
            hook.close();
        }
        self.0.retired.store(true, Ordering::Release);
    }

    pub(in crate::server) fn install_native_retirement_hook(
        &self,
        hook: Arc<NativeRetirementHook>,
    ) -> Result<(), IncomingLedgerError> {
        let mut slot = self
            .0
            .native_retirement
            .lock()
            .map_err(|_| IncomingLedgerError::NativeHookUnavailable)?;
        if slot.retiring || self.is_retired() || hook.is_closed() {
            return Err(IncomingLedgerError::RetiredLink);
        }
        if slot.hook.is_some() {
            return Err(IncomingLedgerError::NativeHookInUse);
        }
        slot.hook = Some(hook);
        Ok(())
    }

    pub(in crate::server) fn is_retired(&self) -> bool {
        self.0.retired.load(Ordering::Acquire)
    }

    pub(super) fn key(&self) -> usize {
        Arc::as_ptr(&self.0) as usize
    }

    pub(super) fn check_live(&self) -> Result<(), IncomingLedgerError> {
        if self.is_retired() {
            Err(IncomingLedgerError::RetiredLink)
        } else {
            Ok(())
        }
    }
}

impl PartialEq for LinkIdentity {
    fn eq(&self, other: &Self) -> bool {
        self.same_link(other)
    }
}

impl Eq for LinkIdentity {}

mod custody;
mod fixture;
mod recorder;
mod refunds;
mod workflow;

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use super::controls::Fault;
use fixture::Cleanup;

pub(super) struct Payload {
    pub(super) id: usize,
    pub(super) drops: Arc<AtomicUsize>,
    pub(super) panic_on_drop: bool,
}
impl fmt::Debug for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("retained final test payload")
    }
}
impl fmt::Display for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("retained final test payload")
    }
}
impl std::error::Error for Payload {}
impl Drop for Payload {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        if self.panic_on_drop {
            panic!("deliberate post-barrier payload disposal");
        }
    }
}

pub(super) fn error(id: usize, drops: &Arc<AtomicUsize>) -> Fault {
    Fault::Error(Box::new(Payload {
        id,
        drops: Arc::clone(drops),
        panic_on_drop: true,
    }))
}
pub(super) fn panic_payload(id: usize, drops: &Arc<AtomicUsize>) -> Fault {
    Fault::Panic(Box::new(Payload {
        id,
        drops: Arc::clone(drops),
        panic_on_drop: true,
    }))
}
fn dispose(cleanup: Cleanup) -> std::thread::Result<()> {
    // ALL atomic and socket barriers were completed by Fixture::complete first.
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(cleanup)))
}

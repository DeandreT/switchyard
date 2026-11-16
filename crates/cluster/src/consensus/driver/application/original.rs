use super::{ApplicationError, ApplicationResult, ApplicationStatus};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use tokio::sync::oneshot;

pub(super) struct Pool {
    state: Mutex<(usize, usize)>,
    max_count: usize,
    max_bytes: usize,
}
impl Pool {
    pub fn new(max_count: usize, max_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new((0, 0)),
            max_count,
            max_bytes,
        })
    }
    pub fn usage(&self) -> (usize, usize) {
        *self.state.lock().expect("application budget")
    }
    pub fn reserve(self: &Arc<Self>, bytes: usize) -> Result<Credit, ApplicationError> {
        let mut s = self.state.lock().expect("application budget");
        let total = s.1.checked_add(bytes).ok_or(ApplicationError::Busy)?;
        if s.0 == self.max_count || total > self.max_bytes {
            return Err(ApplicationError::Busy);
        }
        s.0 += 1;
        s.1 = total;
        Ok(Credit {
            pool: self.clone(),
            bytes,
        })
    }
}
pub(super) struct Credit {
    pool: Arc<Pool>,
    bytes: usize,
}
impl Drop for Credit {
    fn drop(&mut self) {
        let mut s = self.pool.state.lock().expect("application budget");
        s.0 -= 1;
        s.1 -= self.bytes;
    }
}
pub(in crate::consensus::driver) struct Original {
    pub data: Vec<u8>,
    pub context: Vec<u8>,
    pub term: u64,
    pub index: AtomicU64,
    reply: Mutex<Option<oneshot::Sender<Result<ApplicationResult, ApplicationError>>>>,
    _credit: Credit,
}
impl Original {
    pub(super) fn new(
        data: Vec<u8>,
        context: Vec<u8>,
        term: u64,
        credit: Credit,
    ) -> (Arc<Self>, ApplicationReceipt) {
        let (tx, rx) = oneshot::channel();
        let original = Arc::new(Self {
            data,
            context,
            term,
            index: AtomicU64::new(0),
            reply: Mutex::new(Some(tx)),
            _credit: credit,
        });
        (
            original.clone(),
            ApplicationReceipt {
                receiver: Some(rx),
                original: Some(original),
            },
        )
    }
    pub fn finish(&self, result: Result<ApplicationResult, ApplicationError>) {
        if let Some(reply) = self
            .reply
            .lock()
            .expect("original application reply")
            .take()
        {
            let _ = reply.send(result);
        }
    }
}
/// Live original completion only. Dropping/canceling a borrower cannot cancel
/// admitted work. Credit survives active work AND an unobserved completion.
pub struct ApplicationReceipt {
    receiver: Option<oneshot::Receiver<Result<ApplicationResult, ApplicationError>>>,
    original: Option<Arc<Original>>,
}
impl ApplicationReceipt {
    pub async fn wait(&mut self) -> Result<ApplicationResult, ApplicationError> {
        let value = self
            .receiver
            .as_mut()
            .ok_or(ApplicationError::Receipt)?
            .await;
        self.receiver = None;
        self.original = None;
        value.map_err(|_| ApplicationError::Unknown)?
    }
}
pub(super) struct Shared {
    pub pool: Arc<Pool>,
    pub status: Mutex<ApplicationStatus>,
    publication: Mutex<Publication>,
}
#[derive(Default)]
struct Publication {
    retired: bool,
    pending: Vec<Arc<Original>>,
}
impl Shared {
    pub fn new(pool: Arc<Pool>) -> Arc<Self> {
        Arc::new(Self {
            pool,
            status: Mutex::new(ApplicationStatus::default()),
            publication: Mutex::new(Publication::default()),
        })
    }
    pub fn track(&self, original: Arc<Original>, index: u64) {
        original.index.store(index, Ordering::Release);
        let mut publication = self.publication.lock().expect("application originals");
        if publication.retired {
            original.finish(Err(ApplicationError::Unknown));
        }
        publication.pending.push(original);
    }
    pub fn fence(&self, term: u64, leader: bool) {
        let publication = self.publication.lock().expect("application originals");
        for o in &publication.pending {
            if !leader || o.term != term {
                o.finish(Err(ApplicationError::Unknown));
            }
        }
    }
    pub fn retire(&self) {
        // The same short lock encloses the actual reply transfer in complete.
        // Retirement never waits on/interferes with a begun physical write.
        let mut publication = self.publication.lock().expect("application originals");
        publication.retired = true;
        for o in &publication.pending {
            o.finish(Err(ApplicationError::Unknown));
        }
    }
    pub fn close(&self) {
        self.retire();
        // Called only after the serialized original call returned. Remaining
        // phases are not begun and are recovered from the physical image.
        self.publication
            .lock()
            .expect("application originals")
            .pending
            .clear();
    }
    pub fn advance(&self, raft: u64) {
        self.publication
            .lock()
            .expect("application originals")
            .pending
            .retain(|o| {
                if o.index.load(Ordering::Acquire) <= raft {
                    o.finish(Err(ApplicationError::Unknown));
                    false
                } else {
                    true
                }
            });
    }
    pub fn complete(
        &self,
        entry: &raft::eraftpb::Entry,
        ordinal: u64,
        term: u64,
        leader: bool,
        outcome: domain::IndexedApplyOutcome,
    ) {
        let publication = self.publication.lock().expect("application originals");
        if let Some(o) = publication
            .pending
            .iter()
            .find(|o| o.index.load(Ordering::Acquire) == entry.index && o.term == entry.term)
        {
            if publication.retired
                || !leader
                || o.term != term
                || o.term != entry.term
                || o.data.as_slice() != entry.data.as_ref()
                || o.context.as_slice() != entry.context.as_ref()
            {
                o.finish(Err(ApplicationError::Unknown));
            } else {
                o.finish(Ok(ApplicationResult {
                    term,
                    raft_index: entry.index,
                    application_ordinal: ordinal,
                    outcome,
                }));
            }
        }
    }
}

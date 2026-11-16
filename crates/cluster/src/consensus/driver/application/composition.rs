use super::ApplicationLimits;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use storage::{
    ConsensusBackend, Key, Mutation, StateStore, StorageError, StoreSnapshot, Value, WriteBatch,
};

pub(super) fn error(detail: &str) -> StorageError {
    StorageError::Backend {
        operation: "private consensus application composition",
        detail: detail.into(),
    }
}
struct Physical {
    backend: Box<dyn ConsensusBackend>,
    usable: bool,
    bytes: usize,
    rows: usize,
    limits: ApplicationLimits,
}
#[derive(Clone)]
pub(super) struct Hub(Arc<Mutex<Physical>>);
#[derive(Clone, Copy)]
enum Capability {
    ReadOnly,
    Journal,
    Indexed,
}
#[derive(Clone)]
pub(super) struct View {
    hub: Hub,
    capability: Capability,
}
pub(super) struct F2View {
    hub: Hub,
}

impl Hub {
    pub fn new(backend: Box<dyn ConsensusBackend>, limits: ApplicationLimits) -> Self {
        Self(Arc::new(Mutex::new(Physical {
            backend,
            usable: true,
            bytes: 0,
            rows: 0,
            limits,
        })))
    }
    pub fn readonly(&self) -> View {
        View {
            hub: self.clone(),
            capability: Capability::ReadOnly,
        }
    }
    pub fn f2(&self) -> F2View {
        F2View { hub: self.clone() }
    }
    pub fn journal(&self) -> View {
        View {
            hub: self.clone(),
            capability: Capability::Journal,
        }
    }
    pub fn indexed(&self) -> View {
        View {
            hub: self.clone(),
            capability: Capability::Indexed,
        }
    }
    pub fn poison(&self) {
        if let Ok(mut p) = self.0.lock() {
            p.usable = false;
        }
    }
    pub fn uncertain(&self) -> bool {
        self.0.lock().map_or(true, |p| !p.usable)
    }
    pub fn into_backend(self) -> Result<Box<dyn ConsensusBackend>, StorageError> {
        let mutex = Arc::try_unwrap(self.0).map_err(|_| error("original views still owned"))?;
        // Only consuming closed-owner recovery may obtain a poisoned original.
        Ok(mutex
            .into_inner()
            .unwrap_or_else(|e| e.into_inner())
            .backend)
    }
    pub fn image(&self) -> Result<Vec<(Key, Value)>, StorageError> {
        let records = self.scan(
            b"",
            b"",
            self.limits()?
                .max_rows
                .checked_add(1)
                .ok_or_else(|| error("row overflow"))?,
            false,
        )?;
        let mut p = self.0.lock().map_err(|_| error("physical lock poisoned"))?;
        let mut total = 0usize;
        for (k, v) in &records {
            if k.is_empty()
                || k.len() > p.limits.max_key_bytes
                || v.len() > p.limits.max_value_bytes
            {
                return Err(error("physical record outside bound"));
            }
            total = total
                .checked_add(k.len() + v.len())
                .ok_or_else(|| error("image size overflow"))?;
            if total > p.limits.max_image_bytes {
                return Err(error("physical image byte bound"));
            }
        }
        if records.len() > p.limits.max_rows {
            return Err(error("physical image row bound"));
        }
        p.bytes = total;
        p.rows = records.len();
        Ok(records)
    }
    pub fn limits(&self) -> Result<ApplicationLimits, StorageError> {
        Ok(self
            .0
            .lock()
            .map_err(|_| error("physical lock poisoned"))?
            .limits)
    }
    pub fn get(&self, key: &[u8]) -> Result<Option<Value>, StorageError> {
        let p = self.0.lock().map_err(|_| error("physical lock poisoned"))?;
        if !p.usable {
            return Err(error("composition retired"));
        }
        p.backend.get(key)
    }
    fn scan(
        &self,
        prefix: &[u8],
        start: &[u8],
        limit: usize,
        f2: bool,
    ) -> Result<Vec<(Key, Value)>, StorageError> {
        let p = self.0.lock().map_err(|_| error("physical lock poisoned"))?;
        if !p.usable {
            return Err(error("composition retired"));
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut result = Vec::new();
        let mut cursor = start.max(prefix).to_vec();
        let mut previous = None;
        let mut scanned = 0usize;
        let mut physical_bytes = 0usize;
        loop {
            // Native ConsensusBackend accepts <=64. A logical request of 256,
            // 2001, etc. is assembled without forwarding it or clipping it.
            let page = p.backend.scan_from(prefix, &cursor, 64)?;
            if page.len() > 64 {
                return Err(error("oversized physical scan page"));
            }
            if page.is_empty() {
                break;
            }
            for (k, v) in page {
                scanned += 1;
                physical_bytes = physical_bytes
                    .checked_add(k.len() + v.len())
                    .ok_or_else(|| error("scan byte overflow"))?;
                if scanned > p.limits.max_rows.saturating_add(1)
                    || physical_bytes > p.limits.max_image_bytes
                    || k.len() > p.limits.max_key_bytes
                    || v.len() > p.limits.max_value_bytes
                {
                    return Err(error("physical scan capacity exceeded"));
                }
                if !k.starts_with(prefix)
                    || k < cursor
                    || previous.as_ref().is_some_and(|last| &k <= last)
                {
                    return Err(error("physical scan order/progress"));
                }
                cursor = k.clone();
                cursor.push(0);
                previous = Some(k.clone());
                if !f2 || k.first() == Some(&0xf2) {
                    result.push((k, v));
                    if result.len() == limit {
                        return Ok(result);
                    }
                }
            }
        }
        Ok(result)
    }
    fn apply(
        &self,
        batch: WriteBatch,
        allowed: impl Fn(&[u8]) -> bool,
    ) -> Result<(), StorageError> {
        let mut p = self.0.lock().map_err(|_| error("physical lock poisoned"))?;
        if !p.usable {
            return Err(error("composition retired"));
        }
        let mut changed = BTreeMap::new();
        for m in batch.mutations() {
            let (k, value) = match m {
                Mutation::Put { key, value } => (key, Some(value)),
                Mutation::Delete { key } => (key, None),
            };
            if !allowed(k)
                || k.is_empty()
                || k.len() > p.limits.max_key_bytes
                || value.is_some_and(|v| v.len() > p.limits.max_value_bytes)
            {
                return Err(error("private write capability/record bound"));
            }
            changed.insert(k, value);
            if changed.len() > p.limits.max_rows {
                return Err(error("batch row bound"));
            }
        }
        let (mut bytes, mut rows) = (p.bytes, p.rows);
        for (k, value) in changed {
            if let Some(old) = p.backend.get(k)? {
                bytes = bytes
                    .checked_sub(k.len() + old.len())
                    .ok_or_else(|| error("physical accounting"))?;
                rows -= 1;
            }
            if let Some(v) = value {
                bytes = bytes
                    .checked_add(k.len() + v.len())
                    .ok_or_else(|| error("physical size overflow"))?;
                rows += 1;
            }
        }
        if bytes > p.limits.max_image_bytes || rows > p.limits.max_rows {
            return Err(error("committed physical image capacity exhausted"));
        }
        p.usable = false;
        p.backend.apply(batch)?;
        p.bytes = bytes;
        p.rows = rows;
        p.usable = true;
        Ok(())
    }
}
impl ConsensusBackend for F2View {
    fn get(&self, k: &[u8]) -> Result<Option<Value>, StorageError> {
        if k.first() != Some(&0xf2) {
            return Err(error("F2 read capability"));
        }
        self.hub.get(k)
    }
    fn scan_from(&self, p: &[u8], s: &[u8], n: usize) -> Result<Vec<(Key, Value)>, StorageError> {
        self.hub.scan(p, s, n, true)
    }
    fn apply(&mut self, b: WriteBatch) -> Result<(), StorageError> {
        self.hub.apply(b, |k| k.first() == Some(&0xf2))
    }
}
impl StateStore for View {
    fn get(&self, k: &[u8]) -> Result<Option<Value>, StorageError> {
        self.hub.get(k)
    }
    fn scan_from(&self, p: &[u8], s: &[u8], n: usize) -> Result<Vec<(Key, Value)>, StorageError> {
        self.hub.scan(p, s, n, false)
    }
    fn snapshot(&self) -> Result<StoreSnapshot, StorageError> {
        Err(error("private views cannot snapshot or escape an image"))
    }
    fn apply(&self, b: WriteBatch) -> Result<(), StorageError> {
        match self.capability {
            Capability::ReadOnly => Err(error("startup view is read only")),
            Capability::Journal => self.hub.apply(b, |k| k.first() == Some(&0xf0)),
            Capability::Indexed => self
                .hub
                .apply(b, |k| matches!(k.first(), Some(0x00..=0x11 | 0xf1))),
        }
    }
}

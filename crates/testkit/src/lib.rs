#![forbid(unsafe_code)]

use domain::{IdentifierError, NamespaceName};
use storage::MemoryStore;

#[derive(Clone, Debug)]
pub struct TestContext {
    pub namespace: NamespaceName,
    pub store: MemoryStore,
}

impl TestContext {
    pub fn new(namespace: &str) -> Result<Self, IdentifierError> {
        Ok(Self {
            namespace: NamespaceName::new(namespace)?,
            store: MemoryStore::default(),
        })
    }
}

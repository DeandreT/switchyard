//! Explicit test-only entry points into shared lifecycle preparation. Merely
//! compiling this feature changes no ordinary constructor, proposer or recovery
//! policy. These owners require externally exclusive access to their stores.

use crate::{
    BrokerError, Command, CommandKind, CommandOutcome, DurableProposal, DurableProposalAuthority,
    EntityBinding, EntityBindingKind, EntityPath, NamespaceName, QueueConfig, StateMachine,
    Timestamp,
    capacity_fence::{self, CapacityIntent},
    codec, keys,
    machine::{
        PreparationFailure,
        capacity::{self, OwnerImage},
    },
    retained_accounting::{CapacityOwner, RetainedUsage, keys::usage, usage_codec},
};
use storage::{MemoryStore, StateStore, WriteBatch};

/// A sealed, reproducible empty owned baseline. Not a creation/migration
/// proposal, arbitrary snapshot, checkpoint or production admission API.
#[derive(Clone, Debug)]
pub struct TestGenesis {
    owner: CapacityOwner,
    config: QueueConfig,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
}

impl TestGenesis {
    pub fn new(
        namespace: NamespaceName,
        parent: EntityPath,
        config: QueueConfig,
    ) -> Result<Self, BrokerError> {
        let owner = CapacityOwner::new(namespace.clone(), parent.clone(), 1)
            .map_err(|_| BrokerError::CapacityStateCorrupt)?;
        let memory = MemoryStore::default();
        StateMachine::new(memory.clone()).apply(&Command::new(
            namespace,
            parent,
            Timestamp::UNIX_EPOCH,
            CommandKind::CreateQueue { config },
        ))?;
        let mut batch = WriteBatch::default();
        for endpoint in [owner.parent(), owner.shadow()] {
            let selected = if endpoint == owner.parent() {
                config
            } else {
                config.dead_letter_shadow()
            };
            batch.push_put(
                keys::queue_config(owner.namespace(), endpoint),
                capacity_fence::encode_profile(&owner, endpoint, &selected)
                    .map_err(|_| BrokerError::CapacityStateCorrupt)?,
            );
        }
        batch.push_put(
            usage(&owner),
            usage_codec::encode(
                &owner,
                RetainedUsage::new(1, 0, 0, 0).map_err(|_| BrokerError::CapacityStateCorrupt)?,
            )
            .map_err(|_| BrokerError::CapacityStateCorrupt)?,
        );
        memory.apply(batch)?;
        OwnerImage::load(&memory, &owner)?;
        Ok(Self {
            owner,
            config,
            entries: memory.snapshot()?.entries().to_vec(),
        })
    }

    pub fn owner(&self) -> &CapacityOwner {
        &self.owner
    }

    /// The same sealed EMPTY fixture at its checked next generation. This
    /// constructs test data only: it neither reads nor migrates a store, accepts
    /// populated images, nor takes a caller-selected generation or raw rows.
    /// `initialize` still requires a genuinely empty store.
    pub fn successor_fixture(&self) -> Result<Self, BrokerError> {
        let generation = self
            .owner
            .generation()
            .checked_add(1)
            .ok_or(BrokerError::CapacityStateCorrupt)?;
        let owner = CapacityOwner::new(
            self.owner.namespace().clone(),
            self.owner.parent().clone(),
            generation,
        )
        .map_err(|_| BrokerError::CapacityStateCorrupt)?;
        // Frozen catalog DTO for a synthetic queue-fixture head. Check it with
        // the real canonical catalog decoder before accepting the fixture.
        #[derive(serde::Serialize)]
        enum FixtureKind {
            Queue,
        }
        #[derive(serde::Serialize)]
        struct FixtureHead {
            generation: u64,
            kind: FixtureKind,
            retired: bool,
        }
        let head = codec::encode(&FixtureHead {
            generation,
            kind: FixtureKind::Queue,
            retired: false,
        })?;
        if crate::machine::decode_catalog_owner(&head, EntityBindingKind::Queue)? != generation {
            return Err(BrokerError::CapacityStateCorrupt);
        }
        let mut next = Self {
            owner,
            config: self.config,
            entries: self.entries.clone(),
        };
        for (key, value) in &mut next.entries {
            if *key == keys::entity_metadata(next.owner.namespace(), next.owner.parent()) {
                *value = head.clone();
            } else if *key == keys::queue_config(next.owner.namespace(), next.owner.parent()) {
                *value =
                    capacity_fence::encode_profile(&next.owner, next.owner.parent(), &next.config)
                        .map_err(|_| BrokerError::CapacityStateCorrupt)?;
            } else if *key == keys::queue_config(next.owner.namespace(), next.owner.shadow()) {
                *value = capacity_fence::encode_profile(
                    &next.owner,
                    next.owner.shadow(),
                    &next.config.dead_letter_shadow(),
                )
                .map_err(|_| BrokerError::CapacityStateCorrupt)?;
            } else if *key == usage(&next.owner) {
                *value = usage_codec::encode(
                    &next.owner,
                    RetainedUsage::new(generation, 0, 0, 0)
                        .map_err(|_| BrokerError::CapacityStateCorrupt)?,
                )
                .map_err(|_| BrokerError::CapacityStateCorrupt)?;
            }
        }
        let memory = MemoryStore::default();
        next.initialize(&memory)?;
        next.validate(&memory, true)?;
        Ok(next)
    }

    /// Installs this exact empty fixture only into an actually empty store.
    /// No existing state, including a journal, is adopted or overwritten.
    pub fn initialize<S: StateStore>(&self, store: &S) -> Result<(), BrokerError> {
        if !store.snapshot()?.entries().is_empty() {
            return Err(BrokerError::CapacityStateCorrupt);
        }
        let mut batch = WriteBatch::default();
        for (key, value) in &self.entries {
            batch.push_put(key.clone(), value.clone());
        }
        store.apply(batch)?;
        Ok(())
    }

    pub(crate) fn validate<S: StateStore>(
        &self,
        store: &S,
        initial: bool,
    ) -> Result<(), BrokerError> {
        let selected = OwnerImage::load(store, &self.owner)?;
        if selected.config != self.config {
            return Err(BrokerError::CapacityStateCorrupt);
        }
        let image = store.snapshot()?;
        if initial {
            let domain = image
                .entries()
                .iter()
                .filter(|(k, _)| k.first() != Some(&0xF0))
                .cloned()
                .collect::<Vec<_>>();
            if domain != self.entries {
                return Err(BrokerError::CapacityStateCorrupt);
            }
        }
        let mut logical = Vec::new();
        for (key, value) in image.entries() {
            if key == &usage(&self.owner) {
                continue;
            }
            if key.first() == Some(&capacity_fence::DATA_TAG) {
                let original = capacity_fence::original_key(key)
                    .map_err(|_| BrokerError::CapacityStateCorrupt)?;
                let scope =
                    keys::entity_scope_parts(original).ok_or(BrokerError::CapacityStateCorrupt)?;
                if scope.0 != self.owner.namespace().as_str()
                    || (scope.1 != self.owner.parent().as_str()
                        && scope.1 != self.owner.shadow().as_str())
                {
                    return Err(BrokerError::CapacityStateCorrupt);
                }
                logical.push((original.to_vec(), value.clone()));
            } else if [self.owner.parent(), self.owner.shadow()]
                .iter()
                .any(|e| *key == keys::queue_config(self.owner.namespace(), e))
            {
                let (_, endpoint) =
                    keys::catalog_entity_parts(key).ok_or(BrokerError::CapacityStateCorrupt)?;
                let config = capacity_fence::decode_profile(&self.owner, &endpoint, value)
                    .map_err(|_| BrokerError::CapacityStateCorrupt)?;
                logical.push((key.clone(), codec::queue::encode(&config)?));
            } else {
                logical.push((key.clone(), value.clone()));
            }
        }
        logical.sort_by(|a, b| a.0.cmp(&b.0));
        crate::snapshot_validation::validate_complete_state(&logical)
            .map_err(|_| BrokerError::CapacityStateCorrupt)?;
        Ok(())
    }
}

/// Direct test actor. No raw store getter, unchecked constructor or callback.
#[derive(Debug)]
pub struct CapacityHarness<S> {
    store: S,
    genesis: TestGenesis,
    usable: bool,
}

impl<S: StateStore> CapacityHarness<S> {
    pub fn open(store: S, genesis: TestGenesis) -> Result<Self, BrokerError> {
        // An unindexed test actor never adopts an indexed/journal-owned store.
        if !store.scan_prefix(&[0xF0], 1)?.is_empty() || !store.scan_prefix(&[0xF1], 1)?.is_empty()
        {
            return Err(BrokerError::CapacityStateUnsupported);
        }
        genesis.validate(&store, false)?;
        Ok(Self {
            store,
            genesis,
            usable: true,
        })
    }

    fn require_usable(&self) -> Result<(), BrokerError> {
        if self.usable {
            Ok(())
        } else {
            Err(BrokerError::CapacityWriterUnusable)
        }
    }

    pub fn owner(&self) -> &CapacityOwner {
        self.genesis.owner()
    }

    /// Full selected-owner validation, before the caller observes any clock.
    pub fn preflight(
        &self,
        binding: &EntityBinding,
        kind: &CommandKind,
    ) -> Result<(), BrokerError> {
        self.require_usable()?;
        let image = OwnerImage::load(&self.store, self.owner())?;
        image.binding(binding)?;
        capacity::check_kind(kind)
    }

    pub fn bind(&self, target: &EntityPath) -> Result<EntityBinding, BrokerError> {
        self.require_usable()?;
        OwnerImage::load(&self.store, self.owner())?;
        if target != self.owner().parent() && target != self.owner().shadow() {
            return Err(BrokerError::InvalidEntityBinding);
        }
        Ok(EntityBinding::new(
            self.owner().namespace().clone(),
            target.clone(),
            self.owner().parent().clone(),
            EntityBindingKind::Queue,
            self.owner().generation(),
        ))
    }

    pub fn last_applied_time(&self) -> Result<Timestamp, BrokerError> {
        self.require_usable()?;
        StateMachine::new(self.store.clone()).last_applied_time()
    }

    pub fn apply(&mut self, intent: &CapacityIntent) -> Result<CommandOutcome, BrokerError> {
        self.require_usable()?;
        if intent.owner() != self.owner() {
            return Err(BrokerError::InvalidEntityBinding);
        }
        let (outcome, batch) =
            capacity::prepare(&self.store, intent).map_err(PreparationFailure::into_broker)?;
        if !batch.is_empty() {
            self.usable = false;
            self.store.apply(batch)?;
            self.usable = true;
        }
        Ok(outcome)
    }

    /// The ordinary path retains all normal fences. It cannot adopt the owned
    /// endpoint, and does not make that endpoint a prerequisite for other queues.
    pub fn apply_ordinary(
        &mut self,
        proposal: &DurableProposal,
    ) -> Result<CommandOutcome, BrokerError> {
        self.require_usable()?;
        let machine = StateMachine::new(self.store.clone());
        let (outcome, batch) = match proposal.authority() {
            DurableProposalAuthority::Unbound => machine.prepare(proposal.command()),
            DurableProposalAuthority::Bound(binding) => {
                machine.prepare_bound(proposal.command(), binding)
            }
        }
        .map_err(PreparationFailure::into_broker)?;
        if !batch.is_empty() {
            self.usable = false;
            self.store.apply(batch)?;
            self.usable = true;
        }
        Ok(outcome)
    }

    pub fn usage(&self) -> Result<RetainedUsage, BrokerError> {
        self.require_usable()?;
        Ok(OwnerImage::load(&self.store, self.owner())?.usage)
    }

    pub fn is_owned(&self, namespace: &NamespaceName, target: &EntityPath) -> bool {
        namespace == self.owner().namespace()
            && (target == self.owner().parent() || target == self.owner().shadow())
    }

    pub fn bind_ordinary(
        &self,
        namespace: &NamespaceName,
        target: &EntityPath,
    ) -> Result<EntityBinding, BrokerError> {
        self.require_usable()?;
        StateMachine::new(self.store.clone()).bind_entity(namespace, target)
    }

    pub fn preflight_ordinary(
        &self,
        namespace: &NamespaceName,
        target: &EntityPath,
        binding: Option<&EntityBinding>,
    ) -> Result<(), BrokerError> {
        self.require_usable()?;
        if let Some(binding) = binding {
            capacity_fence::check_bound_profile(&self.store, namespace, target)?;
            StateMachine::new(self.store.clone()).validate_binding(binding)?;
        }
        capacity_fence::check_command(&self.store, namespace, target)
    }

    // Read-only catalog projection. Never returns the original store or an
    // executable writer; ordinary default getters are not used on profile V4.
    fn catalog_view(&self) -> Result<StateMachine<MemoryStore>, crate::AdminCatalogError> {
        self.require_usable()
            .map_err(|_| crate::AdminCatalogError::CorruptCatalog)?;
        let image = OwnerImage::load(&self.store, self.owner())
            .map_err(|_| crate::AdminCatalogError::CorruptCatalog)?;
        let memory = MemoryStore::default();
        let mut batch = WriteBatch::default();
        for (key, value) in self.store.snapshot()?.entries() {
            if key == &usage(self.owner()) || key.first() == Some(&capacity_fence::DATA_TAG) {
                continue;
            }
            let config = if *key
                == keys::queue_config(self.owner().namespace(), self.owner().parent())
            {
                Some(image.config)
            } else if *key == keys::queue_config(self.owner().namespace(), self.owner().shadow()) {
                Some(image.config.dead_letter_shadow())
            } else {
                None
            };
            let value = if let Some(config) = config {
                codec::queue::encode(&config)
                    .map_err(|_| crate::AdminCatalogError::CorruptCatalog)?
            } else {
                value.clone()
            };
            batch.push_put(key.clone(), value);
        }
        memory.apply(batch)?;
        Ok(StateMachine::new(memory))
    }

    pub fn admin_queue(
        &self,
        namespace: &NamespaceName,
        path: &EntityPath,
    ) -> Result<Option<crate::AdminQueue>, crate::AdminCatalogError> {
        if !self.is_owned(namespace, path) {
            self.require_usable()
                .map_err(|_| crate::AdminCatalogError::CorruptCatalog)?;
            return StateMachine::new(self.store.clone()).admin_queue(namespace, path);
        }
        self.catalog_view()?.admin_queue(namespace, path)
    }
    pub fn admin_topic(
        &self,
        namespace: &NamespaceName,
        path: &EntityPath,
    ) -> Result<Option<crate::AdminTopic>, crate::AdminCatalogError> {
        if !self.is_owned(namespace, path) {
            self.require_usable()
                .map_err(|_| crate::AdminCatalogError::CorruptCatalog)?;
            return StateMachine::new(self.store.clone()).admin_topic(namespace, path);
        }
        self.catalog_view()?.admin_topic(namespace, path)
    }
    pub fn admin_subscription(
        &self,
        namespace: &NamespaceName,
        topic: &EntityPath,
        name: &crate::SubscriptionName,
    ) -> Result<Option<crate::AdminSubscription>, crate::AdminCatalogError> {
        self.require_usable()
            .map_err(|_| crate::AdminCatalogError::CorruptCatalog)?;
        StateMachine::new(self.store.clone()).admin_subscription(namespace, topic, name)
    }
    pub fn admin_catalog_page(
        &self,
        namespace: &NamespaceName,
        kind: crate::AdminCatalogKind,
        after: Option<&crate::AdminCatalogCursor>,
        limit: usize,
    ) -> Result<crate::AdminCatalogPage, crate::AdminCatalogError> {
        if namespace != self.owner().namespace()
            || matches!(
                kind,
                crate::AdminCatalogKind::Topics | crate::AdminCatalogKind::Subscriptions
            )
        {
            self.require_usable()
                .map_err(|_| crate::AdminCatalogError::CorruptCatalog)?;
            return StateMachine::new(self.store.clone())
                .admin_catalog_page(namespace, kind, after, limit);
        }
        self.catalog_view()?
            .admin_catalog_page(namespace, kind, after, limit)
    }
    pub fn queues_after(
        &self,
        after: Option<&(NamespaceName, EntityPath)>,
        limit: usize,
    ) -> Result<Vec<(NamespaceName, EntityPath)>, BrokerError> {
        self.require_usable()?;
        // Enumeration reads canonical identities only, never decodes profiles.
        StateMachine::new(self.store.clone()).queues_after(after, limit)
    }
    pub fn topics_after(
        &self,
        after: Option<&(NamespaceName, EntityPath)>,
        limit: usize,
    ) -> Result<Vec<(NamespaceName, EntityPath)>, BrokerError> {
        self.require_usable()?;
        StateMachine::new(self.store.clone()).topics_after(after, limit)
    }
}

/// Typed originals for the explicit test replay facade. The ordinary codec is
/// deliberately unchanged. No conversion from Owned into executable Ordinary.
#[derive(Clone, Debug)]
pub enum TestOriginal {
    Ordinary(DurableProposal),
    Owned(CapacityIntent),
}

impl TestOriginal {
    pub fn decode(bytes: &[u8]) -> Result<Self, crate::DurableProposalError> {
        if bytes.get(4..8) == Some(&11u32.to_be_bytes()) {
            Ok(Self::Owned(CapacityIntent::decode(bytes)?))
        } else {
            Ok(Self::Ordinary(DurableProposal::decode(bytes)?))
        }
    }
    pub fn encode(&self) -> Result<Vec<u8>, crate::DurableProposalError> {
        match self {
            Self::Ordinary(p) => p.encode(),
            Self::Owned(p) => p.encode(),
        }
    }
    pub(crate) fn command(&self) -> &Command {
        match self {
            Self::Ordinary(p) => p.command(),
            Self::Owned(p) => p.original().command(),
        }
    }
}

pub use crate::indexed::CapacityIndexedHarness;
